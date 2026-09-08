use std::{
    collections::BTreeMap,
    env,
    io::{Read, Write},
    net::{SocketAddr, TcpStream as StdTcpStream},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tokio_tungstenite::{
    client_async_tls, tungstenite::http::Uri, MaybeTlsStream, WebSocketStream,
};

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("websocket URL is invalid")]
    InvalidUrl,
    #[error("DNS resolution failed: {0}")]
    Dns(#[source] Box<std::io::Error>),
    #[error("TCP connection failed: {0}")]
    Tcp(#[source] Box<std::io::Error>),
    #[error("HTTP proxy tunnel failed: {0}")]
    Proxy(String),
    #[error("websocket error: {0}")]
    WebSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("websocket connection timed out")]
    Timeout,
}
pub type ConnectedWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const AUTO_HTTP_PROXY_PORTS: &[u16] = &[7890, 7891, 7892, 10809, 10808, 1080, 8888];
const PROXY_PROBE_TIMEOUT: Duration = Duration::from_millis(350);

/// Resolves the proxy once for the whole process. Explicit configuration wins;
/// otherwise common local HTTP proxy ports are probed with a real CONNECT
/// request first, then standard proxy variables are honored.
pub fn resolve_http_proxy(explicit: Option<&str>) -> Option<String> {
    if let Some(proxy) = explicit.and_then(normalize_http_proxy) {
        return Some(proxy);
    }
    static AUTO_PROXY: OnceLock<Option<String>> = OnceLock::new();
    AUTO_PROXY.get_or_init(detect_http_proxy).clone()
}

fn detect_http_proxy() -> Option<String> {
    for port in AUTO_HTTP_PROXY_PORTS {
        if probe_http_proxy(*port) {
            let proxy = format!("http://127.0.0.1:{port}");
            eprintln!("auto-detected local HTTP proxy: {proxy}");
            return Some(proxy);
        }
    }
    for key in [
        "ANCHORBELL_HTTP_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "https_proxy",
        "http_proxy",
    ] {
        if let Ok(value) = env::var(key) {
            if let Some(proxy) = normalize_http_proxy(&value) {
                eprintln!("using HTTP proxy from {key}: {proxy}");
                return Some(proxy);
            }
        }
    }
    eprintln!("no HTTP proxy detected; using direct network access");
    None
}

fn normalize_http_proxy(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.contains('@') {
        return None;
    }
    let normalized = if value.contains("://") {
        value.to_owned()
    } else {
        format!("http://{value}")
    };
    let (host, port) = parse_proxy_endpoint(&normalized).ok()?;
    Some(format!("http://{host}:{port}"))
}

fn probe_http_proxy(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let Ok(mut stream) = StdTcpStream::connect_timeout(&address, PROXY_PROBE_TIMEOUT) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(PROXY_PROBE_TIMEOUT));
    let _ = stream.set_write_timeout(Some(PROXY_PROBE_TIMEOUT));
    let request = b"CONNECT fapi.binance.com:443 HTTP/1.1\r\nHost: fapi.binance.com:443\r\nProxy-Connection: Keep-Alive\r\n\r\n";
    if stream.write_all(request).is_err() {
        return false;
    }
    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 512];
    loop {
        let Ok(read) = stream.read(&mut chunk) else {
            return false;
        };
        if read == 0 {
            return false;
        }
        response.extend_from_slice(&chunk[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if response.len() > 4 * 1024 {
            return false;
        }
    }
    String::from_utf8_lossy(&response)
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        == Some("200")
}

pub async fn connect_websocket(
    url: &str,
    timeout_ms: u64,
    http_proxy: Option<&str>,
) -> Result<ConnectedWebSocket, NetworkError> {
    if timeout_ms == 0 {
        return Err(NetworkError::Timeout);
    }
    install_rustls_provider();

    let parsed: Uri = url.parse::<Uri>().map_err(|_| NetworkError::InvalidUrl)?;
    let target_host = parsed.host().ok_or(NetworkError::InvalidUrl)?;
    let target_port = parsed
        .port_u16()
        .or_else(|| match parsed.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(NetworkError::InvalidUrl)?;
    let proxy_endpoint = http_proxy.map(parse_proxy_endpoint).transpose()?;
    let (connect_host, connect_port) = proxy_endpoint
        .as_ref()
        .map(|(host, port)| (host.as_str(), *port))
        .unwrap_or((target_host, target_port));
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::Timeout);
    }
    let mut addresses: Vec<SocketAddr> = tokio::time::timeout(
        remaining,
        tokio::net::lookup_host((connect_host, connect_port)),
    )
    .await
    .map_err(|_| NetworkError::Timeout)?
    .map_err(|error| NetworkError::Dns(Box::new(error)))?
    .collect();
    addresses.sort_by_key(|address| !address.is_ipv4());
    if addresses.is_empty() {
        return Err(NetworkError::Dns(Box::new(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "host resolved to no addresses",
        ))));
    }
    let mut last_tcp_error = None;
    let mut last_proxy_error = None;
    let mut last_websocket_error = None;
    for address in addresses {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_timeout = remaining.min(Duration::from_millis(1_500));
        match tokio::time::timeout(attempt_timeout, TcpStream::connect(address)).await {
            Ok(Ok(mut socket)) => {
                if proxy_endpoint.is_some() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(
                        remaining,
                        establish_proxy_tunnel(&mut socket, target_host, target_port),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            last_proxy_error = Some(error);
                            continue;
                        }
                        Err(_) => break,
                    }
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, client_async_tls(url.to_owned(), socket))
                    .await
                {
                    Ok(Ok((socket, _response))) => return Ok(socket),
                    Ok(Err(error)) => last_websocket_error = Some(error),
                    Err(_) => break,
                }
            }
            Ok(Err(error)) => last_tcp_error = Some(error),
            Err(_) => {}
        }
    }
    if let Some(error) = last_proxy_error {
        return Err(NetworkError::Proxy(error));
    }
    if let Some(error) = last_websocket_error {
        return Err(NetworkError::WebSocket(Box::new(error)));
    }
    if let Some(error) = last_tcp_error {
        return Err(NetworkError::Tcp(Box::new(error)));
    }
    Err(NetworkError::Timeout)
}
fn install_rustls_provider() {
    static PROVIDER: OnceLock<()> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn parse_proxy_endpoint(proxy_url: &str) -> Result<(String, u16), NetworkError> {
    let normalized = if proxy_url.contains("://") {
        proxy_url.to_owned()
    } else {
        format!("http://{proxy_url}")
    };
    let parsed: Uri = normalized
        .parse::<Uri>()
        .map_err(|_| NetworkError::Proxy("invalid HTTP proxy URL".to_owned()))?;
    if parsed.scheme_str().is_some_and(|scheme| scheme != "http") {
        return Err(NetworkError::Proxy(
            "only HTTP CONNECT proxies are supported".to_owned(),
        ));
    }
    let host = parsed
        .host()
        .ok_or_else(|| NetworkError::Proxy("HTTP proxy URL has no host".to_owned()))?;
    Ok((host.to_owned(), parsed.port_u16().unwrap_or(80)))
}

async fn establish_proxy_tunnel(
    socket: &mut TcpStream,
    target_host: &str,
    target_port: u16,
) -> Result<(), String> {
    let authority = format!("{target_host}:{target_port}");
    let request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    socket
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let mut response = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let read = socket
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("proxy closed before CONNECT response".to_owned());
        }
        response.extend_from_slice(&chunk[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if response.len() > 8 * 1024 {
            return Err("proxy CONNECT response exceeds 8 KiB".to_owned());
        }
    }
    let header = String::from_utf8_lossy(&response);
    let status = header.lines().next().unwrap_or_default();
    if status.split_whitespace().nth(1) != Some("200") {
        return Err(format!("proxy CONNECT rejected: {status}"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestClass {
    Public,
    Metadata,
    Account,
    Order,
    UserStream,
}

impl RequestClass {
    fn spacing(self) -> Duration {
        match self {
            Self::Public => Duration::from_millis(20),
            Self::Metadata => Duration::from_millis(100),
            Self::Account => Duration::from_millis(100),
            Self::Order => Duration::from_millis(25),
            Self::UserStream => Duration::from_secs(1),
        }
    }

    fn cooldown(self) -> Duration {
        match self {
            Self::Public => Duration::from_secs(1),
            Self::Metadata => Duration::from_secs(2),
            Self::Account => Duration::from_secs(2),
            Self::Order => Duration::from_secs(1),
            Self::UserStream => Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
struct RequestBucket {
    next_allowed_at: Instant,
    cooldown_until: Instant,
    requests: u64,
    throttled: u64,
    last_status: Option<u16>,
}

impl RequestBucket {
    fn new(now: Instant) -> Self {
        Self {
            next_allowed_at: now,
            cooldown_until: now,
            requests: 0,
            throttled: 0,
            last_status: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestCoordinator {
    buckets: Arc<Mutex<BTreeMap<RequestClass, RequestBucket>>>,
}

impl RequestCoordinator {
    pub fn shared() -> Self {
        static SHARED: OnceLock<RequestCoordinator> = OnceLock::new();
        SHARED
            .get_or_init(|| Self {
                buckets: Arc::new(Mutex::new(BTreeMap::new())),
            })
            .clone()
    }

    pub async fn acquire(&self, class: RequestClass) {
        loop {
            let wait = {
                let now = Instant::now();
                let mut buckets = self.buckets.lock().await;
                let bucket = buckets
                    .entry(class)
                    .or_insert_with(|| RequestBucket::new(now));
                let ready_at = bucket.next_allowed_at.max(bucket.cooldown_until);
                if ready_at <= now {
                    bucket.next_allowed_at = now + class.spacing();
                    bucket.requests = bucket.requests.saturating_add(1);
                    None
                } else {
                    Some(ready_at.saturating_duration_since(now))
                }
            };
            if let Some(wait) = wait {
                tokio::time::sleep(wait).await;
            } else {
                return;
            }
        }
    }

    pub async fn observe_status(
        &self,
        class: RequestClass,
        status: u16,
        retry_after: Option<Duration>,
    ) {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().await;
        let bucket = buckets
            .entry(class)
            .or_insert_with(|| RequestBucket::new(now));
        bucket.last_status = Some(status);
        if matches!(status, 418 | 429) {
            bucket.throttled = bucket.throttled.saturating_add(1);
            bucket.cooldown_until = bucket
                .cooldown_until
                .max(now + retry_after.unwrap_or_else(|| class.cooldown()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_proxy_endpoint_without_enabling_it_by_default() {
        assert_eq!(
            parse_proxy_endpoint("127.0.0.1:7890").unwrap(),
            ("127.0.0.1".to_owned(), 7890)
        );
        assert!(matches!(
            parse_proxy_endpoint("https://127.0.0.1:7890"),
            Err(NetworkError::Proxy(_))
        ));
    }

    #[test]
    fn normalizes_safe_http_proxy_values() {
        assert_eq!(
            normalize_http_proxy("127.0.0.1:7890"),
            Some("http://127.0.0.1:7890".to_owned())
        );
        assert_eq!(
            normalize_http_proxy("http://localhost:8888"),
            Some("http://localhost:8888".to_owned())
        );
        assert_eq!(normalize_http_proxy("socks5://127.0.0.1:1080"), None);
        assert_eq!(
            normalize_http_proxy("http://user:secret@127.0.0.1:7890"),
            None
        );
    }

    #[tokio::test]
    async fn rejects_zero_timeout_before_network_access() {
        assert!(matches!(
            connect_websocket("wss://demo-fstream.binance.com", 0, None).await,
            Err(NetworkError::Timeout)
        ));
    }

    #[tokio::test]
    async fn establishes_http_connect_tunnel() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 512];
            loop {
                let read = tokio::io::AsyncReadExt::read(&mut socket, &mut chunk)
                    .await
                    .unwrap();
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("CONNECT demo.example:443 HTTP/1.1\r\n"));
            assert!(request.contains("Host: demo.example:443\r\n"));
            tokio::io::AsyncWriteExt::write_all(
                &mut socket,
                b"HTTP/1.1 200 Connection Established\r\n\r\n",
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        establish_proxy_tunnel(&mut client, "demo.example", 443)
            .await
            .unwrap();
        server.await.unwrap();
    }
}
