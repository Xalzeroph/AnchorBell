use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

use super::{
    binance::{parse_market_message, BinanceMarketEvent, ParseError},
    connection::{ConnectionAction, ConnectionSupervisor, ReconnectPolicy},
    BinanceSubscription, SubscriptionPlan, SubscriptionPlanError,
};

#[derive(Debug, Error)]
pub enum MarketStreamError {
    #[error("no subscriptions configured")]
    NoSubscriptions,
    #[error("invalid subscription: {0:?}")]
    InvalidSubscription(super::SubscriptionError),
    #[error("invalid subscription plan: {0:?}")]
    InvalidSubscriptionPlan(SubscriptionPlanError),
    #[error("websocket error: {0}")]
    WebSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("websocket connection timed out")]
    ConnectTimeout,
    #[error("market DNS resolution failed: {0}")]
    Dns(#[source] Box<std::io::Error>),
    #[error("market TCP connection failed: {0}")]
    Tcp(#[source] Box<std::io::Error>),
    #[error("market proxy tunnel failed: {0}")]
    Proxy(String),
    #[error("market payload exceeds configured limit")]
    FrameTooLarge,
    #[error("market payload parse failed: {0:?}")]
    Parse(ParseError),
}

#[derive(Debug, Clone)]
pub struct BinanceMarketConfig {
    pub market_ws_base: String,
    pub subscriptions: Vec<BinanceSubscription>,
    pub price_scale: u32,
    pub quantity_scale: u32,
    pub max_frame_bytes: usize,
    pub connect_timeout_ms: u64,
    /// Maximum silence tolerated before the shard is recycled.
    pub read_timeout_ms: u64,
    pub http_proxy: Option<String>,
    pub reconnect: ReconnectPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarketFeed {
    BookTicker,
    ReferenceAndTrades,
}

impl BinanceMarketConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn for_symbols<S: AsRef<str>>(
        market_ws_base: impl Into<String>,
        symbols: &[S],
        feed: BinanceMarketFeed,
        price_scale: u32,
        quantity_scale: u32,
        max_frame_bytes: usize,
        connect_timeout_ms: u64,
        read_timeout_ms: u64,
        http_proxy: Option<String>,
        reconnect: ReconnectPolicy,
        max_subscriptions_per_shard: usize,
    ) -> Result<Vec<Self>, MarketStreamError> {
        let subscriptions = symbols
            .iter()
            .map(|symbol| {
                let subscription = BinanceSubscription::new(symbol.as_ref())
                    .map_err(MarketStreamError::InvalidSubscription)?;
                Ok(match feed {
                    BinanceMarketFeed::BookTicker => subscription.book_ticker_only(),
                    BinanceMarketFeed::ReferenceAndTrades => {
                        subscription.market_reference_and_trades()
                    }
                })
            })
            .collect::<Result<Vec<_>, MarketStreamError>>()?;
        let config = Self {
            market_ws_base: market_ws_base.into(),
            subscriptions,
            price_scale,
            quantity_scale,
            max_frame_bytes,
            connect_timeout_ms,
            read_timeout_ms,
            http_proxy,
            reconnect,
        };
        config.into_shards(max_subscriptions_per_shard)
    }
}

impl BinanceMarketConfig {
    pub fn combined_stream_url(&self) -> Result<String, MarketStreamError> {
        let subscriptions = self.validated_subscriptions()?;
        let streams = subscriptions
            .iter()
            .map(BinanceSubscription::stream_names)
            .collect::<Result<Vec<_>, _>>()
            .map_err(MarketStreamError::InvalidSubscription)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(format!(
            "{}/stream?streams={}",
            self.market_ws_base.trim_end_matches('/'),
            streams.join("/")
        ))
    }

    pub fn subscription_endpoint(&self) -> Result<String, MarketStreamError> {
        if self.subscriptions.is_empty() {
            return Err(MarketStreamError::NoSubscriptions);
        }
        Ok(format!(
            "{}/stream",
            self.market_ws_base.trim_end_matches('/')
        ))
    }

    pub fn subscription_request(&self) -> Result<serde_json::Value, MarketStreamError> {
        let subscriptions = self.validated_subscriptions()?;
        let streams = subscriptions
            .iter()
            .map(BinanceSubscription::stream_names)
            .collect::<Result<Vec<_>, _>>()
            .map_err(MarketStreamError::InvalidSubscription)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(json!({"method":"SUBSCRIBE","params":streams,"id":"anchorbell-market-1"}))
    }

    fn validated_subscriptions(&self) -> Result<Vec<BinanceSubscription>, MarketStreamError> {
        if self.subscriptions.is_empty() {
            return Err(MarketStreamError::NoSubscriptions);
        }
        let plan = SubscriptionPlan::new(self.subscriptions.clone(), self.subscriptions.len())
            .map_err(MarketStreamError::InvalidSubscriptionPlan)?;
        Ok(plan.shards().first().cloned().unwrap_or_default())
    }

    /// Splits a large deterministic universe into independently supervised
    /// connections. Each shard is bounded before any socket is opened.
    pub fn into_shards(
        &self,
        max_subscriptions_per_shard: usize,
    ) -> Result<Vec<Self>, MarketStreamError> {
        let plan = SubscriptionPlan::new(self.subscriptions.clone(), max_subscriptions_per_shard)
            .map_err(MarketStreamError::InvalidSubscriptionPlan)?;
        Ok(plan
            .shards()
            .iter()
            .map(|subscriptions| {
                let mut config = self.clone();
                config.subscriptions = subscriptions.clone();
                config
            })
            .collect())
    }
}

pub struct BinanceMarketStream {
    config: BinanceMarketConfig,
    supervisor: ConnectionSupervisor,
}

impl BinanceMarketStream {
    pub fn new(mut config: BinanceMarketConfig) -> Self {
        config.http_proxy = crate::network::resolve_http_proxy(config.http_proxy.as_deref());
        let supervisor = ConnectionSupervisor::new(config.reconnect);
        Self { config, supervisor }
    }

    pub fn supervisor(&self) -> &ConnectionSupervisor {
        &self.supervisor
    }

    pub async fn run_until_error<F>(&mut self, mut on_event: F) -> Result<(), MarketStreamError>
    where
        F: FnMut(BinanceMarketEvent) + Send,
    {
        let url = self.config.combined_stream_url()?;
        loop {
            self.supervisor.on_connecting();
            let connection = connect_market_stream(
                &url,
                self.config.connect_timeout_ms,
                self.config.http_proxy.as_deref(),
            )
            .await;
            match connection {
                Ok(socket) => {
                    let mut socket = socket;
                    self.supervisor.on_connected();
                    eprintln!("market stream connected: {url}");
                    loop {
                        let message = match tokio::time::timeout(
                            Duration::from_millis(self.config.read_timeout_ms.max(1)),
                            socket.next(),
                        )
                        .await
                        {
                            Ok(message) => message,
                            Err(_) => break,
                        };
                        let Some(message) = message else {
                            break;
                        };
                        let message = match message {
                            Ok(message) => message,
                            Err(error) => {
                                eprintln!("market stream read error; reconnecting: {error}");
                                break;
                            }
                        };
                        match message {
                            Message::Text(text) => {
                                let payload = text.as_bytes();
                                if payload.len() > self.config.max_frame_bytes {
                                    return Err(MarketStreamError::FrameTooLarge);
                                }
                                if text.contains("\"id\"") && text.contains("\"result\"") {
                                    continue;
                                }
                                let event = parse_market_message(
                                    payload,
                                    self.config.price_scale,
                                    self.config.quantity_scale,
                                )
                                .map_err(MarketStreamError::Parse)?;
                                on_event(event);
                            }
                            Message::Binary(payload) => {
                                if payload.len() > self.config.max_frame_bytes {
                                    return Err(MarketStreamError::FrameTooLarge);
                                }
                                let event = parse_market_message(
                                    &payload,
                                    self.config.price_scale,
                                    self.config.quantity_scale,
                                )
                                .map_err(MarketStreamError::Parse)?;
                                on_event(event);
                            }
                            Message::Ping(payload) => {
                                socket.send(Message::Pong(payload)).await.map_err(|error| {
                                    MarketStreamError::WebSocket(Box::new(error))
                                })?;
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    match self.supervisor.on_disconnect() {
                        Some((ConnectionAction::Reconnect, delay)) => {
                            eprintln!(
                                "market stream disconnected; reconnecting in {} ms",
                                delay.as_millis()
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                        Some(_) | None => return Ok(()),
                    }
                }
                Err(error) => match self.supervisor.on_disconnect() {
                    Some((ConnectionAction::Reconnect, delay)) => {
                        eprintln!(
                            "market stream connection failed; reconnecting in {} ms: {error}",
                            delay.as_millis()
                        );
                        tokio::time::sleep(delay).await;
                    }
                    Some(_) | None => return Err(error),
                },
            }
        }
    }
}

async fn connect_market_stream(
    url: &str,
    timeout_ms: u64,
    http_proxy: Option<&str>,
) -> Result<crate::network::ConnectedWebSocket, MarketStreamError> {
    crate::network::connect_websocket(url, timeout_ms, http_proxy)
        .await
        .map_err(map_network_error)
}

fn map_network_error(error: crate::network::NetworkError) -> MarketStreamError {
    match error {
        crate::network::NetworkError::InvalidUrl => MarketStreamError::Dns(Box::new(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid market URL"),
        )),
        crate::network::NetworkError::Dns(error) => MarketStreamError::Dns(error),
        crate::network::NetworkError::Tcp(error) => MarketStreamError::Tcp(error),
        crate::network::NetworkError::Proxy(error) => MarketStreamError::Proxy(error),
        crate::network::NetworkError::WebSocket(error) => MarketStreamError::WebSocket(error),
        crate::network::NetworkError::Timeout => MarketStreamError::ConnectTimeout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_combined_market_stream_url() {
        let config = BinanceMarketConfig {
            market_ws_base: "wss://demo-fstream.binance.com/public/".into(),
            subscriptions: vec![BinanceSubscription::new("ABCUSDT").unwrap()],
            price_scale: 4,
            quantity_scale: 2,
            max_frame_bytes: 1_048_576,
            connect_timeout_ms: 5_000,
            read_timeout_ms: 15_000,
            http_proxy: None,
            reconnect: ReconnectPolicy::default(),
        };
        assert_eq!(
            config.combined_stream_url().unwrap(),
            "wss://demo-fstream.binance.com/public/stream?streams=abcusdt@bookTicker/abcusdt@markPrice@1s"
        );
    }

    #[test]
    fn builds_feed_specific_shards_from_one_symbol_universe() {
        let symbols = ["AAAUSDT", "BBBUSDT"];
        let public = BinanceMarketConfig::for_symbols(
            "wss://fstream.binance.com",
            &symbols,
            BinanceMarketFeed::BookTicker,
            8,
            8,
            1_048_576,
            5_000,
            15_000,
            None,
            ReconnectPolicy::default(),
            64,
        )
        .unwrap();
        let reference = BinanceMarketConfig::for_symbols(
            "wss://fstream.binance.com",
            &symbols,
            BinanceMarketFeed::ReferenceAndTrades,
            8,
            8,
            1_048_576,
            5_000,
            15_000,
            None,
            ReconnectPolicy::default(),
            64,
        )
        .unwrap();

        assert_eq!(public.len(), 1);
        assert_eq!(reference.len(), 1);
        assert_eq!(
            public[0].subscriptions[0].stream_names().unwrap(),
            vec!["aaausdt@bookTicker".to_owned()]
        );
        assert_eq!(
            reference[0].subscriptions[0].stream_names().unwrap(),
            vec![
                "aaausdt@markPrice@1s".to_owned(),
                "aaausdt@aggTrade".to_owned()
            ]
        );
    }

    #[test]
    fn builds_explicit_subscription_request() {
        let config = BinanceMarketConfig {
            market_ws_base: "wss://demo-fstream.binance.com/public".into(),
            subscriptions: vec![BinanceSubscription::new("ABCUSDT").unwrap()],
            price_scale: 4,
            quantity_scale: 2,
            max_frame_bytes: 1_048_576,
            connect_timeout_ms: 5_000,
            read_timeout_ms: 15_000,
            http_proxy: None,
            reconnect: ReconnectPolicy::default(),
        };
        let request = config.subscription_request().unwrap();
        assert_eq!(request["method"], "SUBSCRIBE");
        assert_eq!(request["params"][0], "abcusdt@bookTicker");
        assert_eq!(request["id"], "anchorbell-market-1");
        assert_eq!(
            config.subscription_endpoint().unwrap(),
            "wss://demo-fstream.binance.com/public/stream"
        );
    }

    #[test]
    fn rejects_duplicate_symbols_before_combined_stream_creation() {
        let config = BinanceMarketConfig {
            market_ws_base: "wss://demo-fstream.binance.com/public".into(),
            subscriptions: vec![
                BinanceSubscription::new("ABCUSDT").unwrap(),
                BinanceSubscription::new("abcusdt").unwrap(),
            ],
            price_scale: 4,
            quantity_scale: 2,
            max_frame_bytes: 1_048_576,
            connect_timeout_ms: 5_000,
            read_timeout_ms: 15_000,
            http_proxy: None,
            reconnect: ReconnectPolicy::default(),
        };

        assert!(matches!(
            config.combined_stream_url(),
            Err(MarketStreamError::InvalidSubscriptionPlan(
                SubscriptionPlanError::DuplicateSymbol(symbol)
            )) if symbol == "abcusdt"
        ));
    }

    #[test]
    fn creates_independently_supervised_shards_without_changing_transport_config() {
        let config = BinanceMarketConfig {
            market_ws_base: "wss://demo-fstream.binance.com/public".into(),
            subscriptions: vec![
                BinanceSubscription::new("CCCUSDT").unwrap(),
                BinanceSubscription::new("AAAUSDT").unwrap(),
                BinanceSubscription::new("BBBUSDT").unwrap(),
            ],
            price_scale: 4,
            quantity_scale: 2,
            max_frame_bytes: 1_048_576,
            connect_timeout_ms: 5_000,
            read_timeout_ms: 15_000,
            http_proxy: Some("http://127.0.0.1:7890".into()),
            reconnect: ReconnectPolicy::default(),
        };

        let shards = config.into_shards(2).unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].subscriptions.len(), 2);
        assert_eq!(shards[1].subscriptions.len(), 1);
        assert_eq!(shards[0].http_proxy, config.http_proxy);
        assert_eq!(shards[0].connect_timeout_ms, config.connect_timeout_ms);
        assert_eq!(shards[0].subscriptions[0].symbol, "aaausdt");
    }

    #[test]
    fn rejects_empty_subscription_set() {
        let config = BinanceMarketConfig {
            market_ws_base: "wss://demo-fstream.binance.com".into(),
            subscriptions: Vec::new(),
            price_scale: 4,
            quantity_scale: 2,
            max_frame_bytes: 1_048_576,
            connect_timeout_ms: 5_000,
            read_timeout_ms: 15_000,
            http_proxy: None,
            reconnect: ReconnectPolicy::default(),
        };
        assert!(matches!(
            config.combined_stream_url(),
            Err(MarketStreamError::NoSubscriptions)
        ));
    }
}
