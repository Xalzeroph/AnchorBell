use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anchorbell_engine::{
    backtest::{ConservativeTopOfBook, FillDecision, FillModel, MakerQuote, TopOfBook},
    backtest_report::BacktestReport,
    execution::{
        binance_runtime_config, BinanceAccountStatusResponse, BinanceAccountStatusWire,
        BinanceCredentials, BinanceEnvironment, BinanceOrderWebSocket, BinanceRestClient, DeploymentConfig,
        DeploymentConfigError, Side,
    },
    market::{
        BinanceMarketConfig, BinanceMarketStream, BinanceSubscription, InstrumentRegistryConfig,
        InstrumentRegistrySnapshot, PublicMarketMetadataClient, ReconnectPolicy,
    },
    platform::{HealthSnapshot, RuntimeProfile, SystemRegistry, SystemRole},
    strategy::StrategyProfile,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::Mutex,
};

const BIND_ADDRESS: &str = "127.0.0.1:8787";
const MAX_REQUEST_BYTES: usize = 1_048_576;

#[derive(Clone)]
struct DashboardState {
    session: Arc<Mutex<DashboardSession>>,
    sessions: Arc<Mutex<BTreeMap<String, Arc<Mutex<DashboardSession>>>>>,
    runtimes: Arc<Mutex<RuntimeRegistry>>,
    runtime_sessions: Arc<Mutex<BTreeMap<String, Arc<Mutex<RuntimeRegistry>>>>>,
    registry: Arc<Mutex<SystemRegistry>>,
    auth_token: Option<String>,
    tenant_id: Option<String>,
    session_key: String,
}

impl DashboardState {
    async fn for_session(&self, key: &str) -> Self {
        let key = key.trim();
        let key = if key.is_empty()
            || key.len() > 128
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
        {
            "default"
        } else {
            key
        };
        let session = {
            let mut sessions = self.sessions.lock().await;
            sessions
                .entry(key.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(DashboardSession::default())))
                .clone()
        };
        let runtimes = {
            let mut runtime_sessions = self.runtime_sessions.lock().await;
            runtime_sessions
                .entry(key.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(RuntimeRegistry::default())))
                .clone()
        };
        let mut scoped = self.clone();
        scoped.session = session;
        scoped.runtimes = runtimes;
        scoped.session_key = key.to_owned();
        scoped
    }
}

#[derive(Clone)]
struct DashboardSession {
    config: DeploymentConfig,
    credentials: Option<BinanceCredentials>,
    symbol: String,
    proxy: Option<String>,
}

#[derive(Default)]
struct RuntimeRegistry {
    live: RuntimeProcess,
    simulation: RuntimeProcess,
    backtest: RuntimeProcess,
}

#[derive(Default)]
struct RuntimeProcess {
    child: Option<Child>,
    pid: Option<u32>,
    run_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
    started_at_ms: Option<u64>,
    last_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeStartRequest {
    mode: String,
    input: Option<String>,
    anchors: Option<String>,
    symbols: Option<String>,
    capital_cny: Option<String>,
    proxy: Option<String>,
    duration_secs: Option<u64>,
    max_position: Option<i64>,
    quantity: Option<i64>,
    entry_threshold_bps: Option<i64>,
    queue_ahead: Option<i64>,
    trade_through: Option<i64>,
    market_to_decision_ms: Option<u64>,
    decision_to_exchange_ms: Option<u64>,
    require_flat_at_end: Option<bool>,
    allow_orders: Option<bool>,
}

#[derive(Debug, Serialize)]
struct RuntimeSnapshot {
    mode: String,
    status: String,
    pid: Option<u32>,
    run_dir: Option<String>,
    output_path: Option<String>,
    stdout_path: Option<String>,
    stderr_path: Option<String>,
    started_at_ms: Option<u64>,
    last_message: Option<String>,
}

fn configured_simulation_symbols() -> Result<Vec<String>, String> {
    StrategyProfile::load("config/anchorbell-simulation.json").map(|profile| profile.symbols)
}

fn configured_instrument(
    symbol: &str,
) -> Option<anchorbell_engine::market::InstrumentClassification> {
    InstrumentRegistryConfig::embedded()
        .ok()?
        .by_symbol()
        .remove(&symbol.trim().to_ascii_uppercase())
}

impl DashboardSession {
    fn with_credentials(credentials: Option<BinanceCredentials>) -> Self {
        Self {
            config: DeploymentConfig::from_values(BinanceEnvironment::Testnet, false, false, None)
                .expect("default Testnet configuration must be valid"),
            credentials,
            symbol: configured_simulation_symbols()
                .ok()
                .and_then(|symbols| symbols.into_iter().next())
                .unwrap_or_default(),
            proxy: None,
        }
    }
}

impl Default for DashboardSession {
    fn default() -> Self {
        Self::with_credentials(None)
    }
}

#[derive(Debug, Deserialize)]
struct SessionRequest {
    environment: String,
    api_key: String,
    api_secret: String,
    allow_production: bool,
    allow_order_submission: bool,
    confirmation: String,
    symbol: String,
    proxy: String,
}

#[derive(Debug, Deserialize)]
struct CredentialRequest {
    environment: String,
    api_key: String,
    api_secret: String,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    environment: String,
    has_credentials: bool,
    saved_credentials: bool,
    credential_store_available: bool,
    allow_production: bool,
    allow_order_submission: bool,
    symbol: String,
    region: String,
    proxy_configured: bool,
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    tenant_id: Option<String>,
    session_id: Option<String>,
    body: Vec<u8>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind_address =
        env::var("ANCHORBELL_DASHBOARD_BIND").unwrap_or_else(|_| BIND_ADDRESS.to_owned());
    let auth_token = env::var("ANCHORBELL_DASHBOARD_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let tenant_id = env::var("ANCHORBELL_DASHBOARD_TENANT")
        .ok()
        .filter(|tenant| !tenant.is_empty());
    let loopback = bind_address.starts_with("127.0.0.1:")
        || bind_address.starts_with("localhost:")
        || bind_address.starts_with("[::1]:");
    if !loopback && (auth_token.is_none() || tenant_id.is_none()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "non-loopback dashboard requires token and tenant identity",
        ));
    }
    let listener = TcpListener::bind(&bind_address).await?;
    // Credentials are deliberately session-memory-only. A shared persistent
    // credential store would violate per-session account isolation.
    let saved_testnet_credentials: Option<BinanceCredentials> = None;
    let mut registry = SystemRegistry::default();
    let observed_at_ms = now_ms();
    registry.bootstrap_health(observed_at_ms);
    let dashboard_systems = registry
        .profile_system_ids(RuntimeProfile::Dashboard)
        .expect("dashboard profile must resolve from the system registry");
    for id in dashboard_systems {
        registry
            .report_health(HealthSnapshot::ready(id, observed_at_ms))
            .expect("dashboard registry bootstrap must be valid");
    }
    let default_session = Arc::new(Mutex::new(DashboardSession::with_credentials(
        saved_testnet_credentials,
    )));
    let default_runtimes = Arc::new(Mutex::new(RuntimeRegistry::default()));
    let state = DashboardState {
        session: Arc::clone(&default_session),
        sessions: Arc::new(Mutex::new(BTreeMap::from([(
            "default".to_owned(),
            default_session,
        )]))),
        runtimes: Arc::clone(&default_runtimes),
        runtime_sessions: Arc::new(Mutex::new(BTreeMap::from([(
            "default".to_owned(),
            default_runtimes,
        )]))),
        registry: Arc::new(Mutex::new(registry)),
        auth_token,
        tenant_id,
        session_key: "default".to_owned(),
    };
    println!("AnchorBell dashboard listening on http://{bind_address}");

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, state).await {
                eprintln!("dashboard connection failed: {error}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    state: DashboardState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = read_request(&mut stream).await?;
    let (status, content_type, body) = route(request, state).await;
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n",
        reason_phrase(status),
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn route(request: HttpRequest, state: DashboardState) -> (u16, &'static str, Vec<u8>) {
    if request.path.starts_with("/api/") && !authorized(&request, &state) {
        return json_response(
            401,
            json!({"ok": false, "message": "authentication required"}),
        );
    }
    let session_key = request
        .session_id
        .as_deref()
        .or(request.tenant_id.as_deref())
        .unwrap_or("default");
    let state = state.for_session(session_key).await;
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => text_response(
            200,
            "text/html; charset=utf-8",
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/web/index.html")),
        ),
        ("GET", "/styles.css") => text_response(
            200,
            "text/css; charset=utf-8",
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/web/styles.css")),
        ),
        ("GET", "/app.js") => text_response(
            200,
            "application/javascript; charset=utf-8",
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/web/app.js")),
        ),
        ("GET", "/manifest.webmanifest") => text_response(
            200,
            "application/manifest+json; charset=utf-8",
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/web/manifest.webmanifest"
            )),
        ),
        ("GET", "/sw.js") => text_response(
            200,
            "application/javascript; charset=utf-8",
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/web/sw.js")),
        ),
        ("GET", "/api/status") => json_response(200, status_response(&state).await),
        ("GET", "/api/instruments") => instruments_response(&state).await,
        ("GET", "/api/platform") => platform_response(&state).await,
        ("GET", "/api/schema") => schema_response().await,
        ("GET", "/health") => probe_response("health", 200),
        ("GET", "/live") => probe_response("liveness", 200),
        ("GET", "/ready") => readiness_response(&state).await,
        ("GET", "/api/metrics/simulation") => runtime_metrics("simulation", &state).await,
        ("GET", "/api/metrics/live") => runtime_metrics("live", &state).await,
        ("GET", "/api/metrics/backtest") => runtime_metrics("backtest", &state).await,
        ("GET", "/api/runtimes") => runtimes_response(&state).await,
        ("GET", "/api/runs") => runs_response().await,
        ("GET", "/api/logs/live") => runtime_logs("live", &state).await,
        ("GET", "/api/logs/simulation") => runtime_logs("simulation", &state).await,
        ("GET", "/api/logs/backtest") => runtime_logs("backtest", &state).await,
        ("POST", "/api/runtime/start") => start_runtime(request.body, &state).await,
        ("POST", "/api/runtime/stop") => stop_runtime(request.body, &state).await,
        ("POST", "/api/session") => update_session(request.body, &state).await,
        ("POST", "/api/credentials/save") => save_credentials(request.body, &state).await,
        ("POST", "/api/credentials/delete") => delete_credentials(request.body, &state).await,
        ("POST", "/api/session/clear") => {
            *state.session.lock().await = DashboardSession::default();
            json_response(200, json!({"ok": true, "message": "本地会话已清除"}))
        }
        ("POST", "/api/check/metadata") => metadata_check(&state).await,
        ("POST", "/api/check/market") => market_check(&state).await,
        ("POST", "/api/check/account") => account_check(&state).await,
        ("POST", "/api/check/tradfi-contract") => tradfi_contract_check(&state).await,
        ("POST", "/api/check/open-orders") => open_orders_check(&state).await,
        ("POST", "/api/backtest") => backtest_check(),
        _ => json_response(404, json!({"ok": false, "message": "未找到请求"})),
    }
}

async fn platform_response(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let mut registry = state.registry.lock().await;
    registry.mark_stale_at(now_ms());
    json_response(200, json!({"ok": true, "manifest": registry.manifest()}))
}

async fn schema_response() -> (u16, &'static str, Vec<u8>) {
    let Some(observation) = external_batch_observation() else {
        return json_response(
            200,
            json!({"ok": true, "schema_version": 1, "paths": [], "message": "暂无运行指标 schema"}),
        );
    };
    let mut paths = Vec::new();
    collect_schema_paths(&observation, "", &mut paths);
    paths.sort();
    paths.dedup();
    json_response(
        200,
        json!({
            "ok": true,
            "schema_version": 1,
            "source": "runtime_metrics",
            "observed_at_ms": now_ms(),
            "paths": paths,
        }),
    )
}

fn collect_schema_paths(value: &Value, prefix: &str, paths: &mut Vec<String>) {
    if paths.len() >= 2_000 {
        return;
    }
    match value {
        Value::Object(entries) => {
            for (key, child) in entries {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                paths.push(path.clone());
                collect_schema_paths(child, &path, paths);
            }
        }
        Value::Array(entries) => {
            if let Some(first) = entries.first() {
                collect_schema_paths(first, &format!("{prefix}[]"), paths);
            }
        }
        _ => {}
    }
}

async fn runtimes_response(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let external = external_batch_observation();
    let mut runtimes = state.runtimes.lock().await;
    let local_simulation = mode_snapshot("simulation", &mut runtimes.simulation);
    let simulation = if local_simulation
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("stopped")
        == "stopped"
    {
        external
            .as_ref()
            .map(external_batch_runtime_snapshot)
            .unwrap_or(local_simulation)
    } else {
        local_simulation
    };
    json_response(
        200,
        json!({
            "ok": true,
            "modes": [
                mode_snapshot("live", &mut runtimes.live),
                simulation,
                mode_snapshot("backtest", &mut runtimes.backtest),
            ]
        }),
    )
}

fn mode_snapshot(mode: &str, runtime: &mut RuntimeProcess) -> Value {
    let mut status = "stopped";
    if let Some(child) = runtime.child.as_mut() {
        match child.try_wait() {
            Ok(None) => status = "running",
            Ok(Some(exit)) => {
                status = "exited";
                runtime.last_message = Some(format!("进程已退出：{exit}"));
                runtime.child = None;
                runtime.pid = None;
            }
            Err(error) => {
                status = "unknown";
                runtime.last_message = Some(format!("无法读取进程状态：{error}"));
            }
        }
    }
    serde_json::to_value(RuntimeSnapshot {
        mode: mode.to_owned(),
        status: status.to_owned(),
        pid: runtime.pid,
        run_dir: runtime
            .run_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        output_path: runtime
            .output_path
            .as_ref()
            .map(|path| path.display().to_string()),
        stdout_path: runtime
            .stdout_path
            .as_ref()
            .map(|path| path.display().to_string()),
        stderr_path: runtime
            .stderr_path
            .as_ref()
            .map(|path| path.display().to_string()),
        started_at_ms: runtime.started_at_ms,
        last_message: runtime.last_message.clone(),
    })
    .expect("runtime snapshot is serializable")
}

fn runtime_slot_mut<'a>(
    runtimes: &'a mut RuntimeRegistry,
    mode: &str,
) -> Option<&'a mut RuntimeProcess> {
    match mode {
        "live" => Some(&mut runtimes.live),
        "simulation" => Some(&mut runtimes.simulation),
        "backtest" => Some(&mut runtimes.backtest),
        _ => None,
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine has a repository parent")
        .to_path_buf()
}

fn find_binary(repo: &Path, name: &str) -> Result<PathBuf, String> {
    for profile in [
        "target-review\\debug",
        "target-next\\debug",
        "target\\debug",
        "target\\release",
    ] {
        let candidate = repo.join(profile).join(format!("{name}.exe"));
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(format!("未找到 {name}.exe，请先完成项目编译"))
}

fn resolve_repo_path(repo: &Path, value: Option<String>, default: &str) -> PathBuf {
    let candidate = PathBuf::from(value.unwrap_or_else(|| default.to_owned()));
    if candidate.is_absolute() {
        candidate
    } else {
        repo.join(candidate)
    }
}

fn create_run_dir(repo: &Path, mode: &str) -> Result<PathBuf, String> {
    let path = repo
        .join("target")
        .join("ui-runs")
        .join(format!("{mode}-{}", now_ms()));
    fs::create_dir_all(&path).map_err(|error| format!("无法创建运行目录：{error}"))?;
    Ok(path)
}

fn add_proxy(command: &mut Command, proxy: Option<&String>) {
    if let Some(proxy) = proxy.filter(|value| !value.trim().is_empty()) {
        command.arg("--proxy").arg(proxy);
    }
}

async fn start_runtime(body: Vec<u8>, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let request: RuntimeStartRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return json_response(400, json!({"ok": false, "message": "运行参数格式无效"})),
    };
    let mode = request.mode.trim().to_ascii_lowercase();
    if !matches!(mode.as_str(), "live" | "simulation" | "backtest") {
        return json_response(
            400,
            json!({"ok": false, "message": "运行模式必须是 live、simulation 或 backtest"}),
        );
    }

    {
        let mut runtimes = state.runtimes.lock().await;
        let runtime = runtime_slot_mut(&mut runtimes, &mode).expect("mode validated");
        if let Some(child) = runtime.child.as_mut() {
            match child.try_wait() {
                Ok(None) => {
                    return json_response(
                        409,
                        json!({"ok": false, "message": format!("{mode} 已在运行中", mode = mode)}),
                    )
                }
                Ok(Some(_)) | Err(_) => {
                    runtime.child = None;
                    runtime.pid = None;
                }
            }
        }
    }

    let repo = repo_root();
    let run_dir = match create_run_dir(&repo, &mode) {
        Ok(path) => path,
        Err(message) => return json_response(500, json!({"ok": false, "message": message})),
    };
    let stdout_path = run_dir.join("stdout.log");
    let stderr_path = run_dir.join("stderr.log");
    let stdout = match fs::File::create(&stdout_path) {
        Ok(file) => file,
        Err(error) => {
            return json_response(
                500,
                json!({"ok": false, "message": format!("无法创建标准输出日志：{error}")}),
            )
        }
    };
    let stderr = match fs::File::create(&stderr_path) {
        Ok(file) => file,
        Err(error) => {
            return json_response(
                500,
                json!({"ok": false, "message": format!("无法创建错误日志：{error}")}),
            )
        }
    };

    let (session_config, credentials, session_proxy) = {
        let session = state.session.lock().await;
        (
            session.config,
            session.credentials.clone(),
            session.proxy.clone(),
        )
    };
    let proxy = request.proxy.clone().or(session_proxy);
    let strategy_profile = match StrategyProfile::load("config/anchorbell-simulation.json") {
        Ok(profile) => profile,
        Err(error) => return json_response(500, json!({"ok": false, "message": error})),
    };
    let mut command;
    let mut output_path = None;

    match mode.as_str() {
        "simulation" => {
            let binary = match find_binary(&repo, "anchorbell_simulation") {
                Ok(binary) => binary,
                Err(message) => {
                    return json_response(500, json!({"ok": false, "message": message}))
                }
            };
            let symbols = match request
                .symbols
                .clone()
                .filter(|value| !value.trim().is_empty())
            {
                Some(symbols) => symbols,
                None => match configured_simulation_symbols() {
                    Ok(symbols) if !symbols.is_empty() => symbols.join(","),
                    Ok(_) => {
                        return json_response(
                            500,
                            json!({"ok": false, "message": "simulation profile has no symbols"}),
                        )
                    }
                    Err(error) => {
                        return json_response(500, json!({"ok": false, "message": error}))
                    }
                },
            };
            let run_metrics = run_dir.join("metrics.json");
            let run_records = run_dir.join("records.jsonl");
            let run_market = run_dir.join("market.jsonl");
            let run_anchors = run_dir.join("anchors.json");
            let run_fx = run_dir.join("fx.jsonl");
            command = Command::new(binary);
            command
                .arg("--index-anchors")
                .arg("--symbols")
                .arg(symbols)
                .arg("--environment")
                .arg(strategy_profile.environment.as_str())
                .arg("--price-scale")
                .arg(strategy_profile.price_scale.to_string())
                .arg("--quantity-scale")
                .arg(strategy_profile.quantity_scale.to_string())
                .arg("--capital-cny")
                .arg(request.capital_cny.as_deref().unwrap_or("10000"))
                .arg("--duration-secs")
                .arg(request.duration_secs.unwrap_or(0).to_string())
                .arg("--records")
                .arg(&run_records)
                .arg("--market-records")
                .arg(&run_market)
                .arg("--anchor-report")
                .arg(&run_anchors)
                .arg("--fx-records")
                .arg(&run_fx)
                .arg("--metrics")
                .arg(&run_metrics)
                .arg("--fx-refresh-ms")
                .arg(strategy_profile.fx_refresh_ms.to_string())
                .arg("--fx-max-age-ms")
                .arg(strategy_profile.fx_max_age_ms.to_string())
                .arg("--metrics-refresh-ms")
                .arg(strategy_profile.metrics_refresh_ms.to_string())
                .arg("--index-anchor-refresh-ms")
                .arg(strategy_profile.index_anchor_refresh_ms.to_string())
                .arg("--max-mark-index-gap-bps")
                .arg(strategy_profile.max_mark_index_gap_bps.to_string())
                .arg("--maker-fee-ppm")
                .arg(strategy_profile.fee_schedule.maker_fee_ppm.to_string());
            add_proxy(&mut command, proxy.as_ref());
            output_path = Some(run_metrics);
        }
        "live" => {
            let credentials = match credentials {
                Some(credentials) => credentials,
                None => {
                    return json_response(
                        400,
                        json!({"ok": false, "message": "实盘进程需要先在“环境与安全”中加载 API 凭证"}),
                    )
                }
            };
            if session_config.environment == BinanceEnvironment::Production
                && !session_config.allow_production
            {
                return json_response(
                    400,
                    json!({"ok": false, "message": "Production 尚未显式授权"}),
                );
            }
            let send_orders =
                request.allow_orders.unwrap_or(false) && session_config.allow_live_orders;
            let binary = match find_binary(&repo, "anchorbell_live") {
                Ok(binary) => binary,
                Err(message) => {
                    return json_response(500, json!({"ok": false, "message": message}))
                }
            };
            let (key_name, secret_name) = session_config.environment.credential_env_names();
            command = Command::new(binary);
            command
                .arg("--environment")
                .arg(session_config.environment.as_str())
                .arg("--duration-secs")
                .arg(request.duration_secs.unwrap_or(0).to_string())
                .arg("--price-scale")
                .arg(strategy_profile.price_scale.to_string())
                .arg("--quantity-scale")
                .arg(strategy_profile.quantity_scale.to_string())
                .arg("--max-position")
                .arg(request.max_position.unwrap_or(strategy_profile.max_position).to_string())
                .arg("--quantity")
                .arg(request.quantity.unwrap_or(strategy_profile.requested_quantity).to_string())
                .arg("--entry-threshold-bps")
                .arg(request.entry_threshold_bps.unwrap_or(strategy_profile.entry_threshold_bps).to_string())
                .arg("--max-mark-index-gap-bps")
                .arg(strategy_profile.max_mark_index_gap_bps.to_string())
                .arg("--funding-lead-ms")
                .arg(strategy_profile.funding_lead_ms.to_string())
                .env(
                    "ANCHORBELL_BINANCE_ENV",
                    session_config.environment.as_str(),
                )
                .env(
                    "ANCHORBELL_ENABLE_PRODUCTION",
                    if session_config.allow_production {
                        "1"
                    } else {
                        "0"
                    },
                )
                .env(
                    "ANCHORBELL_ENABLE_ORDER_SUBMISSION",
                    if send_orders { "1" } else { "0" },
                )
                .env(key_name, credentials.api_key.clone())
                .env(secret_name, credentials.api_secret.clone());
            if send_orders {
                command.env(
                    "ANCHORBELL_LIVE_TRADING_CONFIRMATION",
                    "I_UNDERSTAND_REAL_FUNDS_RISK",
                );
                command.arg("--send-orders");
            }
            add_proxy(&mut command, proxy.as_ref());
        }
        "backtest" => {
            let input = resolve_repo_path(
                &repo,
                request.input.clone(),
                "target\\selected-market-records.jsonl",
            );
            let anchors = resolve_repo_path(
                &repo,
                request.anchors.clone(),
                "target\\selected-current-index-anchors.csv",
            );
            if !input.exists() || !anchors.exists() {
                return json_response(
                    400,
                    json!({"ok": false, "message": format!("回测输入文件不存在：input={}，anchors={}", input.display(), anchors.display())}),
                );
            }
            let binary = match find_binary(&repo, "anchorbell_backtest") {
                Ok(binary) => binary,
                Err(message) => {
                    return json_response(500, json!({"ok": false, "message": message}))
                }
            };
            let report_path = stdout_path.clone();
            command = Command::new(binary);
            command
                .arg("--input")
                .arg(input)
                .arg("--anchors")
                .arg(anchors)
                .arg("--records")
                .arg(run_dir.join("records.jsonl"))
                .arg("--price-scale")
                .arg("8")
                .arg("--quantity-scale")
                .arg("8")
                .arg("--entry-threshold-bps")
                .arg(request.entry_threshold_bps.unwrap_or(0).to_string())
                .arg("--max-position")
                .arg(request.max_position.unwrap_or(1).to_string())
                .arg("--quantity")
                .arg(request.quantity.unwrap_or(1).to_string())
                .arg("--queue-ahead")
                .arg(request.queue_ahead.unwrap_or(0).to_string())
                .arg("--trade-through")
                .arg(request.trade_through.unwrap_or(0).to_string())
                .arg("--market-to-decision-ms")
                .arg(request.market_to_decision_ms.unwrap_or(0).to_string())
                .arg("--decision-to-exchange-ms")
                .arg(request.decision_to_exchange_ms.unwrap_or(0).to_string());
            if request.require_flat_at_end.unwrap_or(true) {
                command.arg("--require-flat-at-end");
            }
            output_path = Some(report_path);
        }
        _ => unreachable!("mode validated"),
    }

    command.current_dir(&repo);
    command.stdout(Stdio::from(stdout));
    command.stderr(Stdio::from(stderr));
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return json_response(
                500,
                json!({"ok": false, "message": format!("启动 {mode} 进程失败：{error}")}),
            )
        }
    };
    let pid = child.id();
    let mut runtimes = state.runtimes.lock().await;
    let runtime = runtime_slot_mut(&mut runtimes, &mode).expect("mode validated");
    runtime.child = Some(child);
    runtime.pid = pid;
    runtime.run_dir = Some(run_dir.clone());
    runtime.output_path = output_path.clone();
    runtime.stdout_path = Some(stdout_path.clone());
    runtime.stderr_path = Some(stderr_path.clone());
    runtime.started_at_ms = Some(now_ms());
    runtime.last_message = Some(format!("{mode} 已启动"));
    json_response(
        200,
        json!({
            "ok": true,
            "mode": mode,
            "pid": pid,
            "run_dir": run_dir,
            "output_path": output_path,
            "message": format!("{mode} 进程已启动"),
        }),
    )
}

async fn stop_runtime(body: Vec<u8>, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return json_response(400, json!({"ok": false, "message": "停止参数格式无效"})),
    };
    let mode = request
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if !matches!(mode.as_str(), "live" | "simulation" | "backtest") {
        return json_response(400, json!({"ok": false, "message": "运行模式无效"}));
    }
    let mut runtimes = state.runtimes.lock().await;
    let runtime = runtime_slot_mut(&mut runtimes, &mode).expect("mode validated");
    let Some(child) = runtime.child.as_mut() else {
        return json_response(
            404,
            json!({"ok": false, "message": format!("{mode} 当前未运行")}),
        );
    };
    if let Err(error) = child.kill().await {
        return json_response(
            500,
            json!({"ok": false, "message": format!("停止 {mode} 失败：{error}")}),
        );
    }
    runtime.child = None;
    runtime.pid = None;
    runtime.last_message = Some(format!("{mode} 已由控制台停止"));
    json_response(
        200,
        json!({"ok": true, "message": format!("{mode} 已停止")}),
    )
}

fn external_batch_root() -> Option<PathBuf> {
    env::var_os("ANCHORBELL_BATCH_ROOT")
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
}

fn read_json_file(path: &Path) -> Option<Value> {
    fs::read_to_string(path)
        .ok()
        .and_then(|contents| serde_json::from_str(&contents).ok())
}

fn latest_external_batch() -> Option<(PathBuf, Value)> {
    let root = external_batch_root()?;
    let mut candidates = Vec::new();
    for entry in fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(manifest) = read_json_file(&path.join("run-manifest.json")) else {
            continue;
        };
        let created_at_ms = manifest
            .pointer("/simulation/created_at_ms")
            .and_then(Value::as_u64)
            .or_else(|| manifest.get("created_at_ms").and_then(Value::as_u64))
            .unwrap_or_default();
        candidates.push((created_at_ms, path, manifest));
    }
    candidates.sort_by_key(|candidate| candidate.0);
    candidates.pop().map(|(_, path, manifest)| (path, manifest))
}

fn external_batch_observation() -> Option<Value> {
    let (run_dir, manifest) = latest_external_batch()?;
    let status = read_json_file(&run_dir.join("run-status.json"));
    let experiment_ids: Vec<String> = manifest
        .get("spec_labels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let experiment_id = experiment_ids
        .iter()
        .find(|id| run_dir.join(id).join("metrics.json").is_file())
        .cloned()
        .or_else(|| {
            fs::read_dir(&run_dir)
                .ok()?
                .flatten()
                .map(|entry| entry.path())
                .find(|path| path.join("metrics.json").is_file())
                .and_then(|path| path.file_name()?.to_str().map(str::to_owned))
        })?;
    let metrics_path = run_dir.join(&experiment_id).join("metrics.json");
    let mut metrics = read_json_file(&metrics_path)?;
    let Value::Object(object) = &mut metrics else {
        return None;
    };
    let simulation = manifest.get("simulation");
    let run_id = status
        .as_ref()
        .and_then(|value| value.get("run_id"))
        .and_then(Value::as_str)
        .or_else(|| {
            simulation
                .and_then(|value| value.get("run_id"))
                .and_then(Value::as_str)
        })
        .unwrap_or_default();
    let batch_status = status
        .as_ref()
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let started_at_ms = status
        .as_ref()
        .and_then(|value| value.get("started_at_ms"))
        .and_then(Value::as_u64)
        .or_else(|| {
            simulation
                .and_then(|value| value.get("created_at_ms"))
                .and_then(Value::as_u64)
        });
    let build_identity = status
        .as_ref()
        .and_then(|value| value.get("build_identity"))
        .and_then(Value::as_str)
        .or_else(|| {
            simulation
                .and_then(|value| value.get("build_identity"))
                .and_then(Value::as_str)
        });
    object.insert("source".to_owned(), json!("systemd_batch"));
    object.insert("batch_status".to_owned(), json!(batch_status));
    object.insert("run_id".to_owned(), json!(run_id));
    object.insert("run_dir".to_owned(), json!(run_dir.display().to_string()));
    object.insert("experiment_id".to_owned(), json!(experiment_id));
    object.insert("available_experiments".to_owned(), json!(experiment_ids));
    object.insert("started_at_ms".to_owned(), json!(started_at_ms));
    object.insert("build_identity".to_owned(), json!(build_identity));
    object.insert("observed_at_ms".to_owned(), json!(now_ms()));
    Some(metrics)
}

fn compact_value(value: &Value, depth: usize) -> Value {
    if depth > 12 {
        return json!("[depth limited]");
    }
    match value {
        Value::Object(object) => {
            let mut compact = serde_json::Map::new();
            for (key, child) in object {
                compact.insert(key.clone(), compact_value(child, depth + 1));
            }
            Value::Object(compact)
        }
        Value::Array(values) => {
            const MAX_ITEMS: usize = 64;
            if values.len() <= MAX_ITEMS {
                return Value::Array(
                    values
                        .iter()
                        .map(|child| compact_value(child, depth + 1))
                        .collect(),
                );
            }
            let mut compact = Vec::with_capacity(MAX_ITEMS + 1);
            compact.push(json!({"_truncated_items": values.len() - MAX_ITEMS}));
            compact.extend(
                values
                    .iter()
                    .take(MAX_ITEMS / 2)
                    .map(|child| compact_value(child, depth + 1)),
            );
            compact.extend(
                values
                    .iter()
                    .rev()
                    .take(MAX_ITEMS / 2)
                    .rev()
                    .map(|child| compact_value(child, depth + 1)),
            );
            Value::Array(compact)
        }
        _ => value.clone(),
    }
}

fn manifest_view(value: Option<&Value>) -> Value {
    let Some(manifest) = value else {
        return json!({});
    };
    json!({
        "anchor_source": manifest.get("anchor_source"),
        "price_scale": manifest.get("price_scale"),
        "anchor_kline_interval": manifest.get("anchor_kline_interval"),
        "duration_secs": manifest.get("duration_secs"),
        "entry_threshold_bps": manifest.get("entry_threshold_bps"),
        "fee_ppm": manifest.get("fee_ppm"),
        "fee_schedule": manifest.get("fee_schedule"),
        "market_to_decision_ms": manifest.get("market_to_decision_ms"),
        "decision_to_exchange_ms": manifest.get("decision_to_exchange_ms"),
        "cancel_to_exchange_ms": manifest.get("cancel_to_exchange_ms"),
        "queue_ahead": manifest.get("queue_ahead"),
        "strategy_variants": manifest.get("strategy_variants"),
        "spec_labels": manifest.get("spec_labels"),
    })
}

fn index_view(value: Option<&Value>) -> Value {
    let Some(index) = value else {
        return json!({});
    };
    json!({
        "schema_version": index.get("schema_version"),
        "run_id": index.get("run_id"),
        "status": index.get("status"),
        "evidence_class": index.get("evidence_class"),
        "promotion_verdict": index.get("promotion_verdict"),
        "experiment_plan_id": index.get("experiment_plan_id"),
        "experiments": index.get("experiments"),
    })
}

fn latest_fx_quotes(run_dir: &Path) -> Value {
    let Ok(contents) = fs::read_to_string(run_dir.join("shared-fx.jsonl")) else {
        return json!({});
    };
    let mut quotes = serde_json::Map::new();
    for line in contents.lines().rev() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(currency) = value.get("currency").and_then(Value::as_str) else {
            continue;
        };
        if !quotes.contains_key(currency) {
            quotes.insert(currency.to_owned(), value);
        }
    }
    Value::Object(quotes)
}

async fn runs_response() -> (u16, &'static str, Vec<u8>) {
    let Some(root) = external_batch_root() else {
        return json_response(
            200,
            json!({
                "ok": true,
                "observed_at_ms": now_ms(),
                "run_count": 0,
                "runs": [],
            }),
        );
    };
    let latest_dir = latest_external_batch().map(|(path, _)| path);
    let mut runs = Vec::new();
    let Ok(entries) = fs::read_dir(&root) else {
        return json_response(
            200,
            json!({
                "ok": true,
                "observed_at_ms": now_ms(),
                "run_count": 0,
                "runs": [],
            }),
        );
    };
    for entry in entries.flatten() {
        let run_dir = entry.path();
        if !run_dir.is_dir() {
            continue;
        }
        let manifest = read_json_file(&run_dir.join("run-manifest.json"));
        let index = read_json_file(&run_dir.join("experiment-index.json"));
        let status = read_json_file(&run_dir.join("run-status.json"));
        if manifest.is_none() && index.is_none() {
            continue;
        }
        let created_at_ms = manifest
            .as_ref()
            .and_then(|v| v.get("created_at_ms").and_then(Value::as_u64))
            .or_else(|| {
                index
                    .as_ref()
                    .and_then(|v| v.get("created_at_ms").and_then(Value::as_u64))
            })
            .unwrap_or_default();
        let run_id = status
            .as_ref()
            .and_then(|v| v.get("run_id").and_then(Value::as_str))
            .or_else(|| {
                index
                    .as_ref()
                    .and_then(|v| v.get("run_id").and_then(Value::as_str))
            })
            .unwrap_or_default();
        let run_status = status
            .as_ref()
            .and_then(|v| v.get("status").and_then(Value::as_str))
            .or_else(|| {
                index
                    .as_ref()
                    .and_then(|v| v.get("status").and_then(Value::as_str))
            })
            .unwrap_or("unknown");
        let definitions = index
            .as_ref()
            .and_then(|v| v.get("experiments"))
            .or_else(|| {
                manifest
                    .as_ref()
                    .and_then(|v| v.get("experiment_definitions"))
            });
        let mut methods = Vec::new();
        if let Some(definitions) = definitions.and_then(Value::as_array) {
            for definition in definitions {
                let id = definition
                    .get("experiment_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if id.is_empty() {
                    continue;
                }
                let metrics = read_json_file(&run_dir.join(id).join("metrics.json"));
                let detailed = latest_dir.as_ref().is_some_and(|path| path == &run_dir);
                let symbols = if detailed {
                    compact_value(
                        metrics
                            .as_ref()
                            .and_then(|v| v.get("symbols"))
                            .unwrap_or(&Value::Null),
                        0,
                    )
                } else {
                    Value::Array(
                        metrics
                            .as_ref()
                            .and_then(|v| v.get("symbols"))
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|symbol| {
                                symbol.get("symbol").map(|name| json!({"symbol": name}))
                            })
                            .collect(),
                    )
                };
                let history = if detailed {
                    downsample_history(metrics.as_ref().and_then(|v| v.get("history")))
                } else {
                    json!([])
                };
                methods.push(json!({
                    "id": id,
                    "method": definition.get("method").cloned().unwrap_or(Value::Null),
                    "method_key": definition.get("method_key").cloned().unwrap_or(Value::Null),
                    "role": definition.get("role").cloned().unwrap_or(Value::Null),
                    "ablations": definition.get("ablations").cloned().unwrap_or_else(|| json!([])),
                    "available": metrics.is_some(),
                    "symbols": symbols,
                    "history": history,
                    "summary": metrics.as_ref().and_then(|v| v.get("summary")).cloned().unwrap_or_else(|| json!({})),
                    "risk_metrics": metrics.as_ref().and_then(|v| v.get("risk_metrics")).cloned().unwrap_or_else(|| json!({})),
                    "raw_available": metrics.is_some(),
                }));
            }
        }
        let mut market_data = manifest
            .as_ref()
            .and_then(|v| v.get("index_anchor_conversions"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let latest_fx = latest_fx_quotes(&run_dir);
        if let Value::Object(entries) = &mut market_data {
            for entry in entries.values_mut() {
                let Value::Object(item) = entry else {
                    continue;
                };
                let Some(currency) = item
                    .get("local_currency")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                else {
                    continue;
                };
                let Some(Value::Object(quote)) = latest_fx.get(&currency) else {
                    continue;
                };
                for key in [
                    "buy_local_per_usdt_ppm",
                    "sell_local_per_usdt_ppm",
                    "midpoint_local_per_usdt_ppm",
                ] {
                    if let Some(value) = quote.get(key) {
                        item.insert(key.to_owned(), value.clone());
                    }
                }
                if let Some(value) = quote.get("observed_at_ms") {
                    item.insert("fx_observed_at_ms".to_owned(), value.clone());
                }
                if let Some(value) = quote.get("source") {
                    item.insert("fx_source".to_owned(), value.clone());
                }
            }
        }
        runs.push(json!({
            "name": run_dir.file_name().and_then(|v| v.to_str()).unwrap_or_default(),
            "run_id": run_id,
            "status": run_status,
            "created_at_ms": created_at_ms,
            "started_at_ms": status.as_ref().and_then(|v| v.get("started_at_ms")).cloned().unwrap_or(Value::Null),
            "finished_at_ms": status.as_ref().and_then(|v| v.get("finished_at_ms")).cloned().unwrap_or(Value::Null),
            "policy_id": index.as_ref().and_then(|v| v.get("policy_id")).cloned().unwrap_or(Value::Null),
            "build_identity": status.as_ref().and_then(|v| v.get("build_identity")).cloned()
                .or_else(|| index.as_ref().and_then(|v| v.get("build_identity")).cloned())
                .unwrap_or(Value::Null),
            "methods": methods,
            "market_data": market_data,
            "manifest": manifest_view(manifest.as_ref()),
            "index": index_view(index.as_ref()),
        }));
    }
    runs.sort_by_key(|run| {
        run.get("created_at_ms")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });
    runs.reverse();
    json_response(
        200,
        json!({
            "ok": true,
            "observed_at_ms": now_ms(),
            "root": root.display().to_string(),
            "run_count": runs.len(),
            "runs": runs,
        }),
    )
}

fn downsample_history(value: Option<&Value>) -> Value {
    let Some(Value::Array(points)) = value else {
        return json!([]);
    };
    const MAX_POINTS: usize = 120;
    if points.len() <= MAX_POINTS {
        return Value::Array(points.clone());
    }
    let mut sampled = Vec::with_capacity(MAX_POINTS);
    let last = points.len() - 1;
    for index in 0..MAX_POINTS {
        let source = index * last / (MAX_POINTS - 1);
        sampled.push(points[source].clone());
    }
    Value::Array(sampled)
}

fn external_batch_runtime_snapshot(observation: &Value) -> Value {
    let batch_status = observation
        .get("batch_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let status = match batch_status {
        "running" => "running",
        "completed" | "failed" => "exited",
        _ => "unknown",
    };
    let output_path = observation
        .get("run_dir")
        .and_then(Value::as_str)
        .and_then(|path| {
            observation
                .get("experiment_id")
                .and_then(Value::as_str)
                .map(|id| {
                    PathBuf::from(path)
                        .join(id)
                        .join("metrics.json")
                        .display()
                        .to_string()
                })
        });
    json!({
        "mode": "simulation",
        "status": status,
        "source": "systemd_batch",
        "pid": Value::Null,
        "run_dir": observation.get("run_dir"),
        "output_path": output_path,
        "stdout_path": Value::Null,
        "stderr_path": Value::Null,
        "started_at_ms": observation.get("started_at_ms"),
        "last_message": format!("systemd 批处理 · {} · {}", batch_status, observation.get("run_id").and_then(Value::as_str).unwrap_or("")),
    })
}

async fn runtime_metrics(mode: &str, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let path = {
        let runtimes = state.runtimes.lock().await;
        match mode {
            "live" => runtimes.live.output_path.clone(),
            "simulation" => runtimes.simulation.output_path.clone(),
            "backtest" => runtimes.backtest.output_path.clone(),
            _ => None,
        }
    };
    if let Some(path) = path {
        return match fs::read_to_string(&path) {
            Ok(contents) => match serde_json::from_str::<Value>(&contents) {
                Ok(value) => json_response(200, value),
                Err(error) => json_response(
                    503,
                    json!({"ok": false, "message": format!("指标正在写入：{error}")}),
                ),
            },
            Err(error) => json_response(
                503,
                json!({"ok": false, "message": format!("尚无 {mode} 指标：{error}")}),
            ),
        };
    }
    if mode == "simulation" {
        if let Some(observation) = external_batch_observation() {
            return json_response(200, observation);
        }
    }
    json_response(
        404,
        json!({"ok": false, "message": format!("{mode} 尚未启动")}),
    )
}

async fn runtime_logs(mode: &str, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (stdout_path, stderr_path) = {
        let runtimes = state.runtimes.lock().await;
        let runtime = match mode {
            "live" => &runtimes.live,
            "simulation" => &runtimes.simulation,
            "backtest" => &runtimes.backtest,
            _ => return json_response(400, json!({"ok": false, "message": "运行模式无效"})),
        };
        (runtime.stdout_path.clone(), runtime.stderr_path.clone())
    };
    if mode == "simulation" && stdout_path.is_none() && stderr_path.is_none() {
        if let Some(observation) = external_batch_observation() {
            let stdout = serde_json::to_string_pretty(&observation).unwrap_or_default();
            return json_response(
                200,
                json!({
                    "ok": true,
                    "mode": mode,
                    "source": "systemd_batch",
                    "stdout": stdout,
                    "stderr": "",
                }),
            );
        }
    }
    let read_tail = |path: Option<PathBuf>| -> String {
        let Some(path) = path else {
            return String::new();
        };
        let contents = fs::read_to_string(path).unwrap_or_default();
        let start = contents.len().saturating_sub(20_000);
        contents[start..].to_owned()
    };
    json_response(
        200,
        json!({
            "ok": true,
            "mode": mode,
            "stdout": read_tail(stdout_path),
            "stderr": read_tail(stderr_path),
        }),
    )
}

fn probe_response(kind: &str, status: u16) -> (u16, &'static str, Vec<u8>) {
    json_response(
        status,
        json!({
            "ok": status == 200,
            "service": "anchorbell-dashboard",
            "probe": kind,
        }),
    )
}

async fn readiness_response(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let registry = state.registry.lock().await;
    let report = registry.readiness_for_role(SystemRole::ControlConsole, now_ms());
    let status = if report.ready { 200 } else { 503 };
    json_response(
        status,
        json!({
            "ok": report.ready,
            "service": "anchorbell-dashboard",
            "probe": "readiness",
            "report": report,
        }),
    )
}

async fn status_response(state: &DashboardState) -> Value {
    let session = state.session.lock().await;
    let environment = session.config.environment;
    // Persistence is intentionally disabled so one session can never discover
    // another user's credential presence or secret.
    let saved_credentials = false;
    serde_json::to_value(StatusResponse {
        environment: environment.to_string(),
        has_credentials: session.credentials.is_some(),
        saved_credentials,
        credential_store_available: false,
        allow_production: session.config.allow_production,
        allow_order_submission: session.config.allow_live_orders,
        symbol: session.symbol.clone(),
        region: configured_instrument(&session.symbol)
            .map(|instrument| format!("{:?}", instrument.market_region))
            .unwrap_or_else(|| "未知".to_owned()),
        proxy_configured: session.proxy.is_some(),
    })
    .expect("status response is serializable")
}

async fn update_session(body: Vec<u8>, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let request: SessionRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return json_response(400, json!({"ok": false, "message": "配置格式无效"})),
    };
    let environment: BinanceEnvironment = match request.environment.parse() {
        Ok(environment) => environment,
        Err(_) => {
            return json_response(
                400,
                json!({"ok": false, "message": "环境必须是 testnet 或 production"}),
            )
        }
    };
    let confirmation =
        (!request.confirmation.trim().is_empty()).then_some(request.confirmation.as_str());
    let config = match DeploymentConfig::from_values(
        environment,
        request.allow_production,
        request.allow_order_submission,
        confirmation,
    ) {
        Ok(config) => config,
        Err(error) => return deployment_error(error),
    };
    let (credentials, loaded_from_store) = match (request.api_key.trim(), request.api_secret.trim())
    {
        ("", "") => (None, false),
        (api_key, api_secret) => {
            match BinanceCredentials::from_values(api_key.to_owned(), api_secret.to_owned()) {
                Ok(credentials) => (Some(credentials), false),
                Err(_) => {
                    return json_response(400, json!({"ok": false, "message": "API 凭证不能为空"}))
                }
            }
        }
    };
    let symbol = request.symbol.trim().to_ascii_uppercase();
    if symbol.is_empty() {
        return json_response(400, json!({"ok": false, "message": "交易标的不能为空"}));
    }
    if configured_instrument(&symbol)
        .is_none_or(|instrument| !instrument.simulation_enabled)
    {
        return json_response(
            400,
            json!({
                "ok": false,
                "message": "只允许 instrument registry 中明确启用模拟的标的"
            }),
        );
    }

    let proxy = match request.proxy.trim() {
        "" => None,
        value => Some(value.to_owned()),
    };
    *state.session.lock().await = DashboardSession {
        config,
        credentials,
        symbol,
        proxy,
    };
    json_response(
        200,
        json!({
            "ok": true,
            "message": format!("{}{}，订单权限：{}", if loaded_from_store { "已加载本机保存凭证，" } else { "会话凭证已应用，" }, environment, if config.allow_live_orders { "开启" } else { "关闭" })
        }),
    )
}

async fn save_credentials(body: Vec<u8>, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let request: CredentialRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return json_response(400, json!({"ok": false, "message": "配置格式无效"})),
    };
    let environment: BinanceEnvironment = match request.environment.parse() {
        Ok(environment) => environment,
        Err(_) => {
            return json_response(
                400,
                json!({"ok": false, "message": "环境必须是 testnet 或 production"}),
            )
        }
    };
    let credentials = match BinanceCredentials::from_values(
        request.api_key.trim().to_owned(),
        request.api_secret.trim().to_owned(),
    ) {
        Ok(credentials) => credentials,
        Err(_) => return json_response(400, json!({"ok": false, "message": "API 凭证不能为空"})),
    };
    let mut session = state.session.lock().await;
    if session.config.environment == environment {
        session.credentials = Some(credentials);
    }
    json_response(
        200,
        json!({"ok": true, "message": format!("{} 凭证仅保存在当前会话内", environment)}),
    )
}

async fn delete_credentials(body: Vec<u8>, state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let request: CredentialRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return json_response(400, json!({"ok": false, "message": "配置格式无效"})),
    };
    let environment: BinanceEnvironment = match request.environment.parse() {
        Ok(environment) => environment,
        Err(_) => {
            return json_response(
                400,
                json!({"ok": false, "message": "环境必须是 testnet 或 production"}),
            )
        }
    };
    let mut session = state.session.lock().await;
    if session.config.environment == environment {
        session.credentials = None;
    }
    json_response(
        200,
        json!({"ok": true, "message": format!("{} 当前会话凭证已删除", environment)}),
    )
}

fn deployment_error(error: DeploymentConfigError) -> (u16, &'static str, Vec<u8>) {
    let message = match error {
        DeploymentConfigError::InvalidEnvironment => "环境配置无效",
        DeploymentConfigError::ProductionNotExplicitlyEnabled => "Production 未显式授权",
        DeploymentConfigError::LiveOrdersNotExplicitlyEnabled => "Production 真实订单缺少确认",
    };
    json_response(400, json!({"ok": false, "message": message}))
}

async fn instruments_response(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (environment, proxy) = {
        let session = state.session.lock().await;
        (session.config.environment, session.proxy.clone())
    };
    let registry_config = match InstrumentRegistryConfig::embedded() {
        Ok(config) => config,
        Err(error) => {
            return json_response(500, json!({"ok": false, "message": error.to_string()}))
        }
    };
    let client = match PublicMarketMetadataClient::new(
        environment.endpoints().rest_base.as_str(),
        proxy.as_deref(),
    ) {
        Ok(client) => client,
        Err(error) => {
            return json_response(400, json!({"ok": false, "message": error.to_string()}))
        }
    };
    let exchange_info = match client.exchange_info().await {
        Ok(exchange_info) => exchange_info,
        Err(error) => {
            return json_response(502, json!({"ok": false, "message": error.to_string()}))
        }
    };
    let funding_info = client.funding_info(None).await.unwrap_or_default();
    let funding_by_symbol = funding_info
        .into_iter()
        .map(|item| (item.symbol.clone(), item))
        .collect::<BTreeMap<_, _>>();
    let exchange_rules = exchange_info
        .iter()
        .filter_map(|item| {
            let filters = item.execution_filters().ok()?;
            Some(json!({
                "symbol": item.symbol,
                "status": item.status,
                "contract_type": item.contract_type,
                "price_precision": item.price_precision,
                "quantity_precision": item.quantity_precision,
                "filters": filters,
                "funding": funding_by_symbol.get(&item.symbol),
            }))
        })
        .collect::<Vec<_>>();
    let snapshot = InstrumentRegistrySnapshot::from_exchange_info(exchange_info, &registry_config);
    let profile = StrategyProfile::load("config/anchorbell-simulation.json").ok();
    json_response(
        200,
        json!({
            "ok": true,
            "environment": environment.to_string(),
            "registry": snapshot,
            "exchange_rules": exchange_rules,
            "commission": profile.map(|value| value.fee_schedule),
        }),
    )
}

async fn metadata_check(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (config, symbol, proxy) = {
        let session = state.session.lock().await;
        (
            session.config,
            session.symbol.clone(),
            session.proxy.clone(),
        )
    };
    if config.environment == BinanceEnvironment::Production && !config.allow_production {
        return json_response(
            400,
            json!({"ok": false, "message": "Production 元数据检查需要先显式允许访问 Production"}),
        );
    }
    let client = match PublicMarketMetadataClient::new(
        config.environment.endpoints().rest_base.as_str(),
        proxy.as_deref(),
    ) {
        Ok(client) => client,
        Err(error) => {
            return json_response(400, json!({"ok": false, "message": error.to_string()}))
        }
    };
    let infos = match client.exchange_info().await {
        Ok(infos) => infos,
        Err(error) => {
            return json_response(502, json!({"ok": false, "message": error.to_string()}))
        }
    };
    let Some(metadata) = infos.into_iter().find(|item| item.symbol == symbol) else {
        return json_response(
            200,
            json!({
                "ok": false,
                "environment": config.environment.to_string(),
                "symbol": symbol,
                "message": "所选标的在当前环境 exchangeInfo 中不可用"
            }),
        );
    };
    let snapshot = match client.symbol_snapshot(&symbol, metadata).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return json_response(502, json!({"ok": false, "message": error.to_string()}))
        }
    };
    match snapshot.validate_for_runtime(now_ms()) {
        Ok(()) => {
            let filters = snapshot
                .metadata
                .execution_filters()
                .expect("runtime validation already checked exchange filters");
            json_response(
                200,
                json!({
                    "ok": true,
                    "environment": config.environment.to_string(),
                    "symbol": symbol,
                    "status": snapshot.metadata.status,
                    "contract_type": snapshot.metadata.contract_type,
                    "bid": snapshot.book_ticker.bid_price,
                    "ask": snapshot.book_ticker.ask_price,
                    "mark": snapshot.premium_index.mark_price,
                    "index": snapshot.premium_index.index_price,
                    "funding": snapshot.premium_index.last_funding_rate,
                    "next_funding_time": snapshot.premium_index.next_funding_time_ms,
                    "price_tick": filters.price_tick,
                    "quantity_step": filters.quantity_step,
                    "min_notional": filters.min_notional,
                    "message": "元数据门禁通过"
                }),
            )
        }
        Err(error) => json_response(
            200,
            json!({
                "ok": false,
                "environment": config.environment.to_string(),
                "symbol": symbol,
                "message": format!("元数据门禁未通过：{error}")
            }),
        ),
    }
}

async fn market_check(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (environment, symbol, proxy) = {
        let session = state.session.lock().await;
        (
            session.config.environment,
            session.symbol.clone(),
            session.proxy.clone(),
        )
    };
    let subscription = match BinanceSubscription::new(symbol.clone()) {
        Ok(subscription) => subscription,
        Err(_) => return json_response(400, json!({"ok": false, "message": "交易标的格式无效"})),
    };
    let profile = match StrategyProfile::load("config/anchorbell-simulation.json") {
        Ok(profile) => profile,
        Err(error) => return json_response(500, json!({"ok": false, "message": error})),
    };
    let operational = &binance_runtime_config().operational;
    let config = BinanceMarketConfig {
        market_ws_base: environment.endpoints().market_ws_base,
        subscriptions: vec![subscription],
        price_scale: profile.price_scale,
        quantity_scale: profile.quantity_scale,
        max_frame_bytes: operational.max_frame_bytes,
        connect_timeout_ms: profile.connect_timeout_ms,
        read_timeout_ms: profile.read_timeout_ms,
        http_proxy: proxy,
        reconnect: ReconnectPolicy {
            max_attempts: Some(1),
            ..ReconnectPolicy::default()
        },
    };
    let mut stream = BinanceMarketStream::new(config);
    let mut count = 0_u32;
    let result = tokio::time::timeout(
        Duration::from_millis(
            profile
                .connect_timeout_ms
                .saturating_add(profile.read_timeout_ms),
        ),
        stream.run_until_error(|_| count += 1),
    )
    .await;
    json_response(
        200,
        json!({
            "ok": count > 0,
            "environment": environment.to_string(),
            "symbol": symbol,
            "events": count,
            "timeout": result.is_err(),
            "message": if count > 0 { "行情连接成功" } else { "未收到行情事件" }
        }),
    )
}

async fn account_check(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (config, credentials, proxy) = session_snapshot(state).await;
    let credentials = match credentials {
        Some(credentials) => credentials,
        None => {
            return json_response(
                400,
                json!({"ok": false, "message": "请先在界面注入当前环境凭证"}),
            )
        }
    };
    let policy = config.policy(true);
    let mut socket = match BinanceOrderWebSocket::connect_with_proxy(
        config.environment,
        policy,
        proxy.as_deref(),
    )
    .await
    {
        Ok(socket) => socket,
        Err(error) => {
            return json_response(502, json!({"ok": false, "message": error.to_string()}))
        }
    };
    let timestamp_ms = now_ms();
    let wire = BinanceAccountStatusWire {
        request_id: format!("anchorbell-dashboard-account-{timestamp_ms}"),
        timestamp_ms,
        recv_window_ms: 5_000,
    };
    let payload = match wire.payload(&credentials.api_key, &credentials.api_secret) {
        Ok(payload) => payload,
        Err(_) => return json_response(500, json!({"ok": false, "message": "签名失败"})),
    };
    let response: BinanceAccountStatusResponse = match socket.request_typed(payload).await {
        Ok(response) => response,
        Err(error) => {
            return json_response(502, json!({"ok": false, "message": error.to_string()}))
        }
    };
    json_response(
        200,
        json!({
            "ok": true,
            "environment": config.environment.to_string(),
            "status": response.status,
            "can_trade": response.result.can_trade,
            "positions": response.result.positions.len(),
            "message": "账户只读查询成功"
        }),
    )
}

async fn tradfi_contract_check(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (config, credentials, proxy) = session_snapshot(state).await;
    let credentials = match credentials {
        Some(credentials) => credentials,
        None => {
            return json_response(
                400,
                json!({"ok": false, "message": "请先在界面注入当前环境凭证"}),
            )
        }
    };
    let client =
        match BinanceRestClient::new(config.environment, config.policy(true), proxy.as_deref()) {
            Ok(client) => client,
            Err(error) => {
                return json_response(400, json!({"ok": false, "message": error.to_string()}))
            }
        };
    match client
        .sign_tradfi_contract(&credentials, now_ms(), 5_000)
        .await
    {
        Ok(response) => json_response(
            200,
            json!({
                "ok": true,
                "environment": config.environment.to_string(),
                "code": response.code,
                "message": "TradFi-Perps 协议确认成功"
            }),
        ),
        Err(error) => json_response(502, json!({"ok": false, "message": error.to_string()})),
    }
}

async fn open_orders_check(state: &DashboardState) -> (u16, &'static str, Vec<u8>) {
    let (config, credentials, symbol, proxy) = session_snapshot_with_symbol(state).await;
    let credentials = match credentials {
        Some(credentials) => credentials,
        None => {
            return json_response(
                400,
                json!({"ok": false, "message": "请先在界面注入当前环境凭证"}),
            )
        }
    };
    let client =
        match BinanceRestClient::new(config.environment, config.policy(true), proxy.as_deref()) {
            Ok(client) => client,
            Err(error) => {
                return json_response(400, json!({"ok": false, "message": error.to_string()}))
            }
        };
    match client
        .current_open_orders(&credentials, Some(&symbol), now_ms(), 5_000)
        .await
    {
        Ok(orders) => json_response(
            200,
            json!({
                "ok": true,
                "environment": config.environment.to_string(),
                "symbol": symbol,
                "count": orders.len(),
                "message": "当前挂单只读查询成功"
            }),
        ),
        Err(error) => json_response(502, json!({"ok": false, "message": error.to_string()})),
    }
}

fn backtest_check() -> (u16, &'static str, Vec<u8>) {
    let fixture = [
        (99_i64, 100_i64, 10_i64, 8_i64),
        (100, 101, 4, 6),
        (101, 102, 3, 2),
    ];
    let mut report = BacktestReport::default();
    for (bid, ask, bid_qty, ask_qty) in fixture {
        report.record_event();
        let decision = ConservativeTopOfBook.evaluate(
            MakerQuote {
                side: Side::Sell,
                price_ticks: bid,
                quantity: 5,
            },
            TopOfBook {
                bid_price_ticks: bid,
                ask_price_ticks: ask,
                bid_quantity: bid_qty,
                ask_quantity: ask_qty,
            },
        );
        if let FillDecision::Fill { quantity } = decision {
            report.record_fill(quantity, 1, 0);
            report.record_position(quantity);
        }
    }
    json_response(
        200,
        json!({
            "ok": true,
            "events": report.event_count,
            "fills": report.fill_count,
            "quantity": report.filled_quantity,
            "fees": report.fees_ticks,
            "net_pnl": report.net_pnl_ticks(),
            "peak_position": report.peak_absolute_position,
            "message": "内置确定性回测完成"
        }),
    )
}

async fn session_snapshot(
    state: &DashboardState,
) -> (DeploymentConfig, Option<BinanceCredentials>, Option<String>) {
    let session = state.session.lock().await;
    (
        session.config,
        session.credentials.clone(),
        session.proxy.clone(),
    )
}

async fn session_snapshot_with_symbol(
    state: &DashboardState,
) -> (
    DeploymentConfig,
    Option<BinanceCredentials>,
    String,
    Option<String>,
) {
    let session = state.session.lock().await;
    (
        session.config,
        session.credentials.clone(),
        session.symbol.clone(),
        session.proxy.clone(),
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_millis() as u64
}

fn authorized(request: &HttpRequest, state: &DashboardState) -> bool {
    let Some(expected) = state.auth_token.as_deref() else {
        return true;
    };
    let token_ok = request
        .authorization
        .as_deref()
        .is_some_and(|value| value == format!("Bearer {expected}"));
    let tenant_ok = state
        .tenant_id
        .as_deref()
        .is_none_or(|expected| request.tenant_id.as_deref() == Some(expected));
    token_ok && tenant_ok
}

async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, &'static str> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8_192];
    let header_end = loop {
        let count = stream
            .read(&mut buffer)
            .await
            .map_err(|_| "request read failed")?;
        if count == 0 {
            return Err("request closed");
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err("request too large");
        }
        if let Some(index) = find_bytes(&bytes, b"\r\n\r\n") {
            break index;
        }
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|_| "invalid headers")?;
    let mut lines = header.lines();
    let request_line = lines.next().ok_or("missing request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or("missing method")?.to_owned();
    let path = parts.next().ok_or("missing path")?.to_owned();
    let authorization = header.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.eq_ignore_ascii_case("authorization")).then(|| value.trim().to_owned())
    });
    let tenant_id = header.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.eq_ignore_ascii_case("x-anchorbell-tenant")).then(|| value.trim().to_owned())
    });
    let session_id = header.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.eq_ignore_ascii_case("x-anchorbell-session")).then(|| value.trim().to_owned())
    });
    let content_length = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name.eq_ignore_ascii_case("content-length"))
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let body_start = header_end + 4;
    while bytes.len() < body_start + content_length {
        let count = stream
            .read(&mut buffer)
            .await
            .map_err(|_| "request body read failed")?;
        if count == 0 {
            return Err("request body closed");
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err("request too large");
        }
    }
    Ok(HttpRequest {
        method,
        path,
        authorization,
        tenant_id,
        session_id,
        body: bytes[body_start..body_start + content_length].to_vec(),
    })
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn json_response(status: u16, value: Value) -> (u16, &'static str, Vec<u8>) {
    (
        status,
        "application/json; charset=utf-8",
        serde_json::to_vec(&value).expect("JSON response is serializable"),
    )
}

fn text_response(
    status: u16,
    content_type: &'static str,
    text: &str,
) -> (u16, &'static str, Vec<u8>) {
    (status, content_type, text.as_bytes().to_vec())
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        _ => "Error",
    }
}
