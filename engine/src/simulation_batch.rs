use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{process::Command, sync::mpsc};

use crate::{
    analytics_evidence::{EvidenceAccumulator, EvidenceConfig},
    analytics_validation::{
        evaluate_simulation_promotion, SimulationPromotionGate, SimulationPromotionInput,
        ValidationSummary,
    },
    backtest::realism::{LatencyModel, QueueModel, RealisticFillModel},
    execution::{BinanceEnvironment, SessionCheckpoint},
    market::{
        binance::{parse_price_ticks, parse_quantity, BinanceMarketEvent},
        metadata::BinanceDepthSnapshot,
        recorder::{add_event_lineage, market_event_to_json},
        BinanceC2cFxClient, BinanceC2cFxPoller, BinanceMarketConfig, BinanceMarketFeed,
        BinanceMarketStream, FxPollerConfig, FxUpdate, PublicMarketMetadataClient, ReconnectPolicy,
    },
    orderbook::{LocalOrderBook, OrderBookError},
    runtime::{
        io::{spawn_line_writer, write_json_atomic, AsyncLineWriter},
        reference_authority::fetch as load_index_anchor_set,
        DataQuality, EventEnvelope, EventSource,
    },
    simulation::engine::{
        AnchorSnapshot, PerformancePoint, PositionAllocation, SimulationEngine, SimulationError,
        SimulationPolicyVariant, SimulationSummary,
    },
    strategy::{
        CalibrationSnapshot, CalibrationState, CALIBRATION_MODEL_VERSION,
        CALIBRATION_SCHEMA_VERSION,
    },
};

const MIN_SIMULATION_FREE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const STORAGE_SAFETY_SCHEMA_VERSION: u16 = 1;
const METRICS_HISTORY_CAPACITY: usize = 901;
/// M9 uses a declared calibration population instead of whichever ledger has
/// the largest sample count. This keeps the source auditable and avoids mixing
/// policy-specific order selection into a single unlabeled model.
const M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";

#[derive(Debug, Clone)]
pub struct SimulationBatchSpec {
    pub label: String,
    pub variant: SimulationPolicyVariant,
}

#[derive(Debug, Clone)]
pub struct SimulationBatchConfig {
    /// Human-readable run generation. Each run writes it into its manifest.
    pub policy_id: String,
    pub environment: BinanceEnvironment,
    pub symbols: Vec<String>,
    pub anchors: BTreeMap<String, AnchorSnapshot>,
    pub entry_threshold_bps: i64,
    pub threshold_scale_ppm: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub max_mark_index_gap_bps: i64,
    pub max_anchor_age_ms: u64,
    pub fee_ppm: i64,
    pub quantity_scale: u32,
    pub price_scale: u32,
    pub position_allocations: Option<BTreeMap<String, PositionAllocation>>,
    pub output_root: PathBuf,
    pub specs: Vec<SimulationBatchSpec>,
    pub max_subscriptions_per_shard: usize,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub metrics_refresh_ms: u64,
    pub index_anchor_refresh_ms: u64,
    pub fx_refresh_ms: u64,
    pub fx_max_age_ms: u64,
    pub queue_ahead: i64,
    pub trade_through: i64,
    pub market_to_decision_ms: u64,
    pub decision_to_exchange_ms: u64,
    pub cancel_to_exchange_ms: u64,
    pub quote_reprice_min_interval_ms: u64,
    pub dynamic_capital_refresh_ms: u64,
    /// REST snapshot depth used to seed the live-like local order book.
    pub depth_snapshot_limit: usize,
    pub checkpoint_path: Option<PathBuf>,
    pub checkpoint_session_id: Option<String>,
    pub checkpoint_interval_ms: u64,
    pub duration_secs: u64,
    /// Shared, deterministic evidence test fed exactly once per public event.
    pub evidence: EvidenceConfig,
}

#[derive(Debug, Serialize)]
pub struct SimulationLedgerResult {
    pub label: String,
    pub strategy_variant: String,
    pub evidence_record_id: String,
    pub summary: SimulationSummary,
    pub settlement_status: String,
    pub flatten_requested: bool,
    pub records_written: u64,
    pub records_dropped: u64,
}

#[derive(Debug, Serialize)]
pub struct SimulationBatchResult {
    pub shared_market_records_written: u64,
    pub shared_market_records_dropped: u64,
    pub shared_fx_records_written: u64,
    pub shared_fx_records_dropped: u64,
    pub evidence_summary: crate::analytics_evidence::EvidenceSummary,
    pub analytics_validation_summary: ValidationSummary,
    pub promotion_gate: SimulationPromotionGate,
    pub evidence_records_written: u64,
    pub evidence_records_dropped: u64,
    pub ledgers: Vec<SimulationLedgerResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CalibrationStore {
    schema_version: u32,
    model_version: String,
    updated_at_event_time_ms: u64,
    snapshots: BTreeMap<String, CalibrationSnapshot>,
}

struct Ledger {
    spec: SimulationBatchSpec,
    engine: SimulationEngine,
    record_tx: mpsc::Sender<String>,
    record_writer: tokio::task::JoinHandle<Result<u64, std::io::Error>>,
    record_written: Arc<AtomicU64>,
    record_dropped: Arc<AtomicU64>,
    metrics_path: PathBuf,
    history: VecDeque<PerformancePoint>,
    settlement_status: String,
    flatten_requested: bool,
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
async fn available_storage_bytes(path: &Path) -> Result<u64, SimulationError> {
    let output = Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .await
        .map_err(|error| SimulationError::Io(format!("storage check failed: {error}")))?;
    if !output.status.success() {
        return Err(SimulationError::Io(format!(
            "storage check exited with {}",
            output.status
        )));
    }
    let available_kb = String::from_utf8_lossy(&output.stdout)
        .lines()
        .skip(1)
        .find_map(|line| line.split_whitespace().nth(3))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            SimulationError::Io("storage check returned no available-space value".to_owned())
        })?;
    Ok(available_kb.saturating_mul(1024))
}

async fn enforce_storage_safety(
    output_root: &Path,
    observed_at_ms: u64,
) -> Result<(), SimulationError> {
    let available_bytes = available_storage_bytes(output_root).await?;
    if available_bytes < MIN_SIMULATION_FREE_BYTES {
        let status = serde_json::json!({
            "schema_version": STORAGE_SAFETY_SCHEMA_VERSION,
            "status": "risk_stopped",
            "reason": "simulation_storage_free_space_below_floor",
            "observed_at_ms": observed_at_ms,
            "available_bytes": available_bytes,
            "minimum_free_bytes": MIN_SIMULATION_FREE_BYTES,
        });
        write_json_atomic(&output_root.join("storage-safety.json"), &status).await?;
        return Err(SimulationError::Io(format!(
            "simulation storage safety stop: {available_bytes} bytes available"
        )));
    }
    Ok(())
}

async fn load_calibration_seeds(
    path: &Path,
    source_label: &str,
) -> BTreeMap<String, CalibrationState> {
    let Ok(bytes) = tokio::fs::read(path).await else {
        return BTreeMap::new();
    };
    let Ok(store) = serde_json::from_slice::<CalibrationStore>(&bytes) else {
        return BTreeMap::new();
    };
    if store.schema_version != CALIBRATION_SCHEMA_VERSION
        || store.model_version != CALIBRATION_MODEL_VERSION
    {
        return BTreeMap::new();
    }
    store
        .snapshots
        .into_iter()
        .filter_map(|(key, snapshot)| {
            let (stored_source, symbol) = key.split_once("::")?;
            if stored_source != source_label
                || symbol != snapshot.instrument
                || snapshot.replay().is_err()
            {
                None
            } else {
                Some((symbol.to_owned(), snapshot.state))
            }
        })
        .collect()
}

fn calibration_key(source_label: &str, symbol: &str) -> String {
    format!("{source_label}::{symbol}")
}

fn calibration_rank(snapshot: &CalibrationSnapshot) -> (u64, u64, u64, u64) {
    (
        snapshot.effective_sample_size,
        snapshot.state.fill_events,
        snapshot.state.completed_orders,
        snapshot.window_end_event_time_ms,
    )
}

async fn persist_calibration_store(path: &Path, ledgers: &[Ledger]) -> Result<(), SimulationError> {
    let mut snapshots = BTreeMap::<String, CalibrationSnapshot>::new();
    for ledger in ledgers {
        for (symbol, snapshot) in ledger.engine.calibration_snapshots(0) {
            let key = calibration_key(&ledger.spec.label, &symbol);
            let replace = snapshots
                .get(&key)
                .map(|previous| calibration_rank(&snapshot) > calibration_rank(previous))
                .unwrap_or(true);
            if replace {
                snapshots.insert(key, snapshot);
            }
        }
    }
    let updated_at_event_time_ms = snapshots
        .values()
        .map(|snapshot| snapshot.window_end_event_time_ms)
        .max()
        .unwrap_or(0);
    let store = CalibrationStore {
        schema_version: CALIBRATION_SCHEMA_VERSION,
        model_version: CALIBRATION_MODEL_VERSION.to_owned(),
        updated_at_event_time_ms,
        snapshots,
    };
    write_json_atomic(path, &store).await?;
    Ok(())
}

fn synchronize_m9_calibration(ledgers: &mut [Ledger]) {
    let Some(seeds) = ledgers
        .iter()
        .find(|ledger| ledger.spec.label == M9_CALIBRATION_SOURCE_LABEL)
        .map(|ledger| {
            ledger
                .engine
                .calibration_snapshots(0)
                .into_iter()
                .map(|(symbol, snapshot)| (symbol, snapshot.state))
                .collect::<BTreeMap<_, _>>()
        })
    else {
        return;
    };
    for ledger in ledgers
        .iter_mut()
        .filter(|ledger| ledger.spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc)
    {
        ledger.engine.restore_calibration_states(&seeds);
    }
}

#[allow(clippy::type_complexity)]
fn parse_depth_snapshot(
    snapshot: &BinanceDepthSnapshot,
    price_scale: u32,
    quantity_scale: u32,
) -> Result<(u64, Vec<(i64, i64)>, Vec<(i64, i64)>), SimulationError> {
    let parse = |rows: &Vec<[String; 2]>| {
        rows.iter()
            .map(|[price, quantity]| {
                Ok((
                    parse_price_ticks(price, price_scale)
                        .map_err(|error| {
                            SimulationError::Market(format!("invalid depth price: {error:?}"))
                        })?
                        .0,
                    parse_quantity(quantity, quantity_scale)
                        .map_err(|error| {
                            SimulationError::Market(format!("invalid depth quantity: {error:?}"))
                        })?
                        .0,
                ))
            })
            .collect::<Result<Vec<_>, SimulationError>>()
    };
    Ok((
        snapshot.last_update_id,
        parse(&snapshot.bids)?,
        parse(&snapshot.asks)?,
    ))
}

async fn unique_output_root(root: &Path) -> Result<PathBuf, SimulationError> {
    if !tokio::fs::try_exists(root).await? {
        return Ok(root.to_path_buf());
    }
    for run_number in 1..=10_000_u32 {
        let candidate = PathBuf::from(format!("{}-run-{:03}", root.display(), run_number));
        if !tokio::fs::try_exists(&candidate).await? {
            return Ok(candidate);
        }
    }
    Err(SimulationError::InvalidConfig(
        "batch execution output root has too many retained runs",
    ))
}

fn build_engine(
    config: &SimulationBatchConfig,
    spec: &SimulationBatchSpec,
    calibration_seeds: &BTreeMap<String, CalibrationState>,
) -> Result<SimulationEngine, SimulationError> {
    let realism = RealisticFillModel {
        queue: QueueModel {
            visible_ahead: config.queue_ahead,
            trade_through: config.trade_through,
        },
        latency: LatencyModel {
            market_to_decision_ms: config.market_to_decision_ms,
            decision_to_exchange_ms: config.decision_to_exchange_ms,
            cancel_to_exchange_ms: config.cancel_to_exchange_ms,
        },
    };
    let mut engine = SimulationEngine::new(
        config.anchors.clone(),
        config.entry_threshold_bps,
        config.max_position,
        config.requested_quantity,
        config.max_mark_index_gap_bps,
        config.max_anchor_age_ms,
        config.fee_ppm,
        config.quantity_scale,
    )?
    .with_price_scale(config.price_scale)
    .with_realism(realism)
    .with_live_risk_gates()
    .with_strategy_variant(spec.variant)
    .with_quote_reprice_min_interval_ms(config.quote_reprice_min_interval_ms)
    .with_dynamic_capital_refresh_ms(config.dynamic_capital_refresh_ms)
    .with_threshold_scale_ppm(config.threshold_scale_ppm);
    engine.restore_calibration_states(calibration_seeds);
    if let Some(allocations) = config.position_allocations.clone() {
        engine = engine.with_position_allocations(allocations)?;
    }
    Ok(engine)
}

fn validate(config: &SimulationBatchConfig) -> Result<(), SimulationError> {
    if config.symbols.is_empty()
        || config.specs.is_empty()
        || config.max_subscriptions_per_shard == 0
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution requires symbols, specs, and shard capacity",
        ));
    }
    if config.specs.iter().any(|spec| spec.label.trim().is_empty()) {
        return Err(SimulationError::InvalidConfig(
            "batch execution labels must be non-empty",
        ));
    }
    if config
        .specs
        .windows(2)
        .any(|pair| pair[0].label == pair[1].label)
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution labels must be unique",
        ));
    }
    Ok(())
}
pub async fn run(
    mut config: SimulationBatchConfig,
) -> Result<SimulationBatchResult, SimulationError> {
    validate(&config)?;
    if config.policy_id.trim().is_empty() {
        return Err(SimulationError::InvalidConfig(
            "simulation policy identity must be non-empty",
        ));
    }
    let calibration_store_path = config.output_root.join("calibration/latest.json");
    if let Some(parent) = calibration_store_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    config.output_root = unique_output_root(&config.output_root).await?;
    tokio::fs::create_dir_all(&config.output_root).await?;
    let manifest_created_at_ms = now_ms();
    let parameter_material = serde_json::json!({
        "policy_id": config.policy_id,
        "entry_threshold_bps": config.entry_threshold_bps,
        "threshold_scale_ppm": config.threshold_scale_ppm,
        "fee_ppm": config.fee_ppm,
        "queue_ahead": config.queue_ahead,
        "trade_through": config.trade_through,
        "market_to_decision_ms": config.market_to_decision_ms,
        "decision_to_exchange_ms": config.decision_to_exchange_ms,
        "cancel_to_exchange_ms": config.cancel_to_exchange_ms,
        "dynamic_capital_refresh_ms": config.dynamic_capital_refresh_ms,
        "depth_snapshot_limit": config.depth_snapshot_limit,
        "duration_secs": config.duration_secs,
    });
    let parameter_bytes = serde_json::to_vec(&parameter_material)
        .map_err(|_| SimulationError::InvalidConfig("cannot encode parameter digest"))?;
    let parameter_digest = format!("sha256:{}", hex::encode(Sha256::digest(parameter_bytes)));
    let data_material = serde_json::json!({
        "symbols": config.symbols,
        "anchors": config.anchors.iter().map(|(symbol, anchor)| {
            (symbol, (anchor.close_price_ticks, anchor.observed_at_ms, anchor.valid_until_ms))
        }).collect::<BTreeMap<_, _>>(),
        "environment": config.environment.as_str(),
    });
    let data_bytes = serde_json::to_vec(&data_material)
        .map_err(|_| SimulationError::InvalidConfig("cannot encode data digest"))?;
    let data_digest = format!("sha256:{}", hex::encode(Sha256::digest(data_bytes)));
    let manifest = serde_json::json!({
        "simulation": crate::simulation::SimulationRunManifest::new(
            format!("{}-{}", config.policy_id, manifest_created_at_ms),
            "batch",
            config.policy_id.clone(),
            manifest_created_at_ms,
            config.symbols.clone(),
            config
                .specs
                .iter()
                .map(|spec| spec.variant.label().to_owned())
                .collect(),
        )
        .with_lineage(
            None,
            parameter_digest.clone(),
            data_digest.clone(),
            "isolated",
            None,
        ),
        "policy_id": config.policy_id,
        "created_at_ms": manifest_created_at_ms,
        "parameter_digest": parameter_digest,
        "data_digest": data_digest,
        "strategy_variants": config.specs.iter().map(|spec| spec.variant.label()).collect::<Vec<_>>(),
        "spec_labels": config.specs.iter().map(|spec| spec.label.as_str()).collect::<Vec<_>>(),
        "symbols": config.symbols,
        "output_root": config.output_root,
        "entry_threshold_bps": config.entry_threshold_bps,
        "threshold_scale_ppm": config.threshold_scale_ppm,
        "fee_ppm": config.fee_ppm,
        "queue_ahead": config.queue_ahead,
        "trade_through": config.trade_through,
        "market_to_decision_ms": config.market_to_decision_ms,
        "decision_to_exchange_ms": config.decision_to_exchange_ms,
        "cancel_to_exchange_ms": config.cancel_to_exchange_ms,
        "dynamic_capital_refresh_ms": config.dynamic_capital_refresh_ms,
        "depth_snapshot_limit": config.depth_snapshot_limit,
        "duration_secs": config.duration_secs,
        "evidence": config.evidence.clone(),
    });
    write_json_atomic(&config.output_root.join("run-manifest.json"), &manifest).await?;
    let shared_market_path = config.output_root.join("shared-market.jsonl");
    let shared_fx_path = config.output_root.join("shared-fx.jsonl");
    let AsyncLineWriter {
        sender: market_tx,
        task: market_writer,
        written: market_written,
        dropped: market_dropped,
    } = spawn_line_writer(Some(shared_market_path), 65_536, 1 << 20, 256).await;
    let AsyncLineWriter {
        sender: fx_record_tx,
        task: fx_writer,
        written: fx_written,
        dropped: fx_dropped,
    } = spawn_line_writer(Some(shared_fx_path), 4_096, 1 << 20, 256).await;
    let evidence_path = config.output_root.join("evidence-opportunities.jsonl");
    let AsyncLineWriter {
        sender: evidence_tx,
        task: evidence_writer,
        written: evidence_written,
        dropped: evidence_dropped,
    } = spawn_line_writer(Some(evidence_path), 16_384, 1 << 20, 256).await;
    let mut evidence = EvidenceAccumulator::new(config.evidence.clone());
    let evidence_summary_path = config.output_root.join("evidence-summary.json");
    let analytics_validation_summary = ValidationSummary::default();
    let analytics_validation_path = config.output_root.join("analytics-validation-summary.json");
    write_json_atomic(&analytics_validation_path, &analytics_validation_summary).await?;

    let mut ledgers = Vec::with_capacity(config.specs.len());
    for spec in &config.specs {
        let calibration_source = if spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
        {
            M9_CALIBRATION_SOURCE_LABEL
        } else {
            spec.label.as_str()
        };
        let calibration_seeds =
            load_calibration_seeds(&calibration_store_path, calibration_source).await;
        let dir = config.output_root.join(&spec.label);
        tokio::fs::create_dir_all(&dir).await?;
        let AsyncLineWriter {
            sender: record_tx,
            task: record_writer,
            written: record_written,
            dropped: record_dropped,
        } = spawn_line_writer(Some(dir.join("records.jsonl")), 16_384, 1 << 20, 256).await;
        ledgers.push(Ledger {
            spec: spec.clone(),
            engine: build_engine(&config, spec, &calibration_seeds)?,
            record_tx,
            record_writer,
            record_written,
            record_dropped,
            metrics_path: dir.join("metrics.json"),
            history: VecDeque::with_capacity(METRICS_HISTORY_CAPACITY),
            settlement_status: "not_started".to_owned(),
            flatten_requested: false,
        });
    }

    let fx_currencies = config
        .symbols
        .iter()
        .filter_map(|symbol| crate::strategy::profile_for(symbol).map(|p| p.anchor_currency))
        .collect::<Vec<_>>();
    let mut unique_fx = Vec::new();
    for currency in fx_currencies {
        if !unique_fx.contains(&currency) {
            unique_fx.push(currency);
        }
    }
    let fx_client = BinanceC2cFxClient::new(None)
        .map_err(|error| SimulationError::Market(format!("FX client: {error}")))?;
    let fx_poller = BinanceC2cFxPoller::new(
        fx_client,
        &unique_fx,
        FxPollerConfig {
            refresh_interval_ms: config.fx_refresh_ms,
            max_stale_ms: config.fx_max_age_ms,
            max_backoff_ms: FxPollerConfig::high_frequency().max_backoff_ms,
        },
    )
    .map_err(|error| SimulationError::Market(format!("FX poller: {error}")))?;
    let (fx_tx, mut fx_rx) = mpsc::channel::<FxUpdate>(256);
    let mut fx_task = tokio::spawn(fx_poller.run(fx_tx));
    let (anchor_tx, mut anchor_rx) = mpsc::channel(1);
    let mut anchor_task = if config.index_anchor_refresh_ms > 0 {
        let environment = config.environment;
        let symbols = config.symbols.clone();
        let refresh_ms = config.index_anchor_refresh_ms;
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(refresh_ms.max(1_000))).await;
                if let Ok(anchor_set) = load_index_anchor_set(environment, &symbols, 8, None).await
                {
                    if anchor_tx.send(anchor_set.anchors).await.is_err() {
                        break;
                    }
                }
            }
        }))
    } else {
        None
    };

    let mut shard_tasks = tokio::task::JoinSet::new();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<BinanceMarketEvent>();
    let event_dropped = Arc::new(AtomicU64::new(0));
    let endpoints = config.environment.endpoints();

    // Start buffering diff-depth before the REST snapshot, matching Binance's
    // required bootstrap order and preserving events during snapshot latency.
    let depth_configs = BinanceMarketConfig::for_symbols(
        endpoints.public_market_ws_base,
        &config.symbols,
        BinanceMarketFeed::OrderBookDepth,
        config.price_scale,
        config.quantity_scale,
        1_048_576,
        config.connect_timeout_ms,
        config.read_timeout_ms,
        None,
        ReconnectPolicy::default(),
        config.max_subscriptions_per_shard,
    )
    .map_err(|error| SimulationError::Market(error.to_string()))?;
    for stream_config in depth_configs {
        let tx = event_tx.clone();
        let dropped = Arc::clone(&event_dropped);
        shard_tasks.spawn(async move {
            BinanceMarketStream::run_forever(stream_config, |event| {
                if tx.send(event).is_err() {
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await;
        });
    }

    let depth_client = PublicMarketMetadataClient::new(endpoints.rest_base, None)
        .map_err(|error| SimulationError::Market(format!("depth snapshot client: {error}")))?;
    let mut depth_books = BTreeMap::<String, LocalOrderBook>::new();
    for symbol in &config.symbols {
        // Start immediately with an empty book. The first depth delta marks the
        // book as requiring a snapshot, which is recovered by the background
        // resync supervisor below. Startup never serializes REST snapshots.
        depth_books.insert(symbol.to_ascii_uppercase(), LocalOrderBook::default());
    }

    // A depth gap must never await REST from inside the market event handler.
    // Keep recovery on its own task so book resync cannot starve metrics, FX,
    // anchors, or the other symbols.
    let (depth_resync_request_tx, mut depth_resync_request_rx) =
        mpsc::channel::<String>(config.symbols.len().max(1) * 2);
    let (depth_resync_result_tx, mut depth_resync_result_rx) =
        mpsc::channel::<(String, Result<BinanceDepthSnapshot, String>)>(
            config.symbols.len().max(1) * 2,
        );
    let depth_snapshot_limit = config.depth_snapshot_limit;
    let depth_read_timeout_ms = config.read_timeout_ms.max(1_000);
    let depth_resync_task = tokio::spawn(async move {
        while let Some(symbol) = depth_resync_request_rx.recv().await {
            let result = match tokio::time::timeout(
                Duration::from_millis(depth_read_timeout_ms),
                depth_client.depth_snapshot(&symbol, depth_snapshot_limit),
            )
            .await
            {
                Ok(Ok(snapshot)) => Ok(snapshot),
                Ok(Err(error)) => Err(error.to_string()),
                Err(_) => Err(format!(
                    "depth snapshot timed out after {depth_read_timeout_ms}ms"
                )),
            };
            if depth_resync_result_tx.send((symbol, result)).await.is_err() {
                break;
            }
        }
    });
    let mut next_depth_resync_at_ms = BTreeMap::<String, u64>::new();
    let mut shard_configs = BinanceMarketConfig::for_symbols(
        endpoints.public_market_ws_base,
        &config.symbols,
        BinanceMarketFeed::BookTicker,
        config.price_scale,
        config.quantity_scale,
        1_048_576,
        config.connect_timeout_ms,
        config.read_timeout_ms,
        None,
        ReconnectPolicy::default(),
        config.max_subscriptions_per_shard,
    )
    .map_err(|error| SimulationError::Market(error.to_string()))?;
    shard_configs.extend(
        BinanceMarketConfig::for_symbols(
            endpoints.market_ws_base,
            &config.symbols,
            BinanceMarketFeed::ReferenceAndTrades,
            config.price_scale,
            config.quantity_scale,
            1_048_576,
            config.connect_timeout_ms,
            config.read_timeout_ms,
            None,
            ReconnectPolicy::default(),
            config.max_subscriptions_per_shard,
        )
        .map_err(|error| SimulationError::Market(error.to_string()))?,
    );
    for stream_config in shard_configs {
        let tx = event_tx.clone();
        let dropped = Arc::clone(&event_dropped);
        shard_tasks.spawn(async move {
            BinanceMarketStream::run_forever(stream_config, |event| {
                if tx.send(event).is_err() {
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await;
        });
    }
    drop(event_tx);

    let run_duration = if config.duration_secs == 0 {
        Duration::from_secs(10_000 * 365 * 24 * 60 * 60)
    } else {
        Duration::from_secs(config.duration_secs)
    };
    let mut metrics_interval =
        tokio::time::interval(Duration::from_millis(config.metrics_refresh_ms.max(250)));
    let mut last_received_at_ms = 0_u64;
    let mut event_sequence = 0_u64;
    let mut last_checkpoint_at_ms = 0_u64;
    let mut fx_latest = BTreeMap::<String, FxUpdate>::new();
    let run_result = tokio::time::timeout(run_duration, async {
        loop {
            tokio::select! {
                biased;
                _ = metrics_interval.tick() => {
                    let observed_at = now_ms();
                    enforce_storage_safety(&config.output_root, observed_at).await?;
                    if let (Some(path), Some(session_id)) =
                        (&config.checkpoint_path, config.checkpoint_session_id.as_deref())
                    {
                        if observed_at.saturating_sub(last_checkpoint_at_ms)
                            >= config.checkpoint_interval_ms.max(1_000)
                        {
                            let mut checkpoint =
                                SessionCheckpoint::new(session_id, "simulation", "PORTFOLIO");
                            for ledger in &ledgers {
                                let (
                                    event_at_ms,
                                    position_ticks,
                                    gross_position_ticks,
                                    working_order_ids,
                                    portfolio_positions,
                                ) = ledger.engine.checkpoint_view(&ledger.spec.label);
                                checkpoint.last_event_at_ms =
                                    checkpoint.last_event_at_ms.max(event_at_ms);
                                checkpoint.position_ticks =
                                    checkpoint.position_ticks.saturating_add(position_ticks);
                                checkpoint.gross_position_ticks = checkpoint
                                    .gross_position_ticks
                                    .saturating_add(gross_position_ticks);
                                checkpoint.working_order_ids.extend(working_order_ids);
                                checkpoint.portfolio_positions.extend(portfolio_positions);
                            }
                            checkpoint.risk_stopped = last_received_at_ms == 0
                                || observed_at.saturating_sub(last_received_at_ms)
                                    > config.read_timeout_ms.max(5_000);
                            checkpoint.write_atomic(path).map_err(|error| {
                                SimulationError::Io(format!("checkpoint write failed: {error}"))
                            })?;
                            last_checkpoint_at_ms = observed_at;
                        }
                    }
                    write_json_atomic(&evidence_summary_path, &evidence.summary()).await?;
                    // Copy only state learned before the next market event. M9 therefore
                    // consumes the declared F3 population without same-event lookahead.
                    synchronize_m9_calibration(&mut ledgers);
                    for ledger in &mut ledgers {
                        ledger.history.push_back(ledger.engine.performance_point(observed_at));
                        while ledger.history.len() > METRICS_HISTORY_CAPACITY {
                            ledger.history.pop_front();
                        }
                        let snapshot = ledger.engine.metrics_snapshot_with_history(observed_at, last_received_at_ms, ledger.history.make_contiguous());
                        write_json_atomic(&ledger.metrics_path, &snapshot).await?;
                    }
                    persist_calibration_store(&calibration_store_path, &ledgers).await?;
                }
                event = event_rx.recv() => {
                    let Some(event) = event else { return Err::<(), SimulationError>(SimulationError::Market("all market shards stopped".to_owned())); };
                    let received_at = now_ms();
                    last_received_at_ms = received_at;
                    if let BinanceMarketEvent::DepthUpdate(depth) = &event {
                        let symbol = depth.symbol.to_ascii_uppercase();
                        let resync = {
                            let book = depth_books.get_mut(&symbol).ok_or_else(|| {
                                SimulationError::Market(format!("depth update for unknown symbol {symbol}"))
                            })?;
                            match book.apply_diff(depth) {
                                Ok(_) => false,
                                Err(OrderBookError::SequenceGap { .. })
                                | Err(OrderBookError::SnapshotRequired) => true,
                                Err(error) => {
                                    return Err(SimulationError::Market(format!(
                                        "depth book invalid for {symbol}: {error:?}"
                                    )));
                                }
                            }
                        };
                        if resync {
                            if next_depth_resync_at_ms
                                .get(&symbol)
                                .copied()
                                .is_some_and(|deadline| received_at < deadline)
                            {
                                continue;
                            }
                            next_depth_resync_at_ms
                                .insert(symbol.clone(), received_at.saturating_add(60_000));
                            if depth_resync_request_tx.try_send(symbol.clone()).is_err() {
                                eprintln!(
                                    "depth resync queue full for {symbol}; keeping the book halted"
                                );
                            }
                            continue;
                        }
                    }
                    event_sequence = event_sequence.saturating_add(1);
                    let envelope = EventEnvelope {
                        event_id: format!("market-{event_sequence}").into(),
                        run_id: format!("batch-{}", config.policy_id).into(),
                        causality_id: format!("market-cause-{event_sequence}").into(),
                        source: EventSource::BinancePublic,
                        observed_at_ms: crate::simulation::engine::event_time_ms(&event),
                        received_at_ms: received_at,
                        sequence: event_sequence,
                        state_version: event_sequence,
                        quality: DataQuality::Trusted,
                        payload: event.clone(),
                    };
                    let mut market_value = market_event_to_json(
                        &event,
                        config.price_scale,
                        config.quantity_scale,
                        Some(received_at),
                    );
                    add_event_lineage(&mut market_value, &envelope);
                    let market_line = serde_json::to_string(&market_value)?;
                    market_tx
                        .send(market_line)
                        .await
                        .map_err(|_| SimulationError::Io("market writer stopped".to_owned()))?;
                    for evidence in evidence.observe(&event, received_at, &config.anchors) {
                        let line = serde_json::to_string(&evidence)?;
                        evidence_tx
                            .send(line)
                            .await
                            .map_err(|_| SimulationError::Io("evidence writer stopped".to_owned()))?;
                    }
                    for ledger in &mut ledgers {
                        for record in ledger.engine.on_enveloped_event(&envelope)? {
                            let line = serde_json::to_string(&record)?;
                            ledger
                                .record_tx
                                .send(line)
                                .await
                                .map_err(|_| SimulationError::Io("ledger writer stopped".to_owned()))?;
                        }
                    }
                }
                resync = depth_resync_result_rx.recv() => {
                    let Some((symbol, result)) = resync else {
                        return Err::<(), SimulationError>(SimulationError::Market(
                            "depth resync supervisor stopped".to_owned(),
                        ));
                    };
                    match result {
                        Ok(snapshot) => {
                            let (last_update_id, bids, asks) = parse_depth_snapshot(
                                &snapshot,
                                config.price_scale,
                                config.quantity_scale,
                            )?;
                            depth_books
                                .get_mut(&symbol)
                                .ok_or_else(|| {
                                    SimulationError::Market(format!(
                                        "depth resync book disappeared for {symbol}"
                                    ))
                                })?
                                .load_snapshot(last_update_id, &bids, &asks)
                                .map_err(|error| {
                                    SimulationError::Market(format!(
                                        "depth resync invalid for {symbol}: {error:?}"
                                    ))
                                })?;
                            for ledger in &mut ledgers {
                                ledger.engine.load_depth_snapshot(
                                    &symbol,
                                    last_update_id,
                                    &bids,
                                    &asks,
                                )?;
                            }
                            next_depth_resync_at_ms.remove(&symbol);
                        }
                        Err(error) => {
                            let retry_at = now_ms().saturating_add(60_000);
                            next_depth_resync_at_ms.insert(symbol.clone(), retry_at);
                            eprintln!(
                                "depth resync failed for {symbol}: {error}; retrying after {retry_at}"
                            );
                        }
                    }
                }
                anchor_update = anchor_rx.recv(), if config.index_anchor_refresh_ms > 0 => {
                    if let Some(anchors) = anchor_update {
                        let timestamp = now_ms();
                        config.anchors = anchors.clone();
                        for ledger in &mut ledgers {
                            ledger.engine.refresh_anchors(anchors.clone(), timestamp);
                        }
                    }
                }
                update = fx_rx.recv() => {
                    let Some(update) = update else { return Err::<(), SimulationError>(SimulationError::Market("FX feed stopped".to_owned())); };
                    fx_latest.insert(update.currency.clone(), update.clone());
                    let line = serde_json::to_string(&update)?;
                    fx_record_tx
                        .send(line)
                        .await
                        .map_err(|_| SimulationError::Io("FX writer stopped".to_owned()))?;
                }
                joined = shard_tasks.join_next() => {
                    match joined {
                        Some(Ok(())) => {
                            eprintln!("market shard supervisor ended unexpectedly; waiting for feed recovery");
                        }
                        Some(Err(error)) => {
                            return Err(SimulationError::Market(format!(
                                "market shard task failed: {error}"
                            )));
                        }
                        None => {
                            return Err(SimulationError::Market(
                                "all market shard supervisors stopped".to_owned(),
                            ));
                        }
                    }
                }
                fx_joined = &mut fx_task => {
                    return match fx_joined {
                        Ok(Ok(())) => Err(SimulationError::Market("FX feed stopped".to_owned())),
                        Ok(Err(error)) => Err(SimulationError::Market(format!("FX feed failed: {error}"))),
                        Err(error) => Err(SimulationError::Market(format!("FX task failed: {error}"))),
                    };
                }
            }
        }
    }).await;
    let run_error = match run_result {
        Ok(Err(error)) => Some(error),
        Err(_) if config.duration_secs == 0 => Some(SimulationError::Market(
            "continuous batch execution timeout".to_owned(),
        )),
        _ => None,
    };

    shard_tasks.abort_all();
    while shard_tasks.join_next().await.is_some() {}
    depth_resync_task.abort();
    let _ = depth_resync_task.await;
    fx_task.abort();
    let _ = fx_task.await;
    if let Some(anchor_task) = anchor_task.take() {
        anchor_task.abort();
        let _ = anchor_task.await;
    }
    for ledger in &mut ledgers {
        let settlement = ledger.engine.shutdown(now_ms(), "batch execution stopped");
        ledger.settlement_status = settlement.settlement_status.clone();
        ledger.flatten_requested = settlement.flatten_requested;
        for record in settlement.records {
            let line = serde_json::to_string(&record)?;
            ledger
                .record_tx
                .send(line)
                .await
                .map_err(|_| SimulationError::Io("ledger writer stopped".to_owned()))?;
        }
        let observed_at = now_ms();
        ledger
            .history
            .push_back(ledger.engine.performance_point(observed_at));
        while ledger.history.len() > METRICS_HISTORY_CAPACITY {
            ledger.history.pop_front();
        }
        let snapshot = ledger.engine.metrics_snapshot_with_history(
            observed_at,
            last_received_at_ms,
            ledger.history.make_contiguous(),
        );
        write_json_atomic(&ledger.metrics_path, &snapshot).await?;
    }
    write_json_atomic(&evidence_summary_path, &evidence.summary()).await?;
    drop(market_tx);
    drop(fx_record_tx);
    drop(evidence_tx);
    let market_count = market_writer
        .await
        .map_err(|e| SimulationError::Io(e.to_string()))??;
    let fx_count = fx_writer
        .await
        .map_err(|e| SimulationError::Io(e.to_string()))??;
    let evidence_count = evidence_writer
        .await
        .map_err(|e| SimulationError::Io(e.to_string()))??;
    let evidence_summary = evidence.summary();
    let mut ledger_results = Vec::with_capacity(ledgers.len());
    for ledger in ledgers {
        drop(ledger.record_tx);
        let count = ledger
            .record_writer
            .await
            .map_err(|e| SimulationError::Io(e.to_string()))??;
        ledger_results.push(SimulationLedgerResult {
            label: ledger.spec.label,
            strategy_variant: ledger.spec.variant.label().to_owned(),
            evidence_record_id: evidence.evidence_id(),
            summary: ledger.engine.summary(),
            settlement_status: ledger.settlement_status,
            flatten_requested: ledger.flatten_requested,
            records_written: count.max(ledger.record_written.load(Ordering::Relaxed)),
            records_dropped: ledger.record_dropped.load(Ordering::Relaxed),
        });
    }
    if let Some(error) = run_error {
        return Err(error);
    }
    if event_dropped.load(Ordering::Relaxed) != 0
        || market_dropped.load(Ordering::Relaxed) != 0
        || fx_dropped.load(Ordering::Relaxed) != 0
        || evidence_dropped.load(Ordering::Relaxed) != 0
    {
        return Err(SimulationError::Market(
            "batch execution dropped shared feed records".to_owned(),
        ));
    }
    let promotion_input = SimulationPromotionInput {
        ledger_count: ledger_results.len() as u64,
        orders: ledger_results
            .iter()
            .map(|ledger| ledger.summary.order_count)
            .sum(),
        fills: ledger_results
            .iter()
            .map(|ledger| ledger.summary.fill_count)
            .sum(),
        records_dropped: ledger_results
            .iter()
            .map(|ledger| ledger.records_dropped)
            .sum(),
        valuation_incomplete_ledgers: ledger_results
            .iter()
            .filter(|ledger| !ledger.summary.unrealized_valuation_complete)
            .count() as u64,
        non_flat_ledgers: ledger_results
            .iter()
            .filter(|ledger| !ledger.summary.flat_at_end)
            .count() as u64,
        total_net_pnl_ticks: ledger_results
            .iter()
            .map(|ledger| ledger.summary.net_pnl_ticks)
            .sum(),
    };
    let promotion_gate = evaluate_simulation_promotion(promotion_input);
    write_json_atomic(
        &config.output_root.join("promotion-gate.json"),
        &promotion_gate,
    )
    .await?;
    Ok(SimulationBatchResult {
        shared_market_records_written: market_count.max(market_written.load(Ordering::Relaxed)),
        shared_market_records_dropped: market_dropped.load(Ordering::Relaxed),
        shared_fx_records_written: fx_count.max(fx_written.load(Ordering::Relaxed)),
        shared_fx_records_dropped: fx_dropped.load(Ordering::Relaxed),
        evidence_summary,
        analytics_validation_summary,
        promotion_gate,
        evidence_records_written: evidence_count.max(evidence_written.load(Ordering::Relaxed)),
        evidence_records_dropped: evidence_dropped.load(Ordering::Relaxed),
        ledgers: ledger_results,
    })
}

#[cfg(test)]
mod tests {
    use super::calibration_key;

    #[test]
    fn calibration_store_keys_are_strategy_scoped() {
        assert_eq!(calibration_key("F3_m3", "CXMTUSDT"), "F3_m3::CXMTUSDT");
        assert_ne!(
            calibration_key("F3_m3", "CXMTUSDT"),
            calibration_key("F4_m4", "CXMTUSDT")
        );
    }
}
