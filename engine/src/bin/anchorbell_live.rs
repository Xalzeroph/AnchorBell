use std::{
    collections::BTreeMap,
    env,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anchorbell_engine::{
    execution::{
        binance_runtime_config, BinanceCredentials, BinanceEmergencyReduceOnlyTakerRequest,
        BinanceEnvironment, BinanceMakerOrderRequest, BinanceRestClient, BinanceUserDataStream,
        DeploymentConfig, ExecutionSupervisor, GateDecision, SessionCheckpoint, Side,
        SupervisorConfig, SupervisorState, UserDataEvent,
    },
    market::{
        binance::{BinanceMarketEvent, BookTicker, MarkPrice},
        quote_event, AssetClass, BinanceC2cFxClient, BinanceC2cFxPoller, BinanceMarketConfig,
        BinanceMarketFeed, BinanceMarketStream, BinanceScaledExecutionFilters, FxPollerConfig,
        FxUpdate, InstrumentRegistryConfig, MarketTruthState, PublicMarketMetadataClient,
    },
    runtime::load_index_anchor_set,
    runtime::{
        AuditSink, DataQuality, EventEnvelope, EventSource, RunMode, RunRegistry, RunSpec,
        RunStatus, RuntimeControlPlane, RUN_REGISTRY_SCHEMA_VERSION,
    },
    simulation::{AnchorSnapshot, SimulationEngine, SimulationPolicyVariant, SimulationRecord},
    strategy::{
        adaptive_intent_from_market, calendar_for, profile_for, AnchorCurrency, EquityRegion,
        StrategyProfile, VenueSessionState,
    },
};

#[derive(Debug)]
struct Args {
    environment: BinanceEnvironment,
    duration_secs: u64,
    proxy: Option<String>,
    price_scale: u32,
    quantity_scale: u32,
    max_position: i64,
    quantity: i64,
    entry_threshold_bps: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    funding_lead_ms: u64,
    max_subscriptions_per_shard: usize,
    send_orders: bool,
}

#[derive(Debug)]
enum Event {
    Market(BinanceMarketEvent),
    Fx(FxUpdate),
    User(UserDataEvent),
    RecoveryRequired(String),
    Halt(String),
}

#[derive(Debug, Default)]
struct SymbolState {
    book: Option<BookTicker>,
    mark: Option<MarkPrice>,
    position_ticks: i64,
    last_mark_price_ticks: Option<i64>,
    ewma_abs_return_bps: i64,
    unrealized_profit: String,
}

#[derive(Debug, Clone)]
struct WorkingOrder {
    client_order_id: String,
    side: Side,
    price_ticks: i64,
    quantity_ticks: i64,
}

fn validate_live_instruments(profile: &StrategyProfile, symbols: &[String]) -> Result<(), String> {
    let registry = InstrumentRegistryConfig::embedded()
        .map_err(|error| format!("cannot load instrument registry: {error}"))?;
    let classifications = registry.by_symbol();
    for symbol in symbols {
        let normalized = symbol.trim().to_ascii_uppercase();
        let classification = classifications.get(&normalized).ok_or_else(|| {
            format!("live symbol {normalized} is missing external classification")
        })?;
        if classification.asset_class != profile.asset_class {
            return Err(format!(
                "live asset class mismatch for {normalized}: profile={:?}, symbol={:?}",
                profile.asset_class, classification.asset_class
            ));
        }
        if classification.asset_class == AssetClass::Unknown {
            return Err(format!("live symbol {normalized} has unknown asset class"));
        }
        if !classification.live_enabled {
            return Err(format!(
                "live symbol {normalized} is not enabled by the instrument registry"
            ));
        }
    }
    Ok(())
}

const SHADOW_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, serde::Serialize)]
struct ShadowRecordLine<'a> {
    schema_version: u32,
    live_market_event_sequence: u64,
    live_run_id: &'a str,
    record: &'a SimulationRecord,
}

#[derive(Debug, Default, serde::Serialize)]
struct ShadowDivergenceSummary {
    live_decision_count: u64,
    shadow_decision_count: u64,
    live_order_count: u64,
    shadow_order_count: u64,
    live_fill_count: u64,
    shadow_fill_count: u64,
    live_cancel_count: u64,
    shadow_cancel_count: u64,
    fill_latency_samples: u64,
    fill_latency_total_ms: u64,
    fill_latency_max_ms: u64,
    position_divergence_samples: u64,
    max_abs_position_divergence_ticks: i64,
}

struct ShadowSimulation {
    engine: SimulationEngine,
    records: BufWriter<File>,
    live_observations: BufWriter<File>,
    summary_path: PathBuf,
    live_market_event_sequence: u64,
    divergence: ShadowDivergenceSummary,
    live_order_submitted_at_ms: BTreeMap<String, u64>,
}

impl ShadowSimulation {
    #[allow(clippy::too_many_arguments)]
    fn new(
        run_id: &str,
        shadow_dir: &Path,
        anchors: BTreeMap<String, AnchorSnapshot>,
        args: &Args,
        profile: &StrategyProfile,
        execution_filters: &BTreeMap<String, BinanceScaledExecutionFilters>,
        funding_intervals: &BTreeMap<String, u32>,
        maker_fee_ppm: i64,
        taker_fee_ppm: i64,
    ) -> Result<Self, String> {
        fs::create_dir_all(shadow_dir)
            .map_err(|error| format!("shadow simulation directory failed: {error}"))?;
        let mut emergency_execution = profile.emergency_execution;
        emergency_execution.taker_fee_ppm = taker_fee_ppm;
        let engine = SimulationEngine::new(
            anchors,
            args.entry_threshold_bps,
            args.max_position,
            args.quantity,
            args.max_mark_index_gap_bps,
            args.max_anchor_age_ms,
            maker_fee_ppm,
            args.quantity_scale,
            emergency_execution,
        )
        .map_err(|error| format!("shadow simulation config rejected: {error}"))?
        .with_live_risk_gates()
        .with_strategy_variant(SimulationPolicyVariant::CoreV1)
        .with_fee_schedule_source(profile.fee_schedule.source.clone())
        .with_price_scale(args.price_scale)
        .with_execution_filters(execution_filters.clone())
        .with_funding_intervals(funding_intervals.clone());
        let manifest = serde_json::json!({
            "schema_version": SHADOW_SCHEMA_VERSION,
            "run_id": run_id,
            "mode": "live_shadow_simulation",
            "strategy_variant": SimulationPolicyVariant::CoreV1.label(),
            "market_event_source": "live_binance_public",
            "execution": "simulation_only",
            "fee_ppm": maker_fee_ppm,
            "taker_fee_ppm": taker_fee_ppm,
            "fee_source": "binance_commission_rate",
            "fee_schedule": profile.fee_schedule,
            "entry_threshold_bps": args.entry_threshold_bps,
            "max_position": args.max_position,
            "requested_quantity": args.quantity,
            "max_mark_index_gap_bps": args.max_mark_index_gap_bps,
            "max_anchor_age_ms": args.max_anchor_age_ms,
            "price_scale": args.price_scale,
            "quantity_scale": args.quantity_scale,
        });
        fs::write(
            shadow_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)
                .map_err(|error| format!("shadow manifest encode failed: {error}"))?,
        )
        .map_err(|error| format!("shadow manifest write failed: {error}"))?;
        let records = OpenOptions::new()
            .create(true)
            .append(true)
            .open(shadow_dir.join("simulation-records.jsonl"))
            .map_err(|error| format!("shadow records open failed: {error}"))?;
        let observations = OpenOptions::new()
            .create(true)
            .append(true)
            .open(shadow_dir.join("live-observations.jsonl"))
            .map_err(|error| format!("shadow observations open failed: {error}"))?;
        Ok(Self {
            engine,
            records: BufWriter::new(records),
            live_observations: BufWriter::new(observations),
            summary_path: shadow_dir.join("summary.json"),
            live_market_event_sequence: 0,
            divergence: ShadowDivergenceSummary::default(),
            live_order_submitted_at_ms: BTreeMap::new(),
        })
    }

    fn on_market(
        &mut self,
        run_id: &str,
        event: BinanceMarketEvent,
        observed_at_ms: u64,
        received_at_ms: u64,
    ) -> Result<u64, String> {
        self.live_market_event_sequence = self.live_market_event_sequence.saturating_add(1);
        let sequence = self.live_market_event_sequence;
        let envelope = EventEnvelope {
            event_id: format!("{run_id}-market-{sequence}").into(),
            run_id: run_id.to_owned().into(),
            causality_id: format!("{run_id}-market-stream").into(),
            source: EventSource::BinancePublic,
            observed_at_ms,
            received_at_ms: received_at_ms.max(observed_at_ms),
            sequence,
            state_version: sequence,
            quality: DataQuality::Trusted,
            payload: event,
        };
        let records = self
            .engine
            .on_enveloped_event(&envelope)
            .map_err(|error| format!("shadow market event rejected at {sequence}: {error}"))?;
        for record in &records {
            match record.kind.as_str() {
                "decision" => self.divergence.shadow_decision_count += 1,
                "order_placed" => self.divergence.shadow_order_count += 1,
                "fill" => self.divergence.shadow_fill_count += 1,
                "order_canceled" => self.divergence.shadow_cancel_count += 1,
                _ => {}
            }
            let line = ShadowRecordLine {
                schema_version: SHADOW_SCHEMA_VERSION,
                live_market_event_sequence: sequence,
                live_run_id: run_id,
                record,
            };
            serde_json::to_writer(&mut self.records, &line)
                .map_err(|error| format!("shadow record encode failed: {error}"))?;
            self.records
                .write_all(b"\n")
                .map_err(|error| format!("shadow record write failed: {error}"))?;
        }
        if !records.is_empty() {
            self.records
                .flush()
                .map_err(|error| format!("shadow record flush failed: {error}"))?;
        }
        Ok(sequence)
    }

    fn live_observation(&mut self, value: serde_json::Value) -> Result<(), String> {
        serde_json::to_writer(&mut self.live_observations, &value)
            .map_err(|error| format!("live observation encode failed: {error}"))?;
        self.live_observations
            .write_all(b"\n")
            .map_err(|error| format!("live observation write failed: {error}"))?;
        self.live_observations
            .flush()
            .map_err(|error| format!("live observation flush failed: {error}"))?;
        Ok(())
    }

    fn live_decision(&mut self, value: serde_json::Value) -> Result<(), String> {
        self.divergence.live_decision_count = self.divergence.live_decision_count.saturating_add(1);
        self.live_observation(value)
    }

    fn live_order_submitted(&mut self, client_order_id: &str, submitted_at_ms: u64) {
        self.divergence.live_order_count = self.divergence.live_order_count.saturating_add(1);
        self.live_order_submitted_at_ms
            .insert(client_order_id.to_owned(), submitted_at_ms);
    }

    fn live_user_event(
        &mut self,
        value: &UserDataEvent,
        live_position_ticks: i64,
        received_at_ms: u64,
    ) -> Result<(), String> {
        if let UserDataEvent::OrderUpdate(order) = value {
            if order.execution_type == "TRADE" {
                self.divergence.live_fill_count = self.divergence.live_fill_count.saturating_add(1);
                if let Some(submitted_at_ms) =
                    self.live_order_submitted_at_ms.get(&order.client_order_id)
                {
                    let latency_ms = received_at_ms.saturating_sub(*submitted_at_ms);
                    self.divergence.fill_latency_samples =
                        self.divergence.fill_latency_samples.saturating_add(1);
                    self.divergence.fill_latency_total_ms = self
                        .divergence
                        .fill_latency_total_ms
                        .saturating_add(latency_ms);
                    self.divergence.fill_latency_max_ms =
                        self.divergence.fill_latency_max_ms.max(latency_ms);
                }
            }
            if matches!(order.status.as_str(), "CANCELED" | "EXPIRED" | "REJECTED") {
                self.divergence.live_cancel_count =
                    self.divergence.live_cancel_count.saturating_add(1);
            }
            if matches!(
                order.status.as_str(),
                "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
            ) {
                self.live_order_submitted_at_ms
                    .remove(&order.client_order_id);
            }
        }
        let (_, shadow_position_ticks, _, _, _) = self.engine.checkpoint_view("shadow");
        let delta = live_position_ticks.saturating_sub(shadow_position_ticks);
        let abs_delta = if delta < 0 {
            delta.saturating_neg()
        } else {
            delta
        };
        self.divergence.position_divergence_samples = self
            .divergence
            .position_divergence_samples
            .saturating_add(1);
        self.divergence.max_abs_position_divergence_ticks = self
            .divergence
            .max_abs_position_divergence_ticks
            .max(abs_delta);
        self.live_observation(serde_json::json!({
            "kind": "live_user_event",
            "market_event_sequence": self.live_market_event_sequence,
            "received_at_ms": received_at_ms,
            "payload": value,
        }))
    }
    fn finish(mut self, run_id: &str, timestamp_ms: u64) -> Result<(), String> {
        let records = self.engine.cancel_all(timestamp_ms, "live_shadow_shutdown");
        let sequence = self.live_market_event_sequence;
        for record in &records {
            match record.kind.as_str() {
                "decision" => self.divergence.shadow_decision_count += 1,
                "order_placed" => self.divergence.shadow_order_count += 1,
                "fill" => self.divergence.shadow_fill_count += 1,
                "order_canceled" => self.divergence.shadow_cancel_count += 1,
                _ => {}
            }
            let line = ShadowRecordLine {
                schema_version: SHADOW_SCHEMA_VERSION,
                live_market_event_sequence: sequence,
                live_run_id: run_id,
                record,
            };
            serde_json::to_writer(&mut self.records, &line)
                .map_err(|error| format!("shadow final record encode failed: {error}"))?;
            self.records
                .write_all(b"\n")
                .map_err(|error| format!("shadow final record write failed: {error}"))?;
        }
        self.records
            .flush()
            .map_err(|error| format!("shadow final record flush failed: {error}"))?;
        self.live_observations
            .flush()
            .map_err(|error| format!("live observation final flush failed: {error}"))?;
        let (last_event_at_ms, position_ticks, gross_position_ticks, working_order_ids, positions) =
            self.engine.checkpoint_view("shadow");
        let summary = serde_json::json!({
            "schema_version": SHADOW_SCHEMA_VERSION,
            "run_id": run_id,
            "completed_at_ms": timestamp_ms,
            "last_event_at_ms": last_event_at_ms,
            "position_ticks": position_ticks,
            "gross_position_ticks": gross_position_ticks,
            "working_order_ids": working_order_ids,
            "portfolio_positions": positions,
            "divergence": self.divergence,
            "summary": self.engine.summary(),
        });
        fs::write(
            &self.summary_path,
            serde_json::to_vec_pretty(&summary)
                .map_err(|error| format!("shadow summary encode failed: {error}"))?,
        )
        .map_err(|error| format!("shadow summary write failed: {error}"))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(value) => value,
        Err(error) => fail(&error),
    };
    match run(args).await {
        Ok(code) => process::exit(code),
        Err(error) => {
            eprintln!("live runner failed: {error}");
            process::exit(1);
        }
    }
}

async fn load_execution_filters(
    environment: BinanceEnvironment,
    symbols: &[String],
    price_scale: u32,
    quantity_scale: u32,
    proxy: Option<&str>,
) -> Result<BTreeMap<String, BinanceScaledExecutionFilters>, String> {
    let endpoints = environment.endpoints();
    let metadata_client = PublicMarketMetadataClient::new(endpoints.rest_base.as_str(), proxy)
        .map_err(|error| format!("Binance metadata client rejected: {error}"))?;
    let metadata = metadata_client
        .exchange_info()
        .await
        .map_err(|error| format!("Binance exchangeInfo unavailable: {error}"))?;
    let mut by_symbol = BTreeMap::new();
    for item in metadata {
        by_symbol.insert(item.symbol.to_ascii_uppercase(), item);
    }
    let mut result = BTreeMap::new();
    for symbol in symbols {
        let normalized = symbol.to_ascii_uppercase();
        let item = by_symbol
            .get(&normalized)
            .ok_or_else(|| format!("exchangeInfo missing configured live symbol {normalized}"))?;
        if item.status != "TRADING" || item.contract_type != "TRADIFI_PERPETUAL" {
            return Err(format!(
                "configured live symbol {normalized} is not an active TradFi perpetual: status={}, contract_type={}",
                item.status, item.contract_type
            ));
        }
        let filters = item
            .execution_filters()
            .map_err(|error| format!("Binance filters rejected for {normalized}: {error}"))?
            .scaled(price_scale, quantity_scale)
            .map_err(|error| {
                format!("Binance filter scaling rejected for {normalized}: {error}")
            })?;
        result.insert(normalized, filters);
    }
    Ok(result)
}

fn parse_commission_ppm(value: &str) -> Option<i64> {
    let value = value.trim();
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || whole.parse::<u64>().ok()? > 1
        || fraction.len() > 6
    {
        return None;
    }
    let whole_ppm = whole.parse::<i64>().ok()?.checked_mul(1_000_000)?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i64>()
            .ok()?
            .checked_mul(10_i64.checked_pow((6 - fraction.len()) as u32)?)?
    };
    whole_ppm.checked_add(fraction_value)
}

async fn load_commission_rates(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbols: &[String],
) -> Result<(i64, i64), String> {
    let timestamp = client
        .server_time_ms()
        .await
        .map_err(|error| error.to_string())?;
    let mut observed = None;
    for symbol in symbols {
        let rate = client
            .commission_rate(
                credentials,
                symbol,
                timestamp,
                binance_runtime_config().operational.default_recv_window_ms,
            )
            .await
            .map_err(|error| format!("commissionRate unavailable for {symbol}: {error}"))?;
        let maker = parse_commission_ppm(&rate.maker_commission_rate)
            .ok_or_else(|| format!("invalid maker commission for {symbol}"))?;
        let taker = parse_commission_ppm(&rate.taker_commission_rate)
            .ok_or_else(|| format!("invalid taker commission for {symbol}"))?;
        match observed {
            None => observed = Some((maker, taker)),
            Some((expected_maker, expected_taker))
                if expected_maker == maker && expected_taker == taker => {}
            Some((expected_maker, expected_taker)) => {
                return Err(format!(
                    "per-symbol commission differs: {symbol} maker={maker} taker={taker}, expected maker={expected_maker} taker={expected_taker}"
                ));
            }
        }
    }
    observed.ok_or_else(|| "commissionRate returned no symbols".to_owned())
}

async fn load_funding_intervals(
    environment: BinanceEnvironment,
    symbols: &[String],
    proxy: Option<&str>,
) -> Result<BTreeMap<String, u32>, String> {
    let client = PublicMarketMetadataClient::new(environment.endpoints().rest_base.as_str(), proxy)
        .map_err(|error| format!("funding metadata client construction failed: {error}"))?;
    let mut result = BTreeMap::new();
    for symbol in symbols {
        let normalized = symbol.trim().to_ascii_uppercase();
        let rows = client
            .funding_info(Some(&normalized))
            .await
            .map_err(|error| format!("fundingInfo unavailable for {normalized}: {error}"))?;
        let row = rows
            .into_iter()
            .find(|value| value.symbol.eq_ignore_ascii_case(&normalized))
            .ok_or_else(|| format!("fundingInfo missing configured symbol {normalized}"))?;
        if row.funding_interval_hours == 0 {
            return Err(format!(
                "fundingInfo returned zero interval for {normalized}"
            ));
        }
        row.validate()
            .map_err(|error| format!("invalid fundingInfo for {normalized}: {error}"))?;
        let history = client
            .funding_rate_history(&normalized, 100)
            .await
            .map_err(|error| {
                format!("fundingRate history unavailable for {normalized}: {error}")
            })?;
        if history.is_empty()
            || history.iter().any(|item| {
                item.symbol != normalized
                    || item.funding_time_ms == 0
                    || item.funding_rate.trim().is_empty()
                    || !matches!(item.rate_type.as_str(), "Regular" | "Special")
            })
        {
            return Err(format!(
                "fundingRate history is incomplete for {normalized}"
            ));
        }
        result.insert(normalized, row.funding_interval_hours);
    }
    Ok(result)
}

async fn run(args: Args) -> Result<i32, String> {
    let deployment = DeploymentConfig::from_process_environment()
        .map_err(|error| format!("deployment config rejected: {error:?}"))?;
    if deployment.environment != args.environment {
        return Err(format!(
            "--environment {} does not match ANCHORBELL_BINANCE_ENV={}",
            args.environment, deployment.environment
        ));
    }
    let credentials = load_credentials(args.environment)?;
    let policy = deployment.policy(true);
    if args.send_orders && !policy.allow_live_orders {
        return Err("order submission is disabled by deployment policy".into());
    }
    let strategy_profile = StrategyProfile::load("config/anchorbell-simulation.json")?;
    let symbols = strategy_profile.symbols.clone();
    validate_live_instruments(&strategy_profile, &symbols)?;
    let client = Arc::new(
        BinanceRestClient::new(args.environment, policy, args.proxy.as_deref())
            .map_err(|error| error.to_string())?,
    );
    let execution_filters = load_execution_filters(
        args.environment,
        &symbols,
        args.price_scale,
        args.quantity_scale,
        args.proxy.as_deref(),
    )
    .await?;
    let funding_intervals =
        load_funding_intervals(args.environment, &symbols, args.proxy.as_deref()).await?;
    let (maker_fee_ppm, taker_fee_ppm) =
        load_commission_rates(&client, &credentials, &symbols).await?;
    let position_mode = client
        .position_mode(
            &credentials,
            client.server_time_ms().await.map_err(|e| e.to_string())?,
            binance_runtime_config().operational.default_recv_window_ms,
        )
        .await
        .map_err(|error| format!("position mode unavailable: {error}"))?;
    if position_mode.dual_side_position {
        return Err(
            "Binance account is in Hedge Mode; AnchorBell requires one-way mode before live use"
                .to_owned(),
        );
    }
    let server_time = client.server_time_ms().await.map_err(|e| e.to_string())?;
    if args.send_orders {
        let position_mode = client
            .position_mode(
                &credentials,
                server_time,
                binance_runtime_config().operational.default_recv_window_ms,
            )
            .await
            .map_err(|e| format!("Binance position mode preflight failed: {e}"))?;
        if position_mode.dual_side_position {
            return Err(
                "Hedge Mode is not supported by this order adapter; refusing live orders".into(),
            );
        }
        for symbol in &symbols {
            client
                .commission_rate(
                    &credentials,
                    symbol,
                    server_time,
                    binance_runtime_config().operational.default_recv_window_ms,
                )
                .await
                .map_err(|e| {
                    format!("Binance account commission preflight failed for {symbol}: {e}")
                })?;
        }
    }
    let run_id = format!("live-{}-{}", args.environment.as_str(), now_ms());
    let registry = RunRegistry::new("target/live-runs");
    registry
        .create(
            RunSpec {
                schema_version: RUN_REGISTRY_SCHEMA_VERSION,
                run_id: run_id.clone(),
                mode: RunMode::Live,
                policy_id: strategy_profile.policy_id.clone(),
                capital_currency: "USDT".into(),
                capital_minor_units: args.max_position,
                universe: strategy_profile.universe_id.clone(),
                strategies: vec![strategy_profile.default_strategy_variant.clone()],
                ablations: Vec::new(),
                checkpoint_interval_ms: strategy_profile.checkpoint_interval_ms,
                max_stale_ms: strategy_profile.max_stale_ms,
                auto_restart: true,
                build_identity: env!("CARGO_PKG_VERSION").into(),
            },
            now_ms(),
        )
        .map_err(|error| format!("live run registry create failed: {error}"))?;
    registry
        .claim(
            &run_id,
            format!("live-{}-{}", std::process::id(), args.environment.as_str()),
            now_ms(),
        )
        .map_err(|error| format!("live run registry claim failed: {error}"))?;
    let checkpoint_path = PathBuf::from("target/live-runs")
        .join(&run_id)
        .join("checkpoint.json");
    SessionCheckpoint::new(&run_id, "live", "PORTFOLIO")
        .write_atomic(&checkpoint_path)
        .map_err(|error| format!("live initial checkpoint failed: {error}"))?;
    registry
        .checkpoint(&run_id, checkpoint_path.display().to_string(), now_ms())
        .map_err(|error| format!("live checkpoint registration failed: {error}"))?;
    let anchors = load_index_anchor_set(
        args.environment,
        &symbols,
        args.price_scale,
        &strategy_profile.anchor_kline_interval,
        strategy_profile.anchor_kline_lookback_ms,
        strategy_profile.anchor_kline_limit,
        args.proxy.as_deref(),
    )
    .await
    .map_err(|error| format!("cannot load Binance index anchors: {error}"))?
    .anchors;
    if anchors.len() != symbols.len() {
        return Err(format!(
            "anchor set does not exactly cover configured universe: anchors={}, symbols={}",
            anchors.len(),
            symbols.len()
        ));
    }
    let shadow_dir = checkpoint_path
        .parent()
        .ok_or_else(|| "live checkpoint has no parent directory".to_owned())?
        .join("shadow-simulation");
    let mut shadow = ShadowSimulation::new(
        &run_id,
        &shadow_dir,
        anchors.clone(),
        &args,
        &strategy_profile,
        &execution_filters,
        &funding_intervals,
        maker_fee_ppm,
        taker_fee_ppm,
    )?;
    println!(
        "{}",
        serde_json::json!({
            "event": "live_shadow_simulation_started",
            "run_id": run_id,
            "path": shadow_dir,
            "strategy_variant": SimulationPolicyVariant::CoreV1.label(),
            "execution": "simulation_only",
        })
    );

    let mut control_plane = RuntimeControlPlane::new();
    let mut audit_sink = AuditSink::from_environment("target/runtime-audit.jsonl");
    let mut supervisor = ExecutionSupervisor::new(
        SupervisorConfig {
            max_market_age_ms: strategy_profile.max_stale_ms,
            max_fx_age_ms: strategy_profile.fx_max_age_ms,
            funding_lead_ms: args.funding_lead_ms,
            max_position: args.max_position,
            quantity_scale: args.quantity_scale,
        },
        symbols.clone(),
    )
    .map_err(|reason| format!("supervisor config rejected: {reason:?}"))?;
    let mut state = BTreeMap::<String, SymbolState>::new();
    let mut recovered_working = BTreeMap::<String, WorkingOrder>::new();
    let remote_states = reconcile_account(
        &client,
        &credentials,
        &symbols,
        args.price_scale,
        args.quantity_scale,
    )
    .await?;
    for symbol in &symbols {
        let remote = remote_states
            .get(symbol)
            .ok_or_else(|| format!("authoritative snapshot missing {symbol}"))?;
        state.insert(
            symbol.clone(),
            SymbolState {
                position_ticks: remote.position_ticks,
                unrealized_profit: remote.unrealized_profit.clone(),
                ..Default::default()
            },
        );
        if remote.working_orders.len() > 1 {
            return Err(format!("multiple managed open orders on {symbol}"));
        }
        if let Some(order) = remote.working_orders.first().cloned() {
            recovered_working.insert(symbol.clone(), order);
        }
    }
    control_plane
        .bootstrap_ready(now_ms())
        .map_err(|error| format!("live control plane bootstrap rejected: {error}"))?;
    emit_health_transitions(&mut control_plane, &mut audit_sink).await?;
    registry
        .transition(&run_id, RunStatus::Running, now_ms())
        .map_err(|error| format!("live run registry running failed: {error}"))?;
    registry
        .heartbeat(&run_id, now_ms())
        .map_err(|error| format!("live run registry heartbeat failed: {error}"))?;
    let _heartbeat =
        registry.spawn_heartbeat(run_id.clone(), strategy_profile.run_registry_heartbeat_ms);
    let mut truth = BTreeMap::<String, MarketTruthState>::new();
    let mut truth_sequence = 0_u64;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(16_384);
    let market_overflow = spawn_market(&args, tx.clone(), &symbols, &strategy_profile)?;
    spawn_fx(&args, tx.clone())?;
    let user_overflow =
        spawn_user_data(&args, client.clone(), credentials.clone(), tx.clone()).await?;

    println!(
        "{}",
        serde_json::json!({
            "event":"live_canary_started",
            "environment":args.environment.as_str(),
            "server_time_ms":server_time,
            "symbols":symbols,
            "order_submission":args.send_orders,
            "simulation_orders":false,
        })
    );

    let mut fx_at = BTreeMap::<String, u64>::new();
    let deadline = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(deadline);
    let mut supervisor_ready = false;
    let mut working = recovered_working;
    let mut order_sequence = 0_u64;
    let mut last_shadow_event_sequence = 0_u64;
    let mut last_user_event_time_ms = 0_u64;
    let mut last_gate_blockers = Vec::<String>::new();
    let mut last_checkpoint_at_ms = 0_u64;
    let mut health_tick = tokio::time::interval(Duration::from_millis(250));
    health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = health_tick.tick() => {
                if market_overflow.load(Ordering::Acquire) {
                    supervisor.on_disconnect();
                    return Err(
                        "market event queue overflowed or market producer stopped; risk stopped"
                            .into(),
                    );
                }
                if user_overflow.load(Ordering::Acquire) {
                    supervisor.on_disconnect();
                    return Err(
                        "user data event queue overflowed; risk stopped"
                            .into(),
                    );
                }
                let now = now_ms();
                let readiness = control_plane.readiness(now);
                emit_health_transitions(&mut control_plane, &mut audit_sink).await?;
                let execution_ready = readiness.ready;
                if !execution_ready {
                    if readiness.blockers != last_gate_blockers {
                        println!("{}", serde_json::json!({
                            "event": "system_gate_blocked",
                            "system": readiness.system_id,
                            "blockers": readiness.blockers,
                        }));
                        last_gate_blockers = readiness.blockers.clone();
                    }
                    if supervisor.state() == SupervisorState::Healthy {
                        supervisor.on_disconnect();
                        supervisor_ready = false;
                    }
                    if args.send_orders {
                        for symbol in &symbols {
                            if let Some(order) = working.remove(symbol) {
                                cancel_order(&client, &credentials, symbol, &order.client_order_id).await?;
                            }
                        }
                    }
                } else {
                    last_gate_blockers.clear();
                }
                if now.saturating_sub(last_checkpoint_at_ms) >= strategy_profile.checkpoint_interval_ms {
                    persist_live_checkpoint(
                        &checkpoint_path,
                        &run_id,
                        &state,
                        &working,
                        supervisor.state() != SupervisorState::Healthy,
                    )?;
                    registry
                        .checkpoint(&run_id, checkpoint_path.display().to_string(), now)
                        .map_err(|error| format!("live checkpoint registration failed: {error}"))?;
                    last_checkpoint_at_ms = now;
                }
            }
            event = rx.recv() => {
                let Some(event) = event else {
                    supervisor.on_disconnect();
                    return Err("all event producers stopped; risk stopped".into());
                };
                match event {
                    Event::Market(value) => {
                        let received_at_ms = now_ms();
                        last_shadow_event_sequence = shadow.on_market(
                            &run_id,
                            value.clone(),
                            market_event_time_ms(&value),
                            received_at_ms,
                        )?;
                        let symbol = market_event_symbol(&value).to_owned();
                        if !state.contains_key(&symbol) {
                            return Err(format!("market event outside declared universe: {symbol}"));
                        }
                        apply_market(&mut state, value);
                        if let Some(local) = state.get(&symbol) {
                            if let (Some(book), Some(mark)) = (local.book.as_ref(), local.mark.as_ref()) {
                                truth_sequence = truth_sequence.saturating_add(1);
                                let observed_at = book.event_time_ms.max(mark.event_time_ms);
                                let normalized = quote_event(
                                    run_id.clone(),
                                    symbol.clone(),
                                    truth_sequence,
                                    observed_at,
                                    book.bid_price.0,
                                    book.ask_price.0,
                                    mark.index_price.0,
                                    mark.mark_price.0,
                                );
                                truth.entry(symbol.clone())
                                    .or_default()
                                    .apply(&normalized, now_ms(), strategy_profile.max_stale_ms)
                                    .map_err(|error| format!("market truth rejected for {symbol}: {error}"))?;
                            }
                        }
                        control_plane
                            .observe_market(now_ms())
                            .map_err(|error| format!("market health report rejected: {error}"))?;
                    }
                    Event::Fx(value) => {
                        fx_at.insert(value.currency, value.observed_at_ms);
                        control_plane
                            .observe_reference(value.observed_at_ms)
                            .map_err(|error| format!("reference health report rejected: {error}"))?;
                    }
                    Event::User(value) => {
                        let user_event_time_ms = user_event_time_ms(&value);
                        if user_event_time_ms > 0
                            && user_event_time_ms < last_user_event_time_ms
                        {
                            println!("{}", serde_json::json!({
                                "event": "user_event_out_of_order",
                                "event_time_ms": user_event_time_ms,
                                "last_event_time_ms": last_user_event_time_ms,
                            }));
                            continue;
                        }
                        last_user_event_time_ms =
                            last_user_event_time_ms.max(user_event_time_ms);
                        apply_user(&mut state, &mut working, &value, args.quantity_scale)?;
                        let live_position_ticks = state.values().fold(0_i64, |total, item| {
                            total.saturating_add(item.position_ticks)
                        });
                        shadow.live_user_event(&value, live_position_ticks, now_ms())?;
                        control_plane
                            .observe_user_data(now_ms())
                            .map_err(|error| format!("lifecycle health report rejected: {error}"))?;
                        if let Err(reason) = supervisor.on_user_data(value) {
                            if supervisor.state() != SupervisorState::RiskStopped {
                                return Err(format!("user-data risk halt: {reason:?}"));
                            }
                            supervisor_ready = false;
                        }
                        if supervisor.state() == SupervisorState::Flattening
                            && state.values().all(|item| item.position_ticks == 0)
                        {
                            supervisor.confirm_flattened()
                                .map_err(|reason| format!("flatten confirmation rejected: {reason:?}"))?;
                            println!("{}", serde_json::json!({"event":"flatten_confirmed"}));
                        }
                    }
                    Event::RecoveryRequired(reason) => {
                        supervisor.on_disconnect();
                        supervisor_ready = false;
                        println!("{}", serde_json::json!({
                            "event": "recovery_required",
                            "reason": reason,
                            "state": format!("{:?}", supervisor.state()),
                        }));
                    }
                    Event::Halt(reason) => {
                        supervisor.on_disconnect();
                        return Err(reason);
                    }
                }
                emit_health_transitions(&mut control_plane, &mut audit_sink).await?;

                let now = now_ms();
                for symbol in &symbols {
                    let local = state
                        .get(symbol)
                        .ok_or_else(|| format!("initialized state missing for {symbol}"))?;
                    let profile = profile_for(symbol)
                        .ok_or_else(|| format!("no profile for {symbol}"))?;
                    let market_at = local.mark.as_ref().map(|v| v.event_time_ms)
                        .into_iter()
                        .chain(local.book.as_ref().map(|v| v.event_time_ms))
                        .max().unwrap_or(0);
                    let currency = profile.anchor_currency.as_str().to_owned();
                    let anchor_ready = anchors.get(symbol)
                        .is_some_and(|anchor| anchor.valid_at(now, args.max_anchor_age_ms));
                    let next_funding = local.mark.as_ref()
                        .map(|v| v.next_funding_time_ms).unwrap_or(0);
                    supervisor.observe_symbol(
                        symbol,
                        market_at,
                        fx_at.get(&currency).copied().unwrap_or(0),
                        anchor_ready,
                        equity_is_closed(profile.region),
                        next_funding > now,
                        next_funding,
                        local.position_ticks,
                    ).map_err(|reason| format!("observation rejected: {reason:?}"))?;
                }
                let execution_ready = control_plane.execution_ready(now);
                if execution_ready && supervisor.state() == SupervisorState::RiskStopped {
                    supervisor.on_reconnect()
                        .map_err(|reason| format!("reconnect transition rejected: {reason:?}"))?;
                    let remote_states = reconcile_account(
                        &client,
                        &credentials,
                        &symbols,
                        args.price_scale,
                        args.quantity_scale,
                    )
                    .await?;
                    for symbol in &symbols {
                        let remote = remote_states
                            .get(symbol)
                            .ok_or_else(|| format!("authoritative snapshot missing {symbol}"))?;
                        if let Some(local) = state.get_mut(symbol) {
                            local.position_ticks = remote.position_ticks;
                            local.unrealized_profit = remote.unrealized_profit.clone();
                        }
                        working.remove(symbol);
                        if remote.working_orders.len() > 1 {
                            return Err(format!("multiple managed open orders on {symbol}"));
                        }
                        if let Some(order) = remote.working_orders.first().cloned() {
                            working.insert(symbol.clone(), order);
                        }
                        supervisor
                            .adopt_remote_position(symbol, remote.position_ticks)
                            .map_err(|reason| format!("remote position adoption rejected: {reason:?}"))?;
                    }
                    supervisor
                        .mark_snapshot_loaded(now_ms())
                        .map_err(|reason| format!("authoritative snapshot rejected: {reason:?}"))?;
                    supervisor.reconciliation_clean()
                        .map_err(|reason| format!("recovery reconciliation rejected: {reason:?}"))?;
                    supervisor_ready = true;
                    println!("{}", serde_json::json!({"event":"supervisor_recovered"}));
                } else if !supervisor_ready && supervisor.state() == SupervisorState::Synchronizing {
                    supervisor.reconciliation_clean()
                        .map_err(|reason| format!("initial reconciliation rejected: {reason:?}"))?;
                    supervisor_ready = true;
                    println!("{}", serde_json::json!({"event":"supervisor_healthy"}));
                }

                if !execution_ready && args.send_orders {
                    for symbol in &symbols {
                        if let Some(order) = working.remove(symbol) {
                            cancel_order(&client, &credentials, symbol, &order.client_order_id).await?;
                        }
                    }
                }
                if execution_ready
                    && supervisor.state() == SupervisorState::Healthy
                {
                    for symbol in &symbols {
                        let local = state
                            .get(symbol)
                            .ok_or_else(|| format!("initialized state missing for {symbol}"))?;
                        let anchor = anchors
                            .get(symbol)
                            .ok_or_else(|| format!("validated anchor missing for {symbol}"))?;
                        let Some(intent) =
                            make_intent(symbol, local, anchor.close_price_ticks, &args)
                        else {
                            continue;
                        };
                        let (gate_decision, decision_audit) =
                            supervisor.evaluate_with_audit(symbol, intent, now);
                        let decision_payload = serde_json::to_value(&decision_audit)
                            .map_err(|error| format!("live decision observation encode failed: {error}"))?;
                        shadow.live_decision(serde_json::json!({
                            "kind": "live_decision",
                            "market_event_sequence": last_shadow_event_sequence,
                            "received_at_ms": now,
                            "symbol": symbol,
                            "gate": format!("{gate_decision:?}"),
                            "decision": decision_payload,
                        }))?;
                        println!("{}", serde_json::json!({
                            "event": "decision_audit",
                            "decision": decision_audit,
                        }));
                        match gate_decision {
                            GateDecision::Allow => {
                                if args.send_orders && !working.contains_key(symbol) {
                                    order_sequence = order_sequence.saturating_add(1);
                                    let order = place_order(PlaceOrderRequest {
                                        client: &client,
                                        credentials: &credentials,
                                        symbol,
                                        intent,
                                        price_scale: args.price_scale,
                                        quantity_scale: args.quantity_scale,
                                        now,
                                        sequence: order_sequence,
                                        reduce_only: false,
                                        execution_filters: *execution_filters
                                            .get(symbol)
                                            .ok_or_else(|| format!("missing execution filters for {symbol}"))?,
                                        mark_price_ticks: local
                                            .mark
                                            .as_ref()
                                            .ok_or_else(|| format!("missing mark price for {symbol}"))?
                                            .mark_price
                                            .0,
                                        maximum_quantity: args
                                            .max_position
                                            .saturating_sub(
                                                local.position_ticks.checked_abs().unwrap_or(i64::MAX),
                                            ),
                                    }).await?;
                                    shadow.live_order_submitted(&order.client_order_id, now);
                                    println!("{}", serde_json::json!({
                                        "event":"order_accepted",
                                        "symbol":symbol,
                                        "client_order_id":order.client_order_id,
                                        "side":side_name(order.side),
                                        "price_ticks":order.price_ticks,
                                        "quantity_ticks":order.quantity_ticks,
                                    }));
                                    working.insert(symbol.clone(), order);
                                }
                            }
                            GateDecision::Flatten(reason) => {
                                supervisor.begin_flatten()
                                    .map_err(|r| format!("flatten transition rejected: {r:?}"))?;
                                if args.send_orders {
                                    cancel_symbol_orders(&client, &credentials, symbol).await?;
                                    working.remove(symbol);
                                } else {
                                    println!("{}", serde_json::json!({
                                        "event": "order_mutation_skipped",
                                        "action": "cancel_symbol_orders",
                                        "symbol": symbol,
                                        "reason": "read_only_mode",
                                    }));
                                }
                                if args.send_orders {
                                    let local = state
                                        .get(symbol)
                                        .ok_or_else(|| format!("initialized state missing for {symbol}"))?;
                                    if local.position_ticks != 0 {
                                        if let Some(book) = local.book.as_ref() {
                                            let side = if local.position_ticks > 0 {
                                                Side::Sell
                                            } else {
                                                Side::Buy
                                            };
                                            let price = if side == Side::Sell {
                                                book.bid_price.0
                                            } else {
                                                book.ask_price.0
                                            };
                                            let intent = anchorbell_engine::execution::OrderIntent {
                                                symbol: stable_symbol_id(symbol),
                                                side,
                                                price,
                                                quantity: local.position_ticks.unsigned_abs()
                                                    .min(i64::MAX as u64) as i64,
                                                post_only: true,
                                                reduce_only: true,
                                            };
                                            order_sequence = order_sequence.saturating_add(1);
                                            let order = place_order(PlaceOrderRequest {
                                        client: &client,
                                        credentials: &credentials,
                                        symbol,
                                        intent,
                                        price_scale: args.price_scale,
                                        quantity_scale: args.quantity_scale,
                                        now,
                                        sequence: order_sequence,
                                        reduce_only: true,
                                        execution_filters: *execution_filters
                                            .get(symbol)
                                            .ok_or_else(|| format!("missing execution filters for {symbol}"))?,
                                        mark_price_ticks: local
                                            .mark
                                            .as_ref()
                                            .ok_or_else(|| format!("missing mark price for {symbol}"))?
                                            .mark_price
                                            .0,
                                        maximum_quantity: local
                                            .position_ticks
                                            .checked_abs()
                                            .unwrap_or(i64::MAX),
                                    }).await?;
                                            working.insert(symbol.clone(), order);
                                        }
                                    }
                                }
                                println!("{}", serde_json::json!({
                                    "event":"flatten_required",
                                    "symbol":symbol,
                                    "reason":format!("{reason:?}"),
                                }));
                            }
                            GateDecision::Halt(
                                anchorbell_engine::execution::GateReason::NotHealthy
                                | anchorbell_engine::execution::GateReason::MarketStale
                                | anchorbell_engine::execution::GateReason::FxStale
                                | anchorbell_engine::execution::GateReason::AnchorUnavailable,
                            ) => {
                                // Startup and transient feed gaps block new risk.
                                // Cancel a live quote before waiting for recovery.
                                if args.send_orders {
                                    if let Some(order) = working.remove(symbol) {
                                        cancel_order(
                                            &client, &credentials, symbol,
                                            &order.client_order_id,
                                        ).await?;
                                    }
                                }
                            }
                            GateDecision::Halt(reason) => {
                                return Err(format!("gate halt for {symbol}: {reason:?}"));
                            }
                            GateDecision::NoAction(_) => {}
                        }
                    }
                }
            }
        }
    }

    for symbol in &symbols {
        if let Some(order) = working.get(symbol).cloned() {
            if args.send_orders {
                cancel_order(&client, &credentials, symbol, &order.client_order_id).await?;
                working.remove(symbol);
            } else {
                println!(
                    "{}",
                    serde_json::json!({
                        "event": "order_mutation_skipped",
                        "action": "cancel_order",
                        "symbol": symbol,
                        "client_order_id": order.client_order_id,
                        "reason": "read_only_mode",
                    })
                );
            }
        }
    }
    let final_remote = reconcile_account(
        &client,
        &credentials,
        &symbols,
        args.price_scale,
        args.quantity_scale,
    )
    .await
    .map_err(|error| format!("final flat-state reconciliation failed: {error}"))?;
    let residual = final_remote
        .iter()
        .filter_map(|(symbol, remote)| {
            if remote.position_ticks != 0 || !remote.working_orders.is_empty() {
                Some(format!(
                    "{symbol}:position_ticks={},working_orders={}",
                    remote.position_ticks,
                    remote.working_orders.len()
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if !residual.is_empty() {
        let reason = format!(
            "normal live completion blocked by residual remote state: {}",
            residual.join(",")
        );
        let _ = registry.fail(&run_id, reason.clone(), now_ms());
        eprintln!("ALERT: {reason}");
        return Err(reason);
    }
    persist_live_checkpoint(
        &checkpoint_path,
        &run_id,
        &state,
        &working,
        supervisor.state() != SupervisorState::Healthy,
    )?;
    shadow.finish(&run_id, now_ms())?;
    registry
        .transition(&run_id, RunStatus::Completed, now_ms())
        .map_err(|error| format!("live run registry completion failed: {error}"))?;
    println!(
        "{}",
        serde_json::json!({
            "event":"live_canary_stopped",
            "state":format!("{:?}", supervisor.state()),
            "symbols":symbols,
            "order_submission":args.send_orders,
            "authoritative_remote_state":true,
        })
    );
    Ok(0)
}

#[derive(Debug, Clone)]
struct RemoteSymbolState {
    position_ticks: i64,
    unrealized_profit: String,
    working_orders: Vec<WorkingOrder>,
}

async fn reconcile_account(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbols: &[String],
    price_scale: u32,
    quantity_scale: u32,
) -> Result<BTreeMap<String, RemoteSymbolState>, String> {
    let timestamp = client.server_time_ms().await.map_err(|e| e.to_string())?;
    let snapshot = client
        .authoritative_account_snapshot(
            credentials,
            timestamp,
            binance_runtime_config().operational.default_recv_window_ms,
        )
        .await
        .map_err(|e| e.to_string())?;
    let is_configured = |symbol: &str| {
        symbols
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(symbol))
    };

    for order in &snapshot.open_orders {
        if !is_configured(&order.symbol) {
            return Err(format!(
                "untracked external open order on unknown symbol {}: {}; refusing live start",
                order.symbol, order.client_order_id
            ));
        }
    }

    let mut states = BTreeMap::new();
    for symbol in symbols {
        let mut working_orders = Vec::new();
        for order in snapshot
            .open_orders
            .iter()
            .filter(|order| order.symbol.eq_ignore_ascii_case(symbol))
        {
            if order.client_order_id.starts_with("anchorbell-") {
                let side = match order.side.as_str() {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    _ => return Err(format!("managed order has invalid side on {symbol}")),
                };
                let price_ticks = parse_ticks(&order.price, price_scale)
                    .ok_or_else(|| format!("managed order has invalid price on {symbol}"))?;
                let quantity_ticks = parse_ticks(&order.original_quantity, quantity_scale)
                    .ok_or_else(|| format!("managed order has invalid quantity on {symbol}"))?;
                working_orders.push(WorkingOrder {
                    client_order_id: order.client_order_id.clone(),
                    side,
                    price_ticks,
                    quantity_ticks,
                });
            } else {
                return Err(format!(
                    "untracked external open order on {symbol}: {}; refusing live start",
                    order.client_order_id
                ));
            }
        }

        let mut rows = snapshot
            .positions
            .iter()
            .filter(|row| row.symbol.eq_ignore_ascii_case(symbol));
        let row = rows
            .next()
            .ok_or_else(|| format!("positionRisk missing {symbol}"))?;
        if rows.next().is_some() {
            return Err(format!("multiple position legs for {symbol}"));
        }
        if row.position_side != "BOTH" {
            return Err(format!(
                "hedge mode position leg {symbol}/{:?} is unsupported; live orders are one-way only",
                row.position_side
            ));
        }
        let position = parse_ticks(&row.position_amount, quantity_scale)
            .ok_or_else(|| format!("invalid position precision for {symbol}"))?;
        states.insert(
            symbol.to_ascii_uppercase(),
            RemoteSymbolState {
                position_ticks: position,
                unrealized_profit: row.unrealized_profit.clone(),
                working_orders,
            },
        );
    }
    Ok(states)
}

fn market_event_symbol(event: &BinanceMarketEvent) -> &str {
    match event {
        BinanceMarketEvent::BookTicker(value) => &value.symbol,
        BinanceMarketEvent::MarkPrice(value) => &value.symbol,
        BinanceMarketEvent::AggTrade(value) => &value.symbol,
        BinanceMarketEvent::DepthUpdate(value) => &value.symbol,
    }
}

fn market_event_time_ms(event: &BinanceMarketEvent) -> u64 {
    match event {
        BinanceMarketEvent::BookTicker(value) => value.event_time_ms,
        BinanceMarketEvent::MarkPrice(value) => value.event_time_ms,
        BinanceMarketEvent::AggTrade(value) => value.event_time_ms,
        BinanceMarketEvent::DepthUpdate(value) => value.event_time_ms,
    }
}

fn apply_market(state: &mut BTreeMap<String, SymbolState>, event: BinanceMarketEvent) {
    match event {
        BinanceMarketEvent::BookTicker(value) => {
            if let Some(local) = state.get_mut(&value.symbol) {
                local.book = Some(value);
            }
        }
        BinanceMarketEvent::MarkPrice(value) => {
            if let Some(local) = state.get_mut(&value.symbol) {
                if let Some(previous) = local.last_mark_price_ticks {
                    if previous > 0 && value.mark_price.0 > 0 {
                        let change_bps =
                            ((i128::from(value.mark_price.0) - i128::from(previous)).abs() * 10_000
                                / i128::from(previous))
                            .clamp(0, i128::from(i64::MAX)) as i64;
                        local.ewma_abs_return_bps = ewma(local.ewma_abs_return_bps, change_bps);
                    }
                }
                local.last_mark_price_ticks = Some(value.mark_price.0);
                local.mark = Some(value);
            }
        }
        BinanceMarketEvent::AggTrade(_) => {}
        BinanceMarketEvent::DepthUpdate(_) => {}
    }
}

fn ewma(previous: i64, sample: i64) -> i64 {
    if previous <= 0 {
        sample.max(0)
    } else {
        ((i128::from(previous) * 7 + i128::from(sample.max(0)) * 3) / 10)
            .clamp(0, i128::from(i64::MAX)) as i64
    }
}

fn user_event_time_ms(event: &UserDataEvent) -> u64 {
    match event {
        UserDataEvent::OrderUpdate(value) => value.event_time_ms,
        UserDataEvent::AccountUpdate(value) => value.event_time_ms,
        UserDataEvent::ListenKeyExpired => 0,
        UserDataEvent::Unmodeled { event_time_ms, .. } => *event_time_ms,
    }
}

fn apply_user(
    state: &mut BTreeMap<String, SymbolState>,
    working: &mut BTreeMap<String, WorkingOrder>,
    event: &UserDataEvent,
    quantity_scale: u32,
) -> Result<(), String> {
    match event {
        UserDataEvent::OrderUpdate(update) => {
            if matches!(
                update.status.as_str(),
                "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
            ) {
                working.remove(&update.symbol);
            }
        }
        UserDataEvent::AccountUpdate(update) => {
            for position in &update.positions {
                let value = parse_ticks(&position.position_amount, quantity_scale)
                    .ok_or_else(|| format!("invalid account precision for {}", position.symbol))?;
                if let Some(local) = state.get_mut(&position.symbol) {
                    local.position_ticks = value;
                    local.unrealized_profit = position.unrealized_profit.clone();
                }
            }
        }
        UserDataEvent::ListenKeyExpired => {}
        UserDataEvent::Unmodeled { .. } => {}
    }
    Ok(())
}

fn make_intent(
    symbol: &str,
    state: &SymbolState,
    anchor_ticks: i64,
    args: &Args,
) -> Option<anchorbell_engine::execution::OrderIntent> {
    let book = state.book.as_ref()?;
    let mark = state.mark.as_ref()?;
    let now = now_ms();
    let market_at = mark.event_time_ms.max(book.event_time_ms);
    adaptive_intent_from_market(
        stable_symbol_id(symbol),
        book.bid_price,
        book.bid_quantity.0,
        book.ask_price,
        book.ask_quantity.0,
        anchorbell_engine::strategy::PriceTicks(anchor_ticks),
        mark.index_price,
        mark.mark_price,
        state.position_ticks,
        args.max_position,
        args.quantity,
        state.ewma_abs_return_bps,
        args.entry_threshold_bps,
        binance_runtime_config()
            .operational
            .default_inventory_skew_bps,
        args.max_mark_index_gap_bps,
        now.saturating_sub(market_at),
        binance_runtime_config().operational.max_signal_age_ms,
    )
}

fn spawn_market(
    args: &Args,
    tx: tokio::sync::mpsc::Sender<Event>,
    symbols: &[String],
    profile: &StrategyProfile,
) -> Result<Arc<AtomicBool>, String> {
    let endpoints = args.environment.endpoints();
    let reconnect = anchorbell_engine::market::ReconnectPolicy {
        max_attempts: None,
        ..Default::default()
    };
    let mut shards = BinanceMarketConfig::for_symbols(
        endpoints.market_ws_base,
        symbols,
        BinanceMarketFeed::ReferenceAndTrades,
        args.price_scale,
        args.quantity_scale,
        binance_runtime_config().operational.max_frame_bytes,
        profile.connect_timeout_ms,
        profile.read_timeout_ms,
        args.proxy.clone(),
        reconnect,
        args.max_subscriptions_per_shard,
    )
    .map_err(|e| format!("{e:?}"))?;
    let mut book_ticker_shards = BinanceMarketConfig::for_symbols(
        endpoints.public_market_ws_base,
        symbols,
        BinanceMarketFeed::BookTicker,
        args.price_scale,
        args.quantity_scale,
        binance_runtime_config().operational.max_frame_bytes,
        profile.connect_timeout_ms,
        profile.read_timeout_ms,
        args.proxy.clone(),
        reconnect,
        args.max_subscriptions_per_shard,
    )
    .map_err(|e| format!("{e:?}"))?;
    shards.append(&mut book_ticker_shards);

    let market_overflow = Arc::new(AtomicBool::new(false));
    for shard in shards {
        let producer = tx.clone();
        let overflow = Arc::clone(&market_overflow);
        tokio::spawn(async move {
            BinanceMarketStream::run_forever(shard, |event| {
                if producer.try_send(Event::Market(event)).is_err() {
                    overflow.store(true, Ordering::Release);
                }
            })
            .await;
        });
    }
    Ok(market_overflow)
}

fn spawn_fx(args: &Args, tx: tokio::sync::mpsc::Sender<Event>) -> Result<(), String> {
    let client = BinanceC2cFxClient::new(args.proxy.as_deref()).map_err(|e| e.to_string())?;
    let currencies = vec![AnchorCurrency::Cny, AnchorCurrency::Hkd];
    let poller = BinanceC2cFxPoller::new(client, &currencies, FxPollerConfig::high_frequency())
        .map_err(|e| e.to_string())?;
    let (fx_tx, mut fx_rx) = tokio::sync::mpsc::channel::<FxUpdate>(128);
    let producer = tx.clone();
    tokio::spawn(async move {
        if let Err(error) = poller.run(fx_tx).await {
            let _ = producer
                .send(Event::Halt(format!("FX stream ended: {error}")))
                .await;
        }
    });
    tokio::spawn(async move {
        while let Some(update) = fx_rx.recv().await {
            if tx.send(Event::Fx(update)).await.is_err() {
                break;
            }
        }
    });
    Ok(())
}

async fn spawn_user_data(
    args: &Args,
    client: Arc<BinanceRestClient>,
    credentials: BinanceCredentials,
    tx: tokio::sync::mpsc::Sender<Event>,
) -> Result<Arc<AtomicBool>, String> {
    let initial_listen_key = client
        .start_user_data_stream(&credentials)
        .await
        .map_err(|e| e.to_string())?;
    let environment = args.environment;
    let proxy = args.proxy.clone();
    let task_tx = tx.clone();
    let user_overflow = Arc::new(AtomicBool::new(false));
    let user_overflow_for_task = Arc::clone(&user_overflow);
    let initial_for_task = initial_listen_key.clone();
    tokio::spawn(async move {
        let mut listen_key = initial_for_task;
        loop {
            let stream =
                match BinanceUserDataStream::new(environment, listen_key.clone(), proxy.clone()) {
                    Ok(stream) => stream,
                    Err(error) => {
                        let _ = task_tx
                            .send(Event::RecoveryRequired(format!(
                                "user data session construction failed: {error}"
                            )))
                            .await;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
            let keepalive_client = Arc::clone(&client);
            let keepalive_credentials = credentials.clone();
            let keepalive_key = listen_key.clone();
            let keepalive_tx = task_tx.clone();
            let mut keepalive_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(
                    binance_runtime_config()
                        .operational
                        .listen_key_keepalive_interval_ms,
                ));
                let _ = interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(error) = keepalive_client
                        .keepalive_user_data_stream(&keepalive_credentials, &keepalive_key)
                        .await
                    {
                        let _ = keepalive_tx
                            .send(Event::RecoveryRequired(format!(
                                "listen key keepalive failed: {error}"
                            )))
                            .await;
                        return;
                    }
                }
            });
            let event_tx = task_tx.clone();
            let overflow = Arc::clone(&user_overflow_for_task);
            let result = tokio::select! {
                result = stream.run(|event| {
                    if event_tx.try_send(Event::User(event)).is_err() {
                        overflow.store(true, Ordering::Release);
                    }
                }) => format!("user data stream ended: {result:?}"),
                result = &mut keepalive_task => format!("user data keepalive ended: {result:?}"),
            };
            keepalive_task.abort();
            let _ = keepalive_task.await;
            let _ = task_tx.send(Event::RecoveryRequired(result)).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            loop {
                match client.start_user_data_stream(&credentials).await {
                    Ok(next_key) => {
                        listen_key = next_key;
                        break;
                    }
                    Err(error) => {
                        let _ = task_tx
                            .send(Event::RecoveryRequired(format!(
                                "cannot renew listen key: {error}"
                            )))
                            .await;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    });
    Ok(user_overflow)
}

// This edge adapter keeps authentication, order identity, and exchange scales explicit.
#[allow(clippy::too_many_arguments)]
fn persist_live_checkpoint(
    path: &Path,
    session_id: &str,
    state: &BTreeMap<String, SymbolState>,
    working: &BTreeMap<String, WorkingOrder>,
    risk_stopped: bool,
) -> Result<(), String> {
    let mut checkpoint = SessionCheckpoint::new(session_id, "live", "PORTFOLIO");
    for (symbol, local) in state {
        checkpoint.position_ticks = checkpoint
            .position_ticks
            .saturating_add(local.position_ticks);
        checkpoint.gross_position_ticks = checkpoint
            .gross_position_ticks
            .saturating_add(local.position_ticks.checked_abs().unwrap_or(i64::MAX));
        checkpoint
            .portfolio_positions
            .insert(format!("live::{symbol}"), local.position_ticks);
    }
    checkpoint.working_order_ids = working
        .iter()
        .map(|(symbol, order)| format!("live::{symbol}:{}", order.client_order_id))
        .collect();
    checkpoint.last_event_at_ms = state
        .values()
        .flat_map(|local| {
            [
                local.book.as_ref().map(|value| value.event_time_ms),
                local.mark.as_ref().map(|value| value.event_time_ms),
            ]
            .into_iter()
            .flatten()
        })
        .max()
        .unwrap_or(0);
    checkpoint.risk_stopped = risk_stopped;
    checkpoint
        .write_atomic(path)
        .map_err(|error| format!("live checkpoint write failed: {error}"))
}

struct PlaceOrderRequest<'a> {
    client: &'a BinanceRestClient,
    credentials: &'a BinanceCredentials,
    symbol: &'a str,
    intent: anchorbell_engine::execution::OrderIntent,
    price_scale: u32,
    quantity_scale: u32,
    now: u64,
    sequence: u64,
    reduce_only: bool,
    execution_filters: BinanceScaledExecutionFilters,
    mark_price_ticks: i64,
    maximum_quantity: i64,
}

async fn place_order(request: PlaceOrderRequest<'_>) -> Result<WorkingOrder, String> {
    let PlaceOrderRequest {
        client,
        credentials,
        symbol,
        mut intent,
        price_scale,
        quantity_scale,
        now,
        sequence,
        reduce_only,
        execution_filters,
        mark_price_ticks,
        maximum_quantity,
    } = request;
    intent.price = execution_filters
        .normalize_price(intent.price, intent.side == Side::Buy, intent.post_only)
        .map_err(|reason| {
            format!("Binance exchange price gate rejected order for {symbol}: {reason}")
        })?;
    intent.quantity = execution_filters
        .normalize_quantity(
            intent.quantity,
            intent.price,
            maximum_quantity,
            quantity_scale,
        )
        .map_err(|reason| {
            format!("Binance exchange quantity gate rejected order for {symbol}: {reason}")
        })?;
    execution_filters
        .validate_order(
            intent.price,
            intent.quantity,
            mark_price_ticks,
            intent.side == Side::Buy,
            quantity_scale,
        )
        .map_err(|reason| {
            format!("Binance exchange filter rejected order for {symbol}: {reason}")
        })?;
    let client_order_id = format!("anchorbell-{}-{}", now, sequence);
    let server_time = client.server_time_ms().await.map_err(|e| e.to_string())?;
    if intent.is_emergency_taker() {
        if !reduce_only {
            return Err("emergency taker intent must be reduce-only".to_owned());
        }
        let _response = client
            .place_emergency_reduce_only_taker(
                credentials,
                BinanceEmergencyReduceOnlyTakerRequest {
                    symbol: symbol.to_owned(),
                    side: intent.side,
                    price: format_ticks(intent.price, price_scale),
                    quantity: format_ticks(intent.quantity, quantity_scale),
                    client_order_id: client_order_id.clone(),
                },
                server_time,
                binance_runtime_config().operational.default_recv_window_ms,
            )
            .await
            .map_err(|e| e.to_string())?;
    } else {
        if !intent.post_only {
            return Err("non-maker intent missing adaptive taker authorization".to_owned());
        }
        let _response = client
            .place_maker_order(
                credentials,
                BinanceMakerOrderRequest {
                    symbol: symbol.to_owned(),
                    side: intent.side,
                    price: format_ticks(intent.price, price_scale),
                    quantity: format_ticks(intent.quantity, quantity_scale),
                    client_order_id: client_order_id.clone(),
                    reduce_only,
                },
                server_time,
                binance_runtime_config().operational.default_recv_window_ms,
            )
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(WorkingOrder {
        client_order_id,
        side: intent.side,
        price_ticks: intent.price,
        quantity_ticks: intent.quantity,
    })
}

async fn cancel_symbol_orders(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
) -> Result<(), String> {
    client
        .cancel_all_open_orders(
            credentials,
            symbol,
            client.server_time_ms().await.map_err(|e| e.to_string())?,
            binance_runtime_config().operational.default_recv_window_ms,
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn cancel_order(
    client: &BinanceRestClient,
    credentials: &BinanceCredentials,
    symbol: &str,
    client_order_id: &str,
) -> Result<(), String> {
    client
        .cancel_order(
            credentials,
            symbol,
            client_order_id,
            client.server_time_ms().await.map_err(|e| e.to_string())?,
            binance_runtime_config().operational.default_recv_window_ms,
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn equity_is_closed(region: EquityRegion) -> bool {
    let (_, minute) = local_clock();
    let weekday = local_clock().0;
    let state = calendar_for(region).detailed_state_at(weekday, minute, false, 30, true);
    matches!(
        state,
        VenueSessionState::Closed | VenueSessionState::MiddayBreak
    )
}

fn local_clock() -> (u8, u16) {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = seconds / 86_400;
    let weekday = ((days + 4) % 7 + 1) as u8;
    let minute = ((seconds + 8 * 3_600) % 86_400 / 60) as u16;
    (weekday, minute)
}

fn load_credentials(environment: BinanceEnvironment) -> Result<BinanceCredentials, String> {
    BinanceCredentials::from_environment_for(environment)
        .map_err(|error| format!("credentials unavailable: {error:?}"))
}

fn stable_symbol_id(symbol: &str) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in symbol.bytes() {
        hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}

fn parse_ticks(value: &str, scale: u32) -> Option<i64> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |v| (true, v));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > scale as usize
    {
        return None;
    }
    let multiplier = 10_i128.checked_pow(scale)?;
    let whole = whole.parse::<i128>().ok()?.checked_mul(multiplier)?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .ok()?
            .checked_mul(10_i128.checked_pow(scale.saturating_sub(fraction.len() as u32))?)?
    };
    let signed = if negative {
        whole.checked_add(fraction)?.checked_neg()?
    } else {
        whole.checked_add(fraction)?
    };
    i64::try_from(signed).ok()
}

fn format_ticks(value: i64, scale: u32) -> String {
    let negative = value < 0;
    let value = value.unsigned_abs();
    let multiplier = 10_u64.pow(scale.min(18));
    let whole = value / multiplier;
    let fraction = value % multiplier;
    if scale == 0 {
        return format!("{}{}", if negative { "-" } else { "" }, whole);
    }
    format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    )
}

async fn emit_health_transitions(
    control_plane: &mut RuntimeControlPlane,
    audit_sink: &mut AuditSink,
) -> Result<(), String> {
    for transition in control_plane.drain_health_events() {
        audit_sink
            .append_health_transition(transition.clone(), now_ms())
            .await
            .map_err(|error| format!("runtime audit append failed: {error}"))?;
        println!(
            "{}",
            serde_json::json!({
                "event": "system_health_transition",
                "system": transition.system_id,
                "from": format!("{:?}", transition.from),
                "to": format!("{:?}", transition.to),
                "stale": transition.stale,
                "observed_at_ms": transition.observed_at_ms,
                "diagnostics": transition.diagnostics,
            })
        );
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn parse_args() -> Result<Args, String> {
    let profile = StrategyProfile::load("config/anchorbell-simulation.json")
        .map_err(|error| format!("cannot load strategy profile: {error}"))?;
    let mut args = Args {
        environment: profile.environment,
        duration_secs: profile.duration_secs,
        proxy: None,
        price_scale: profile.price_scale,
        quantity_scale: profile.quantity_scale,
        max_position: profile.max_position,
        quantity: profile.requested_quantity,
        entry_threshold_bps: profile.entry_threshold_bps,
        max_mark_index_gap_bps: profile.max_mark_index_gap_bps,
        max_anchor_age_ms: profile.max_anchor_age_ms,
        funding_lead_ms: profile.funding_lead_ms,
        max_subscriptions_per_shard: profile.max_subscriptions_per_shard,
        send_orders: false,
    };
    let mut values = env::args().skip(1);
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--help" | "-h" => {
                println!("anchorbell_live [--environment testnet|production] [--duration-secs N] [--proxy URL] [--send-orders]");
                process::exit(0);
            }
            "--environment" => {
                args.environment = values
                    .next()
                    .ok_or("missing --environment")?
                    .parse()
                    .map_err(|_| "invalid --environment".to_owned())?
            }
            "--duration-secs" => args.duration_secs = parse_next(&mut values, &flag)?,
            "--proxy" => args.proxy = Some(values.next().ok_or("missing --proxy")?),
            "--price-scale" => args.price_scale = parse_next(&mut values, &flag)?,
            "--quantity-scale" => args.quantity_scale = parse_next(&mut values, &flag)?,
            "--max-position" => args.max_position = parse_next(&mut values, &flag)?,
            "--quantity" => args.quantity = parse_next(&mut values, &flag)?,
            "--entry-threshold-bps" => args.entry_threshold_bps = parse_next(&mut values, &flag)?,
            "--max-mark-index-gap-bps" => {
                args.max_mark_index_gap_bps = parse_next(&mut values, &flag)?
            }
            "--max-anchor-age-ms" => args.max_anchor_age_ms = parse_next(&mut values, &flag)?,
            "--funding-lead-ms" => args.funding_lead_ms = parse_next(&mut values, &flag)?,
            "--max-subscriptions-per-shard" => {
                args.max_subscriptions_per_shard = parse_next(&mut values, &flag)?
            }
            "--send-orders" => args.send_orders = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.duration_secs == 0
        || args.price_scale > 18
        || args.quantity_scale > 18
        || args.max_position <= 0
        || args.quantity <= 0
        || args.entry_threshold_bps < 0
        || args.max_mark_index_gap_bps < 0
        || args.max_subscriptions_per_shard == 0
    {
        return Err("invalid numeric configuration".into());
    }
    Ok(args)
}

fn parse_next<T: std::str::FromStr>(
    values: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String> {
    values
        .next()
        .ok_or_else(|| format!("missing {flag}"))?
        .parse()
        .map_err(|_| format!("invalid value for {flag}"))
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    process::exit(2);
}
