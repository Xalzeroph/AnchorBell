from pathlib import Path

runtime = Path("engine/src/simulation/runtime.rs")
text = runtime.read_text()
start = text.index("// The explicit arguments keep replay assumptions visible and deterministic.")
end = text.index("\nfn now_ms()", start)
replacement = r'''// Legacy wrappers preserve the stable replay API. New research and OOS
// validation should use ReplayConfig so strategy and execution assumptions are
// carried by one auditable object.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub price_scale: u32,
    pub quantity_scale: u32,
    pub entry_threshold_bps: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub max_mark_index_gap_bps: i64,
    pub max_anchor_age_ms: u64,
    pub fee_ppm: i64,
    pub realism: crate::backtest::realism::RealisticFillModel,
    pub strategy_variant: SimulationPolicyVariant,
    pub threshold_scale_ppm: i64,
    pub quote_reprice_min_interval_ms: u64,
    pub dynamic_capital_refresh_ms: u64,
    pub live_risk_gates: bool,
    pub funding_controller_enabled: bool,
    pub capital_usdt_ticks: Option<i64>,
    pub calibration_seeds: BTreeMap<String, CalibrationState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayEvaluation {
    pub summary: SimulationSummary,
    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
}

#[allow(clippy::too_many_arguments)]
pub fn replay_jsonl(
    input_path: &Path,
    output_path: Option<&Path>,
    anchors: BTreeMap<String, AnchorSnapshot>,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
) -> Result<SimulationSummary, SimulationError> {
    replay_jsonl_with_realism(
        input_path,
        output_path,
        anchors,
        price_scale,
        quantity_scale,
        entry_threshold_bps,
        max_position,
        requested_quantity,
        max_mark_index_gap_bps,
        max_anchor_age_ms,
        fee_ppm,
        crate::backtest::realism::RealisticFillModel::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn replay_jsonl_with_realism(
    input_path: &Path,
    output_path: Option<&Path>,
    anchors: BTreeMap<String, AnchorSnapshot>,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    realism: crate::backtest::realism::RealisticFillModel,
) -> Result<SimulationSummary, SimulationError> {
    replay_jsonl_with_config(
        input_path,
        output_path,
        anchors,
        ReplayConfig {
            price_scale,
            quantity_scale,
            entry_threshold_bps,
            max_position,
            requested_quantity,
            max_mark_index_gap_bps,
            max_anchor_age_ms,
            fee_ppm,
            realism,
            strategy_variant: SimulationPolicyVariant::M0Fixed,
            threshold_scale_ppm: 1_000_000,
            quote_reprice_min_interval_ms: 0,
            dynamic_capital_refresh_ms: 60_000,
            live_risk_gates: false,
            funding_controller_enabled: true,
            capital_usdt_ticks: None,
            calibration_seeds: BTreeMap::new(),
        },
    )
    .map(|evaluation| evaluation.summary)
}

pub fn replay_jsonl_with_config(
    input_path: &Path,
    output_path: Option<&Path>,
    anchors: BTreeMap<String, AnchorSnapshot>,
    config: ReplayConfig,
) -> Result<ReplayEvaluation, SimulationError> {
    if config.threshold_scale_ppm <= 0
        || config.threshold_scale_ppm > 1_000_000
        || config.dynamic_capital_refresh_ms == 0
        || config.capital_usdt_ticks.is_some_and(|capital| capital <= 0)
        || (!config.funding_controller_enabled
            && config.strategy_variant != SimulationPolicyVariant::M8FundingAware)
    {
        return Err(SimulationError::InvalidConfig(
            "invalid replay policy configuration",
        ));
    }
    let allocations = config
        .capital_usdt_ticks
        .map(|capital| {
            allocate_positions(
                &anchors,
                capital,
                &BTreeMap::new(),
                config.quantity_scale,
            )
        })
        .transpose()?;
    let mut engine = SimulationEngine::new(
        anchors,
        config.entry_threshold_bps,
        config.max_position,
        config.requested_quantity,
        config.max_mark_index_gap_bps,
        config.max_anchor_age_ms,
        config.fee_ppm,
        config.quantity_scale,
    )?
    .with_price_scale(config.price_scale)
    .with_strategy_variant(config.strategy_variant)
    .with_funding_controller_enabled(config.funding_controller_enabled)
    .with_realism(config.realism)
    .with_threshold_scale_ppm(config.threshold_scale_ppm)
    .with_quote_reprice_min_interval_ms(config.quote_reprice_min_interval_ms)
    .with_dynamic_capital_refresh_ms(config.dynamic_capital_refresh_ms);
    if config.live_risk_gates {
        engine = engine.with_live_risk_gates();
    }
    engine.restore_calibration_states(&config.calibration_seeds);
    if let Some(allocations) = allocations {
        engine = engine.with_position_allocations(allocations)?;
    }

    let reader = BufReader::new(File::open(input_path)?);
    if let Some(parent) = output_path
        .and_then(Path::parent)
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut output = output_path
        .map(|path| File::create(path).map(BufWriter::new))
        .transpose()?;
    let mut previous_ms = None;
    let mut event_sequence = 0_u64;
    let mut last_risk_sample_ms = None;
    let mut risk_points = Vec::<(u64, i64)>::new();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let envelope = serde_json::from_str::<serde_json::Value>(&line).ok();
        let payload = envelope
            .as_ref()
            .and_then(|value| value.get("payload"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or(line.as_str());
        if payload.contains("\"id\"") && payload.contains("\"result\"") {
            continue;
        }
        let received_at_ms = envelope
            .as_ref()
            .and_then(|value| value.get("received_at_ms"))
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| u64::try_from(value).ok())
            .or_else(|| {
                envelope
                    .as_ref()
                    .and_then(|value| value.get("_anchorbell_received_at_ms"))
                    .and_then(serde_json::Value::as_u64)
            });
        let event = crate::market::binance::parse_market_message(
            payload.as_bytes(),
            config.price_scale,
            config.quantity_scale,
        )
        .map_err(|error| SimulationError::ReplayParse {
            line: line_number,
            error,
        })?;
        let symbol = event_symbol(&event).to_ascii_uppercase();
        if !engine.states.contains_key(&symbol) {
            return Err(SimulationError::ReplaySymbolNotConfigured(symbol));
        }
        let event_timestamp_ms = event_time_ms(&event);
        let timestamp_ms = received_at_ms.unwrap_or(event_timestamp_ms);
        if previous_ms.is_some_and(|previous| timestamp_ms < previous) {
            return Err(SimulationError::ReplayOutOfOrder {
                previous_ms: previous_ms.unwrap(),
                current_ms: timestamp_ms,
            });
        }
        previous_ms = Some(timestamp_ms);
        event_sequence = event_sequence.saturating_add(1);
        let event_envelope = EventEnvelope {
            event_id: format!("replay-{event_sequence}").into(),
            run_id: "replay".into(),
            causality_id: format!("replay-cause-{event_sequence}").into(),
            source: EventSource::Replay,
            observed_at_ms: event_timestamp_ms,
            received_at_ms: timestamp_ms,
            sequence: event_sequence,
            state_version: event_sequence,
            quality: DataQuality::Trusted,
            payload: event,
        };
        for record in engine.on_enveloped_event(&event_envelope)? {
            if let Some(output) = output.as_mut() {
                serde_json::to_writer(&mut *output, &record)?;
                output.write_all(b"\n")?;
            }
        }
        if config.capital_usdt_ticks.is_some()
            && last_risk_sample_ms.is_none_or(|last| {
                timestamp_ms.saturating_sub(last) >= RISK_SAMPLE_INTERVAL_MS
            })
        {
            let point = engine.performance_point(timestamp_ms);
            risk_points.push((timestamp_ms, point.net_pnl_ticks));
            last_risk_sample_ms = Some(timestamp_ms);
        }
    }

    let final_timestamp_ms = previous_ms.unwrap_or(0);
    for record in engine.cancel_all(final_timestamp_ms, "replay window ended") {
        if let Some(output) = output.as_mut() {
            serde_json::to_writer(&mut *output, &record)?;
            output.write_all(b"\n")?;
        }
    }
    if let Some(output) = output.as_mut() {
        output.flush()?;
    }
    let summary = engine.summary();
    if config.capital_usdt_ticks.is_some() {
        match risk_points.last_mut() {
            Some(last) if last.0 == final_timestamp_ms => last.1 = summary.net_pnl_ticks,
            _ => risk_points.push((final_timestamp_ms, summary.net_pnl_ticks)),
        }
    }
    let risk_metrics = config
        .capital_usdt_ticks
        .map(|capital| calculate_risk_metrics(&risk_points, capital));
    Ok(ReplayEvaluation {
        summary,
        risk_metrics,
        risk_samples: risk_points.len(),
    })
}
'''
runtime.write_text(text[:start] + replacement + text[end:])

backtest = Path("engine/src/bin/anchorbell_backtest.rs")
backtest.write_text(r'''use std::{
    collections::BTreeMap, env, fs::File, io::Read, path::PathBuf, process, str::FromStr,
};

use anchorbell_engine::{
    backtest::realism::{LatencyModel, QueueModel, RealisticFillModel},
    platform::RuntimeProfile,
    runtime::health_reporter::{timestamp_ms, RuntimeHealthReporter},
    simulation::{
        load_anchor_file, replay_jsonl_with_config, ReplayConfig, SimulationPolicyVariant,
    },
    strategy::{universe::instrument_for, CalibrationSnapshot, CalibrationState},
};
use sha2::{Digest, Sha256};

#[derive(Debug)]
struct Args {
    input: PathBuf,
    anchors: PathBuf,
    records: Option<PathBuf>,
    calibration_store: Option<PathBuf>,
    calibration_source_label: String,
    price_scale: u32,
    quantity_scale: u32,
    entry_threshold_bps: i64,
    max_position: i64,
    requested_quantity: i64,
    max_mark_index_gap_bps: i64,
    max_anchor_age_ms: u64,
    fee_ppm: i64,
    queue_ahead: i64,
    trade_through: i64,
    market_to_decision_ms: u64,
    decision_to_exchange_ms: u64,
    cancel_to_exchange_ms: u64,
    strategy_variant: SimulationPolicyVariant,
    threshold_scale_ppm: i64,
    quote_reprice_min_interval_ms: u64,
    dynamic_capital_refresh_ms: u64,
    live_risk_gates: bool,
    funding_controller_enabled: bool,
    capital_usdt_ticks: Option<i64>,
    require_flat_at_end: bool,
}

#[tokio::main]
async fn main() {
    let mut health = RuntimeHealthReporter::new("target/backtest-runtime-audit.jsonl");
    health
        .start(RuntimeProfile::Backtest, timestamp_ms())
        .await
        .unwrap_or_else(|error| fail(format!("backtest health bootstrap failed: {error}")));
    let args = parse_args().unwrap_or_else(|message| fail(message));
    let anchors = load_anchor_file(&args.anchors)
        .unwrap_or_else(|error| fail(format!("cannot load anchors: {error}")));
    if let Some(symbol) = anchors.keys().find(|symbol| instrument_for(symbol).is_none()) {
        fail(format!(
            "anchor symbol is outside the selected execution universe: {symbol}"
        ));
    }
    let calibration_seeds = args
        .calibration_store
        .as_deref()
        .map(|path| load_calibration_seeds(path, &args.calibration_source_label))
        .transpose()
        .unwrap_or_else(|error| fail(error))
        .unwrap_or_default();
    if args.strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
        && calibration_seeds.is_empty()
    {
        fail("M9 replay requires --calibration-store from the training window");
    }
    let input_sha256 = sha256_file(&args.input)
        .unwrap_or_else(|error| fail(format!("cannot hash input: {error}")));
    let evaluation = match replay_jsonl_with_config(
        &args.input,
        args.records.as_deref(),
        anchors.clone(),
        ReplayConfig {
            price_scale: args.price_scale,
            quantity_scale: args.quantity_scale,
            entry_threshold_bps: args.entry_threshold_bps,
            max_position: args.max_position,
            requested_quantity: args.requested_quantity,
            max_mark_index_gap_bps: args.max_mark_index_gap_bps,
            max_anchor_age_ms: args.max_anchor_age_ms,
            fee_ppm: args.fee_ppm,
            realism: RealisticFillModel {
                queue: QueueModel {
                    visible_ahead: args.queue_ahead,
                    trade_through: args.trade_through,
                },
                latency: LatencyModel {
                    market_to_decision_ms: args.market_to_decision_ms,
                    decision_to_exchange_ms: args.decision_to_exchange_ms,
                    cancel_to_exchange_ms: args.cancel_to_exchange_ms,
                },
            },
            strategy_variant: args.strategy_variant,
            threshold_scale_ppm: args.threshold_scale_ppm,
            quote_reprice_min_interval_ms: args.quote_reprice_min_interval_ms,
            dynamic_capital_refresh_ms: args.dynamic_capital_refresh_ms,
            live_risk_gates: args.live_risk_gates,
            funding_controller_enabled: args.funding_controller_enabled,
            capital_usdt_ticks: args.capital_usdt_ticks,
            calibration_seeds,
        },
    ) {
        Ok(evaluation) => evaluation,
        Err(error) => {
            let reason = error.to_string();
            let _ = health
                .halted("simulation.backtest", timestamp_ms(), &reason)
                .await;
            fail(format!("backtest failed: {error}"));
        }
    };
    health
        .ready("simulation.backtest", timestamp_ms())
        .await
        .unwrap_or_else(|error| fail(format!("backtest health completion failed: {error}")));
    if args.require_flat_at_end && !evaluation.summary.flat_at_end {
        fail(format!(
            "backtest ended with unmanaged exposure: position={}, working_orders={}",
            evaluation.summary.current_absolute_position, evaluation.summary.working_orders
        ));
    }
    let report = serde_json::json!({
        "input": args.input,
        "input_sha256": input_sha256,
        "anchors": anchors.len(),
        "strategy_variant": args.strategy_variant.label(),
        "funding_controller_enabled": args.funding_controller_enabled,
        "calibration_source_label": args.calibration_source_label,
        "calibration_seed_count": args.calibration_store.as_ref().map(|_| 1).unwrap_or(0),
        "price_scale": args.price_scale,
        "quantity_scale": args.quantity_scale,
        "entry_threshold_bps": args.entry_threshold_bps,
        "threshold_scale_ppm": args.threshold_scale_ppm,
        "max_position": args.max_position,
        "requested_quantity": args.requested_quantity,
        "capital_usdt_ticks": args.capital_usdt_ticks,
        "max_mark_index_gap_bps": args.max_mark_index_gap_bps,
        "max_anchor_age_ms": args.max_anchor_age_ms,
        "maker_fee_ppm": args.fee_ppm,
        "queue_ahead": args.queue_ahead,
        "trade_through": args.trade_through,
        "market_to_decision_ms": args.market_to_decision_ms,
        "decision_to_exchange_ms": args.decision_to_exchange_ms,
        "cancel_to_exchange_ms": args.cancel_to_exchange_ms,
        "quote_reprice_min_interval_ms": args.quote_reprice_min_interval_ms,
        "dynamic_capital_refresh_ms": args.dynamic_capital_refresh_ms,
        "live_risk_gates": args.live_risk_gates,
        "require_flat_at_end": args.require_flat_at_end,
        "risk_samples": evaluation.risk_samples,
        "risk_metrics": evaluation.risk_metrics,
        "summary": evaluation.summary,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("report is serializable")
    );
}

fn load_calibration_seeds(
    path: &std::path::Path,
    source_label: &str,
) -> Result<BTreeMap<String, CalibrationState>, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("cannot read calibration store {}: {error}", path.display()))?;
    let root: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid calibration store {}: {error}", path.display()))?;
    let snapshots = root
        .get("snapshots")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "calibration store is missing snapshots".to_owned())?;
    let mut seeds = BTreeMap::new();
    for (key, value) in snapshots {
        let Some((stored_source, symbol)) = key.split_once("::") else {
            continue;
        };
        if stored_source != source_label {
            continue;
        }
        let snapshot: CalibrationSnapshot = serde_json::from_value(value.clone())
            .map_err(|error| format!("invalid calibration snapshot {key}: {error}"))?;
        let state = snapshot
            .replay()
            .map_err(|error| format!("unreplayable calibration snapshot {key}: {error}"))?;
        if state.instrument == symbol {
            seeds.insert(symbol.to_owned(), state);
        }
    }
    if seeds.is_empty() {
        return Err(format!(
            "calibration store contains no snapshots for source {source_label}"
        ));
    }
    Ok(seeds)
}

fn parse_args() -> Result<Args, String> {
    let mut input = None;
    let mut anchors = None;
    let mut records = None;
    let mut calibration_store = None;
    let mut calibration_source_label = "F3_m3".to_owned();
    let mut price_scale = 8;
    let mut quantity_scale = 8;
    let mut entry_threshold_bps = 0;
    let mut max_position = 1;
    let mut requested_quantity = 1;
    let mut max_mark_index_gap_bps = 50;
    let mut max_anchor_age_ms = 0;
    let mut fee_ppm = 200;
    let mut queue_ahead = 0;
    let mut trade_through = 0;
    let mut market_to_decision_ms = 0;
    let mut decision_to_exchange_ms = 0;
    let mut cancel_to_exchange_ms = 0;
    let mut strategy_variant = SimulationPolicyVariant::M0Fixed;
    let mut threshold_scale_ppm = 1_000_000;
    let mut quote_reprice_min_interval_ms = 0;
    let mut dynamic_capital_refresh_ms = 60_000;
    let mut live_risk_gates = false;
    let mut funding_controller_enabled = true;
    let mut capital_usdt_ticks = None;
    let mut require_flat_at_end = false;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            "--input" => input = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--anchors" => anchors = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--records" => records = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--calibration-store" => {
                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,
            "--price-scale" => price_scale = parse(&mut args, &flag)?,
            "--quantity-scale" => quantity_scale = parse(&mut args, &flag)?,
            "--entry-threshold-bps" => entry_threshold_bps = parse(&mut args, &flag)?,
            "--max-position" => max_position = parse(&mut args, &flag)?,
            "--quantity" => requested_quantity = parse(&mut args, &flag)?,
            "--max-mark-index-gap-bps" => max_mark_index_gap_bps = parse(&mut args, &flag)?,
            "--max-anchor-age-ms" => max_anchor_age_ms = parse(&mut args, &flag)?,
            "--maker-fee-ppm" | "--fee-ppm" => fee_ppm = parse(&mut args, &flag)?,
            "--queue-ahead" => queue_ahead = parse(&mut args, &flag)?,
            "--trade-through" => trade_through = parse(&mut args, &flag)?,
            "--market-to-decision-ms" => market_to_decision_ms = parse(&mut args, &flag)?,
            "--decision-to-exchange-ms" => decision_to_exchange_ms = parse(&mut args, &flag)?,
            "--cancel-to-exchange-ms" => cancel_to_exchange_ms = parse(&mut args, &flag)?,
            "--strategy-variant" => {
                strategy_variant = parse_strategy_variant(&next(&mut args, &flag)?)?
            }
            "--threshold-scale-ppm" => threshold_scale_ppm = parse(&mut args, &flag)?,
            "--quote-reprice-min-interval-ms" => {
                quote_reprice_min_interval_ms = parse(&mut args, &flag)?
            }
            "--dynamic-capital-refresh-ms" => {
                dynamic_capital_refresh_ms = parse(&mut args, &flag)?
            }
            "--live-risk-gates" => live_risk_gates = true,
            "--ablate-funding" => funding_controller_enabled = false,
            "--capital-usdt" => {
                capital_usdt_ticks = Some(parse_usdt_ticks(&next(&mut args, &flag)?)?)
            }
            "--require-flat-at-end" => require_flat_at_end = true,
            unknown => return Err(format!("unknown option {unknown}; use --help")),
        }
    }
    if !funding_controller_enabled && strategy_variant != SimulationPolicyVariant::M8FundingAware {
        return Err("--ablate-funding is valid only with --strategy-variant m8".to_owned());
    }
    Ok(Args {
        input: input.ok_or("missing --input")?,
        anchors: anchors.ok_or("missing --anchors")?,
        records,
        calibration_store,
        calibration_source_label,
        price_scale,
        quantity_scale,
        entry_threshold_bps,
        max_position,
        requested_quantity,
        max_mark_index_gap_bps,
        max_anchor_age_ms,
        fee_ppm,
        queue_ahead,
        trade_through,
        market_to_decision_ms,
        decision_to_exchange_ms,
        cancel_to_exchange_ms,
        strategy_variant,
        threshold_scale_ppm,
        quote_reprice_min_interval_ms,
        dynamic_capital_refresh_ms,
        live_risk_gates,
        funding_controller_enabled,
        capital_usdt_ticks,
        require_flat_at_end,
    })
}

fn parse_strategy_variant(value: &str) -> Result<SimulationPolicyVariant, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "m0" | "m0_fixed" | "fixed" => Ok(SimulationPolicyVariant::M0Fixed),
        "m1" | "m1_adaptive_risk" | "adaptive" => Ok(SimulationPolicyVariant::M1AdaptiveRisk),
        "m2" | "m2_microstructure" | "microstructure" => {
            Ok(SimulationPolicyVariant::M2Microstructure)
        }
        "m3" | "m3_fill_aware" | "fill_aware" => Ok(SimulationPolicyVariant::M3FillAware),
        "m4" | "m4_statistical" | "statistical" => Ok(SimulationPolicyVariant::M4Statistical),
        "m5" | "m5_robust" | "robust" => Ok(SimulationPolicyVariant::M5Robust),
        "m6" | "m6_dynamic_capital" | "dynamic_capital" => {
            Ok(SimulationPolicyVariant::M6DynamicCapital)
        }
        "m7" | "m7_evidence_gated" | "evidence_gated" => {
            Ok(SimulationPolicyVariant::M7EvidenceGated)
        }
        "m8" | "m8_funding_aware" | "funding_aware" => {
            Ok(SimulationPolicyVariant::M8FundingAware)
        }
        "m9" | "m9_deadline_causal_dro_mpc" | "deadline_causal_dro_mpc" => {
            Ok(SimulationPolicyVariant::M9DeadlineCausalDroMpc)
        }
        _ => Err("invalid --strategy-variant; expected m0..m9".to_owned()),
    }
}

fn parse_usdt_ticks(value: &str) -> Result<i64, String> {
    let value = value.trim();
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || fraction.len() > 8
        || !whole.chars().all(|c| c.is_ascii_digit())
        || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        return Err("--capital-usdt must be a non-negative decimal with at most 8 places".into());
    }
    let whole = whole
        .parse::<i64>()
        .map_err(|_| "--capital-usdt is too large")?;
    let mut fraction = fraction.to_owned();
    while fraction.len() < 8 {
        fraction.push('0');
    }
    let fraction = fraction
        .parse::<i64>()
        .map_err(|_| "invalid --capital-usdt")?;
    whole
        .checked_mul(100_000_000)
        .and_then(|ticks| ticks.checked_add(fraction))
        .filter(|ticks| *ticks > 0)
        .ok_or_else(|| "--capital-usdt must be positive".to_owned())
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn parse<T>(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Debug,
{
    next(args, flag)?
        .parse()
        .map_err(|error| format!("invalid {flag}: {error:?}"))
}

fn sha256_file(path: &PathBuf) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

fn print_usage() {
    eprintln!(
        "usage: anchorbell_backtest --input EVENTS.jsonl --anchors ANCHORS.csv [options]\n\
         options: --records PATH --strategy-variant m0..m9 --capital-usdt N\n\
         --calibration-store PATH --calibration-source-label LABEL --ablate-funding\n\
         --price-scale N --quantity-scale N --entry-threshold-bps N\n\
         --threshold-scale-ppm N --max-position N --quantity N\n\
         --max-mark-index-gap-bps N --max-anchor-age-ms N --maker-fee-ppm N\n\
         --queue-ahead N --trade-through N --market-to-decision-ms N\n\
         --decision-to-exchange-ms N --cancel-to-exchange-ms N\n\
         --quote-reprice-min-interval-ms N --dynamic-capital-refresh-ms N\n\
         --live-risk-gates --require-flat-at-end"
    );
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    print_usage();
    process::exit(2);
}
''')
