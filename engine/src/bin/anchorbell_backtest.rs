use std::{collections::BTreeMap, env, fs::File, io::Read, path::PathBuf, process, str::FromStr};

use anchorbell_engine::{
    backtest::realism::{LatencyModel, QueueModel, RealisticFillModel},
    platform::RuntimeProfile,
    runtime::{timestamp_ms, RuntimeHealthReporter},
    simulation::{
        load_anchor_file, replay_jsonl_with_config, ReplayConfig, SimulationPolicyVariant,
    },
    strategy::{
        universe::instrument_for, CalibrationSnapshot, CalibrationState, CALIBRATION_MODEL_VERSION,
        CALIBRATION_SCHEMA_VERSION,
    },
};
use sha2::{Digest, Sha256};

#[derive(Debug)]
struct Args {
    input: PathBuf,
    anchors: PathBuf,
    records: Option<PathBuf>,
    calibration_store: Option<PathBuf>,
    calibration_output: Option<PathBuf>,
    calibration_source_label: String,
    freeze_calibration: bool,
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
    portfolio_drawdown_soft_limit_bps: i64,
    portfolio_drawdown_hard_limit_bps: i64,
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
    if let Some(symbol) = anchors
        .keys()
        .find(|symbol| instrument_for(symbol).is_none())
    {
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
    if args.freeze_calibration && calibration_seeds.is_empty() {
        fail("--freeze-calibration requires --calibration-store from a prior training window");
    }
    if args.freeze_calibration && args.calibration_output.is_some() {
        fail("--calibration-output cannot be combined with --freeze-calibration");
    }
    let calibration_seed_count = calibration_seeds.len();
    let calibration_store_sha256 = args
        .calibration_store
        .as_deref()
        .map(sha256_file)
        .transpose()
        .unwrap_or_else(|error| fail(format!("cannot hash calibration store: {error}")));
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
            portfolio_drawdown_limits_bps: drawdown_limits(
                args.portfolio_drawdown_soft_limit_bps,
                args.portfolio_drawdown_hard_limit_bps,
            ),
            calibration_updates_enabled: !args.freeze_calibration,
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
    let calibration_output_sha256 = args.calibration_output.as_deref().map(|path| {
        write_calibration_store(
            path,
            &args.calibration_source_label,
            &evaluation.calibration_snapshots,
        )
        .unwrap_or_else(|error| fail(error));
        sha256_file(path).unwrap_or_else(|error| {
            fail(format!(
                "cannot hash calibration output {}: {error}",
                path.display()
            ))
        })
    });
    let report = serde_json::json!({
        "input": args.input,
        "input_sha256": input_sha256,
        "anchors": anchors.len(),
        "strategy_variant": args.strategy_variant.label(),
        "funding_controller_enabled": args.funding_controller_enabled,
        "calibration_source_label": args.calibration_source_label,
        "calibration_model_version": CALIBRATION_MODEL_VERSION,
        "calibration_updates_enabled": !args.freeze_calibration,
        "calibration_seed_count": calibration_seed_count,
        "calibration_snapshot_count": evaluation.calibration_snapshots.len(),
        "calibration_store_sha256": calibration_store_sha256,
        "calibration_output": args.calibration_output,
        "calibration_output_sha256": calibration_output_sha256,
        "price_scale": args.price_scale,
        "quantity_scale": args.quantity_scale,
        "entry_threshold_bps": args.entry_threshold_bps,
        "threshold_scale_ppm": args.threshold_scale_ppm,
        "max_position": args.max_position,
        "requested_quantity": args.requested_quantity,
        "capital_usdt_ticks": args.capital_usdt_ticks,
        "portfolio_drawdown_soft_limit_bps": args.portfolio_drawdown_soft_limit_bps,
        "portfolio_drawdown_hard_limit_bps": args.portfolio_drawdown_hard_limit_bps,
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

fn write_calibration_store(
    path: &std::path::Path,
    source_label: &str,
    snapshots: &BTreeMap<String, CalibrationSnapshot>,
) -> Result<(), String> {
    if source_label.trim().is_empty() {
        return Err("calibration source label cannot be empty".to_owned());
    }
    if snapshots.is_empty() {
        return Err("training replay produced no calibration snapshots".to_owned());
    }
    let keyed = snapshots
        .iter()
        .map(|(symbol, snapshot)| (format!("{source_label}::{symbol}"), snapshot.clone()))
        .collect::<BTreeMap<_, _>>();
    let updated_at_event_time_ms = snapshots
        .values()
        .map(|snapshot| snapshot.window_end_event_time_ms)
        .max()
        .unwrap_or(0);
    let store = serde_json::json!({
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "model_version": CALIBRATION_MODEL_VERSION,
        "updated_at_event_time_ms": updated_at_event_time_ms,
        "source_label": source_label,
        "snapshots": keyed,
    });
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!(
                "cannot create calibration output {}: {error}",
                parent.display()
            )
        })?;
    }
    let bytes = serde_json::to_vec_pretty(&store)
        .map_err(|error| format!("cannot encode calibration output: {error}"))?;
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes).map_err(|error| {
        format!(
            "cannot write calibration output {}: {error}",
            temporary.display()
        )
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        format!(
            "cannot install calibration output {}: {error}",
            path.display()
        )
    })?;
    Ok(())
}

fn load_calibration_seeds(
    path: &std::path::Path,
    source_label: &str,
) -> Result<BTreeMap<String, CalibrationState>, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("cannot read calibration store {}: {error}", path.display()))?;
    let root: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid calibration store {}: {error}", path.display()))?;
    let schema_version = root
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "calibration store is missing schema_version".to_owned())?;
    if schema_version != u64::from(CALIBRATION_SCHEMA_VERSION) {
        return Err(format!(
            "unsupported calibration schema {schema_version}; expected {CALIBRATION_SCHEMA_VERSION}"
        ));
    }
    let model_version = root
        .get("model_version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "calibration store is missing model_version".to_owned())?;
    if model_version != CALIBRATION_MODEL_VERSION {
        return Err(format!(
            "unsupported calibration model {model_version}; expected {CALIBRATION_MODEL_VERSION}"
        ));
    }
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
    let mut calibration_output = None;
    let mut calibration_source_label = "F3_m3".to_owned();
    let mut freeze_calibration = false;
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
    let mut portfolio_drawdown_soft_limit_bps = 0;
    let mut portfolio_drawdown_hard_limit_bps = 0;
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
            "--calibration-output" => {
                calibration_output = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,
            "--freeze-calibration" => freeze_calibration = true,
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
            "--dynamic-capital-refresh-ms" => dynamic_capital_refresh_ms = parse(&mut args, &flag)?,
            "--live-risk-gates" => live_risk_gates = true,
            "--ablate-funding" => funding_controller_enabled = false,
            "--capital-usdt" => {
                capital_usdt_ticks = Some(parse_usdt_ticks(&next(&mut args, &flag)?)?)
            }
            "--portfolio-drawdown-soft-bps" => {
                portfolio_drawdown_soft_limit_bps = parse(&mut args, &flag)?
            }
            "--portfolio-drawdown-hard-bps" => {
                portfolio_drawdown_hard_limit_bps = parse(&mut args, &flag)?
            }
            "--require-flat-at-end" => require_flat_at_end = true,
            unknown => return Err(format!("unknown option {unknown}; use --help")),
        }
    }
    if !funding_controller_enabled && strategy_variant != SimulationPolicyVariant::M8FundingAware {
        return Err("--ablate-funding is valid only with --strategy-variant m8".to_owned());
    }
    drawdown_limits(
        portfolio_drawdown_soft_limit_bps,
        portfolio_drawdown_hard_limit_bps,
    )
    .map(|_| ())
    .ok_or_else(|| "portfolio drawdown requires 0/0 or 0 < soft < hard <= 10000 bps".to_owned())?;
    if drawdown_limits(
        portfolio_drawdown_soft_limit_bps,
        portfolio_drawdown_hard_limit_bps,
    )
    .is_some()
        && capital_usdt_ticks.is_none()
    {
        return Err("portfolio drawdown limits require --capital-usdt".to_owned());
    }
    Ok(Args {
        input: input.ok_or("missing --input")?,
        anchors: anchors.ok_or("missing --anchors")?,
        records,
        calibration_store,
        calibration_output,
        calibration_source_label,
        freeze_calibration,
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
        portfolio_drawdown_soft_limit_bps,
        portfolio_drawdown_hard_limit_bps,
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
        "m8" | "m8_funding_aware" | "funding_aware" => Ok(SimulationPolicyVariant::M8FundingAware),
        "m9" | "m9_deadline_causal_dro_mpc" | "deadline_causal_dro_mpc" => {
            Ok(SimulationPolicyVariant::M9DeadlineCausalDroMpc)
        }
        _ => Err("invalid --strategy-variant; expected m0..m9".to_owned()),
    }
}

fn drawdown_limits(soft: i64, hard: i64) -> Option<(i64, i64)> {
    if soft == 0 && hard == 0 {
        None
    } else if soft > 0 && hard > soft && hard <= 10_000 {
        Some((soft, hard))
    } else {
        None
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

fn sha256_file(path: &std::path::Path) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

fn print_usage() {
    eprintln!(
        "usage: anchorbell_backtest --input EVENTS.jsonl --anchors ANCHORS.csv [options]\n\
         options: --records PATH --strategy-variant m0..m9 --capital-usdt N --portfolio-drawdown-soft-bps N --portfolio-drawdown-hard-bps N\n\
         --calibration-store PATH --calibration-output PATH --calibration-source-label LABEL --freeze-calibration --ablate-funding\n\
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

#[cfg(test)]
mod calibration_store_regression_tests {
    use super::*;

    fn temporary_store(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "anchorbell-{name}-{}-{nonce}.json",
            std::process::id()
        ))
    }

    #[test]
    fn calibration_store_round_trip_preserves_replayable_state() {
        let path = temporary_store("calibration-round-trip");
        let mut state = CalibrationState::new("TESTUSDT");
        state.observe_market(
            100,
            Some(2_000_000_000_000),
            Some(1_000_000_000_000),
            Some(3_000_000_000_000),
        );
        state.observe_order_placed(101);
        state.observe_fill(102, 101, 5, 20);
        state.observe_order_terminal(103, 101);
        state.observe_markout(104, 750_000_000_000);
        let snapshots =
            BTreeMap::from([("TESTUSDT".to_owned(), state.snapshot(2_000_000_000_000))]);

        write_calibration_store(&path, "TRAIN", &snapshots).unwrap();
        let loaded = load_calibration_seeds(&path, "TRAIN").unwrap();
        let restored = loaded.get("TESTUSDT").unwrap();
        assert_eq!(restored.instrument, state.instrument);
        assert_eq!(restored.first_event_time_ms, state.first_event_time_ms);
        assert_eq!(restored.last_event_time_ms, state.last_event_time_ms);
        assert_eq!(restored.orders_placed, state.orders_placed);
        assert_eq!(restored.fill_events, state.fill_events);
        assert_eq!(restored.completed_orders, state.completed_orders);

        let root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            root.get("schema_version")
                .and_then(serde_json::Value::as_u64),
            Some(u64::from(CALIBRATION_SCHEMA_VERSION))
        );
        assert_eq!(
            root.get("model_version")
                .and_then(serde_json::Value::as_str),
            Some(CALIBRATION_MODEL_VERSION)
        );
        assert_eq!(
            root.get("source_label").and_then(serde_json::Value::as_str),
            Some("TRAIN")
        );
        assert!(root
            .get("snapshots")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|snapshots| snapshots.contains_key("TRAIN::TESTUSDT")));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn calibration_store_rejects_schema_and_model_drift() {
        let path = temporary_store("calibration-version-drift");
        let state = CalibrationState::new("TESTUSDT");
        let snapshots =
            BTreeMap::from([("TESTUSDT".to_owned(), state.snapshot(2_000_000_000_000))]);
        write_calibration_store(&path, "TRAIN", &snapshots).unwrap();

        let mut root: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        root["schema_version"] = serde_json::json!(u64::from(CALIBRATION_SCHEMA_VERSION) + 1);
        std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        assert!(load_calibration_seeds(&path, "TRAIN")
            .unwrap_err()
            .contains("unsupported calibration schema"));

        root["schema_version"] = serde_json::json!(CALIBRATION_SCHEMA_VERSION);
        root["model_version"] = serde_json::json!("future-calibration-model");
        std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
        assert!(load_calibration_seeds(&path, "TRAIN")
            .unwrap_err()
            .contains("unsupported calibration model"));

        let _ = std::fs::remove_file(path);
    }
}
