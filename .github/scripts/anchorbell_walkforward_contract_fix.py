from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CAL = ROOT / "engine" / "src" / "strategy" / "calibration.rs"
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
BACKTEST = ROOT / "engine" / "src" / "bin" / "anchorbell_backtest.rs"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


cal = CAL.read_text(encoding="utf-8")
runtime = RUNTIME.read_text(encoding="utf-8")
backtest = BACKTEST.read_text(encoding="utf-8")

# ---- Calibration state: make update/freeze semantics explicit and transient. ----
cal = replace_once(
    cal,
    '''    pub last_residual_abs_pico_bps: Option<i64>,
    pub last_residual_event_time_ms: Option<u64>,
}''',
    '''    pub last_residual_abs_pico_bps: Option<i64>,
    pub last_residual_event_time_ms: Option<u64>,
    #[serde(skip)]
    frozen: bool,
}''',
    "calibration freeze field",
)
cal = replace_once(
    cal,
    '''            last_residual_abs_pico_bps: None,
            last_residual_event_time_ms: None,
        }
    }

    fn touch(&mut self, time: u64) {''',
    '''            last_residual_abs_pico_bps: None,
            last_residual_event_time_ms: None,
            frozen: false,
        }
    }

    pub fn set_updates_enabled(&mut self, enabled: bool) {
        self.frozen = !enabled;
    }

    fn touch(&mut self, time: u64) {''',
    "calibration update switch",
)
for signature, label in [
    ('''    pub fn observe_market(
        &mut self,
        time: u64,
        ret: Option<i64>,
        spread: Option<i64>,
        residual: Option<i64>,
    ) {
        self.touch(time);''', "market calibration freeze"),
    ('''    pub fn observe_order_placed(&mut self, time: u64) {
        self.touch(time);''', "order calibration freeze"),
    ('''    pub fn observe_fill(&mut self, time: u64, placed_at: u64, quantity: i64, displayed_depth: i64) {
        self.touch(time);''', "fill calibration freeze"),
    ('''    pub fn observe_order_terminal(&mut self, time: u64, placed_at: u64) {
        self.touch(time);''', "terminal calibration freeze"),
    ('''    pub fn observe_markout(&mut self, time: u64, markout: i64) {
        self.touch(time);''', "markout calibration freeze"),
]:
    guarded = signature.replace("        self.touch(time);", "        if self.frozen {\n            return;\n        }\n        self.touch(time);")
    cal = replace_once(cal, signature, guarded, label)

if "frozen_calibration_ignores_validation_observations" not in cal:
    cal += '''

#[cfg(test)]
mod freeze_regression_tests {
    use super::CalibrationState;

    #[test]
    fn frozen_calibration_ignores_validation_observations() {
        let mut state = CalibrationState::new("TEST");
        state.observe_market(10, Some(1), Some(2), Some(3));
        let before_time = state.last_event_time_ms;
        let before_returns = state.return_abs_pico_bps.len();
        let before_orders = state.orders_placed;
        state.set_updates_enabled(false);
        state.observe_market(20, Some(4), Some(5), Some(6));
        state.observe_order_placed(20);
        state.observe_fill(20, 10, 1, 10);
        state.observe_order_terminal(20, 10);
        state.observe_markout(20, 7);
        assert_eq!(state.last_event_time_ms, before_time);
        assert_eq!(state.return_abs_pico_bps.len(), before_returns);
        assert_eq!(state.orders_placed, before_orders);
        assert_eq!(state.fill_events, 0);
        assert_eq!(state.completed_orders, 0);
        assert!(state.adverse_markout_pico_bps.is_empty());
        state.set_updates_enabled(true);
        state.observe_order_placed(30);
        assert_eq!(state.orders_placed, before_orders + 1);
        assert_eq!(state.last_event_time_ms, 30);
    }
}
'''

# ---- Replay contract: frozen OOS seeds and strict temporal separation. ----
runtime = replace_once(
    runtime,
    '''    #[error("replay event symbol is not configured: {0}")]
    ReplaySymbolNotConfigured(String),''',
    '''    #[error("replay event symbol is not configured: {0}")]
    ReplaySymbolNotConfigured(String),
    #[error("calibration seed time {seed_ms} is not strictly before replay start {replay_ms}")]
    CalibrationSeedNotPrior { seed_ms: u64, replay_ms: u64 },''',
    "calibration horizon error",
)
runtime = replace_once(
    runtime,
    '''    pub funding_controller_enabled: bool,
    pub capital_usdt_ticks: Option<i64>,
    pub calibration_seeds: BTreeMap<String, CalibrationState>,''',
    '''    pub funding_controller_enabled: bool,
    pub capital_usdt_ticks: Option<i64>,
    pub calibration_updates_enabled: bool,
    pub calibration_seeds: BTreeMap<String, CalibrationState>,''',
    "ReplayConfig calibration update flag",
)
runtime = replace_once(
    runtime,
    '''    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
}''',
    '''    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
    pub calibration_snapshots: BTreeMap<String, CalibrationSnapshot>,
}''',
    "ReplayEvaluation calibration snapshots",
)
runtime = replace_once(
    runtime,
    '''            funding_controller_enabled: true,
            capital_usdt_ticks: None,
            calibration_seeds: BTreeMap::new(),''',
    '''            funding_controller_enabled: true,
            capital_usdt_ticks: None,
            calibration_updates_enabled: true,
            calibration_seeds: BTreeMap::new(),''',
    "legacy replay calibration mode",
)
runtime = replace_once(
    runtime,
    '''    pub fn restore_calibration_states(&mut self, seeds: &BTreeMap<String, CalibrationState>) {
        for (symbol, seed) in seeds {
            if seed.instrument != symbol.as_str() {
                continue;
            }
            if let Some(state) = self.states.get_mut(symbol) {
                state.calibration = seed.clone();
            }
        }
    }
''',
    '''    pub fn restore_calibration_states(&mut self, seeds: &BTreeMap<String, CalibrationState>) {
        for (symbol, seed) in seeds {
            if seed.instrument != symbol.as_str() {
                continue;
            }
            if let Some(state) = self.states.get_mut(symbol) {
                state.calibration = seed.clone();
            }
        }
    }

    pub fn set_calibration_updates_enabled(&mut self, enabled: bool) {
        for state in self.states.values_mut() {
            state.calibration.set_updates_enabled(enabled);
        }
    }
''',
    "engine calibration update switch",
)
runtime = replace_once(
    runtime,
    '''    engine.restore_calibration_states(&config.calibration_seeds);
    if let Some(allocations) = allocations {''',
    '''    engine.restore_calibration_states(&config.calibration_seeds);
    engine.set_calibration_updates_enabled(config.calibration_updates_enabled);
    if let Some(allocations) = allocations {''',
    "replay calibration freeze application",
)

if "fn validate_calibration_seed_horizon(" not in runtime:
    anchor = '''pub struct ReplayEvaluation {
    pub summary: SimulationSummary,
    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
    pub calibration_snapshots: BTreeMap<String, CalibrationSnapshot>,
}
'''
    helper = '''pub struct ReplayEvaluation {
    pub summary: SimulationSummary,
    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
    pub calibration_snapshots: BTreeMap<String, CalibrationSnapshot>,
}

fn validate_calibration_seed_horizon(
    seeds: &BTreeMap<String, CalibrationState>,
    replay_start_ms: u64,
) -> Result<(), SimulationError> {
    let seed_ms = seeds
        .values()
        .map(|seed| seed.last_event_time_ms)
        .max()
        .unwrap_or(0);
    if seed_ms != 0 && seed_ms >= replay_start_ms {
        return Err(SimulationError::CalibrationSeedNotPrior {
            seed_ms,
            replay_ms: replay_start_ms,
        });
    }
    Ok(())
}
'''
    if anchor not in runtime:
        raise SystemExit("missing anchor: calibration horizon helper")
    runtime = runtime.replace(anchor, helper, 1)

runtime = replace_once(
    runtime,
    '''        let event_timestamp_ms = event_time_ms(&event);
        let timestamp_ms = received_at_ms.unwrap_or(event_timestamp_ms);
        if previous_ms.is_some_and(|previous| timestamp_ms < previous) {''',
    '''        let event_timestamp_ms = event_time_ms(&event);
        let timestamp_ms = received_at_ms.unwrap_or(event_timestamp_ms);
        if previous_ms.is_none() {
            validate_calibration_seed_horizon(&config.calibration_seeds, timestamp_ms)?;
        }
        if previous_ms.is_some_and(|previous| timestamp_ms < previous) {''',
    "strict calibration seed horizon",
)
runtime = replace_once(
    runtime,
    '''    let risk_metrics = config
        .capital_usdt_ticks
        .map(|capital| calculate_risk_metrics(&risk_points, capital));
    Ok(ReplayEvaluation {
        summary,
        risk_metrics,
        risk_samples: risk_points.len(),
    })''',
    '''    let risk_metrics = config
        .capital_usdt_ticks
        .map(|capital| calculate_risk_metrics(&risk_points, capital));
    let calibration_snapshots =
        engine.calibration_snapshots(ppm_to_pico_bps(config.fee_ppm.saturating_mul(2)));
    Ok(ReplayEvaluation {
        summary,
        risk_metrics,
        risk_samples: risk_points.len(),
        calibration_snapshots,
    })''',
    "replay calibration output",
)

# Remove the currently duplicated allocator test attribute while touching the file.
duplicate_test = "    #[test]\n    #[test]\n"
if duplicate_test in runtime:
    if runtime.count(duplicate_test) != 1:
        raise SystemExit("unexpected number of duplicated test attributes")
    runtime = runtime.replace(duplicate_test, "    #[test]\n", 1)

if "calibration_seed_must_strictly_precede_oos_replay" not in runtime:
    runtime += '''

#[cfg(test)]
mod walkforward_regression_tests {
    use super::*;

    #[test]
    fn calibration_seed_must_strictly_precede_oos_replay() {
        let mut seed = CalibrationState::new("TEST");
        seed.last_event_time_ms = 100;
        let seeds = BTreeMap::from([("TEST".to_owned(), seed)]);
        assert!(matches!(
            validate_calibration_seed_horizon(&seeds, 100),
            Err(SimulationError::CalibrationSeedNotPrior { .. })
        ));
        assert!(matches!(
            validate_calibration_seed_horizon(&seeds, 99),
            Err(SimulationError::CalibrationSeedNotPrior { .. })
        ));
        assert!(validate_calibration_seed_horizon(&seeds, 101).is_ok());
    }
}
'''

# ---- Backtest CLI: training output + frozen OOS input with lineage. ----
backtest = replace_once(
    backtest,
    '''    strategy::{universe::instrument_for, CalibrationSnapshot, CalibrationState},''',
    '''    strategy::{
        universe::instrument_for, CalibrationSnapshot, CalibrationState,
        CALIBRATION_MODEL_VERSION, CALIBRATION_SCHEMA_VERSION,
    },''',
    "backtest calibration constants",
)
backtest = replace_once(
    backtest,
    '''    calibration_store: Option<PathBuf>,
    calibration_source_label: String,''',
    '''    calibration_store: Option<PathBuf>,
    calibration_output: Option<PathBuf>,
    calibration_source_label: String,
    freeze_calibration: bool,''',
    "backtest walk-forward args",
)
backtest = replace_once(
    backtest,
    '''    if args.strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
        && calibration_seeds.is_empty()
    {
        fail("M9 replay requires --calibration-store from the training window");
    }
    let input_sha256''',
    '''    if args.strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
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
    let input_sha256''',
    "backtest calibration mode validation",
)
backtest = replace_once(
    backtest,
    '''            funding_controller_enabled: args.funding_controller_enabled,
            capital_usdt_ticks: args.capital_usdt_ticks,
            calibration_seeds,''',
    '''            funding_controller_enabled: args.funding_controller_enabled,
            capital_usdt_ticks: args.capital_usdt_ticks,
            calibration_updates_enabled: !args.freeze_calibration,
            calibration_seeds,''',
    "backtest replay calibration mode",
)
backtest = replace_once(
    backtest,
    '''    if args.require_flat_at_end && !evaluation.summary.flat_at_end {
        fail(format!(
            "backtest ended with unmanaged exposure: position={}, working_orders={}",
            evaluation.summary.current_absolute_position, evaluation.summary.working_orders
        ));
    }
    let report = serde_json::json!({''',
    '''    if args.require_flat_at_end && !evaluation.summary.flat_at_end {
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
        .unwrap_or_else(fail);
        sha256_file(path).unwrap_or_else(|error| {
            fail(format!("cannot hash calibration output {}: {error}", path.display()))
        })
    });
    let report = serde_json::json!({''',
    "backtest calibration output write",
)
backtest = replace_once(
    backtest,
    '''        "calibration_source_label": args.calibration_source_label,
        "calibration_seed_count": args.calibration_store.as_ref().map(|_| 1).unwrap_or(0),''',
    '''        "calibration_source_label": args.calibration_source_label,
        "calibration_model_version": CALIBRATION_MODEL_VERSION,
        "calibration_updates_enabled": !args.freeze_calibration,
        "calibration_seed_count": calibration_seed_count,
        "calibration_snapshot_count": evaluation.calibration_snapshots.len(),
        "calibration_store_sha256": calibration_store_sha256,
        "calibration_output": args.calibration_output,
        "calibration_output_sha256": calibration_output_sha256,''',
    "backtest calibration lineage report",
)

if "fn write_calibration_store(" not in backtest:
    anchor = '''fn load_calibration_seeds(
    path: &std::path::Path,
    source_label: &str,
) -> Result<BTreeMap<String, CalibrationState>, String> {'''
    helper = '''fn write_calibration_store(
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
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!("cannot create calibration output {}: {error}", parent.display())
        })?;
    }
    let bytes = serde_json::to_vec_pretty(&store)
        .map_err(|error| format!("cannot encode calibration output: {error}"))?;
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes).map_err(|error| {
        format!("cannot write calibration output {}: {error}", temporary.display())
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        format!("cannot install calibration output {}: {error}", path.display())
    })?;
    Ok(())
}

fn load_calibration_seeds(
    path: &std::path::Path,
    source_label: &str,
) -> Result<BTreeMap<String, CalibrationState>, String> {'''
    if anchor not in backtest:
        raise SystemExit("missing anchor: calibration store writer")
    backtest = backtest.replace(anchor, helper, 1)

backtest = replace_once(
    backtest,
    '''    let root: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid calibration store {}: {error}", path.display()))?;
    let snapshots = root''',
    '''    let root: serde_json::Value = serde_json::from_slice(&bytes)
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
    let snapshots = root''',
    "calibration store schema validation",
)
backtest = replace_once(
    backtest,
    '''    let mut calibration_store = None;
    let mut calibration_source_label = "F3_m3".to_owned();''',
    '''    let mut calibration_store = None;
    let mut calibration_output = None;
    let mut calibration_source_label = "F3_m3".to_owned();
    let mut freeze_calibration = false;''',
    "backtest calibration parser state",
)
backtest = replace_once(
    backtest,
    '''            "--calibration-store" => {
                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,''',
    '''            "--calibration-store" => {
                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-output" => {
                calibration_output = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,
            "--freeze-calibration" => freeze_calibration = true,''',
    "backtest calibration flags",
)
backtest = replace_once(
    backtest,
    '''        calibration_store,
        calibration_source_label,
        price_scale,''',
    '''        calibration_store,
        calibration_output,
        calibration_source_label,
        freeze_calibration,
        price_scale,''',
    "backtest calibration args construction",
)
backtest = replace_once(
    backtest,
    '''fn sha256_file(path: &PathBuf) -> Result<String, std::io::Error> {''',
    '''fn sha256_file(path: &std::path::Path) -> Result<String, std::io::Error> {''',
    "generic sha256 path",
)
backtest = replace_once(
    backtest,
    '''         --calibration-store PATH --calibration-source-label LABEL --ablate-funding\n\\
''',
    '''         --calibration-store PATH --calibration-output PATH --calibration-source-label LABEL\n\\
         --freeze-calibration --ablate-funding\n\\
''',
    "backtest walk-forward usage",
)

CAL.write_text(cal, encoding="utf-8")
RUNTIME.write_text(runtime, encoding="utf-8")
BACKTEST.write_text(backtest, encoding="utf-8")
print(
    "causal walk-forward calibration contract applied: "
    f"calibration={len(cal.encode('utf-8'))}, "
    f"runtime={len(runtime.encode('utf-8'))}, "
    f"backtest={len(backtest.encode('utf-8'))} bytes"
)
