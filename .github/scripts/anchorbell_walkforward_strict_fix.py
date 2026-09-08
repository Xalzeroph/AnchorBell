from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
BACKTEST = ROOT / "engine" / "src" / "bin" / "anchorbell_backtest.rs"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


# Replay exposes the final causal calibration state so a TRAINING replay can
# persist it. OOS replays consume a previously persisted store; they never
# write a replacement store in the same formal validation invocation.
runtime = RUNTIME.read_text(encoding="utf-8")
runtime = replace_once(
    runtime,
    '''pub struct ReplayEvaluation {
    pub summary: SimulationSummary,
    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
}
''',
    '''pub struct ReplayEvaluation {
    pub summary: SimulationSummary,
    pub risk_metrics: Option<RiskMetrics>,
    pub risk_samples: usize,
    /// Final causal calibration state. Only training replays may persist it.
    pub calibration_snapshots: BTreeMap<String, CalibrationSnapshot>,
}
''',
    "ReplayEvaluation calibration snapshots",
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
    })
''',
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
    })
''',
    "replay calibration export",
)
RUNTIME.write_text(runtime, encoding="utf-8")


backtest = BACKTEST.read_text(encoding="utf-8")
backtest = replace_once(
    backtest,
    '''    strategy::{universe::instrument_for, CalibrationSnapshot, CalibrationState},
''',
    '''    strategy::{
        universe::instrument_for, CalibrationSnapshot, CalibrationState,
        CALIBRATION_MODEL_VERSION, CALIBRATION_SCHEMA_VERSION,
    },
''',
    "calibration schema imports",
)
backtest = replace_once(
    backtest,
    '''    calibration_store: Option<PathBuf>,
    calibration_source_label: String,
''',
    '''    calibration_store: Option<PathBuf>,
    calibration_output: Option<PathBuf>,
    calibration_source_label: String,
    formal_oos: bool,
''',
    "backtest calibration args",
)

old_bootstrap = '''    let calibration_seeds = args
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
'''
new_bootstrap = '''    let input_sha256 = sha256_file(&args.input)
        .unwrap_or_else(|error| fail(format!("cannot hash input: {error}")));
    let calibration_bundle = args
        .calibration_store
        .as_deref()
        .map(|path| load_calibration_seeds(path, &args.calibration_source_label))
        .transpose()
        .unwrap_or_else(|error| fail(error));
    let calibration_seed_count = calibration_bundle
        .as_ref()
        .map(|bundle| bundle.seeds.len())
        .unwrap_or(0);
    if args.formal_oos {
        if let Some(bundle) = calibration_bundle.as_ref() {
            let training_digest = bundle.training_input_sha256.as_deref().unwrap_or_else(|| {
                fail("formal OOS calibration store is missing training input lineage")
            });
            if training_digest == input_sha256 {
                fail("formal OOS input is identical to the calibration training input");
            }
        }
    }
    let calibration_seeds = calibration_bundle
        .map(|bundle| bundle.seeds)
        .unwrap_or_default();
    if args.strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
        && calibration_seeds.is_empty()
    {
        fail("M9 replay requires --calibration-store from the training window");
    }
'''
backtest = replace_once(backtest, old_bootstrap, new_bootstrap, "walk-forward bootstrap")

backtest = replace_once(
    backtest,
    '''    if args.require_flat_at_end && !evaluation.summary.flat_at_end {
        fail(format!(
            "backtest ended with unmanaged exposure: position={}, working_orders={}",
            evaluation.summary.current_absolute_position, evaluation.summary.working_orders
        ));
    }
    let report = serde_json::json!({
''',
    '''    if args.require_flat_at_end && !evaluation.summary.flat_at_end {
        fail(format!(
            "backtest ended with unmanaged exposure: position={}, working_orders={}",
            evaluation.summary.current_absolute_position, evaluation.summary.working_orders
        ));
    }
    if let Some(path) = args.calibration_output.as_deref() {
        write_calibration_store(
            path,
            &args.calibration_source_label,
            &input_sha256,
            &evaluation.calibration_snapshots,
        )
        .unwrap_or_else(fail);
    }
    let report = serde_json::json!({
''',
    "training calibration write",
)
backtest = replace_once(
    backtest,
    '''        "calibration_source_label": args.calibration_source_label,
        "calibration_seed_count": args.calibration_store.as_ref().map(|_| 1).unwrap_or(0),
''',
    '''        "calibration_source_label": args.calibration_source_label,
        "calibration_seed_count": calibration_seed_count,
        "calibration_snapshot_count": evaluation.calibration_snapshots.len(),
        "calibration_output": args.calibration_output,
        "validation_role": if args.formal_oos {
            "oos"
        } else if args.calibration_output.is_some() {
            "training"
        } else {
            "exploratory"
        },
''',
    "walk-forward report lineage",
)

# Persist only training-origin calibration, with exact data lineage. Formal OOS
# later requires this provenance and rejects the identical market-data file.
loader_anchor = '''fn load_calibration_seeds(
    path: &std::path::Path,
    source_label: &str,
) -> Result<BTreeMap<String, CalibrationState>, String> {
'''
loader_replacement = '''#[derive(Debug)]
struct CalibrationSeedBundle {
    seeds: BTreeMap<String, CalibrationState>,
    training_input_sha256: Option<String>,
}

fn write_calibration_store(
    path: &std::path::Path,
    source_label: &str,
    training_input_sha256: &str,
    snapshots: &BTreeMap<String, CalibrationSnapshot>,
) -> Result<(), String> {
    if source_label.trim().is_empty() || training_input_sha256.trim().is_empty() {
        return Err("training calibration lineage must be non-empty".to_owned());
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
        "provenance": {
            "role": "training",
            "source_label": source_label,
            "training_input_sha256": training_input_sha256,
        },
        "snapshots": keyed,
    });
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create calibration output {}: {error}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(&store)
        .map_err(|error| format!("cannot encode calibration output: {error}"))?;
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes)
        .map_err(|error| format!("cannot write calibration output {}: {error}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .map_err(|error| format!("cannot install calibration output {}: {error}", path.display()))?;
    Ok(())
}

fn load_calibration_seeds(
    path: &std::path::Path,
    source_label: &str,
) -> Result<CalibrationSeedBundle, String> {
'''
backtest = replace_once(backtest, loader_anchor, loader_replacement, "calibration bundle loader")

# Capture optional training provenance without breaking legacy stores in
# exploratory mode. Formal OOS enforces its presence in main().
backtest = replace_once(
    backtest,
    '''    let snapshots = root
        .get("snapshots")
''',
    '''    let provenance = root.get("provenance");
    if let Some(stored_source) = provenance
        .and_then(|value| value.get("source_label"))
        .and_then(serde_json::Value::as_str)
    {
        if stored_source != source_label {
            return Err(format!(
                "calibration provenance source {stored_source} does not match requested {source_label}"
            ));
        }
    }
    let training_input_sha256 = provenance
        .and_then(|value| value.get("training_input_sha256"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let snapshots = root
        .get("snapshots")
''',
    "calibration provenance read",
)
backtest = replace_once(
    backtest,
    '''    Ok(seeds)
}

fn parse_args() -> Result<Args, String> {
''',
    '''    Ok(CalibrationSeedBundle {
        seeds,
        training_input_sha256,
    })
}

fn parse_args() -> Result<Args, String> {
''',
    "calibration bundle return",
)

backtest = replace_once(
    backtest,
    '''    let mut calibration_store = None;
    let mut calibration_source_label = "F3_m3".to_owned();
''',
    '''    let mut calibration_store = None;
    let mut calibration_output = None;
    let mut calibration_source_label = "F3_m3".to_owned();
    let mut formal_oos = false;
''',
    "walk-forward parser state",
)
backtest = replace_once(
    backtest,
    '''            "--calibration-store" => {
                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,
''',
    '''            "--calibration-store" => {
                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-output" => {
                calibration_output = Some(PathBuf::from(next(&mut args, &flag)?))
            }
            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,
            "--formal-oos" => formal_oos = true,
''',
    "walk-forward parser flags",
)

# Insert leakage rules before the existing funding-ablation validation.
backtest = replace_once(
    backtest,
    '''    if !funding_controller_enabled && strategy_variant != SimulationPolicyVariant::M8FundingAware {
''',
    '''    if calibration_output.is_some() && calibration_store.is_some() {
        return Err("training --calibration-output cannot consume --calibration-store".to_owned());
    }
    if formal_oos && calibration_output.is_some() {
        return Err("formal OOS is read-only with respect to calibration".to_owned());
    }
    if calibration_output.is_some()
        && strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
    {
        return Err("M9 cannot self-train a calibration store; train a declared source strategy first".to_owned());
    }
    if !funding_controller_enabled && strategy_variant != SimulationPolicyVariant::M8FundingAware {
''',
    "walk-forward leakage rules",
)
backtest = replace_once(
    backtest,
    '''        calibration_store,
        calibration_source_label,
''',
    '''        calibration_store,
        calibration_output,
        calibration_source_label,
        formal_oos,
''',
    "walk-forward args construction",
)

# Keep help self-documenting if the existing phrase is still present.
backtest = backtest.replace(
    "--calibration-store PATH --calibration-source-label LABEL",
    "--calibration-store PATH --calibration-output PATH --calibration-source-label LABEL --formal-oos",
)

# Remove one known duplicated test attribute left by an earlier transformer.
backtest = backtest.replace("#[test]\n    #[test]\n", "#[test]\n", 1)

BACKTEST.write_text(backtest, encoding="utf-8")
print("strict training-to-OOS calibration lineage repair complete")
