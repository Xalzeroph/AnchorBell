from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count == 0:
        if new in text:
            print(f"already patched: {path}")
            return
        raise SystemExit(f"anchor not found in {path}: {old[:180]!r}")
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}")
    p.write_text(text.replace(old, new, 1))
    print(f"patched: {path}")


runtime = "engine/src/simulation/runtime.rs"
replace_once(
    runtime,
    '''pub struct ReplayEvaluation {\n    pub summary: SimulationSummary,\n    pub risk_metrics: Option<RiskMetrics>,\n    pub risk_samples: usize,\n}''',
    '''pub struct ReplayEvaluation {\n    pub summary: SimulationSummary,\n    pub risk_metrics: Option<RiskMetrics>,\n    pub risk_samples: usize,\n    /// Final replay calibration state. Training folds may persist this for a\n    /// later OOS fold; validation folds must never feed it back into themselves.\n    pub calibration_snapshots: BTreeMap<String, CalibrationSnapshot>,\n}''',
)
replace_once(
    runtime,
    '''    let risk_metrics = config\n        .capital_usdt_ticks\n        .map(|capital| calculate_risk_metrics(&risk_points, capital));\n    Ok(ReplayEvaluation {\n        summary,\n        risk_metrics,\n        risk_samples: risk_points.len(),\n    })''',
    '''    let risk_metrics = config\n        .capital_usdt_ticks\n        .map(|capital| calculate_risk_metrics(&risk_points, capital));\n    let calibration_snapshots =\n        engine.calibration_snapshots(ppm_to_pico_bps(config.fee_ppm.saturating_mul(2)));\n    Ok(ReplayEvaluation {\n        summary,\n        risk_metrics,\n        risk_samples: risk_points.len(),\n        calibration_snapshots,\n    })''',
)

backtest = "engine/src/bin/anchorbell_backtest.rs"
replace_once(
    backtest,
    '''    strategy::{universe::instrument_for, CalibrationSnapshot, CalibrationState},''',
    '''    strategy::{\n        universe::instrument_for, CalibrationSnapshot, CalibrationState,\n        CALIBRATION_MODEL_VERSION, CALIBRATION_SCHEMA_VERSION,\n    },''',
)
replace_once(
    backtest,
    '''    calibration_store: Option<PathBuf>,\n    calibration_source_label: String,''',
    '''    calibration_store: Option<PathBuf>,\n    calibration_output: Option<PathBuf>,\n    calibration_source_label: String,''',
)
replace_once(
    backtest,
    '''    if args.strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc\n        && calibration_seeds.is_empty()\n    {\n        fail("M9 replay requires --calibration-store from the training window");\n    }\n    let input_sha256''',
    '''    if args.strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc\n        && calibration_seeds.is_empty()\n    {\n        fail("M9 replay requires --calibration-store from the training window");\n    }\n    let calibration_seed_count = calibration_seeds.len();\n    let input_sha256''',
)
replace_once(
    backtest,
    '''    if args.require_flat_at_end && !evaluation.summary.flat_at_end {\n        fail(format!(\n            "backtest ended with unmanaged exposure: position={}, working_orders={}",\n            evaluation.summary.current_absolute_position, evaluation.summary.working_orders\n        ));\n    }\n    let report = serde_json::json!({''',
    '''    if args.require_flat_at_end && !evaluation.summary.flat_at_end {\n        fail(format!(\n            "backtest ended with unmanaged exposure: position={}, working_orders={}",\n            evaluation.summary.current_absolute_position, evaluation.summary.working_orders\n        ));\n    }\n    if let Some(path) = args.calibration_output.as_deref() {\n        write_calibration_store(\n            path,\n            &args.calibration_source_label,\n            &evaluation.calibration_snapshots,\n        )\n        .unwrap_or_else(fail);\n    }\n    let report = serde_json::json!({''',
)
replace_once(
    backtest,
    '''        "calibration_source_label": args.calibration_source_label,\n        "calibration_seed_count": args.calibration_store.as_ref().map(|_| 1).unwrap_or(0),''',
    '''        "calibration_source_label": args.calibration_source_label,\n        "calibration_seed_count": calibration_seed_count,\n        "calibration_snapshot_count": evaluation.calibration_snapshots.len(),\n        "calibration_output": args.calibration_output,''',
)
replace_once(
    backtest,
    '''fn load_calibration_seeds(\n    path: &std::path::Path,''',
    '''fn write_calibration_store(\n    path: &std::path::Path,\n    source_label: &str,\n    snapshots: &BTreeMap<String, CalibrationSnapshot>,\n) -> Result<(), String> {\n    if source_label.trim().is_empty() {\n        return Err("calibration source label cannot be empty".to_owned());\n    }\n    if snapshots.is_empty() {\n        return Err("training replay produced no calibration snapshots".to_owned());\n    }\n    let keyed = snapshots\n        .iter()\n        .map(|(symbol, snapshot)| {\n            (\n                format!("{source_label}::{symbol}"),\n                snapshot.clone(),\n            )\n        })\n        .collect::<BTreeMap<_, _>>();\n    let updated_at_event_time_ms = snapshots\n        .values()\n        .map(|snapshot| snapshot.window_end_event_time_ms)\n        .max()\n        .unwrap_or(0);\n    let store = serde_json::json!({\n        "schema_version": CALIBRATION_SCHEMA_VERSION,\n        "model_version": CALIBRATION_MODEL_VERSION,\n        "updated_at_event_time_ms": updated_at_event_time_ms,\n        "snapshots": keyed,\n    });\n    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {\n        std::fs::create_dir_all(parent).map_err(|error| {\n            format!("cannot create calibration output {}: {error}", parent.display())\n        })?;\n    }\n    let bytes = serde_json::to_vec_pretty(&store)\n        .map_err(|error| format!("cannot encode calibration output: {error}"))?;\n    let temporary = path.with_extension("tmp");\n    std::fs::write(&temporary, bytes).map_err(|error| {\n        format!("cannot write calibration output {}: {error}", temporary.display())\n    })?;\n    std::fs::rename(&temporary, path).map_err(|error| {\n        format!("cannot install calibration output {}: {error}", path.display())\n    })?;\n    Ok(())\n}\n\nfn load_calibration_seeds(\n    path: &std::path::Path,''',
)
replace_once(
    backtest,
    '''    let mut calibration_store = None;\n    let mut calibration_source_label = "F3_m3".to_owned();''',
    '''    let mut calibration_store = None;\n    let mut calibration_output = None;\n    let mut calibration_source_label = "F3_m3".to_owned();''',
)
replace_once(
    backtest,
    '''            "--calibration-store" => {\n                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))\n            }\n            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,''',
    '''            "--calibration-store" => {\n                calibration_store = Some(PathBuf::from(next(&mut args, &flag)?))\n            }\n            "--calibration-output" => {\n                calibration_output = Some(PathBuf::from(next(&mut args, &flag)?))\n            }\n            "--calibration-source-label" => calibration_source_label = next(&mut args, &flag)?,''',
)
replace_once(
    backtest,
    '''        calibration_store,\n        calibration_source_label,''',
    '''        calibration_store,\n        calibration_output,\n        calibration_source_label,''',
)
replace_once(
    backtest,
    '''         --calibration-store PATH --calibration-source-label LABEL --ablate-funding\\n\\''',
    '''         --calibration-store PATH --calibration-output PATH\\n\\\n         --calibration-source-label LABEL --ablate-funding\\n\\''',
)
