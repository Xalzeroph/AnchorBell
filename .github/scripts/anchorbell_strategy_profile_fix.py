from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        print(f"already repaired: {label}")
        return text
    raise SystemExit(f"missing anchor: {label}")


# Export the audited profile schema from the simulation facade.
simulation_path = ROOT / "engine" / "src" / "simulation.rs"
simulation = simulation_path.read_text(encoding="utf-8")
if '#[path = "simulation/profile.rs"]' not in simulation:
    simulation = replace_once(
        simulation,
        '#[path = "simulation/orchestration.rs"]\npub mod orchestration;\n',
        '#[path = "simulation/orchestration.rs"]\npub mod orchestration;\n#[path = "simulation/profile.rs"]\npub mod profile;\n',
        "simulation profile module",
    )
if "pub use profile::*;" not in simulation:
    simulation = replace_once(
        simulation,
        "pub use orchestration::*;\n",
        "pub use orchestration::*;\npub use profile::*;\n",
        "simulation profile export",
    )
simulation_path.write_text(simulation, encoding="utf-8")


# Remove the hard-coded M9 calibration population from orchestration and carry
# the declared source/plan identity through validation, seeding and lineage.
batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")
batch = batch.replace(
    '/// avoids mixing policy-specific order selection into a single unlabeled model.\nconst M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";\n',
    '/// avoids mixing policy-specific order selection into a single unlabeled model.\n',
    1,
)

batch = replace_once(
    batch,
    '''    /// Human-readable run generation. Each run writes it into its manifest.\n    pub policy_id: String,\n    pub environment: BinanceEnvironment,\n''',
    '''    /// Human-readable run generation. Each run writes it into its manifest.\n    pub policy_id: String,\n    /// Identity of the experiment matrix that produced `specs`.\n    pub experiment_plan_id: String,\n    /// Declared non-M9 ledger used to warm-start M9 calibration.\n    pub m9_calibration_source_label: String,\n    pub environment: BinanceEnvironment,\n''',
    "batch profile identity fields",
)

batch = replace_once(
    batch,
    '''fn warm_start_m9_from_source(ledgers: &mut [Ledger]) {\n    let seeds = ledgers\n        .iter()\n        .find(|ledger| ledger.spec.label == M9_CALIBRATION_SOURCE_LABEL)\n''',
    '''fn warm_start_m9_from_source(ledgers: &mut [Ledger], source_label: &str) {\n    let seeds = ledgers\n        .iter()\n        .find(|ledger| ledger.spec.label == source_label)\n''',
    "configurable M9 warm-start source",
)

validation_anchor = '''    let mut candidate_identities = BTreeSet::new();\n    if config\n        .specs\n        .iter()\n        .map(candidate_identity)\n        .any(|identity| !candidate_identities.insert(identity))\n    {\n        return Err(SimulationError::InvalidConfig(\n            "batch execution contains duplicate candidate semantics",\n        ));\n    }\n'''
validation_new = validation_anchor + '''    if config.experiment_plan_id.trim().is_empty() {\n        return Err(SimulationError::InvalidConfig(\n            "batch execution requires experiment_plan_id",\n        ));\n    }\n    let source_label = config.m9_calibration_source_label.trim();\n    if source_label.is_empty() {\n        return Err(SimulationError::InvalidConfig(\n            "batch execution requires m9_calibration_source_label",\n        ));\n    }\n    let source = config\n        .specs\n        .iter()\n        .find(|spec| spec.label == source_label)\n        .ok_or(SimulationError::InvalidConfig(\n            "M9 calibration source must be present in the experiment matrix",\n        ))?;\n    if source.variant < SimulationPolicyVariant::M3FillAware\n        || source.variant > SimulationPolicyVariant::M8FundingAware\n    {\n        return Err(SimulationError::InvalidConfig(\n            "M9 calibration source must be a fill-aware M3-M8 ledger",\n        ));\n    }\n'''
batch = replace_once(batch, validation_anchor, validation_new, "M9 calibration source validation")

parameter_anchor = '''        "policy_id": config.policy_id,\n        "entry_threshold_bps": config.entry_threshold_bps,\n'''
parameter_new = '''        "policy_id": config.policy_id,\n        "experiment_plan_id": config.experiment_plan_id,\n        "m9_calibration_source_label": config.m9_calibration_source_label,\n        "specs": config.specs.iter().map(|spec| serde_json::json!({\n            "label": spec.label,\n            "strategy_variant": spec.variant.label(),\n            "ablations": spec.ablations,\n        })).collect::<Vec<_>>(),\n        "entry_threshold_bps": config.entry_threshold_bps,\n        "max_position": config.max_position,\n        "requested_quantity": config.requested_quantity,\n        "max_mark_index_gap_bps": config.max_mark_index_gap_bps,\n        "max_anchor_age_ms": config.max_anchor_age_ms,\n        "quantity_scale": config.quantity_scale,\n        "price_scale": config.price_scale,\n        "max_subscriptions_per_shard": config.max_subscriptions_per_shard,\n        "connect_timeout_ms": config.connect_timeout_ms,\n        "read_timeout_ms": config.read_timeout_ms,\n        "metrics_refresh_ms": config.metrics_refresh_ms,\n        "index_anchor_refresh_ms": config.index_anchor_refresh_ms,\n        "fx_refresh_ms": config.fx_refresh_ms,\n        "fx_max_age_ms": config.fx_max_age_ms,\n        "quote_reprice_min_interval_ms": config.quote_reprice_min_interval_ms,\n        "checkpoint_interval_ms": config.checkpoint_interval_ms,\n        "entry_threshold_bps": config.entry_threshold_bps,\n'''
batch = replace_once(batch, parameter_anchor, parameter_new, "complete profile parameter lineage")

manifest_anchor = '''        "policy_id": config.policy_id,\n        "created_at_ms": manifest_created_at_ms,\n'''
manifest_new = '''        "policy_id": config.policy_id,\n        "experiment_plan_id": config.experiment_plan_id,\n        "m9_calibration_source_label": config.m9_calibration_source_label,\n        "created_at_ms": manifest_created_at_ms,\n'''
batch = replace_once(batch, manifest_anchor, manifest_new, "profile manifest lineage")

batch = replace_once(
    batch,
    '''        let calibration_source = if spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc\n        {\n            M9_CALIBRATION_SOURCE_LABEL\n        } else {\n            spec.label.as_str()\n        };\n''',
    '''        let calibration_source = if spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc\n        {\n            config.m9_calibration_source_label.as_str()\n        } else {\n            spec.label.as_str()\n        };\n''',
    "M9 seed source",
)

batch = replace_once(
    batch,
    "                    warm_start_m9_from_source(&mut ledgers);\n",
    "                    warm_start_m9_from_source(&mut ledgers, &config.m9_calibration_source_label);\n",
    "M9 runtime warm-start source",
)

batch_path.write_text(batch, encoding="utf-8")


# The batch entrypoint is profile-only. This removes the second experiment-plan
# switch (`--include-m9`) and keeps production startup identical to the audited
# server contract. The lock/process guard below the prefix is preserved.
bin_path = ROOT / "engine" / "src" / "bin" / "anchorbell_simulation_batch.rs"
binary = bin_path.read_text(encoding="utf-8")
guard_index = binary.index("struct SimulationBatchInstanceGuard")
suffix = binary[guard_index:]
new_prefix = r'''use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    process,
    time::Duration,
};

use anchorbell_engine::{
    analytics_evidence::EvidenceConfig,
    execution::SessionCheckpoint,
    platform::RuntimeProfile,
    runtime::{
        health_reporter::{timestamp_ms, RuntimeHealthReporter},
        run_registry::{RunMode, RunRegistry, RunSpec, RunStatus, RUN_REGISTRY_SCHEMA_VERSION},
    },
    simulation::{
        allocate_positions, load_index_anchor_set,
        orchestration::{run, SimulationBatchConfig, SimulationBatchSpec},
        PositionMode, SimulationStrategyProfile,
    },
};

#[derive(Debug)]
struct Args {
    strategy_profile: PathBuf,
}

fn main() {
    let args = parse_args().unwrap_or_else(|error| fail(error));
    let profile = SimulationStrategyProfile::load(&args.strategy_profile)
        .unwrap_or_else(|error| fail(error));
    let environment = profile
        .environment_value()
        .unwrap_or_else(|error| fail(error));
    let capital_usdt = profile
        .capital_usdt_ticks()
        .unwrap_or_else(|error| fail(error));
    let _instance_guard =
        claim_single_simulation_batch_instance().unwrap_or_else(|error| fail(error));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| fail(format!("cannot create runtime: {error}")));
    runtime.block_on(async move {
        let mut health = RuntimeHealthReporter::new("target/batch-runtime-audit.jsonl");
        health
            .start(RuntimeProfile::Batch, timestamp_ms())
            .await
            .unwrap_or_else(|error| fail(format!("batch health bootstrap failed: {error}")));

        let execution = profile.execution.clone();
        let experiment_plan = profile.experiment_plan.clone();
        let run_id = format!("batch-{}-{}", profile.policy_id, timestamp_ms());
        let strategies = experiment_plan
            .experiments
            .iter()
            .map(|experiment| experiment.strategy.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let ablations = experiment_plan
            .experiments
            .iter()
            .flat_map(|experiment| experiment.ablations.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let registry = RunRegistry::new(profile.output_root.join("runs"));
        registry
            .create(
                RunSpec {
                    schema_version: RUN_REGISTRY_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    mode: RunMode::Simulation,
                    policy_id: profile.policy_id.clone(),
                    capital_currency: "USDT".into(),
                    capital_minor_units: capital_usdt,
                    universe: "frozen-close-ah".into(),
                    strategies,
                    ablations,
                    checkpoint_interval_ms: execution.checkpoint_interval_ms,
                    max_stale_ms: 5_000,
                    auto_restart: true,
                    build_identity: env!("CARGO_PKG_VERSION").into(),
                },
                timestamp_ms(),
            )
            .unwrap_or_else(|error| fail(format!("run registry create failed: {error}")));
        registry
            .claim(
                &run_id,
                format!("batch-{}-{}", std::process::id(), profile.policy_id),
                timestamp_ms(),
            )
            .unwrap_or_else(|error| fail(format!("run registry claim failed: {error}")));
        let checkpoint_path = profile
            .output_root
            .join("runs")
            .join(&run_id)
            .join("checkpoint.json");
        SessionCheckpoint::new(&run_id, "simulation", "PORTFOLIO")
            .write_atomic(&checkpoint_path)
            .unwrap_or_else(|error| fail(format!("initial checkpoint failed: {error}")));
        registry
            .checkpoint(
                &run_id,
                checkpoint_path.display().to_string(),
                timestamp_ms(),
            )
            .unwrap_or_else(|error| fail(format!("run checkpoint registration failed: {error}")));

        // Bootstrap from current index/FX references. Profile validation requires
        // index anchors, and transient public-data failures fail closed by retrying.
        let anchors = loop {
            let result = tokio::time::timeout(
                Duration::from_millis(execution.read_timeout_ms),
                load_index_anchor_set(
                    environment,
                    &profile.symbols,
                    execution.price_scale,
                    None,
                ),
            )
            .await;
            match result {
                Ok(Ok(set)) => break set.anchors,
                Ok(Err(error)) => {
                    eprintln!("index anchor bootstrap unavailable: {error}; retrying in 5s");
                }
                Err(_) => {
                    eprintln!(
                        "index anchor bootstrap timed out after {}ms; retrying in 5s",
                        execution.read_timeout_ms
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        };
        let anchors = anchors
            .into_iter()
            .filter(|(symbol, _)| {
                profile
                    .symbols
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(symbol))
            })
            .collect::<BTreeMap<_, _>>();
        let modes = BTreeMap::<String, PositionMode>::new();
        let allocations = allocate_positions(
            &anchors,
            capital_usdt,
            &modes,
            execution.quantity_scale,
        )
        .unwrap_or_else(|error| {
            fail(format!("cannot allocate simulation-batch capital: {error}"))
        });
        let specs = experiment_plan
            .runtime_specs_with_ablations()
            .unwrap_or_else(|error| fail(format!("invalid experiment plan: {error}")))
            .into_iter()
            .map(|(label, variant, ablations)| SimulationBatchSpec {
                label,
                variant,
                ablations,
            })
            .collect();

        registry
            .transition(&run_id, RunStatus::Running, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry running failed: {error}")));
        registry
            .heartbeat(&run_id, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry heartbeat failed: {error}")));
        let heartbeat_task = registry.spawn_heartbeat(
            run_id.clone(),
            execution.checkpoint_interval_ms.max(1_000),
        );
        let config = SimulationBatchConfig {
            policy_id: profile.policy_id,
            experiment_plan_id: experiment_plan.plan_id,
            m9_calibration_source_label: profile.m9_calibration_source_label,
            environment,
            symbols: profile.symbols,
            anchors,
            entry_threshold_bps: execution.entry_threshold_bps,
            threshold_scale_ppm: execution.threshold_scale_ppm,
            max_position: execution.max_position,
            requested_quantity: execution.requested_quantity,
            max_mark_index_gap_bps: execution.max_mark_index_gap_bps,
            max_anchor_age_ms: execution.max_anchor_age_ms,
            fee_ppm: execution.fee_ppm,
            quantity_scale: execution.quantity_scale,
            price_scale: execution.price_scale,
            position_allocations: Some(allocations),
            output_root: profile.output_root,
            specs,
            max_subscriptions_per_shard: execution.max_subscriptions_per_shard,
            connect_timeout_ms: execution.connect_timeout_ms,
            read_timeout_ms: execution.read_timeout_ms,
            metrics_refresh_ms: execution.metrics_refresh_ms,
            index_anchor_refresh_ms: execution.index_anchor_refresh_ms,
            fx_refresh_ms: execution.fx_refresh_ms,
            fx_max_age_ms: execution.fx_max_age_ms,
            queue_ahead: execution.queue_ahead,
            trade_through: execution.trade_through,
            market_to_decision_ms: execution.market_to_decision_ms,
            decision_to_exchange_ms: execution.decision_to_exchange_ms,
            cancel_to_exchange_ms: execution.cancel_to_exchange_ms,
            quote_reprice_min_interval_ms: execution.quote_reprice_min_interval_ms,
            dynamic_capital_refresh_ms: execution.dynamic_capital_refresh_ms,
            depth_snapshot_limit: execution.depth_snapshot_limit,
            checkpoint_path: Some(checkpoint_path),
            checkpoint_session_id: Some(run_id.clone()),
            checkpoint_interval_ms: execution.checkpoint_interval_ms,
            duration_secs: profile.duration_secs,
            validation_fold_id: profile.fold_id,
            validation_stress_profile: profile.stress_profile,
            evidence: EvidenceConfig::default(),
        };
        let result = match run(config).await {
            Ok(result) => result,
            Err(error) => {
                let reason = error.to_string();
                let now_ms = timestamp_ms();
                let _ = health.halted("simulation.runtime", now_ms, &reason).await;
                let _ = registry.fail(&run_id, reason.clone(), now_ms);
                fail(format!("batch execution failed: {reason}"));
            }
        };
        heartbeat_task.abort();
        registry
            .transition(&run_id, RunStatus::Completed, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry completion failed: {error}")));
        health
            .ready("simulation.runtime", timestamp_ms())
            .await
            .unwrap_or_else(|error| fail(format!("batch health completion failed: {error}")));
        println!(
            "{}",
            serde_json::to_string_pretty(&result).expect("lab result is serializable")
        );
    });
}

fn parse_args() -> Result<Args, String> {
    let mut strategy_profile = None;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--strategy-profile" => {
                if strategy_profile.is_some() {
                    return Err("--strategy-profile may be supplied only once".to_owned());
                }
                strategy_profile = Some(PathBuf::from(next(&mut args, &flag)?));
            }
            "--include-m9" => {
                return Err(
                    "--include-m9 was removed; declare M9 in the strategy profile experiment_plan"
                        .to_owned(),
                );
            }
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            other => {
                return Err(format!(
                    "direct batch option {other} was removed; use --strategy-profile"
                ));
            }
        }
    }
    Ok(Args {
        strategy_profile: strategy_profile.ok_or("missing --strategy-profile")?,
    })
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value"))
}

'''
binary = new_prefix + suffix
old_usage_start = binary.index("fn print_usage() {")
old_fail_start = binary.index("fn fail(", old_usage_start)
new_usage = '''fn print_usage() {
    eprintln!("usage: anchorbell_simulation_batch --strategy-profile PROFILE.json");
    eprintln!(
        "the profile is the single source of truth for the experiment matrix, M9 calibration source, capital, execution economics, latency, and validation-fold identity"
    );
}

'''
binary = binary[:old_usage_start] + new_usage + binary[old_fail_start:]
bin_path.write_text(binary, encoding="utf-8")


# Deployment uses exactly the same profile contract that is validated in code.
unit_path = ROOT / "deploy" / "systemd" / "anchorbell-simulation.service"
unit = unit_path.read_text(encoding="utf-8")
exec_lines = [line for line in unit.splitlines() if line.startswith("ExecStart=")]
if len(exec_lines) != 1:
    raise SystemExit("systemd unit must contain exactly one ExecStart")
unit = unit.replace(
    exec_lines[0],
    "ExecStart=/opt/anchorbell/target/release/anchorbell_simulation_batch --strategy-profile /opt/anchorbell/config/anchorbell-simulation.json",
    1,
)
unit_path.write_text(unit, encoding="utf-8")

print("strategy-profile convergence repair complete")
