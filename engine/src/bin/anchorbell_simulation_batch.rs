use std::{
    collections::BTreeMap,
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
        PositionMode,
    },
    strategy::StrategyProfile,
};

#[derive(Debug)]
struct Args {
    strategy_profile: PathBuf,
}

fn main() {
    let args = parse_args().unwrap_or_else(|error| fail(error));
    let profile = StrategyProfile::load(&args.strategy_profile).unwrap_or_else(|error| fail(error));
    let environment = profile.environment().unwrap_or_else(|error| fail(error));
    let capital_usdt = profile
        .capital_usdt_ticks()
        .unwrap_or_else(|error| fail(error));
    let experiment_plan = profile
        .experiment_plan()
        .unwrap_or_else(|error| fail(error));
    let specs = experiment_plan
        .runtime_specs_with_ablations()
        .unwrap_or_else(|error| fail(format!("invalid experiment plan: {error}")))
        .into_iter()
        .map(|(label, variant, ablations)| SimulationBatchSpec {
            label,
            variant,
            ablations,
        })
        .collect::<Vec<_>>();
    let mut strategies = Vec::<String>::new();
    let mut ablations = Vec::<String>::new();
    for experiment in &profile.experiments {
        if !strategies.contains(&experiment.strategy) {
            strategies.push(experiment.strategy.clone());
        }
        for ablation in &experiment.ablations {
            if !ablations.contains(ablation) {
                ablations.push(ablation.clone());
            }
        }
    }

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

        let policy_id = profile.policy_id.clone();
        let output_root = profile.output_root.clone();
        let run_id = format!("batch-{policy_id}-{}", timestamp_ms());
        let registry = RunRegistry::new(output_root.join("runs"));
        registry
            .create(
                RunSpec {
                    schema_version: RUN_REGISTRY_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    mode: RunMode::Simulation,
                    policy_id: policy_id.clone(),
                    capital_currency: "USDT".into(),
                    capital_minor_units: capital_usdt,
                    universe: "strategy-profile-tradfi".into(),
                    strategies,
                    ablations,
                    checkpoint_interval_ms: profile.checkpoint_interval_ms,
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
                format!("batch-{}-{policy_id}", std::process::id()),
                timestamp_ms(),
            )
            .unwrap_or_else(|error| fail(format!("run registry claim failed: {error}")));
        let checkpoint_path = output_root
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

        // The profile is authoritative, but index anchors remain runtime data.
        // Never reuse a file-backed anchor across process restarts.
        let anchor_bootstrap_timeout_ms = profile
            .connect_timeout_ms
            .saturating_add(profile.read_timeout_ms)
            .max(1_000);
        let anchors = loop {
            let result = tokio::time::timeout(
                Duration::from_millis(anchor_bootstrap_timeout_ms),
                load_index_anchor_set(environment, &profile.symbols, profile.price_scale, None),
            )
            .await;
            match result {
                Ok(Ok(set)) => break set.anchors,
                Ok(Err(error)) => {
                    eprintln!("index anchor bootstrap unavailable: {error}; retrying in 5s");
                }
                Err(_) => {
                    eprintln!(
                        "index anchor bootstrap timed out after {anchor_bootstrap_timeout_ms}ms; retrying in 5s"
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
        let allocations = allocate_positions(
            &anchors,
            capital_usdt,
            &BTreeMap::<String, PositionMode>::new(),
            profile.quantity_scale,
        )
        .unwrap_or_else(|error| fail(format!("cannot allocate simulation-batch capital: {error}")));

        registry
            .transition(&run_id, RunStatus::Running, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry running failed: {error}")));
        registry
            .heartbeat(&run_id, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry heartbeat failed: {error}")));
        let heartbeat_task = registry.spawn_heartbeat(
            run_id.clone(),
            profile.checkpoint_interval_ms.max(1_000),
        );

        let config = SimulationBatchConfig {
            policy_id: policy_id.clone(),
            environment,
            symbols: profile.symbols.clone(),
            anchors,
            entry_threshold_bps: profile.entry_threshold_bps,
            threshold_scale_ppm: profile.threshold_scale_ppm,
            max_position: profile.max_position,
            requested_quantity: profile.requested_quantity,
            max_mark_index_gap_bps: profile.max_mark_index_gap_bps,
            max_anchor_age_ms: profile.max_anchor_age_ms,
            fee_ppm: profile.fee_ppm,
            quantity_scale: profile.quantity_scale,
            price_scale: profile.price_scale,
            position_allocations: Some(allocations),
            portfolio_drawdown_soft_limit_bps: profile.portfolio_drawdown_soft_limit_bps,
            portfolio_drawdown_hard_limit_bps: profile.portfolio_drawdown_hard_limit_bps,
            output_root,
            specs,
            m9_calibration_source_label: profile.m9_calibration_source_label.clone(),
            max_subscriptions_per_shard: profile.max_subscriptions_per_shard,
            connect_timeout_ms: profile.connect_timeout_ms,
            read_timeout_ms: profile.read_timeout_ms,
            metrics_refresh_ms: profile.metrics_refresh_ms,
            index_anchor_refresh_ms: profile.index_anchor_refresh_ms,
            fx_refresh_ms: profile.fx_refresh_ms,
            fx_max_age_ms: profile.fx_max_age_ms,
            queue_ahead: profile.queue_ahead,
            trade_through: profile.trade_through,
            market_to_decision_ms: profile.market_to_decision_ms,
            decision_to_exchange_ms: profile.decision_to_exchange_ms,
            cancel_to_exchange_ms: profile.cancel_to_exchange_ms,
            quote_reprice_min_interval_ms: profile.quote_reprice_min_interval_ms,
            dynamic_capital_refresh_ms: profile.dynamic_capital_refresh_ms,
            depth_snapshot_limit: profile.depth_snapshot_limit,
            checkpoint_path: Some(checkpoint_path),
            checkpoint_session_id: Some(run_id.clone()),
            checkpoint_interval_ms: profile.checkpoint_interval_ms,
            duration_secs: profile.duration_secs,
            validation_fold_id: profile.validation_fold_id.clone(),
            validation_stress_profile: profile.validation_stress_profile.clone(),
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
                strategy_profile =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        "--strategy-profile requires a path".to_owned()
                    })?));
            }
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            other => {
                return Err(format!(
                    "unknown option {other}; batch runtime is profile-authoritative"
                ))
            }
        }
    }
    Ok(Args {
        strategy_profile: strategy_profile
            .ok_or_else(|| "--strategy-profile is required".to_owned())?,
    })
}

struct SimulationBatchInstanceGuard {
    path: PathBuf,
    pid: u32,
}

impl Drop for SimulationBatchInstanceGuard {
    fn drop(&mut self) {
        let owned_by_me = fs::read_to_string(&self.path)
            .ok()
            .and_then(|contents| contents.lines().next().map(str::to_owned))
            .and_then(|value| value.parse::<u32>().ok())
            == Some(self.pid);
        if owned_by_me {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn claim_single_simulation_batch_instance() -> Result<SimulationBatchInstanceGuard, String> {
    let path = env::temp_dir().join("anchorbell-simulation-batch.lock");
    let pid = process::id();

    for _ in 0..3 {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "{pid}").map_err(|error| {
                    let _ = fs::remove_file(&path);
                    format!("cannot write simulation-batch instance lock: {error}")
                })?;
                return Ok(SimulationBatchInstanceGuard { path, pid });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let old_pid = fs::read_to_string(&path)
                    .ok()
                    .and_then(|contents| contents.lines().next().map(str::to_owned))
                    .and_then(|value| value.parse::<u32>().ok());
                if let Some(old_pid) = old_pid.filter(|old_pid| *old_pid != pid) {
                    if simulation_batch_process_matches(old_pid) {
                        terminate_simulation_batch_process(old_pid);
                        std::thread::sleep(Duration::from_millis(500));
                        if simulation_batch_process_matches(old_pid) {
                            return Err(format!(
                                "cannot clean up previous AnchorBell simulation-batch process {old_pid}"
                            ));
                        }
                    }
                }
                let _ = fs::remove_file(&path);
            }
            Err(error) => {
                return Err(format!(
                    "cannot claim simulation-batch instance lock: {error}"
                ));
            }
        }
    }

    Err("simulation-batch instance lock is contended".to_owned())
}

#[cfg(windows)]
fn simulation_batch_process_matches(pid: u32) -> bool {
    let script = format!(
        "$p=Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\" -ErrorAction SilentlyContinue;          if ($p -and $p.Name -eq 'anchorbell_simulation_batch.exe' -and          $p.ExecutablePath -like '*AnchorBell*') {{ exit 0 }} else {{ exit 1 }}"
    );
    process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn simulation_batch_process_matches(pid: u32) -> bool {
    fs::read_to_string(format!("/proc/{pid}/cmdline"))
        .map(|command_line| command_line.contains("anchorbell_simulation_batch"))
        .unwrap_or(false)
}

#[cfg(windows)]
fn terminate_simulation_batch_process(pid: u32) {
    let _ = process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .status();
}

#[cfg(not(windows))]
fn terminate_simulation_batch_process(pid: u32) {
    let _ = process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status();
}

fn print_usage() {
    eprintln!("usage: anchorbell_simulation_batch --strategy-profile PATH");
    eprintln!(
        "all strategy, experiment, capital, timing, realism, and OOS/stress settings come from the versioned profile"
    );
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    print_usage();
    process::exit(2);
}
