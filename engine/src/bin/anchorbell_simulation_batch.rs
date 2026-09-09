use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    process,
    str::FromStr,
    time::Duration,
};

use anchorbell_engine::{
    analytics_evidence::EvidenceConfig,
    execution::{BinanceEnvironment, SessionCheckpoint},
    platform::RuntimeProfile,
    runtime::{
        timestamp_ms, RunMode, RunRegistry, RunSpec, RunStatus, RuntimeHealthReporter,
        RUN_REGISTRY_SCHEMA_VERSION,
    },
    simulation::{
        allocate_positions, compiled_build_identity, load_index_anchor_set, run, PositionMode,
        SimulationBatchConfig, SimulationBatchSpec,
    },
    strategy::StrategyProfile,
};

const DEFAULT_PROFILE_PATH: &str = "config/anchorbell-simulation.json";

#[derive(Debug)]
struct Args {
    profile_path: PathBuf,
    policy_id: Option<String>,
    environment: Option<BinanceEnvironment>,
    index_anchors: Option<bool>,
    symbols: Option<Vec<String>>,
    output_root: Option<PathBuf>,
    capital_usdt: Option<String>,
    duration_secs: Option<u64>,
}

fn main() {
    let args = parse_args().unwrap_or_else(|error| fail(error));
    let profile = StrategyProfile::load(&args.profile_path).unwrap_or_else(|error| fail(error));
    let policy_id = args
        .policy_id
        .clone()
        .unwrap_or_else(|| profile.policy_id.clone());
    let environment = args.environment.unwrap_or(profile.environment);
    let index_anchors = args.index_anchors.unwrap_or(profile.index_anchors);
    let symbols = args
        .symbols
        .clone()
        .unwrap_or_else(|| profile.symbols.clone());
    let output_root: PathBuf = args
        .output_root
        .clone()
        .unwrap_or_else(|| profile.output_root.clone().into());
    let capital_usdt = args
        .capital_usdt
        .as_deref()
        .map(|value| parse_decimal(value, 8))
        .transpose()
        .unwrap_or_else(|error| fail(error))
        .unwrap_or_else(|| {
            profile
                .capital_usdt_ticks()
                .unwrap_or_else(|error| fail(error))
        });
    let duration_secs = args.duration_secs.unwrap_or(profile.duration_secs);
    if !index_anchors {
        fail("batch execution requires live --index-anchors");
    }
    let _instance_guard =
        claim_single_simulation_batch_instance().unwrap_or_else(|error| fail(error));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| fail(format!("cannot create runtime: {error}")));
    runtime.block_on(async move {
        let mut health = RuntimeHealthReporter::new(profile.runtime_audit_path.clone());
        health
            .start(RuntimeProfile::Batch, timestamp_ms())
            .await
            .unwrap_or_else(|error| fail(format!("batch health bootstrap failed: {error}")));
        let run_id = format!("batch-{}-{}", policy_id, timestamp_ms());
        let experiment_plan = profile
            .experiment_plan()
            .unwrap_or_else(|error| fail(error));
        let strategies = experiment_plan
            .experiments
            .iter()
            .map(|experiment| experiment.strategy.clone())
            .collect::<Vec<_>>();
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
                    universe: profile.universe_id.clone(),
                    strategies,
                    ablations: experiment_plan
                        .experiments
                        .iter()
                        .flat_map(|experiment| experiment.ablations.iter().cloned())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    checkpoint_interval_ms: profile.checkpoint_interval_ms,
                    max_stale_ms: profile.max_stale_ms,
                    auto_restart: true,
                    build_identity: compiled_build_identity(),
                },
                timestamp_ms(),
            )
            .unwrap_or_else(|error| fail(format!("run registry create failed: {error}")));
        registry
            .claim(
                &run_id,
                format!("batch-{}-{}", std::process::id(), policy_id),
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
        // Never reuse a local anchor for a live simulation run. Bootstrap must obtain
        // the current Binance index/FX-derived anchor set before any market
        // event is admitted; transient REST failures wait and retry.
        let anchor_set = loop {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                load_index_anchor_set(
                    environment,
                    &symbols,
                    profile.price_scale,
                    &profile.anchor_kline_interval,
                    profile.anchor_kline_lookback_ms,
                    profile.anchor_kline_limit,
                    None,
                ),
            )
            .await;
            match result {
                Ok(Ok(set)) => break set,
                Ok(Err(error)) => {
                    eprintln!("index anchor bootstrap unavailable: {error}; retrying in 5s");
                }
                Err(_) => {
                    eprintln!("index anchor bootstrap timed out after 15s; retrying in 5s");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        };
        let index_anchor_conversions = anchor_set.conversions;
        let anchors = anchor_set
            .anchors
            .into_iter()
            .filter(|(symbol, _)| {
                symbols
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(symbol))
            })
            .collect::<BTreeMap<_, _>>();
        let modes = BTreeMap::<String, PositionMode>::new();
        let allocations =
            allocate_positions(&anchors, capital_usdt, &modes, profile.quantity_scale)
                .unwrap_or_else(|error| {
                    fail(format!("cannot allocate simulation-batch capital: {error}"))
                });
        let specs = experiment_plan
            .runtime_specs_with_ablations()
            .unwrap_or_else(|error| fail(format!("invalid experiment plan: {error}")))
            .into_iter()
            .map(|spec| SimulationBatchSpec {
                label: spec.label,
                strategy_key: spec.strategy,
                variant: spec.variant,
                ablations: spec.ablations,
                role: spec.role,
                parent_experiment_id: spec.parent_experiment_id,
                execution_overlay: spec.execution_overlay,
                evidence_policy: spec.evidence_policy,
            })
            .collect();
        registry
            .transition(&run_id, RunStatus::Running, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry running failed: {error}")));
        registry
            .heartbeat(&run_id, timestamp_ms())
            .unwrap_or_else(|error| fail(format!("run registry heartbeat failed: {error}")));
        let heartbeat_task =
            registry.spawn_heartbeat(run_id.clone(), profile.run_registry_heartbeat_ms);
        let config = SimulationBatchConfig {
            policy_id,
            experiment_plan_id: experiment_plan.plan_id.clone(),
            experiment_plan_digest: experiment_plan.digest(),
            universe_id: profile.universe_id.clone(),
            environment,
            symbols,
            anchors,
            index_anchor_conversions,
            entry_threshold_bps: profile.entry_threshold_bps,
            threshold_scale_ppm: profile.threshold_scale_ppm,
            max_position: profile.max_position,
            requested_quantity: profile.requested_quantity,
            max_mark_index_gap_bps: profile.max_mark_index_gap_bps,
            max_anchor_age_ms: profile.max_anchor_age_ms,
            fee_ppm: profile.fee_ppm,
            fee_schedule: profile.fee_schedule.clone(),
            quantity_scale: profile.quantity_scale,
            price_scale: profile.price_scale,
            position_allocations: Some(allocations),
            output_root,
            specs,
            max_subscriptions_per_shard: profile.max_subscriptions_per_shard,
            connect_timeout_ms: profile.connect_timeout_ms,
            read_timeout_ms: profile.read_timeout_ms,
            metrics_refresh_ms: profile.metrics_refresh_ms,
            index_anchor_refresh_ms: if index_anchors {
                profile.index_anchor_refresh_ms
            } else {
                0
            },
            anchor_kline_interval: profile.anchor_kline_interval.clone(),
            anchor_kline_lookback_ms: profile.anchor_kline_lookback_ms,
            anchor_kline_limit: profile.anchor_kline_limit,
            fx_refresh_ms: profile.fx_refresh_ms,
            fx_max_age_ms: profile.fx_max_age_ms,
            queue_ahead: profile.queue_ahead,
            trade_through: profile.trade_through,
            market_to_decision_ms: profile.market_to_decision_ms,
            decision_to_exchange_ms: profile.decision_to_exchange_ms,
            cancel_to_exchange_ms: profile.cancel_to_exchange_ms,
            quote_reprice_min_interval_ms: profile.quote_reprice_min_interval_ms,
            emergency_execution: profile.emergency_execution,
            dynamic_capital_refresh_ms: profile.dynamic_capital_refresh_ms,
            depth_snapshot_limit: profile.depth_snapshot_limit,
            checkpoint_path: Some(checkpoint_path),
            checkpoint_session_id: Some(run_id.clone()),
            checkpoint_interval_ms: profile.checkpoint_interval_ms,
            duration_secs,
            evidence: EvidenceConfig::default(),
            m9_calibration_source_label: profile.m9_calibration_source_label.clone(),
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
    let mut profile_path = PathBuf::from(DEFAULT_PROFILE_PATH);
    let mut policy_id = None;
    let mut environment = None;
    let mut index_anchors = None;
    let mut symbols = None;
    let mut output_root = None;
    let mut capital_usdt = None;
    let mut duration_secs = None;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--strategy-profile" => profile_path = PathBuf::from(next(&mut args, &flag)?),
            "--policy-id" => policy_id = Some(next(&mut args, &flag)?),
            "--index-anchors" => index_anchors = Some(true),
            "--environment" => {
                environment = Some(
                    next(&mut args, &flag)?
                        .parse()
                        .map_err(|_| "invalid --environment".to_owned())?,
                );
            }
            "--symbols" => {
                symbols = Some(
                    next(&mut args, &flag)?
                        .split(',')
                        .map(|s| s.trim().to_ascii_uppercase())
                        .filter(|s| !s.is_empty())
                        .collect(),
                )
            }
            "--output-root" => output_root = Some(PathBuf::from(next(&mut args, &flag)?)),
            "--capital-usdt" => capital_usdt = Some(next(&mut args, &flag)?),
            "--duration-secs" => duration_secs = Some(parse(&mut args, &flag)?),
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            other => return Err(format!("unknown option {other}")),
        }
    }
    Ok(Args {
        profile_path,
        policy_id,
        environment,
        index_anchors,
        symbols,
        output_root,
        capital_usdt,
        duration_secs,
    })
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn parse<T: FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Debug,
{
    next(args, flag)?
        .parse()
        .map_err(|e| format!("invalid {flag}: {e:?}"))
}
fn parse_decimal(value: &str, scale: u32) -> Result<i64, String> {
    let value = value.trim();
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    let fraction_len = fraction.len() as u32;
    if parts.next().is_some()
        || whole.is_empty()
        || fraction.len() > scale as usize
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err("expected a positive decimal".to_owned());
    }
    let unit = 10_i128.pow(scale);
    let whole = whole
        .parse::<i128>()
        .map_err(|_| "decimal overflows".to_owned())?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| "decimal overflows".to_owned())?
    };
    let scaled = whole
        .checked_mul(unit)
        .and_then(|v| v.checked_add(fraction_value * 10_i128.pow(scale - fraction_len)))
        .ok_or_else(|| "decimal overflows".to_owned())?;
    i64::try_from(scaled).map_err(|_| "decimal overflows".to_owned())
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
    eprintln!("usage: anchorbell_simulation_batch [--strategy-profile PATH] [--policy-id ID] [--index-anchors] [--environment production] [--symbols S1,S2] [--output-root PATH] [--capital-usdt N] [--duration-secs N]");
    eprintln!(
        "execution economics, queueing, latency, and refresh cadence are controlled by the unified runtime profile"
    );
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    print_usage();
    process::exit(2);
}
