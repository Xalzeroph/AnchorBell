use std::{
    collections::BTreeMap,
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
        health_reporter::{timestamp_ms, RuntimeHealthReporter},
        run_registry::{RunMode, RunRegistry, RunSpec, RunStatus, RUN_REGISTRY_SCHEMA_VERSION},
    },
    simulation::{
        allocate_positions, load_index_anchor_set,
        orchestration::{run, SimulationBatchConfig, SimulationBatchSpec},
        PositionMode,
    },
};

const DEFAULT_SYMBOLS: &str =
    "CXMTUSDT,UNITREEUSDT,GIGADEVUSDT,HK0625USDT,MINIMAXUSDT,ZHIPUUSDT,ZHONGJIUSDT";

// Execution economics and transport behavior are one internal profile. They are
// derived from venue rules and runtime safety, not exposed as strategy knobs.
const ENTRY_THRESHOLD_BPS: i64 = 5;
const THRESHOLD_SCALE_PPM: i64 = 700_000;
const MAX_MARK_INDEX_GAP_BPS: i64 = 50;
const FEE_PPM: i64 = 200;
const QUEUE_AHEAD: i64 = 0;
const TRADE_THROUGH: i64 = 0;
const MARKET_TO_DECISION_MS: u64 = 0;
const DECISION_TO_EXCHANGE_MS: u64 = 0;
const CANCEL_TO_EXCHANGE_MS: u64 = 0;
const QUOTE_REPRICE_MIN_INTERVAL_MS: u64 = 750;
const DYNAMIC_CAPITAL_REFRESH_MS: u64 = 60_000;

#[derive(Debug)]
struct Args {
    policy_id: String,
    environment: BinanceEnvironment,
    index_anchors: bool,
    symbols: Vec<String>,
    output_root: PathBuf,
    capital_usdt: i64,
    duration_secs: u64,
    fold_id: Option<String>,
    stress_fold: bool,
    include_m9: bool,
}

fn main() {
    let args = parse_args().unwrap_or_else(|error| fail(error));
    if !args.index_anchors {
        fail("batch execution requires live --index-anchors");
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
        let run_id = format!("batch-{}-{}", args.policy_id, timestamp_ms());
        let include_m9 = args.include_m9;
        let strategies = if include_m9 {
            (1..=9).map(|n| format!("m{n}")).collect()
        } else {
            (1..=8).map(|n| format!("m{n}")).collect()
        };
        let experiment_plan = if include_m9 {
            anchorbell_engine::simulation::experiment_plan::ExperimentPlan::m1_to_m9()
        } else {
            anchorbell_engine::simulation::experiment_plan::ExperimentPlan::m1_to_m8()
        };
        let registry = RunRegistry::new(args.output_root.join("runs"));
        registry
            .create(
                RunSpec {
                    schema_version: RUN_REGISTRY_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    mode: RunMode::Simulation,
                    policy_id: args.policy_id.clone(),
                    capital_currency: "USDT".into(),
                    capital_minor_units: args.capital_usdt,
                    universe: "frozen-close-ah".into(),
                    strategies,
                    ablations: vec!["funding".into()],
                    checkpoint_interval_ms: 5_000,
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
                format!("batch-{}-{}", std::process::id(), args.policy_id),
                timestamp_ms(),
            )
            .unwrap_or_else(|error| fail(format!("run registry claim failed: {error}")));
        let checkpoint_path = args
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
        // Never reuse a local anchor for a live simulation run. Bootstrap must obtain
        // the current Binance index/FX-derived anchor set before any market
        // event is admitted; transient REST failures wait and retry.
        let anchors = loop {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                load_index_anchor_set(args.environment, &args.symbols, 8, None),
            )
            .await;
            match result {
                Ok(Ok(set)) => break set.anchors,
                Ok(Err(error)) => {
                    eprintln!("index anchor bootstrap unavailable: {error}; retrying in 5s");
                }
                Err(_) => {
                    eprintln!("index anchor bootstrap timed out after 15s; retrying in 5s");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        };
        let anchors = anchors
            .into_iter()
            .filter(|(symbol, _)| {
                args.symbols
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(symbol))
            })
            .collect::<BTreeMap<_, _>>();
        let modes = BTreeMap::<String, PositionMode>::new();
        let allocations = allocate_positions(&anchors, args.capital_usdt, &modes, 8)
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
        let heartbeat_task = registry.spawn_heartbeat(run_id.clone(), 5_000);
        let config = SimulationBatchConfig {
            policy_id: args.policy_id,
            environment: args.environment,
            symbols: args.symbols,
            anchors,
            entry_threshold_bps: ENTRY_THRESHOLD_BPS,
            threshold_scale_ppm: THRESHOLD_SCALE_PPM,
            max_position: 10_000_000,
            requested_quantity: 1_000_000,
            max_mark_index_gap_bps: MAX_MARK_INDEX_GAP_BPS,
            max_anchor_age_ms: 120_000,
            fee_ppm: FEE_PPM,
            quantity_scale: 8,
            price_scale: 8,
            position_allocations: Some(allocations),
            output_root: args.output_root,
            specs,
            max_subscriptions_per_shard: 64,
            connect_timeout_ms: 5_000,
            read_timeout_ms: 15_000,
            metrics_refresh_ms: 1_000,
            index_anchor_refresh_ms: if args.index_anchors { 60_000 } else { 0 },
            fx_refresh_ms: 30_000,
            fx_max_age_ms: 120_000,
            queue_ahead: QUEUE_AHEAD,
            trade_through: TRADE_THROUGH,
            market_to_decision_ms: MARKET_TO_DECISION_MS,
            decision_to_exchange_ms: DECISION_TO_EXCHANGE_MS,
            cancel_to_exchange_ms: CANCEL_TO_EXCHANGE_MS,
            quote_reprice_min_interval_ms: QUOTE_REPRICE_MIN_INTERVAL_MS,
            dynamic_capital_refresh_ms: DYNAMIC_CAPITAL_REFRESH_MS,
            // Keep REST weight bounded; resync is throttled on 418/429.
            depth_snapshot_limit: 100,
            checkpoint_path: Some(checkpoint_path),
            checkpoint_session_id: Some(run_id.clone()),
            checkpoint_interval_ms: 5_000,
            duration_secs: args.duration_secs,
            validation_fold_id: args.fold_id,
            validation_stress: args.stress_fold,
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
    let mut policy_id = "M7-policy_matrix-r13".to_owned();
    let mut environment = BinanceEnvironment::Production;
    let mut index_anchors = true;
    let mut symbols = DEFAULT_SYMBOLS
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut output_root = PathBuf::from("target\\simulation-batch-20260904-M7");
    let mut capital_usdt = 1_500_i64.checked_mul(100_000_000).unwrap();
    let mut duration_secs = 0;
    let mut fold_id = None;
    let mut stress_fold = false;
    let mut include_m9 = false;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--policy-id" => policy_id = next(&mut args, &flag)?,
            "--index-anchors" => index_anchors = true,
            "--environment" => {
                environment = next(&mut args, &flag)?
                    .parse()
                    .map_err(|_| "invalid --environment".to_owned())?;
            }
            "--symbols" => {
                symbols = next(&mut args, &flag)?
                    .split(',')
                    .map(|s| s.trim().to_ascii_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
            "--output-root" => output_root = PathBuf::from(next(&mut args, &flag)?),
            "--capital-usdt" => capital_usdt = parse_decimal(&next(&mut args, &flag)?, 8)?,
            "--duration-secs" => duration_secs = parse(&mut args, &flag)?,
            "--fold-id" => fold_id = Some(next(&mut args, &flag)?),
            "--stress-fold" => stress_fold = true,
            "--include-m9" => include_m9 = true,
            "--help" | "-h" => {
                print_usage();
                process::exit(0);
            }
            other => return Err(format!("unknown option {other}")),
        }
    }
    if symbols.is_empty() {
        return Err("--symbols cannot be empty".to_owned());
    }
    if stress_fold && fold_id.is_none() {
        return Err("--stress-fold requires --fold-id".to_owned());
    }
    if fold_id
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("--fold-id cannot be empty".to_owned());
    }
    if fold_id.is_some() && duration_secs == 0 {
        return Err("--fold-id requires finite --duration-secs".to_owned());
    }
    Ok(Args {
        policy_id,
        environment,
        index_anchors,
        symbols,
        output_root,
        capital_usdt,
        duration_secs,
        fold_id,
        stress_fold,
        include_m9,
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
    eprintln!("usage: anchorbell_simulation_batch [--policy-id M6] --index-anchors [--environment production] [--symbols S1,S2] [--output-root PATH] [--capital-usdt N] [--duration-secs N] [--include-m9]");
    eprintln!(
        "defaults: shared feed + M1..M8; pass --include-m9 to opt into the separate M1..M9 plan"
    );
    eprintln!(
        "execution economics, queueing, latency, and refresh cadence are controlled by the unified runtime profile"
    );
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    print_usage();
    process::exit(2);
}
