from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


# 1) Authoritative profile schema. This is intentionally v2: historical v1
# encoded M8_no_funding as m7+funding, which is no longer a valid ablation.
config_rs = r'''use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::{
    execution::BinanceEnvironment,
    simulation::experiment_plan::{ExperimentPlan, ExperimentSpec},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StrategyProfile {
    pub schema_version: u16,
    pub policy_id: String,
    pub environment: String,
    pub index_anchors: bool,
    pub symbols: Vec<String>,
    pub output_root: PathBuf,
    pub capital_usdt: String,
    pub duration_secs: u64,
    pub entry_threshold_bps: i64,
    pub threshold_scale_ppm: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub max_mark_index_gap_bps: i64,
    pub max_anchor_age_ms: u64,
    pub fee_ppm: i64,
    pub quantity_scale: u32,
    pub price_scale: u32,
    pub queue_ahead: i64,
    pub trade_through: i64,
    pub market_to_decision_ms: u64,
    pub decision_to_exchange_ms: u64,
    pub cancel_to_exchange_ms: u64,
    pub quote_reprice_min_interval_ms: u64,
    pub dynamic_capital_refresh_ms: u64,
    pub depth_snapshot_limit: usize,
    pub checkpoint_interval_ms: u64,
    pub max_subscriptions_per_shard: usize,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub metrics_refresh_ms: u64,
    pub index_anchor_refresh_ms: u64,
    pub fx_refresh_ms: u64,
    pub fx_max_age_ms: u64,
    pub m9_calibration_source_label: String,
    #[serde(default)]
    pub validation_fold_id: Option<String>,
    #[serde(default)]
    pub validation_stress_profile: Option<String>,
    pub experiments: Vec<ExperimentSpec>,
}

impl StrategyProfile {
    pub const SCHEMA_VERSION: u16 = 2;

    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read strategy profile {}: {error}", path.display()))?;
        let profile = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| format!("invalid strategy profile {}: {error}", path.display()))?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn environment(&self) -> Result<BinanceEnvironment, String> {
        BinanceEnvironment::from_str(&self.environment)
            .map_err(|_| format!("unsupported profile environment {}", self.environment))
    }

    pub fn capital_usdt_ticks(&self) -> Result<i64, String> {
        parse_positive_decimal_ticks(&self.capital_usdt, 8)
    }

    pub fn experiment_plan(&self) -> Result<ExperimentPlan, String> {
        let plan = ExperimentPlan {
            schema_version: ExperimentPlan::SCHEMA_VERSION,
            plan_id: format!("strategy-profile:{}", self.policy_id),
            experiments: self.experiments.clone(),
        };
        plan.runtime_specs_with_ablations()
            .map_err(|error| format!("invalid profile experiment plan: {error}"))?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(format!(
                "unsupported strategy profile schema {}; expected {}",
                self.schema_version,
                Self::SCHEMA_VERSION
            ));
        }
        if self.policy_id.trim().is_empty() {
            return Err("strategy profile policy_id cannot be empty".to_owned());
        }
        self.environment()?;
        if !self.index_anchors {
            return Err("strategy profile must use live index anchors".to_owned());
        }
        if self.symbols.is_empty() {
            return Err("strategy profile symbols cannot be empty".to_owned());
        }
        let mut symbols = BTreeSet::new();
        for symbol in &self.symbols {
            let normalized = symbol.trim().to_ascii_uppercase();
            if normalized.is_empty()
                || !normalized.bytes().all(|byte| byte.is_ascii_alphanumeric())
                || !symbols.insert(normalized)
            {
                return Err("strategy profile symbols must be unique ASCII symbols".to_owned());
            }
        }
        if self.output_root.as_os_str().is_empty() || self.capital_usdt_ticks()? <= 0 {
            return Err("strategy profile requires output_root and positive capital".to_owned());
        }
        if self.entry_threshold_bps < 0
            || !(1..=1_000_000).contains(&self.threshold_scale_ppm)
            || self.max_position <= 0
            || self.requested_quantity <= 0
            || self.max_mark_index_gap_bps < 0
            || self.fee_ppm < 0
            || self.quantity_scale > 18
            || self.price_scale > 18
            || self.queue_ahead < 0
            || self.trade_through < 0
            || self.depth_snapshot_limit == 0
            || self.checkpoint_interval_ms == 0
            || self.max_subscriptions_per_shard == 0
            || self.connect_timeout_ms == 0
            || self.read_timeout_ms == 0
            || self.metrics_refresh_ms == 0
            || self.fx_refresh_ms == 0
            || self.fx_max_age_ms == 0
            || self.dynamic_capital_refresh_ms == 0
        {
            return Err("strategy profile contains invalid runtime limits".to_owned());
        }
        if self.index_anchor_refresh_ms == 0 {
            return Err("strategy profile index-anchor refresh must be enabled".to_owned());
        }
        if self.experiments.is_empty() {
            return Err("strategy profile experiments cannot be empty".to_owned());
        }
        let plan = self.experiment_plan()?;
        for experiment in &plan.experiments {
            let mut ablations = BTreeSet::new();
            for ablation in &experiment.ablations {
                if !ablations.insert(ablation.as_str()) {
                    return Err(format!(
                        "duplicate ablation {ablation} for {}",
                        experiment.label
                    ));
                }
                if ablation != "funding" || experiment.strategy != "m8" {
                    return Err(format!(
                        "unsupported ablation {ablation} for strategy {}",
                        experiment.strategy
                    ));
                }
            }
        }
        if self.m9_calibration_source_label.trim().is_empty() {
            return Err("m9_calibration_source_label cannot be empty".to_owned());
        }
        if plan.experiments.iter().any(|experiment| experiment.strategy == "m9") {
            let source = plan
                .experiments
                .iter()
                .find(|experiment| experiment.label == self.m9_calibration_source_label)
                .ok_or_else(|| "M9 calibration source is not present in experiments".to_owned())?;
            if source.strategy == "m9" || !source.ablations.is_empty() {
                return Err(
                    "M9 calibration source must be a non-M9, non-ablated candidate".to_owned(),
                );
            }
        }
        match (
            self.validation_fold_id.as_deref(),
            self.validation_stress_profile.as_deref(),
        ) {
            (None, Some(_)) => {
                return Err("validation stress profile requires validation_fold_id".to_owned())
            }
            (Some(fold_id), _) if fold_id.trim().is_empty() => {
                return Err("validation_fold_id cannot be empty".to_owned())
            }
            (Some(_), _) if self.duration_secs == 0 => {
                return Err("validation folds require finite duration_secs".to_owned())
            }
            _ => {}
        }
        Ok(())
    }
}

fn parse_positive_decimal_ticks(value: &str, scale: u32) -> Result<i64, String> {
    let value = value.trim();
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty()
        || fraction.len() > scale as usize
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("capital_usdt must be a positive decimal".to_owned());
    }
    let unit = 10_i128.pow(scale);
    let whole = whole
        .parse::<i128>()
        .map_err(|_| "capital_usdt overflows".to_owned())?;
    let fraction_ticks = if fraction.is_empty() {
        0
    } else {
        let fraction_value = fraction
            .parse::<i128>()
            .map_err(|_| "capital_usdt overflows".to_owned())?;
        fraction_value
            .checked_mul(10_i128.pow(scale - fraction.len() as u32))
            .ok_or_else(|| "capital_usdt overflows".to_owned())?
    };
    let ticks = whole
        .checked_mul(unit)
        .and_then(|value| value.checked_add(fraction_ticks))
        .ok_or_else(|| "capital_usdt overflows".to_owned())?;
    let ticks = i64::try_from(ticks).map_err(|_| "capital_usdt overflows".to_owned())?;
    if ticks <= 0 {
        return Err("capital_usdt must be positive".to_owned());
    }
    Ok(ticks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shipped_profile() -> StrategyProfile {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../config/anchorbell-simulation.json");
        StrategyProfile::load(&path).unwrap()
    }

    #[test]
    fn shipped_profile_is_valid_and_preserves_the_true_m8_ablation() {
        let profile = shipped_profile();
        assert_eq!(profile.schema_version, StrategyProfile::SCHEMA_VERSION);
        assert_eq!(profile.capital_usdt_ticks().unwrap(), 1500 * 100_000_000);
        let no_funding = profile
            .experiments
            .iter()
            .find(|experiment| experiment.label == "M8_no_funding")
            .unwrap();
        assert_eq!(no_funding.strategy, "m8");
        assert_eq!(no_funding.ablations, vec!["funding".to_owned()]);
    }

    #[test]
    fn funding_ablation_cannot_be_mislabelled_as_m7() {
        let mut profile = shipped_profile();
        profile
            .experiments
            .iter_mut()
            .find(|experiment| experiment.label == "M8_no_funding")
            .unwrap()
            .strategy = "m7".to_owned();
        assert!(profile.validate().is_err());
    }

    #[test]
    fn m9_source_must_resolve_to_an_unablated_non_m9_candidate() {
        let mut profile = shipped_profile();
        profile.m9_calibration_source_label = "M9_full".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "missing".to_owned();
        assert!(profile.validate().is_err());
    }
}
'''
(ROOT / "engine/src/strategy/config.rs").write_text(config_rs, encoding="utf-8")


# 2) Export profile through the strategy facade.
mod_path = ROOT / "engine/src/strategy/mod.rs"
mod_text = mod_path.read_text(encoding="utf-8")
if "pub mod config;" not in mod_text:
    mod_text = replace_once(
        mod_text,
        "pub mod calibration;\n",
        "pub mod calibration;\npub mod config;\n",
        "strategy config module",
    )
if "pub use config::StrategyProfile;" not in mod_text:
    mod_text = replace_once(
        mod_text,
        "pub use calibration::{\n",
        "pub use config::StrategyProfile;\npub use calibration::{\n",
        "strategy profile export",
    )
mod_path.write_text(mod_text, encoding="utf-8")


# 3) Make M9 calibration source an explicit batch-runtime contract.
batch_path = ROOT / "engine/src/simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")
batch = batch.replace(
    '''/// M9 uses a declared calibration population instead of whichever ledger has
/// the largest sample count. This keeps the warm-start source auditable and
const MIN_SIMULATION_FREE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const STORAGE_SAFETY_SCHEMA_VERSION: u16 = 1;
/// avoids mixing policy-specific order selection into a single unlabeled model.
const M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";
''',
    '''const MIN_SIMULATION_FREE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const STORAGE_SAFETY_SCHEMA_VERSION: u16 = 1;
''',
)
if "pub m9_calibration_source_label: String," not in batch:
    batch = replace_once(
        batch,
        '''    pub specs: Vec<SimulationBatchSpec>,
    pub max_subscriptions_per_shard: usize,
''',
        '''    pub specs: Vec<SimulationBatchSpec>,
    /// Declared candidate population used to seed/warm-start M9 calibration.
    pub m9_calibration_source_label: String,
    pub max_subscriptions_per_shard: usize,
''',
        "batch m9 source field",
    )
batch = replace_once(
    batch,
    '''fn warm_start_m9_from_source(ledgers: &mut [Ledger]) {
    let seeds = ledgers
        .iter()
        .find(|ledger| ledger.spec.label == M9_CALIBRATION_SOURCE_LABEL)
''',
    '''fn warm_start_m9_from_source(ledgers: &mut [Ledger], source_label: &str) {
    let seeds = ledgers
        .iter()
        .find(|ledger| ledger.spec.label == source_label)
''',
    "runtime m9 warm source",
)

validate_anchor = '''    let mut candidate_identities = BTreeSet::new();
    if config
        .specs
        .iter()
        .map(candidate_identity)
        .any(|identity| !candidate_identities.insert(identity))
    {
        return Err(SimulationError::InvalidConfig(
            "batch execution contains duplicate candidate semantics",
        ));
    }
'''
validate_replacement = validate_anchor + '''    if config
        .specs
        .iter()
        .any(|spec| spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc)
    {
        let source = config
            .specs
            .iter()
            .find(|spec| spec.label == config.m9_calibration_source_label)
            .ok_or(SimulationError::InvalidConfig(
                "M9 calibration source is missing from the batch",
            ))?;
        if source.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
            || !source.ablations.is_empty()
        {
            return Err(SimulationError::InvalidConfig(
                "M9 calibration source must be non-M9 and non-ablated",
            ));
        }
    }
'''
if "M9 calibration source is missing from the batch" not in batch:
    batch = replace_once(batch, validate_anchor, validate_replacement, "batch m9 source validation")

batch = replace_once(
    batch,
    '''        "policy_id": config.policy_id,
        "entry_threshold_bps": config.entry_threshold_bps,
''',
    '''        "policy_id": config.policy_id,
        "m9_calibration_source_label": config.m9_calibration_source_label,
        "entry_threshold_bps": config.entry_threshold_bps,
''',
    "parameter digest m9 source",
)
batch = replace_once(
    batch,
    '''        "policy_id": config.policy_id,
        "created_at_ms": manifest_created_at_ms,
''',
    '''        "policy_id": config.policy_id,
        "m9_calibration_source_label": config.m9_calibration_source_label,
        "created_at_ms": manifest_created_at_ms,
''',
    "manifest m9 source",
)
batch = replace_once(
    batch,
    '''        let calibration_source = if spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
        {
            M9_CALIBRATION_SOURCE_LABEL
        } else {
            spec.label.as_str()
        };
''',
    '''        let calibration_source = if spec.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
        {
            config.m9_calibration_source_label.as_str()
        } else {
            spec.label.as_str()
        };
''',
    "calibration seed source",
)
batch = replace_once(
    batch,
    '''                    warm_start_m9_from_source(&mut ledgers);
''',
    '''                    warm_start_m9_from_source(
                        &mut ledgers,
                        &config.m9_calibration_source_label,
                    );
''',
    "periodic m9 warm source",
)
if 'm9_calibration_source_label: "F3_m3".to_owned(),' not in batch:
    batch = replace_once(
        batch,
        '''            specs: vec![],
            max_subscriptions_per_shard: 1,
''',
        '''            specs: vec![],
            m9_calibration_source_label: "F3_m3".to_owned(),
            max_subscriptions_per_shard: 1,
''',
        "stress test m9 source",
    )
batch_path.write_text(batch, encoding="utf-8")


# 4) Replace the hard-coded CLI with one authoritative strategy-profile input.
cli_path = ROOT / "engine/src/bin/anchorbell_simulation_batch.rs"
cli = cli_path.read_text(encoding="utf-8")
cli = cli.replace("    str::FromStr,\n", "")
if "strategy::StrategyProfile," not in cli:
    cli = cli.replace(
        '''    simulation::{
        allocate_positions, load_index_anchor_set,
        orchestration::{run, SimulationBatchConfig, SimulationBatchSpec},
        PositionMode,
    },
''',
        '''    simulation::{
        allocate_positions, load_index_anchor_set,
        orchestration::{run, SimulationBatchConfig, SimulationBatchSpec},
        PositionMode,
    },
    strategy::StrategyProfile,
''',
    )

start = cli.index("const DEFAULT_SYMBOLS:") if "const DEFAULT_SYMBOLS:" in cli else -1
main_start = cli.index("fn main() {")
if start >= 0:
    cli = cli[:start] + '''#[derive(Debug)]
struct Args {
    strategy_profile: PathBuf,
}

''' + cli[main_start:]
    main_start = cli.index("fn main() {")

guard_start = cli.index("struct SimulationBatchInstanceGuard")
if "let profile = StrategyProfile::load" not in cli[:guard_start]:
    new_main = r'''fn main() {
    let args = parse_args().unwrap_or_else(fail);
    let profile = StrategyProfile::load(&args.strategy_profile).unwrap_or_else(fail);
    let environment = profile.environment().unwrap_or_else(fail);
    let capital_usdt = profile.capital_usdt_ticks().unwrap_or_else(fail);
    let experiment_plan = profile.experiment_plan().unwrap_or_else(fail);
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
        claim_single_simulation_batch_instance().unwrap_or_else(fail);
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
                strategy_profile = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| "--strategy-profile requires a path".to_owned())?,
                ));
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

'''
    cli = cli[:main_start] + new_main + cli[guard_start:]

old_usage_start = cli.index("fn print_usage() {")
fail_start = cli.index("fn fail(", old_usage_start)
new_usage = r'''fn print_usage() {
    eprintln!("usage: anchorbell_simulation_batch --strategy-profile PATH");
    eprintln!(
        "all strategy, experiment, capital, timing, realism, and OOS/stress settings come from the versioned profile"
    );
}

'''
cli = cli[:old_usage_start] + new_usage + cli[fail_start:]
cli_path.write_text(cli, encoding="utf-8")


# 5) Ship the corrected v2 profile. Base execution assumptions remain explicit;
# stress folds apply separate adverse profiles rather than mutating this baseline.
profile_json = r'''{
  "schema_version": 2,
  "policy_id": "M1-M9-pico-bps-server-20260908-v2",
  "environment": "production",
  "index_anchors": true,
  "symbols": [
    "CXMTUSDT",
    "UNITREEUSDT",
    "GIGADEVUSDT",
    "HK0625USDT",
    "MINIMAXUSDT",
    "ZHIPUUSDT",
    "ZHONGJIUSDT"
  ],
  "output_root": "/var/lib/anchorbell/simulation-batch-m1-m9-pico-20260908-v2",
  "capital_usdt": "1500",
  "duration_secs": 0,
  "entry_threshold_bps": 5,
  "threshold_scale_ppm": 700000,
  "max_position": 10000000,
  "requested_quantity": 1000000,
  "max_mark_index_gap_bps": 50,
  "max_anchor_age_ms": 120000,
  "fee_ppm": 200,
  "quantity_scale": 8,
  "price_scale": 8,
  "queue_ahead": 0,
  "trade_through": 0,
  "market_to_decision_ms": 0,
  "decision_to_exchange_ms": 0,
  "cancel_to_exchange_ms": 0,
  "quote_reprice_min_interval_ms": 750,
  "dynamic_capital_refresh_ms": 60000,
  "depth_snapshot_limit": 100,
  "checkpoint_interval_ms": 5000,
  "max_subscriptions_per_shard": 64,
  "connect_timeout_ms": 5000,
  "read_timeout_ms": 15000,
  "metrics_refresh_ms": 1000,
  "index_anchor_refresh_ms": 60000,
  "fx_refresh_ms": 30000,
  "fx_max_age_ms": 120000,
  "m9_calibration_source_label": "F3_m3",
  "validation_fold_id": null,
  "validation_stress_profile": null,
  "experiments": [
    {"label": "F1_m1", "strategy": "m1", "ablations": []},
    {"label": "F2_m2", "strategy": "m2", "ablations": []},
    {"label": "F3_m3", "strategy": "m3", "ablations": []},
    {"label": "F4_m4", "strategy": "m4", "ablations": []},
    {"label": "F5_m5", "strategy": "m5", "ablations": []},
    {"label": "F6_m6", "strategy": "m6", "ablations": []},
    {"label": "F7_m7", "strategy": "m7", "ablations": []},
    {"label": "M8_full", "strategy": "m8", "ablations": []},
    {"label": "M8_no_funding", "strategy": "m8", "ablations": ["funding"]},
    {"label": "M9_full", "strategy": "m9", "ablations": []}
  ]
}
'''
profile_path = ROOT / "config/anchorbell-simulation.json"
profile_path.parent.mkdir(parents=True, exist_ok=True)
profile_path.write_text(profile_json, encoding="utf-8")


# 6) Deployment unit consumes only the versioned profile.
service_path = ROOT / "deploy/systemd/anchorbell-simulation.service"
service = service_path.read_text(encoding="utf-8")
lines = service.splitlines()
for index, line in enumerate(lines):
    if line.startswith("ExecStart="):
        lines[index] = (
            "ExecStart=/opt/anchorbell/target/release/anchorbell_simulation_batch "
            "--strategy-profile /opt/anchorbell/config/anchorbell-simulation.json"
        )
        break
else:
    raise SystemExit("systemd ExecStart missing")
service_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

print("authoritative strategy-profile migration complete")
