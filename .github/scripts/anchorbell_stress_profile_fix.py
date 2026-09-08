from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing anchor: {label}")
    return text.replace(old, new, 1)


# 1) OOS schema: a stress fold is valid only with a recognized, explicit profile.
oos_path = ROOT / "engine" / "src" / "oos_validation.rs"
oos = oos_path.read_text(encoding="utf-8")

if "EXECUTION_ADVERSE_STRESS_PROFILE_V1" not in oos:
    oos = replace_once(
        oos,
        '''use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

''',
        '''use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

/// Synthetic execution stress, not a claim about measured production latency.
/// The simulation batch applies this profile before any candidate engine is built.
pub const EXECUTION_ADVERSE_STRESS_PROFILE_V1: &str =
    "synthetic_fee2x_latency_50_100_150_v1";

''',
        "stress profile constant",
    )

if "pub stress_profile: Option<String>" not in oos:
    oos = replace_once(
        oos,
        '''    pub data_digest: String,
    pub stress: bool,
    pub net_return_bps: f64,
''',
        '''    pub data_digest: String,
    pub stress: bool,
    /// None for ordinary OOS folds; recognized explicit scenario id for stress folds.
    pub stress_profile: Option<String>,
    pub net_return_bps: f64,
''',
        "fold stress profile field",
    )
    oos = replace_once(
        oos,
        '''fn valid_sha256_digest(value: &str) -> bool {
''',
        '''fn valid_stress_profile(stress: bool, profile: Option<&str>) -> bool {
    match (stress, profile) {
        (false, None) => true,
        (true, Some(EXECUTION_ADVERSE_STRESS_PROFILE_V1)) => true,
        _ => false,
    }
}

fn valid_sha256_digest(value: &str) -> bool {
''',
        "stress profile validator",
    )
    oos = replace_once(
        oos,
        '''            && valid_sha256_digest(&self.data_digest)
            && self.net_return_bps.is_finite()
''',
        '''            && valid_sha256_digest(&self.data_digest)
            && valid_stress_profile(self.stress, self.stress_profile.as_deref())
            && self.net_return_bps.is_finite()
''',
        "metric stress profile validation",
    )

if "pub stress_profile: Option<String>,\n    pub candidates" not in oos:
    oos = replace_once(
        oos,
        '''pub struct OosFoldBundle {
    pub methodology_id: String,
    pub fold_id: String,
    pub stress: bool,
    pub candidates: BTreeMap<String, OosFoldMetrics>,
}
''',
        '''pub struct OosFoldBundle {
    pub methodology_id: String,
    pub fold_id: String,
    pub stress: bool,
    pub stress_profile: Option<String>,
    pub candidates: BTreeMap<String, OosFoldMetrics>,
}
''',
        "bundle stress profile field",
    )

# Bump bundle methodology because stress provenance becomes mandatory.
oos = oos.replace("anchorbell-oos-fold-bundle-v1", "anchorbell-oos-fold-bundle-v2")

old_bundle_validation = '''        if self.methodology_id != "anchorbell-oos-fold-bundle-v2"
            || self.fold_id.trim().is_empty()
            || self.candidates.is_empty()
        {
            return Err("invalid_fold_bundle_identity");
        }
        if self.candidates.iter().any(|(candidate_id, metrics)| {
            candidate_id.trim().is_empty()
                || !metrics.valid()
                || metrics.fold_id != self.fold_id
                || metrics.stress != self.stress
        }) {
'''
new_bundle_validation = '''        if self.methodology_id != "anchorbell-oos-fold-bundle-v2"
            || self.fold_id.trim().is_empty()
            || self.candidates.is_empty()
            || !valid_stress_profile(self.stress, self.stress_profile.as_deref())
        {
            return Err("invalid_fold_bundle_identity");
        }
        if self.candidates.iter().any(|(candidate_id, metrics)| {
            candidate_id.trim().is_empty()
                || !metrics.valid()
                || metrics.fold_id != self.fold_id
                || metrics.stress != self.stress
                || metrics.stress_profile != self.stress_profile
        }) {
'''
if old_bundle_validation in oos:
    oos = replace_once(oos, old_bundle_validation, new_bundle_validation, "bundle profile consistency")
elif "metrics.stress_profile != self.stress_profile" not in oos:
    raise SystemExit("bundle profile consistency is neither original nor repaired")

# Candidate coverage must include the stress scenario identity.
old_coverage_type = '''    let mut expected_coverage: Option<BTreeSet<(String, bool, String)>> = None;
'''
new_coverage_type = '''    let mut expected_coverage: Option<BTreeSet<(String, bool, Option<String>, String)>> = None;
'''
if old_coverage_type in oos:
    oos = replace_once(oos, old_coverage_type, new_coverage_type, "coverage stress profile type")

old_coverage_insert = '''            coverage.insert((fold.fold_id.clone(), fold.stress, fold.data_digest.clone()));
'''
new_coverage_insert = '''            coverage.insert((
                fold.fold_id.clone(),
                fold.stress,
                fold.stress_profile.clone(),
                fold.data_digest.clone(),
            ));
'''
if old_coverage_insert in oos:
    oos = replace_once(oos, old_coverage_insert, new_coverage_insert, "coverage stress profile identity")

# Test helper creates semantically valid folds automatically.
old_test_metric = '''            data_digest: format!("sha256:{digest_seed:064x}"),
            stress,
            net_return_bps: ret,
'''
new_test_metric = '''            data_digest: format!("sha256:{digest_seed:064x}"),
            stress,
            stress_profile: stress.then(|| EXECUTION_ADVERSE_STRESS_PROFILE_V1.to_owned()),
            net_return_bps: ret,
'''
if old_test_metric in oos:
    oos = replace_once(oos, old_test_metric, new_test_metric, "test fold stress profile")
elif "stress_profile: stress.then" not in oos:
    raise SystemExit("test fold stress profile is neither original nor repaired")

# Existing bundle fixtures need bundle-level profile fields.
oos = oos.replace(
    '''            fold_id: "o1".to_owned(),
            stress: false,
            candidates: first_candidates,
''',
    '''            fold_id: "o1".to_owned(),
            stress: false,
            stress_profile: None,
            candidates: first_candidates,
''',
)
oos = oos.replace(
    '''            fold_id: "s1".to_owned(),
            stress: true,
            candidates: second_candidates,
''',
    '''            fold_id: "s1".to_owned(),
            stress: true,
            stress_profile: Some(EXECUTION_ADVERSE_STRESS_PROFILE_V1.to_owned()),
            candidates: second_candidates,
''',
)

if "stress_label_without_profile_is_rejected" not in oos:
    anchor = '''    #[test]
    fn stable_candidate_passes_hard_oos_and_stress_gates() {
'''
    tests = '''    #[test]
    fn stress_label_without_profile_is_rejected() {
        let mut invalid = fold("s-missing-profile", true, 0.0, 0.2, 2.0);
        invalid.stress_profile = None;
        let result = evaluate_robust_candidate(
            &[
                fold("o1", false, 3.0, 1.0, 1.0),
                fold("o2", false, 3.0, 1.0, 1.0),
                fold("o3", false, 3.0, 1.0, 1.0),
                invalid,
                fold("s2", true, 0.0, 0.2, 2.0),
                fold("s3", true, 0.0, 0.2, 2.0),
            ],
            RobustSelectionConstraints::default(),
        );
        assert!(!result.eligible);
        assert_eq!(result.reason, "invalid_fold_metrics");
    }

'''
    oos = replace_once(oos, anchor, tests + anchor, "stress provenance test")

oos_path.write_text(oos, encoding="utf-8")


# 2) Batch: replace tag-only stress boolean with an applied synthetic scenario.
batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")

old_oos_import = '''    oos_validation::{OosFoldBundle, OosFoldMetrics},
'''
new_oos_import = '''    oos_validation::{
        OosFoldBundle, OosFoldMetrics, EXECUTION_ADVERSE_STRESS_PROFILE_V1,
    },
'''
if old_oos_import in batch:
    batch = replace_once(batch, old_oos_import, new_oos_import, "batch stress constant import")
elif "EXECUTION_ADVERSE_STRESS_PROFILE_V1" not in batch:
    raise SystemExit("batch stress constant import is neither original nor repaired")

batch = batch.replace(
    '''    pub validation_fold_id: Option<String>,
    pub validation_stress: bool,
''',
    '''    pub validation_fold_id: Option<String>,
    /// Explicit synthetic execution scenario; None means ordinary OOS fold.
    pub validation_stress_profile: Option<String>,
''',
)

if "fn apply_validation_stress_profile" not in batch:
    anchor = '''fn candidate_identity(spec: &SimulationBatchSpec) -> String {
'''
    helper = '''const STRESS_MIN_FEE_PPM: i64 = 400;
const STRESS_MARKET_TO_DECISION_MS: u64 = 50;
const STRESS_DECISION_TO_EXCHANGE_MS: u64 = 100;
const STRESS_CANCEL_TO_EXCHANGE_MS: u64 = 150;

fn apply_validation_stress_profile(
    config: &mut SimulationBatchConfig,
) -> Result<(), SimulationError> {
    let Some(profile) = config.validation_stress_profile.as_deref() else {
        return Ok(());
    };
    match profile {
        EXECUTION_ADVERSE_STRESS_PROFILE_V1 => {
            // Synthetic adverse execution scenario. These are explicit stress
            // assumptions, not estimates of observed production performance.
            config.fee_ppm = config.fee_ppm.saturating_mul(2).max(STRESS_MIN_FEE_PPM);
            config.market_to_decision_ms = config
                .market_to_decision_ms
                .max(STRESS_MARKET_TO_DECISION_MS);
            config.decision_to_exchange_ms = config
                .decision_to_exchange_ms
                .max(STRESS_DECISION_TO_EXCHANGE_MS);
            config.cancel_to_exchange_ms = config
                .cancel_to_exchange_ms
                .max(STRESS_CANCEL_TO_EXCHANGE_MS);
            Ok(())
        }
        _ => Err(SimulationError::InvalidConfig(
            "unknown validation stress profile",
        )),
    }
}

'''
    batch = replace_once(batch, anchor, helper + anchor, "stress application helper")

# Replace validation rules.
old_validation = '''    if let Some(fold_id) = config.validation_fold_id.as_deref() {
        if fold_id.trim().is_empty() {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a non-empty fold id",
            ));
        }
        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.position_allocations.is_none() {
            return Err(SimulationError::InvalidConfig(
                "validation folds require explicit capital allocations",
            ));
        }
    } else if config.validation_stress {
        return Err(SimulationError::InvalidConfig(
            "stress validation requires a fold id",
        ));
    }
'''
new_validation = '''    if let Some(fold_id) = config.validation_fold_id.as_deref() {
        if fold_id.trim().is_empty() {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a non-empty fold id",
            ));
        }
        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.position_allocations.is_none() {
            return Err(SimulationError::InvalidConfig(
                "validation folds require explicit capital allocations",
            ));
        }
        if config.validation_stress_profile.as_deref().is_some_and(|profile| {
            profile != EXECUTION_ADVERSE_STRESS_PROFILE_V1
        }) {
            return Err(SimulationError::InvalidConfig(
                "unknown validation stress profile",
            ));
        }
    } else if config.validation_stress_profile.is_some() {
        return Err(SimulationError::InvalidConfig(
            "stress validation requires a fold id",
        ));
    }
'''
if old_validation in batch:
    batch = replace_once(batch, old_validation, new_validation, "stress validation rules")
elif "validation_stress_profile" not in batch:
    raise SystemExit("stress validation rules are neither original nor repaired")

# Apply stress before parameter/data lineage is materialized or engines are built.
run_anchor = '''    validate(&config)?;
    if config.policy_id.trim().is_empty() {
'''
run_replacement = '''    validate(&config)?;
    apply_validation_stress_profile(&mut config)?;
    if config.policy_id.trim().is_empty() {
'''
if run_anchor in batch:
    batch = replace_once(batch, run_anchor, run_replacement, "apply stress before run lineage")
elif "apply_validation_stress_profile(&mut config)?;" not in batch:
    raise SystemExit("stress application call is neither original nor repaired")

batch = batch.replace(
    '''        "validation_fold_id": config.validation_fold_id,
        "validation_stress": config.validation_stress,
''',
    '''        "validation_fold_id": config.validation_fold_id,
        "validation_stress_profile": config.validation_stress_profile,
''',
)

# Generated fold semantics are derived from applied profile, never from a raw boolean.
old_fold_header = '''    let oos_fold_bundle = if let Some(fold_id) = config.validation_fold_id.as_deref() {
        let capital_ticks = config
'''
new_fold_header = '''    let oos_fold_bundle = if let Some(fold_id) = config.validation_fold_id.as_deref() {
        let stress_profile = config.validation_stress_profile.clone();
        let is_stress = stress_profile.is_some();
        let capital_ticks = config
'''
if old_fold_header in batch:
    batch = replace_once(batch, old_fold_header, new_fold_header, "derive stress from applied profile")
elif "let stress_profile = config.validation_stress_profile.clone();" not in batch:
    raise SystemExit("stress derivation is neither original nor repaired")

batch = batch.replace(
    '''                    stress: config.validation_stress,
                    net_return_bps: risk.total_return_pct * 100.0,
''',
    '''                    stress: is_stress,
                    stress_profile: stress_profile.clone(),
                    net_return_bps: risk.total_return_pct * 100.0,
''',
)
batch = batch.replace(
    '''            methodology_id: "anchorbell-oos-fold-bundle-v1".to_owned(),
            fold_id: fold_id.to_owned(),
            stress: config.validation_stress,
            candidates,
''',
    '''            methodology_id: "anchorbell-oos-fold-bundle-v2".to_owned(),
            fold_id: fold_id.to_owned(),
            stress: is_stress,
            stress_profile,
            candidates,
''',
)

# Add unit coverage for actual parameter mutation.
if "execution_adverse_stress_profile_changes_economics_and_latency" not in batch:
    test_import_old = '''mod tests {
    use super::{calibration_key, candidate_identity, SimulationBatchSpec};
    use crate::simulation::engine::SimulationPolicyVariant;
'''
    test_import_new = '''mod tests {
    use super::{
        apply_validation_stress_profile, calibration_key, candidate_identity, SimulationBatchConfig,
        SimulationBatchSpec,
    };
    use crate::{
        analytics_evidence::EvidenceConfig,
        execution::BinanceEnvironment,
        oos_validation::EXECUTION_ADVERSE_STRESS_PROFILE_V1,
        simulation::engine::SimulationPolicyVariant,
    };
    use std::{collections::BTreeMap, path::PathBuf};
'''
    batch = replace_once(batch, test_import_old, test_import_new, "stress test imports")
    test_anchor = '''    #[test]
    fn candidate_identity_normalizes_ablation_order_and_ignores_label() {
'''
    stress_test = '''    #[test]
    fn execution_adverse_stress_profile_changes_economics_and_latency() {
        let mut config = SimulationBatchConfig {
            policy_id: "test".to_owned(),
            environment: BinanceEnvironment::Testnet,
            symbols: vec!["CXMTUSDT".to_owned()],
            anchors: BTreeMap::new(),
            entry_threshold_bps: 5,
            threshold_scale_ppm: 1_000_000,
            max_position: 1,
            requested_quantity: 1,
            max_mark_index_gap_bps: 50,
            max_anchor_age_ms: 0,
            fee_ppm: 200,
            quantity_scale: 8,
            price_scale: 8,
            position_allocations: Some(BTreeMap::new()),
            output_root: PathBuf::from("target/test-stress"),
            specs: vec![],
            max_subscriptions_per_shard: 1,
            connect_timeout_ms: 1,
            read_timeout_ms: 1,
            metrics_refresh_ms: 1_000,
            index_anchor_refresh_ms: 0,
            fx_refresh_ms: 1_000,
            fx_max_age_ms: 1_000,
            queue_ahead: 0,
            trade_through: 0,
            market_to_decision_ms: 0,
            decision_to_exchange_ms: 0,
            cancel_to_exchange_ms: 0,
            quote_reprice_min_interval_ms: 750,
            dynamic_capital_refresh_ms: 60_000,
            depth_snapshot_limit: 100,
            checkpoint_path: None,
            checkpoint_session_id: None,
            checkpoint_interval_ms: 5_000,
            duration_secs: 60,
            validation_fold_id: Some("stress-1".to_owned()),
            validation_stress_profile: Some(EXECUTION_ADVERSE_STRESS_PROFILE_V1.to_owned()),
            evidence: EvidenceConfig::default(),
        };
        apply_validation_stress_profile(&mut config).unwrap();
        assert_eq!(config.fee_ppm, 400);
        assert_eq!(config.market_to_decision_ms, 50);
        assert_eq!(config.decision_to_exchange_ms, 100);
        assert_eq!(config.cancel_to_exchange_ms, 150);
    }

'''
    batch = replace_once(batch, test_anchor, stress_test + test_anchor, "stress application unit test")

batch_path.write_text(batch, encoding="utf-8")


# 3) Batch CLI: explicit profile replaces the unsafe tag-only --stress-fold flag.
cli_path = ROOT / "engine" / "src" / "bin" / "anchorbell_simulation_batch.rs"
cli = cli_path.read_text(encoding="utf-8")

cli = cli.replace(
    '''    fold_id: Option<String>,
    stress_fold: bool,
    include_m9: bool,
''',
    '''    fold_id: Option<String>,
    stress_profile: Option<String>,
    include_m9: bool,
''',
)
cli = cli.replace(
    '''            validation_fold_id: args.fold_id,
            validation_stress: args.stress_fold,
''',
    '''            validation_fold_id: args.fold_id,
            validation_stress_profile: args.stress_profile,
''',
)
cli = cli.replace(
    '''    let mut fold_id = None;
    let mut stress_fold = false;
    let mut include_m9 = false;
''',
    '''    let mut fold_id = None;
    let mut stress_profile = None;
    let mut include_m9 = false;
''',
)
cli = cli.replace(
    '''            "--fold-id" => fold_id = Some(next(&mut args, &flag)?),
            "--stress-fold" => stress_fold = true,
            "--include-m9" => include_m9 = true,
''',
    '''            "--fold-id" => fold_id = Some(next(&mut args, &flag)?),
            "--stress-profile" => stress_profile = Some(next(&mut args, &flag)?),
            "--stress-fold" => {
                return Err("--stress-fold was removed; use --stress-profile synthetic_fee2x_latency_50_100_150_v1".to_owned())
            }
            "--include-m9" => include_m9 = true,
''',
)
cli = cli.replace(
    '''    if stress_fold && fold_id.is_none() {
        return Err("--stress-fold requires --fold-id".to_owned());
    }
''',
    '''    if stress_profile.is_some() && fold_id.is_none() {
        return Err("--stress-profile requires --fold-id".to_owned());
    }
''',
)
cli = cli.replace(
    '''        fold_id,
        stress_fold,
        include_m9,
''',
    '''        fold_id,
        stress_profile,
        include_m9,
''',
)
cli = cli.replace(
    '''usage: anchorbell_simulation_batch [--policy-id M6] --index-anchors [--environment production] [--symbols S1,S2] [--output-root PATH] [--capital-usdt N] [--duration-secs N] [--include-m9]''',
    '''usage: anchorbell_simulation_batch [--policy-id M6] --index-anchors [--environment production] [--symbols S1,S2] [--output-root PATH] [--capital-usdt N] [--duration-secs N] [--fold-id ID] [--stress-profile synthetic_fee2x_latency_50_100_150_v1] [--include-m9]''',
)

cli_path.write_text(cli, encoding="utf-8")
print("auditable economic stress profile repair complete")
