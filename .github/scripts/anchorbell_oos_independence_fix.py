from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing anchor: {label}")
    return text.replace(old, new, 1)


# OOS domain invariants: every candidate competes on the same folds; duplicated
# data windows cannot inflate either the OOS or stress sample count.
oos_path = ROOT / "engine" / "src" / "oos_validation.rs"
oos = oos_path.read_text(encoding="utf-8")

if "pub data_digest: String" not in oos:
    oos = replace_once(
        oos,
        '''pub struct OosFoldMetrics {
    pub fold_id: String,
    pub stress: bool,
''',
        '''pub struct OosFoldMetrics {
    pub fold_id: String,
    /// SHA-256 of the exact shared market-event ledger used by this fold.
    pub data_digest: String,
    pub stress: bool,
''',
        "fold data digest field",
    )
    oos = replace_once(
        oos,
        '''        !self.fold_id.trim().is_empty()
            && self.net_return_bps.is_finite()
''',
        '''        !self.fold_id.trim().is_empty()
            && valid_sha256_digest(&self.data_digest)
            && self.net_return_bps.is_finite()
''',
        "fold digest validation",
    )
    digest_helper_anchor = '''impl OosFoldMetrics {
    fn valid(&self) -> bool {
'''
    digest_helper = '''fn valid_sha256_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

'''
    oos = replace_once(oos, digest_helper_anchor, digest_helper + digest_helper_anchor, "digest helper")

if "pub fn validate_candidate_fold_coverage" not in oos:
    merge_anchor = '''pub fn merge_fold_bundles(
    bundles: &[OosFoldBundle],
) -> Result<BTreeMap<String, Vec<OosFoldMetrics>>, &'static str> {
'''
    coverage_fn = '''pub fn validate_candidate_fold_coverage(
    candidates: &BTreeMap<String, Vec<OosFoldMetrics>>,
) -> Result<(), &'static str> {
    if candidates.is_empty() {
        return Err("candidate_folds_required");
    }
    let mut expected_coverage: Option<BTreeSet<(String, bool, String)>> = None;
    for (candidate_id, folds) in candidates {
        if candidate_id.trim().is_empty() || folds.is_empty() || folds.iter().any(|fold| !fold.valid()) {
            return Err("invalid_candidate_fold_metrics");
        }
        let mut fold_ids = BTreeSet::new();
        let mut oos_data = BTreeSet::new();
        let mut stress_data = BTreeSet::new();
        let mut coverage = BTreeSet::new();
        for fold in folds {
            if !fold_ids.insert(fold.fold_id.clone()) {
                return Err("duplicate_fold_id_within_candidate");
            }
            let data_set = if fold.stress {
                &mut stress_data
            } else {
                &mut oos_data
            };
            if !data_set.insert(fold.data_digest.clone()) {
                return Err("duplicate_data_window_within_fold_class");
            }
            coverage.insert((fold.fold_id.clone(), fold.stress, fold.data_digest.clone()));
        }
        match &expected_coverage {
            Some(expected) if expected != &coverage => return Err("candidate_fold_coverage_mismatch"),
            None => expected_coverage = Some(coverage),
            _ => {}
        }
    }
    Ok(())
}

'''
    oos = replace_once(oos, merge_anchor, coverage_fn + merge_anchor, "candidate fold coverage function")

# Strengthen bundle merge so a candidate cannot disappear from a difficult fold.
old_merge_core = '''    let mut seen_folds = BTreeSet::new();
    let mut candidates = BTreeMap::<String, Vec<OosFoldMetrics>>::new();
    for bundle in bundles {
        bundle.validate()?;
        if !seen_folds.insert(bundle.fold_id.clone()) {
            return Err("duplicate_fold_id");
        }
        for (candidate_id, metrics) in &bundle.candidates {
            candidates
                .entry(candidate_id.clone())
                .or_default()
                .push(metrics.clone());
        }
    }
    for folds in candidates.values_mut() {
        folds.sort_by(|left, right| left.fold_id.cmp(&right.fold_id));
    }
    Ok(candidates)
'''
new_merge_core = '''    let mut seen_folds = BTreeSet::new();
    let mut expected_candidates: Option<BTreeSet<String>> = None;
    let mut candidates = BTreeMap::<String, Vec<OosFoldMetrics>>::new();
    for bundle in bundles {
        bundle.validate()?;
        if !seen_folds.insert(bundle.fold_id.clone()) {
            return Err("duplicate_fold_id");
        }
        let bundle_candidates = bundle.candidates.keys().cloned().collect::<BTreeSet<_>>();
        match &expected_candidates {
            Some(expected) if expected != &bundle_candidates => {
                return Err("candidate_universe_mismatch")
            }
            None => expected_candidates = Some(bundle_candidates),
            _ => {}
        }
        for (candidate_id, metrics) in &bundle.candidates {
            candidates
                .entry(candidate_id.clone())
                .or_default()
                .push(metrics.clone());
        }
    }
    for folds in candidates.values_mut() {
        folds.sort_by(|left, right| left.fold_id.cmp(&right.fold_id));
    }
    validate_candidate_fold_coverage(&candidates)?;
    Ok(candidates)
'''
if old_merge_core in oos:
    oos = replace_once(oos, old_merge_core, new_merge_core, "strict bundle merge")
elif "candidate_universe_mismatch" not in oos:
    raise SystemExit("bundle merge is neither original nor repaired")

# Individual evaluation must also fail closed when called outside the selector.
old_eval_guard = '''    if folds.iter().any(|fold| !fold.valid()) {
        return base("invalid_fold_metrics");
    }

    let oos = folds.iter().filter(|fold| !fold.stress).collect::<Vec<_>>();
'''
new_eval_guard = '''    if folds.iter().any(|fold| !fold.valid()) {
        return base("invalid_fold_metrics");
    }
    let mut fold_ids = BTreeSet::new();
    let mut oos_data = BTreeSet::new();
    let mut stress_data = BTreeSet::new();
    for fold in folds {
        if !fold_ids.insert(fold.fold_id.clone()) {
            return base("duplicate_fold_id");
        }
        let data_set = if fold.stress {
            &mut stress_data
        } else {
            &mut oos_data
        };
        if !data_set.insert(fold.data_digest.clone()) {
            return base("duplicate_data_window");
        }
    }

    let oos = folds.iter().filter(|fold| !fold.stress).collect::<Vec<_>>();
'''
if old_eval_guard in oos:
    oos = replace_once(oos, old_eval_guard, new_eval_guard, "evaluation independence guard")
elif "duplicate_data_window" not in oos:
    raise SystemExit("evaluation independence guard is neither original nor repaired")

# Update test fold helper with deterministic valid SHA-256 provenance.
old_test_fold = '''    fn fold(id: &str, stress: bool, ret: f64, sharpe: f64, dd: f64) -> OosFoldMetrics {
        OosFoldMetrics {
            fold_id: id.to_owned(),
            stress,
'''
new_test_fold = '''    fn fold(id: &str, stress: bool, ret: f64, sharpe: f64, dd: f64) -> OosFoldMetrics {
        let digest_seed = id.bytes().fold(0_u8, |acc, byte| acc.wrapping_add(byte));
        OosFoldMetrics {
            fold_id: id.to_owned(),
            data_digest: format!("sha256:{digest_seed:064x}"),
            stress,
'''
if old_test_fold in oos:
    oos = replace_once(oos, old_test_fold, new_test_fold, "test fold digest")
elif "data_digest: format!" not in oos:
    raise SystemExit("test fold helper is neither original nor repaired")

if "candidate_fold_coverage_mismatch_is_rejected" not in oos:
    test_anchor = '''    #[test]
    fn stable_candidate_passes_hard_oos_and_stress_gates() {
'''
    tests = '''    #[test]
    fn candidate_fold_coverage_mismatch_is_rejected() {
        let mut candidates = BTreeMap::new();
        candidates.insert(
            "a".to_owned(),
            vec![fold("o1", false, 1.0, 1.0, 1.0), fold("s1", true, -1.0, 0.2, 2.0)],
        );
        candidates.insert("b".to_owned(), vec![fold("o1", false, 1.0, 1.0, 1.0)]);
        assert_eq!(
            validate_candidate_fold_coverage(&candidates).unwrap_err(),
            "candidate_fold_coverage_mismatch"
        );
    }

    #[test]
    fn duplicate_data_window_cannot_inflate_oos_fold_count() {
        let mut first = fold("o1", false, 1.0, 1.0, 1.0);
        let mut second = fold("o2", false, 2.0, 1.1, 1.0);
        second.data_digest = first.data_digest.clone();
        let result = evaluate_robust_candidate(
            &[first.clone(), second, fold("o3", false, 3.0, 1.2, 1.0),
              fold("s1", true, 0.0, 0.2, 2.0), fold("s2", true, 0.0, 0.2, 2.0),
              fold("s3", true, 0.0, 0.2, 2.0)],
            RobustSelectionConstraints::default(),
        );
        assert!(!result.eligible);
        assert_eq!(result.reason, "duplicate_data_window");
        first.data_digest = format!("sha256:{:064x}", 999_u64);
        assert!(first.valid());
    }

'''
    oos = replace_once(oos, test_anchor, tests + test_anchor, "coverage and independence tests")

oos_path.write_text(oos, encoding="utf-8")


# Batch: hash the exact shared market ledger after the writer is closed and put
# that digest on every candidate metric in the fold bundle.
batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")
if "io::Read," not in batch:
    batch = replace_once(
        batch,
        '''use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
''',
        '''use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::Read,
    path::{Path, PathBuf},
''',
        "batch read import",
    )

if "fn sha256_file(path: &Path)" not in batch:
    anchor = '''async fn available_storage_bytes(path: &Path) -> Result<u64, SimulationError> {
'''
    helper = '''fn sha256_file(path: &Path) -> Result<String, SimulationError> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

'''
    batch = replace_once(batch, anchor, helper + anchor, "market ledger digest helper")

if "let validation_market_data_digest" not in batch:
    writer_anchor = '''    let evidence_summary = evidence.summary();
    let mut ledger_results = Vec::with_capacity(ledgers.len());
'''
    writer_replacement = '''    let evidence_summary = evidence.summary();
    let validation_market_data_digest = if config.validation_fold_id.is_some() {
        Some(sha256_file(&config.output_root.join("shared-market.jsonl"))?)
    } else {
        None
    };
    let mut ledger_results = Vec::with_capacity(ledgers.len());
'''
    batch = replace_once(batch, writer_anchor, writer_replacement, "validation market digest")

old_fold_metric = '''                OosFoldMetrics {
                    fold_id: fold_id.to_owned(),
                    stress: config.validation_stress,
'''
new_fold_metric = '''                OosFoldMetrics {
                    fold_id: fold_id.to_owned(),
                    data_digest: validation_market_data_digest
                        .as_ref()
                        .expect("validation digest exists when fold id is configured")
                        .clone(),
                    stress: config.validation_stress,
'''
if old_fold_metric in batch:
    batch = replace_once(batch, old_fold_metric, new_fold_metric, "batch fold exact data digest")
elif "validation_market_data_digest" not in batch:
    raise SystemExit("batch fold data digest is neither original nor repaired")

batch_path.write_text(batch, encoding="utf-8")


# Selector: enforce equal fold coverage even for legacy/direct --input mode.
selector_path = ROOT / "engine" / "src" / "bin" / "anchorbell_oos_select.rs"
selector = selector_path.read_text(encoding="utf-8")
old_selector_import = '''    compare_robust_candidates, evaluate_robust_candidate, merge_fold_bundles, OosFoldBundle,
    OosFoldMetrics, RobustCandidateEvaluation, RobustSelectionConstraints,
'''
new_selector_import = '''    compare_robust_candidates, evaluate_robust_candidate, merge_fold_bundles,
    validate_candidate_fold_coverage, OosFoldBundle, OosFoldMetrics, RobustCandidateEvaluation,
    RobustSelectionConstraints,
'''
if old_selector_import in selector:
    selector = replace_once(selector, old_selector_import, new_selector_import, "selector coverage import")
elif "validate_candidate_fold_coverage" not in selector:
    raise SystemExit("selector coverage import is neither original nor repaired")

if "candidate_coverage" not in selector:
    validation_anchor = '''    let constraints = request.constraints.unwrap_or_default();
    let mut candidates = request
'''
    validation_block = '''    let candidate_coverage = request
        .candidates
        .iter()
        .map(|candidate| (candidate.candidate_id.clone(), candidate.folds.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    validate_candidate_fold_coverage(&candidate_coverage)
        .unwrap_or_else(|reason| fail(format!("invalid candidate fold coverage: {reason}")));

    let constraints = request.constraints.unwrap_or_default();
    let mut candidates = request
'''
    selector = replace_once(selector, validation_anchor, validation_block, "selector coverage validation")

selector_path.write_text(selector, encoding="utf-8")
print("OOS independence and coverage repair complete")
