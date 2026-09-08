use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

/// Synthetic execution stress, not a claim about measured production latency.
/// The simulation batch applies this profile before any candidate engine is built.
pub const EXECUTION_ADVERSE_STRESS_PROFILE_V1: &str = "synthetic_fee2x_latency_50_100_150_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OosFoldMetrics {
    pub fold_id: String,
    /// SHA-256 of the exact shared market-event ledger used by this fold.
    pub data_digest: String,
    pub stress: bool,
    /// None for ordinary OOS folds; recognized explicit scenario id for stress folds.
    pub stress_profile: Option<String>,
    pub net_return_bps: f64,
    pub sharpe_ratio: Option<f64>,
    pub sortino_ratio: Option<f64>,
    pub max_drawdown_pct: f64,
    pub fee_drag_bps: f64,
    pub trades: u64,
}

fn valid_stress_profile(stress: bool, profile: Option<&str>) -> bool {
    match (stress, profile) {
        (false, None) => true,
        (true, Some(EXECUTION_ADVERSE_STRESS_PROFILE_V1)) => true,
        _ => false,
    }
}

fn valid_sha256_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl OosFoldMetrics {
    fn valid(&self) -> bool {
        !self.fold_id.trim().is_empty()
            && valid_sha256_digest(&self.data_digest)
            && valid_stress_profile(self.stress, self.stress_profile.as_deref())
            && self.net_return_bps.is_finite()
            && self.max_drawdown_pct.is_finite()
            && self.max_drawdown_pct >= 0.0
            && self.fee_drag_bps.is_finite()
            && self.fee_drag_bps >= 0.0
            && self.sharpe_ratio.is_none_or(f64::is_finite)
            && self.sortino_ratio.is_none_or(f64::is_finite)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OosFoldBundle {
    pub methodology_id: String,
    pub fold_id: String,
    pub stress: bool,
    pub stress_profile: Option<String>,
    pub candidates: BTreeMap<String, OosFoldMetrics>,
}

impl OosFoldBundle {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.methodology_id != "anchorbell-oos-fold-bundle-v2"
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
            return Err("invalid_fold_bundle_metrics");
        }
        Ok(())
    }
}

pub fn validate_candidate_fold_coverage(
    candidates: &BTreeMap<String, Vec<OosFoldMetrics>>,
) -> Result<(), &'static str> {
    if candidates.is_empty() {
        return Err("candidate_folds_required");
    }
    let mut expected_coverage: Option<BTreeSet<(String, bool, Option<String>, String)>> = None;
    for (candidate_id, folds) in candidates {
        if candidate_id.trim().is_empty()
            || folds.is_empty()
            || folds.iter().any(|fold| !fold.valid())
        {
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
            coverage.insert((
                fold.fold_id.clone(),
                fold.stress,
                fold.stress_profile.clone(),
                fold.data_digest.clone(),
            ));
        }
        match &expected_coverage {
            Some(expected) if expected != &coverage => {
                return Err("candidate_fold_coverage_mismatch")
            }
            None => expected_coverage = Some(coverage),
            _ => {}
        }
    }
    Ok(())
}

pub fn merge_fold_bundles(
    bundles: &[OosFoldBundle],
) -> Result<BTreeMap<String, Vec<OosFoldMetrics>>, &'static str> {
    if bundles.is_empty() {
        return Err("fold_bundles_required");
    }
    let mut seen_folds = BTreeSet::new();
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
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct RobustSelectionConstraints {
    pub min_oos_folds: usize,
    pub min_stress_folds: usize,
    pub min_trades_per_oos_fold: u64,
    pub max_oos_drawdown_pct: f64,
    pub max_stress_drawdown_pct: f64,
    pub max_stress_loss_bps: f64,
    pub min_stress_survival_ppm: u32,
}

impl Default for RobustSelectionConstraints {
    fn default() -> Self {
        Self {
            min_oos_folds: 3,
            min_stress_folds: 3,
            min_trades_per_oos_fold: 10,
            max_oos_drawdown_pct: 5.0,
            max_stress_drawdown_pct: 10.0,
            max_stress_loss_bps: 50.0,
            min_stress_survival_ppm: 666_667,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RobustCandidateEvaluation {
    pub eligible: bool,
    pub reason: String,
    pub oos_fold_count: usize,
    pub stress_fold_count: usize,
    pub stress_survival_ppm: u32,
    pub lower_quartile_net_return_bps: f64,
    pub median_net_return_bps: f64,
    pub median_sharpe_ratio: f64,
    pub median_sortino_ratio: f64,
    pub worst_max_drawdown_pct: f64,
    pub median_fee_drag_bps: f64,
    pub return_mad_bps: f64,
}

fn percentile(values: &[f64], ppm: u32) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let index = ((sorted.len() - 1) as u128 * u128::from(ppm) / 1_000_000) as usize;
    sorted[index]
}

fn median(values: &[f64]) -> f64 {
    percentile(values, 500_000)
}

fn mad(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let center = median(values);
    let deviations = values
        .iter()
        .map(|value| (value - center).abs())
        .collect::<Vec<_>>();
    median(&deviations)
}

pub fn evaluate_robust_candidate(
    folds: &[OosFoldMetrics],
    constraints: RobustSelectionConstraints,
) -> RobustCandidateEvaluation {
    let invalid_constraints = constraints.min_oos_folds == 0
        || constraints.max_oos_drawdown_pct <= 0.0
        || constraints.max_stress_drawdown_pct <= 0.0
        || constraints.max_stress_loss_bps < 0.0
        || constraints.min_stress_survival_ppm > 1_000_000;
    let base = |reason: &str| RobustCandidateEvaluation {
        eligible: false,
        reason: reason.to_owned(),
        oos_fold_count: 0,
        stress_fold_count: 0,
        stress_survival_ppm: 0,
        lower_quartile_net_return_bps: 0.0,
        median_net_return_bps: 0.0,
        median_sharpe_ratio: 0.0,
        median_sortino_ratio: 0.0,
        worst_max_drawdown_pct: 0.0,
        median_fee_drag_bps: 0.0,
        return_mad_bps: 0.0,
    };
    if invalid_constraints {
        return base("invalid_constraints");
    }
    if folds.iter().any(|fold| !fold.valid()) {
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
    let stress = folds.iter().filter(|fold| fold.stress).collect::<Vec<_>>();
    if oos.len() < constraints.min_oos_folds {
        return base("insufficient_oos_folds");
    }
    if stress.len() < constraints.min_stress_folds {
        return base("insufficient_stress_folds");
    }
    if oos
        .iter()
        .any(|fold| fold.trades < constraints.min_trades_per_oos_fold)
    {
        return base("insufficient_oos_trades");
    }
    if oos
        .iter()
        .any(|fold| fold.sharpe_ratio.is_none() || fold.sortino_ratio.is_none())
    {
        return base("risk_metrics_incomplete");
    }

    let returns = oos
        .iter()
        .map(|fold| fold.net_return_bps)
        .collect::<Vec<_>>();
    let sharpes = oos
        .iter()
        .filter_map(|fold| fold.sharpe_ratio)
        .collect::<Vec<_>>();
    let sortinos = oos
        .iter()
        .filter_map(|fold| fold.sortino_ratio)
        .collect::<Vec<_>>();
    let fees = oos.iter().map(|fold| fold.fee_drag_bps).collect::<Vec<_>>();
    let worst_drawdown = oos
        .iter()
        .map(|fold| fold.max_drawdown_pct)
        .fold(0.0_f64, f64::max);
    let survivors = stress
        .iter()
        .filter(|fold| {
            fold.net_return_bps >= -constraints.max_stress_loss_bps
                && fold.max_drawdown_pct <= constraints.max_stress_drawdown_pct
        })
        .count();
    let stress_survival_ppm = (survivors as u128 * 1_000_000 / stress.len().max(1) as u128) as u32;

    let lower_quartile = percentile(&returns, 250_000);
    let median_return = median(&returns);
    let median_sharpe = median(&sharpes);
    let median_sortino = median(&sortinos);
    let median_fee = median(&fees);
    let return_mad = mad(&returns);

    let mut evaluation = RobustCandidateEvaluation {
        eligible: false,
        reason: "candidate_rejected".to_owned(),
        oos_fold_count: oos.len(),
        stress_fold_count: stress.len(),
        stress_survival_ppm,
        lower_quartile_net_return_bps: lower_quartile,
        median_net_return_bps: median_return,
        median_sharpe_ratio: median_sharpe,
        median_sortino_ratio: median_sortino,
        worst_max_drawdown_pct: worst_drawdown,
        median_fee_drag_bps: median_fee,
        return_mad_bps: return_mad,
    };
    evaluation.reason = if worst_drawdown > constraints.max_oos_drawdown_pct {
        "oos_drawdown_limit_exceeded"
    } else if stress_survival_ppm < constraints.min_stress_survival_ppm {
        "stress_survival_below_floor"
    } else if lower_quartile <= 0.0 {
        "lower_quartile_return_not_positive"
    } else if median_sharpe <= 0.0 || median_sortino <= 0.0 {
        "risk_adjusted_return_not_positive"
    } else {
        evaluation.eligible = true;
        "eligible"
    }
    .to_owned();
    evaluation
}

pub fn compare_robust_candidates(
    left: &RobustCandidateEvaluation,
    right: &RobustCandidateEvaluation,
) -> Ordering {
    left.eligible
        .cmp(&right.eligible)
        .then_with(|| left.stress_survival_ppm.cmp(&right.stress_survival_ppm))
        .then_with(|| {
            left.lower_quartile_net_return_bps
                .total_cmp(&right.lower_quartile_net_return_bps)
        })
        .then_with(|| {
            left.median_sharpe_ratio
                .total_cmp(&right.median_sharpe_ratio)
        })
        .then_with(|| {
            left.median_sortino_ratio
                .total_cmp(&right.median_sortino_ratio)
        })
        .then_with(|| {
            right
                .worst_max_drawdown_pct
                .total_cmp(&left.worst_max_drawdown_pct)
        })
        .then_with(|| right.return_mad_bps.total_cmp(&left.return_mad_bps))
        .then_with(|| {
            right
                .median_fee_drag_bps
                .total_cmp(&left.median_fee_drag_bps)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold(id: &str, stress: bool, ret: f64, sharpe: f64, dd: f64) -> OosFoldMetrics {
        let digest_seed = id.bytes().fold(0_u8, |acc, byte| acc.wrapping_add(byte));
        OosFoldMetrics {
            fold_id: id.to_owned(),
            data_digest: format!("sha256:{digest_seed:064x}"),
            stress,
            stress_profile: stress.then(|| EXECUTION_ADVERSE_STRESS_PROFILE_V1.to_owned()),
            net_return_bps: ret,
            sharpe_ratio: Some(sharpe),
            sortino_ratio: Some(sharpe * 1.2),
            max_drawdown_pct: dd,
            fee_drag_bps: 4.0,
            trades: 40,
        }
    }

    #[test]
    fn merge_fold_bundles_groups_candidates_and_rejects_duplicate_fold_identity() {
        let mut first_candidates = BTreeMap::new();
        first_candidates.insert("m7|".to_owned(), fold("o1", false, 5.0, 1.0, 1.0));
        let first = OosFoldBundle {
            methodology_id: "anchorbell-oos-fold-bundle-v2".to_owned(),
            fold_id: "o1".to_owned(),
            stress: false,
            stress_profile: None,
            candidates: first_candidates,
        };
        let mut second_candidates = BTreeMap::new();
        second_candidates.insert("m7|".to_owned(), fold("s1", true, -2.0, 0.2, 2.0));
        let second = OosFoldBundle {
            methodology_id: "anchorbell-oos-fold-bundle-v2".to_owned(),
            fold_id: "s1".to_owned(),
            stress: true,
            stress_profile: Some(EXECUTION_ADVERSE_STRESS_PROFILE_V1.to_owned()),
            candidates: second_candidates,
        };
        let merged = merge_fold_bundles(&[first.clone(), second]).unwrap();
        assert_eq!(merged["m7|"].len(), 2);
        assert_eq!(
            merge_fold_bundles(&[first.clone(), first]).unwrap_err(),
            "duplicate_fold_id"
        );
    }

    #[test]
    fn candidate_fold_coverage_mismatch_is_rejected() {
        let mut candidates = BTreeMap::new();
        candidates.insert(
            "a".to_owned(),
            vec![
                fold("o1", false, 1.0, 1.0, 1.0),
                fold("s1", true, -1.0, 0.2, 2.0),
            ],
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
            &[
                first.clone(),
                second,
                fold("o3", false, 3.0, 1.2, 1.0),
                fold("s1", true, 0.0, 0.2, 2.0),
                fold("s2", true, 0.0, 0.2, 2.0),
                fold("s3", true, 0.0, 0.2, 2.0),
            ],
            RobustSelectionConstraints::default(),
        );
        assert!(!result.eligible);
        assert_eq!(result.reason, "duplicate_data_window");
        first.data_digest = format!("sha256:{:064x}", 999_u64);
        assert!(first.valid());
    }

    #[test]
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

    #[test]
    fn stable_candidate_passes_hard_oos_and_stress_gates() {
        let folds = vec![
            fold("o1", false, 12.0, 1.1, 1.2),
            fold("o2", false, 10.0, 1.0, 1.0),
            fold("o3", false, 8.0, 0.9, 1.4),
            fold("s1", true, -5.0, 0.2, 3.0),
            fold("s2", true, 1.0, 0.3, 2.5),
            fold("s3", true, -8.0, 0.1, 4.0),
        ];
        let result = evaluate_robust_candidate(&folds, RobustSelectionConstraints::default());
        assert!(result.eligible);
        assert_eq!(result.reason, "eligible");
        assert_eq!(result.stress_survival_ppm, 1_000_000);
    }

    #[test]
    fn one_spectacular_fold_cannot_hide_a_negative_lower_quartile() {
        let folds = vec![
            fold("o1", false, 200.0, 4.0, 1.0),
            fold("o2", false, -2.0, 0.4, 1.0),
            fold("o3", false, -4.0, 0.3, 1.0),
            fold("s1", true, 0.0, 0.1, 2.0),
            fold("s2", true, 0.0, 0.1, 2.0),
            fold("s3", true, 0.0, 0.1, 2.0),
        ];
        let result = evaluate_robust_candidate(&folds, RobustSelectionConstraints::default());
        assert!(!result.eligible);
        assert_eq!(result.reason, "lower_quartile_return_not_positive");
    }

    #[test]
    fn ranking_prefers_lower_tail_risk_before_headline_return() {
        let safe = RobustCandidateEvaluation {
            eligible: true,
            reason: "eligible".into(),
            oos_fold_count: 3,
            stress_fold_count: 3,
            stress_survival_ppm: 1_000_000,
            lower_quartile_net_return_bps: 8.0,
            median_net_return_bps: 10.0,
            median_sharpe_ratio: 1.0,
            median_sortino_ratio: 1.2,
            worst_max_drawdown_pct: 1.0,
            median_fee_drag_bps: 3.0,
            return_mad_bps: 1.0,
        };
        let mut fragile = safe.clone();
        fragile.median_net_return_bps = 50.0;
        fragile.stress_survival_ppm = 666_667;
        assert_eq!(
            compare_robust_candidates(&safe, &fragile),
            Ordering::Greater
        );
    }
}
