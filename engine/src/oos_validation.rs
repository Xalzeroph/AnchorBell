use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OosFoldMetrics {
    pub fold_id: String,
    pub stress: bool,
    pub net_return_bps: f64,
    pub sharpe_ratio: Option<f64>,
    pub sortino_ratio: Option<f64>,
    pub max_drawdown_pct: f64,
    pub fee_drag_bps: f64,
    pub trades: u64,
}

impl OosFoldMetrics {
    fn valid(&self) -> bool {
        !self.fold_id.trim().is_empty()
            && self.net_return_bps.is_finite()
            && self.max_drawdown_pct.is_finite()
            && self.max_drawdown_pct >= 0.0
            && self.fee_drag_bps.is_finite()
            && self.fee_drag_bps >= 0.0
            && self.sharpe_ratio.is_none_or(f64::is_finite)
            && self.sortino_ratio.is_none_or(f64::is_finite)
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OosFoldBundle {
    pub candidate_id: String,
    pub folds: Vec<OosFoldMetrics>,
}

pub fn validate_candidate_fold_coverage(
    candidates: &BTreeMap<String, Vec<OosFoldMetrics>>,
) -> Result<(), String> {
    if candidates.is_empty() {
        return Err("candidate list cannot be empty".to_owned());
    }
    for (candidate_id, folds) in candidates {
        if candidate_id.trim().is_empty() {
            return Err("candidate id cannot be empty".to_owned());
        }
        if folds.is_empty() {
            return Err(format!("candidate {candidate_id} has no folds"));
        }
        let mut ids = BTreeSet::new();
        for fold in folds {
            if !fold.valid() {
                return Err(format!("candidate {candidate_id} contains invalid fold"));
            }
            if !ids.insert(fold.fold_id.as_str()) {
                return Err(format!(
                    "candidate {candidate_id} contains duplicate fold {}",
                    fold.fold_id
                ));
            }
        }
    }
    Ok(())
}

pub fn merge_fold_bundles(
    bundles: &[OosFoldBundle],
) -> Result<BTreeMap<String, Vec<OosFoldMetrics>>, String> {
    if bundles.is_empty() {
        return Err("fold bundle list cannot be empty".to_owned());
    }
    let mut merged = BTreeMap::<String, Vec<OosFoldMetrics>>::new();
    for bundle in bundles {
        if bundle.candidate_id.trim().is_empty() {
            return Err("fold bundle candidate id cannot be empty".to_owned());
        }
        merged
            .entry(bundle.candidate_id.clone())
            .or_default()
            .extend(bundle.folds.clone());
    }
    validate_candidate_fold_coverage(&merged)?;
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold(id: &str, stress: bool, ret: f64, sharpe: f64, dd: f64) -> OosFoldMetrics {
        OosFoldMetrics {
            fold_id: id.to_owned(),
            stress,
            net_return_bps: ret,
            sharpe_ratio: Some(sharpe),
            sortino_ratio: Some(sharpe * 1.2),
            max_drawdown_pct: dd,
            fee_drag_bps: 4.0,
            trades: 40,
        }
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
    fn ranking_prefers_lower_tail_risk_beforeheadline_return() {
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
