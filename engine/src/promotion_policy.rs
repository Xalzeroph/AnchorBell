//! Configuration-driven candidate promotion and ranking.
//!
//! This module is deliberately separate from execution. It turns OOS/stress
//! fold evidence into an auditable research disposition. Every business
//! threshold is supplied by a serialized policy; there is no production
//! fallback policy in code.

use crate::oos_validation::{
    evaluate_robust_candidate, OosFoldMetrics, RobustCandidateEvaluation,
    RobustSelectionConstraints,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

pub const PROMOTION_POLICY_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RiskAdjustedPromotionPolicy {
    pub schema_version: u16,
    pub policy_id: String,
    pub min_oos_folds: usize,
    pub min_stress_folds: usize,
    pub min_trades_per_oos_fold: u64,
    pub max_oos_drawdown_pct: f64,
    pub max_stress_drawdown_pct: f64,
    pub max_stress_loss_bps: f64,
    pub min_stress_survival_ppm: u32,
    pub min_oos_return_bps: f64,
    pub min_oos_positive_return_ppm: u32,
    pub min_lower_quartile_net_return_bps: f64,
    pub min_median_sharpe_ratio: f64,
    pub min_median_sortino_ratio: f64,
    pub max_return_mad_bps: f64,
    pub max_median_fee_drag_bps: f64,
}

impl RiskAdjustedPromotionPolicy {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read promotion policy {}: {error}", path.display()))?;
        let policy: Self = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid promotion policy {}: {error}", path.display()))?;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), String> {
        let finite = [
            self.max_oos_drawdown_pct,
            self.max_stress_drawdown_pct,
            self.max_stress_loss_bps,
            self.min_oos_return_bps,
            self.min_lower_quartile_net_return_bps,
            self.min_median_sharpe_ratio,
            self.min_median_sortino_ratio,
            self.max_return_mad_bps,
            self.max_median_fee_drag_bps,
        ]
        .into_iter()
        .all(f64::is_finite);
        if self.schema_version != PROMOTION_POLICY_SCHEMA_VERSION
            || self.policy_id.trim().is_empty()
            || self.min_oos_folds == 0
            || self.min_stress_folds == 0
            || self.min_trades_per_oos_fold == 0
            || !finite
            || self.max_oos_drawdown_pct <= 0.0
            || self.max_stress_drawdown_pct <= 0.0
            || self.max_stress_loss_bps < 0.0
            || self.min_stress_survival_ppm > 1_000_000
            || self.min_oos_positive_return_ppm > 1_000_000
            || self.max_return_mad_bps < 0.0
            || self.max_median_fee_drag_bps < 0.0
        {
            return Err("promotion policy contains invalid thresholds".to_owned());
        }
        Ok(())
    }

    pub fn robust_constraints(&self) -> RobustSelectionConstraints {
        RobustSelectionConstraints {
            min_oos_folds: self.min_oos_folds,
            min_stress_folds: self.min_stress_folds,
            min_trades_per_oos_fold: self.min_trades_per_oos_fold,
            max_oos_drawdown_pct: self.max_oos_drawdown_pct,
            max_stress_drawdown_pct: self.max_stress_drawdown_pct,
            max_stress_loss_bps: self.max_stress_loss_bps,
            min_stress_survival_ppm: self.min_stress_survival_ppm,
        }
    }

    pub fn digest(&self) -> Result<String, String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| "policy serialization failed".to_owned())?;
        Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
    }

    pub fn evaluate(
        &self,
        folds: &[OosFoldMetrics],
    ) -> RiskAdjustedCandidateEvaluation {
        if let Err(reason) = self.validate() {
            return RiskAdjustedCandidateEvaluation::rejected(reason);
        }

        let robust = evaluate_robust_candidate(folds, self.robust_constraints());
        if !robust.eligible {
            return RiskAdjustedCandidateEvaluation::from_robust(robust);
        }

        let oos = folds.iter().filter(|fold| !fold.stress).collect::<Vec<_>>();
        let positive_oos = oos
            .iter()
            .filter(|fold| fold.net_return_bps >= self.min_oos_return_bps)
            .count();
        let positive_oos_survival_ppm =
            (positive_oos as u128 * 1_000_000 / oos.len().max(1) as u128) as u32;
        let worst_oos_return = oos
            .iter()
            .map(|fold| fold.net_return_bps)
            .fold(f64::INFINITY, f64::min);
        let score = robust.lower_quartile_net_return_bps
            / robust.worst_max_drawdown_pct.max(1.0);

        let reason = if worst_oos_return < self.min_oos_return_bps {
            "oos_return_floor_not_met"
        } else if positive_oos_survival_ppm < self.min_oos_positive_return_ppm {
            "oos_positive_return_survival_below_floor"
        } else if robust.lower_quartile_net_return_bps < self.min_lower_quartile_net_return_bps {
            "lower_quartile_return_floor_not_met"
        } else if robust.median_sharpe_ratio < self.min_median_sharpe_ratio {
            "median_sharpe_floor_not_met"
        } else if robust.median_sortino_ratio < self.min_median_sortino_ratio {
            "median_sortino_floor_not_met"
        } else if robust.return_mad_bps > self.max_return_mad_bps {
            "return_instability_limit_exceeded"
        } else if robust.median_fee_drag_bps > self.max_median_fee_drag_bps {
            "median_fee_drag_limit_exceeded"
        } else {
            "eligible"
        };

        RiskAdjustedCandidateEvaluation {
            eligible: reason == "eligible",
            stage: if reason == "eligible" {
                PromotionStage::PaperCandidate
            } else {
                PromotionStage::Rejected
            },
            reason: reason.to_owned(),
            robust,
            positive_oos_survival_ppm,
            worst_oos_return_bps: worst_oos_return,
            risk_adjusted_score: score,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum PromotionStage {
    Rejected,
    InsufficientEvidence,
    PaperCandidate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskAdjustedCandidateEvaluation {
    pub eligible: bool,
    pub stage: PromotionStage,
    pub reason: String,
    pub robust: RobustCandidateEvaluation,
    pub positive_oos_survival_ppm: u32,
    pub worst_oos_return_bps: f64,
    pub risk_adjusted_score: f64,
}

impl RiskAdjustedCandidateEvaluation {
    fn rejected(reason: String) -> Self {
        Self {
            eligible: false,
            stage: PromotionStage::Rejected,
            reason,
            robust: empty_robust_evaluation(),
            positive_oos_survival_ppm: 0,
            worst_oos_return_bps: 0.0,
            risk_adjusted_score: 0.0,
        }
    }

    fn from_robust(robust: RobustCandidateEvaluation) -> Self {
        let insufficient = matches!(
            robust.reason.as_str(),
            "insufficient_oos_folds"
                | "insufficient_stress_folds"
                | "insufficient_oos_trades"
                | "risk_metrics_incomplete"
        );
        Self {
            eligible: false,
            stage: if insufficient {
                PromotionStage::InsufficientEvidence
            } else {
                PromotionStage::Rejected
            },
            reason: robust.reason.clone(),
            robust,
            positive_oos_survival_ppm: 0,
            worst_oos_return_bps: 0.0,
            risk_adjusted_score: 0.0,
        }
    }
}

pub fn compare_risk_adjusted_candidates(
    left: &RiskAdjustedCandidateEvaluation,
    right: &RiskAdjustedCandidateEvaluation,
) -> std::cmp::Ordering {
    left.eligible
        .cmp(&right.eligible)
        .then_with(|| left.stage.cmp(&right.stage))
        .then_with(|| {
            left.risk_adjusted_score
                .total_cmp(&right.risk_adjusted_score)
        })
        .then_with(|| {
            left.positive_oos_survival_ppm
                .cmp(&right.positive_oos_survival_ppm)
        })
        .then_with(|| {
            left.robust
                .stress_survival_ppm
                .cmp(&right.robust.stress_survival_ppm)
        })
        .then_with(|| {
            left.robust
                .median_sharpe_ratio
                .total_cmp(&right.robust.median_sharpe_ratio)
        })
        .then_with(|| {
            right
                .robust
                .worst_max_drawdown_pct
                .total_cmp(&left.robust.worst_max_drawdown_pct)
        })
        .then_with(|| right.robust.return_mad_bps.total_cmp(&left.robust.return_mad_bps))
}

fn empty_robust_evaluation() -> RobustCandidateEvaluation {
    RobustCandidateEvaluation {
        eligible: false,
        reason: "invalid_policy".to_owned(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RiskAdjustedPromotionPolicy {
        RiskAdjustedPromotionPolicy {
            schema_version: PROMOTION_POLICY_SCHEMA_VERSION,
            policy_id: "test".to_owned(),
            min_oos_folds: 3,
            min_stress_folds: 3,
            min_trades_per_oos_fold: 10,
            max_oos_drawdown_pct: 5.0,
            max_stress_drawdown_pct: 10.0,
            max_stress_loss_bps: 50.0,
            min_stress_survival_ppm: 666_667,
            min_oos_return_bps: 0.0,
            min_oos_positive_return_ppm: 666_667,
            min_lower_quartile_net_return_bps: 0.0,
            min_median_sharpe_ratio: 0.5,
            min_median_sortino_ratio: 0.5,
            max_return_mad_bps: 25.0,
            max_median_fee_drag_bps: 25.0,
        }
    }

    fn fold(id: &str, stress: bool, ret: f64, sharpe: f64, dd: f64) -> OosFoldMetrics {
        OosFoldMetrics {
            fold_id: id.to_owned(),
            stress,
            net_return_bps: ret,
            sharpe_ratio: Some(sharpe),
            sortino_ratio: Some(sharpe),
            max_drawdown_pct: dd,
            fee_drag_bps: 4.0,
            trades: 40,
        }
    }

    #[test]
    fn policy_requires_explicit_business_thresholds() {
        let mut value = policy();
        value.min_oos_folds = 0;
        assert!(value.validate().is_err());
    }

    #[test]
    fn stable_candidate_becomes_paper_candidate() {
        let folds = vec![
            fold("o1", false, 12.0, 1.1, 1.2),
            fold("o2", false, 10.0, 1.0, 1.0),
            fold("o3", false, 8.0, 0.9, 1.4),
            fold("s1", true, -5.0, 0.2, 3.0),
            fold("s2", true, 1.0, 0.3, 2.5),
            fold("s3", true, -8.0, 0.1, 4.0),
        ];
        let result = policy().evaluate(&folds);
        assert!(result.eligible);
        assert_eq!(result.stage, PromotionStage::PaperCandidate);
        assert!(result.risk_adjusted_score > 0.0);
    }

    #[test]
    fn one_negative_oos_fold_is_not_promotion_ready() {
        let folds = vec![
            fold("o1", false, 12.0, 1.1, 1.2),
            fold("o2", false, 6.0, 1.0, 1.0),
            fold("o3", false, 8.0, 0.9, 1.4),
            fold("s1", true, 0.0, 0.2, 3.0),
            fold("s2", true, 1.0, 0.3, 2.5),
            fold("s3", true, 0.0, 0.1, 4.0),
        ];
        let mut configured = policy();
        configured.min_oos_return_bps = 10.0;
        let result = configured.evaluate(&folds);
        assert!(!result.eligible);
        assert_eq!(result.reason, "oos_return_floor_not_met");
    }
}
