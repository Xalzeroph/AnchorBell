use crate::policy::{StrategyNode, StrategyPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreInvariants {
    pub anchor_bound: bool,
    pub closed_window: bool,
    pub causal_information: bool,
    pub binance_contract: bool,
    pub maker_entry: bool,
    pub reconciled_account: bool,
}

impl CoreInvariants {
    pub const fn immutable() -> Self {
        Self {
            anchor_bound: true,
            closed_window: true,
            causal_information: true,
            binance_contract: true,
            maker_entry: true,
            reconciled_account: true,
        }
    }
    pub fn holds(self) -> bool {
        self.anchor_bound
            && self.closed_window
            && self.causal_information
            && self.binance_contract
            && self.maker_entry
            && self.reconciled_account
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyArtifact {
    pub version: String,
    pub plan: StrategyPlan,
    pub evidence_digest: String,
    pub sealed_set_digest: String,
    pub effective_sample_size: u64,
    pub robust_value_pico_bps: i64,
    pub max_drawdown_pico_bps: i64,
    pub simplicity_score: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotionThresholds {
    pub minimum_effective_sample_size: u64,
    pub maximum_drawdown_pico_bps: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionRejection {
    InvalidInvariants,
    MissingEvidence,
    MissingSealedSet,
    InsufficientHistory,
    DrawdownLimit,
    NotBetterThanChampion,
    MissingRequiredSemantics,
}

pub struct PromotionGate {
    pub invariants: CoreInvariants,
    pub thresholds: PromotionThresholds,
}

impl PromotionGate {
    pub fn approve(
        &self,
        champion: &StrategyArtifact,
        challenger: &StrategyArtifact,
    ) -> Result<(), PromotionRejection> {
        if !self.invariants.holds() {
            return Err(PromotionRejection::InvalidInvariants);
        }
        if challenger.evidence_digest.is_empty() {
            return Err(PromotionRejection::MissingEvidence);
        }
        if challenger.sealed_set_digest.is_empty() {
            return Err(PromotionRejection::MissingSealedSet);
        }
        if challenger.effective_sample_size < self.thresholds.minimum_effective_sample_size {
            return Err(PromotionRejection::InsufficientHistory);
        }
        if challenger.max_drawdown_pico_bps > self.thresholds.maximum_drawdown_pico_bps {
            return Err(PromotionRejection::DrawdownLimit);
        }
        if !required_semantics(&challenger.plan) {
            return Err(PromotionRejection::MissingRequiredSemantics);
        }
        if challenger.robust_value_pico_bps < champion.robust_value_pico_bps
            || (challenger.robust_value_pico_bps == champion.robust_value_pico_bps
                && challenger.simplicity_score >= champion.simplicity_score)
        {
            return Err(PromotionRejection::NotBetterThanChampion);
        }
        Ok(())
    }
}

fn required_semantics(plan: &StrategyPlan) -> bool {
    [
        StrategyNode::BindAnchor,
        StrategyNode::RequireClosedWindow,
        StrategyNode::EstimateJointOutcome,
        StrategyNode::AdmitIfRobustValueBeatsWait,
        StrategyNode::QuotePassive,
        StrategyNode::ReconcileBeforeResume,
    ]
    .iter()
    .all(|required| plan.nodes.contains(required))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(version: &str, value: i64) -> StrategyArtifact {
        StrategyArtifact {
            version: version.into(),
            plan: StrategyPlan::anchor_closed_maker(version),
            evidence_digest: "evidence".into(),
            sealed_set_digest: "sealed".into(),
            effective_sample_size: 100,
            robust_value_pico_bps: value,
            max_drawdown_pico_bps: 10,
            simplicity_score: 1,
        }
    }

    #[test]
    fn evolution_cannot_remove_anchor_or_closed_invariants() {
        let gate = PromotionGate {
            invariants: CoreInvariants::immutable(),
            thresholds: PromotionThresholds {
                minimum_effective_sample_size: 30,
                maximum_drawdown_pico_bps: 100,
            },
        };
        assert!(gate
            .approve(&artifact("champion", 10), &artifact("challenger", 20))
            .is_ok());
        let blocked = PromotionGate {
            invariants: CoreInvariants {
                closed_window: false,
                ..CoreInvariants::immutable()
            },
            ..gate
        };
        assert_eq!(
            blocked.approve(&artifact("champion", 10), &artifact("challenger", 20)),
            Err(PromotionRejection::InvalidInvariants)
        );
    }

    #[test]
    fn evolution_cannot_trade_robust_value_for_simplicity() {
        let gate = PromotionGate {
            invariants: CoreInvariants::immutable(),
            thresholds: PromotionThresholds {
                minimum_effective_sample_size: 30,
                maximum_drawdown_pico_bps: 100,
            },
        };
        let mut simpler_but_worse = artifact("challenger", 9);
        simpler_but_worse.simplicity_score = 0;
        assert_eq!(
            gate.approve(&artifact("champion", 10), &simpler_but_worse),
            Err(PromotionRejection::NotBetterThanChampion)
        );
    }
}
