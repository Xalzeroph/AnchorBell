use crate::policy::StrategyPlan;

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
    InvalidArtifactIdentity,
    InvalidEvidenceDigest,
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
        if champion.version.trim().is_empty()
            || champion.version != champion.plan.version
            || !champion.plan.is_legal()
            || challenger.version.trim().is_empty()
            || challenger.plan.version != challenger.version
        {
            return Err(PromotionRejection::InvalidArtifactIdentity);
        }
        if !is_sha256_digest(&challenger.evidence_digest) {
            return Err(if challenger.evidence_digest.is_empty() {
                PromotionRejection::MissingEvidence
            } else {
                PromotionRejection::InvalidEvidenceDigest
            });
        }
        if !is_sha256_digest(&challenger.sealed_set_digest) {
            return Err(if challenger.sealed_set_digest.is_empty() {
                PromotionRejection::MissingSealedSet
            } else {
                PromotionRejection::InvalidEvidenceDigest
            });
        }
        if challenger.max_drawdown_pico_bps < 0 {
            return Err(PromotionRejection::DrawdownLimit);
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
    plan.is_legal()
}

fn is_sha256_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(version: &str, value: i64) -> StrategyArtifact {
        StrategyArtifact {
            version: version.into(),
            plan: StrategyPlan::anchor_closed_maker(version),
            evidence_digest: "a".repeat(64),
            sealed_set_digest: "b".repeat(64),
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
    fn evolution_rejects_identity_and_digest_mismatches() {
        let gate = PromotionGate {
            invariants: CoreInvariants::immutable(),
            thresholds: PromotionThresholds {
                minimum_effective_sample_size: 30,
                maximum_drawdown_pico_bps: 100,
            },
        };
        let champion = artifact("champion", 10);
        let mut challenger = artifact("challenger", 20);
        challenger.plan.version = "wrong".into();
        assert_eq!(
            gate.approve(&champion, &challenger),
            Err(PromotionRejection::InvalidArtifactIdentity)
        );
        challenger.plan.version = "challenger".into();
        challenger.evidence_digest = "bad".into();
        assert_eq!(
            gate.approve(&champion, &challenger),
            Err(PromotionRejection::InvalidEvidenceDigest)
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
