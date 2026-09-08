use crate::{
    simulation::engine::SimulationPolicyVariant,
    strategy::method_catalog::resolve as resolve_strategy_method,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ExperimentRole {
    Control,
    #[default]
    Incremental,
    Ablation,
    Challenger,
    SafetyOverlay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExperimentSpec {
    pub label: String,
    pub strategy: String,
    #[serde(default)]
    pub ablations: Vec<String>,
    #[serde(default)]
    pub role: ExperimentRole,
    #[serde(default)]
    pub parent_experiment_id: Option<String>,
    #[serde(default = "default_execution_overlay")]
    pub execution_overlay: String,
    #[serde(default = "default_evidence_policy")]
    pub evidence_policy: String,
}

fn default_execution_overlay() -> String {
    "maker_only".to_owned()
}

fn default_evidence_policy() -> String {
    "oos_required".to_owned()
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentRuntimeSpec {
    pub label: String,
    pub strategy: String,
    pub variant: SimulationPolicyVariant,
    pub ablations: Vec<String>,
    pub role: ExperimentRole,
    pub parent_experiment_id: Option<String>,
    pub execution_overlay: String,
    pub evidence_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExperimentPlan {
    pub schema_version: u16,
    pub plan_id: String,
    pub experiments: Vec<ExperimentSpec>,
}

impl ExperimentPlan {
    pub const SCHEMA_VERSION: u16 = 1;

    pub fn from_specs(
        plan_id: String,
        experiments: Vec<ExperimentSpec>,
    ) -> Result<Self, &'static str> {
        let plan = Self {
            schema_version: Self::SCHEMA_VERSION,
            plan_id,
            experiments,
        };
        plan.validate()?;
        Ok(plan)
    }

    #[cfg(test)]
    pub fn m1_to_m8() -> Self {
        let names = [
            ("F0_m0", "m0"),
            ("F1_m1", "m1"),
            ("F2_m2", "m2"),
            ("F3_m3", "m3"),
            ("F4_m4", "m4"),
            ("F5_m5", "m5"),
            ("F6_m6", "m6"),
            ("F7_m7", "m7"),
            ("M8_full", "m8"),
        ];
        let mut experiments = names
            .into_iter()
            .map(|(label, strategy)| ExperimentSpec {
                label: label.into(),
                strategy: strategy.into(),
                ablations: vec![],
                role: if label == "F0_m0" {
                    ExperimentRole::Control
                } else {
                    ExperimentRole::Incremental
                },
                parent_experiment_id: None,
                execution_overlay: "maker_only".into(),
                evidence_policy: "oos_required".into(),
            })
            .collect::<Vec<_>>();
        for index in 1..experiments.len() {
            experiments[index].parent_experiment_id = Some(experiments[index - 1].label.clone());
        }
        // The default matrix contains one ledger per hypothesis. Historical
        // R1-R7 copies were deterministic duplicates, not independent trials;
        // keeping them in the default run inflated order/fill/PnL totals and
        // contaminated model comparison. Replays belong to a separate
        // repeatability plan with an explicit seed and are never mixed into
        // economic hypothesis testing.
        experiments.push(ExperimentSpec {
            label: "M8_no_funding".into(),
            strategy: "m8".into(),
            ablations: vec!["funding".into()],
            role: ExperimentRole::Ablation,
            parent_experiment_id: Some("M8_full".into()),
            execution_overlay: "maker_only".into(),
            evidence_policy: "oos_required".into(),
        });
        Self {
            schema_version: Self::SCHEMA_VERSION,
            plan_id: "m1-m8-single-ledger-ablation-matrix".into(),
            experiments,
        }
    }

    /// The M1-M8 matrix plus the full M9 challenger. Kept separate so an
    /// existing M1-M8 run cannot silently change its strategy population.
    #[cfg(test)]
    pub fn m1_to_m9() -> Self {
        let mut plan = Self::m1_to_m8();
        plan.plan_id = "m1-m9-single-ledger-ablation-matrix".into();
        plan.experiments.push(ExperimentSpec {
            label: "M9_full".into(),
            strategy: "m9".into(),
            ablations: vec![],
            role: ExperimentRole::Challenger,
            parent_experiment_id: Some("M8_full".into()),
            execution_overlay: "maker_only".into(),
            evidence_policy: "oos_required".into(),
        });
        plan
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != Self::SCHEMA_VERSION || self.plan_id.trim().is_empty() {
            return Err("invalid experiment plan identity");
        }
        if self.experiments.is_empty()
            || self.experiments.iter().any(|e| {
                e.label.trim().is_empty() || e.strategy.trim().is_empty() || {
                    let mut values = BTreeSet::new();
                    e.ablations.iter().any(|ablation| {
                        ablation.trim().is_empty() || !values.insert(ablation.trim().to_owned())
                    })
                }
            })
        {
            return Err("experiment plan contains an invalid experiment");
        }
        let mut labels = BTreeSet::new();
        let mut identities = BTreeSet::new();
        let mut ordered_labels = BTreeSet::new();
        for experiment in &self.experiments {
            if !labels.insert(experiment.label.as_str()) {
                return Err("experiment labels must be unique");
            }
            if !matches!(
                experiment.execution_overlay.as_str(),
                "maker_only" | "emergency_reduce_only_taker"
            ) {
                return Err("experiment execution overlay is unsupported");
            }
            if !matches!(
                experiment.evidence_policy.as_str(),
                "pre_screen_only" | "oos_required" | "stress_required" | "promotion_required"
            ) {
                return Err("experiment evidence policy is unsupported");
            }
            if let Some(parent) = experiment.parent_experiment_id.as_deref() {
                if parent == experiment.label || !ordered_labels.contains(parent) {
                    return Err("experiment parent must refer to an earlier experiment");
                }
            }
            ordered_labels.insert(experiment.label.clone());
            let mut ablations = experiment.ablations.clone();
            ablations.sort();
            if !identities.insert((experiment.strategy.clone(), ablations)) {
                return Err("experiment identities must be unique");
            }
            resolve_strategy_method(&experiment.strategy, &experiment.ablations)?;
        }
        Ok(())
    }

    pub fn runtime_specs_with_ablations(&self) -> Result<Vec<ExperimentRuntimeSpec>, &'static str> {
        self.validate()?;
        self.experiments
            .iter()
            .map(|experiment| {
                let variant = resolve_strategy_method(&experiment.strategy, &experiment.ablations)?;
                Ok(ExperimentRuntimeSpec {
                    label: experiment.label.clone(),
                    strategy: experiment.strategy.clone(),
                    variant,
                    ablations: experiment.ablations.clone(),
                    role: experiment.role.clone(),
                    parent_experiment_id: experiment.parent_experiment_id.clone(),
                    execution_overlay: experiment.execution_overlay.clone(),
                    evidence_policy: experiment.evidence_policy.clone(),
                })
            })
            .collect()
    }

    pub fn runtime_specs(&self) -> Result<Vec<(String, SimulationPolicyVariant)>, &'static str> {
        self.runtime_specs_with_ablations().map(|specs| {
            specs
                .into_iter()
                .map(|spec| (spec.label, spec.variant))
                .collect()
        })
    }

    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("experiment plan is serializable");
        format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_plan_contains_the_full_matrix() {
        let plan = ExperimentPlan::m1_to_m8();
        assert_eq!(plan.experiments.len(), 10);
        assert_eq!(plan.runtime_specs().unwrap().len(), 10);
        let no_funding = plan
            .experiments
            .iter()
            .find(|experiment| experiment.label == "M8_no_funding")
            .unwrap();
        assert_eq!(no_funding.strategy, "m8");
        let runtime = plan.runtime_specs_with_ablations().unwrap();
        let runtime = runtime
            .iter()
            .find(|spec| spec.label == "M8_no_funding")
            .unwrap();
        assert_eq!(runtime.variant, SimulationPolicyVariant::M7EvidenceGated);
        assert_eq!(runtime.ablations, vec!["funding".to_owned()]);
        assert_eq!(runtime.role, ExperimentRole::Ablation);
        assert_eq!(runtime.parent_experiment_id.as_deref(), Some("M8_full"));
    }

    #[test]
    fn digest_changes_when_experiment_lineage_changes() {
        let mut plan = ExperimentPlan::m1_to_m8();
        let original = plan.digest();
        plan.experiments[0].evidence_policy = "stress_required".into();
        assert_ne!(original, plan.digest());
    }

    #[test]
    fn parent_must_be_earlier_and_overlay_must_be_registered() {
        let mut plan = ExperimentPlan::m1_to_m8();
        plan.experiments[0].parent_experiment_id = Some("M8_full".into());
        assert_eq!(
            plan.validate(),
            Err("experiment parent must refer to an earlier experiment")
        );
        plan.experiments[0].parent_experiment_id = None;
        plan.experiments[0].execution_overlay = "unknown".into();
        assert_eq!(
            plan.validate(),
            Err("experiment execution overlay is unsupported")
        );
    }

    #[test]
    fn m9_plan_is_explicit_and_keeps_m1_to_m8_stable() {
        let plan = ExperimentPlan::m1_to_m9();
        assert_eq!(plan.experiments.len(), 11);
        assert_eq!(
            plan.runtime_specs().unwrap().last().unwrap().1,
            SimulationPolicyVariant::M9DeadlineCausalDroMpc
        );
    }
}
