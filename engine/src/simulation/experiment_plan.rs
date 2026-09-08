use crate::{
    simulation::engine::SimulationPolicyVariant,
    strategy::method_catalog::resolve as resolve_strategy_method,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExperimentSpec {
    pub label: String,
    pub strategy: String,
    pub ablations: Vec<String>,
}

pub type ExperimentRuntimeSpec = (String, SimulationPolicyVariant, Vec<String>);

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
            })
            .collect::<Vec<_>>();
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
        for experiment in &self.experiments {
            if !labels.insert(experiment.label.as_str()) {
                return Err("experiment labels must be unique");
            }
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
                Ok((
                    experiment.label.clone(),
                    variant,
                    experiment.ablations.clone(),
                ))
            })
            .collect()
    }

    pub fn runtime_specs(&self) -> Result<Vec<(String, SimulationPolicyVariant)>, &'static str> {
        self.runtime_specs_with_ablations().map(|specs| {
            specs
                .into_iter()
                .map(|(label, variant, _)| (label, variant))
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_plan_contains_the_full_matrix() {
        let plan = ExperimentPlan::m1_to_m8();
        assert_eq!(plan.experiments.len(), 9);
        assert_eq!(plan.runtime_specs().unwrap().len(), 9);
        let no_funding = plan
            .experiments
            .iter()
            .find(|experiment| experiment.label == "M8_no_funding")
            .unwrap();
        assert_eq!(no_funding.strategy, "m8");
        let runtime = plan.runtime_specs_with_ablations().unwrap();
        let (_, variant, ablations) = runtime
            .iter()
            .find(|(label, _, _)| label == "M8_no_funding")
            .unwrap();
        assert_eq!(*variant, SimulationPolicyVariant::M7EvidenceGated);
        assert_eq!(ablations, &vec!["funding".to_owned()]);
    }

    #[test]
    fn m9_plan_is_explicit_and_keeps_m1_to_m8_stable() {
        let plan = ExperimentPlan::m1_to_m9();
        assert_eq!(plan.experiments.len(), 10);
        assert_eq!(
            plan.runtime_specs().unwrap().last().unwrap().1,
            SimulationPolicyVariant::M9DeadlineCausalDroMpc
        );
    }
}
