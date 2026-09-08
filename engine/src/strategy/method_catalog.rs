//! Single source of truth for registered strategy methods.
//!
//! A method is registered once here. Experiment plans, method lineage, runtime
//! variant resolution, and generated manifests all consume this catalog.

use super::method_graph::MethodLayer;
use crate::simulation::engine::SimulationPolicyVariant;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct StrategyMethodDescriptor {
    pub id: &'static str,
    pub key: &'static str,
    pub label: &'static str,
    pub layer: MethodLayer,
    pub parent: Option<&'static str>,
    pub variant: SimulationPolicyVariant,
    pub overlays: &'static [&'static str],
    pub required_features: &'static [&'static str],
    pub immutable_overrides: &'static [&'static str],
    pub supported_ablations: &'static [&'static str],
    pub implementation: &'static str,
}

inventory::collect!(StrategyMethodDescriptor);

macro_rules! register_method {
    ($id:literal, $key:literal, $label:literal, $layer:expr, $parent:expr,
     $variant:expr, $overlays:expr, $features:expr, $overrides:expr,
     $ablations:expr, $implementation:literal) => {
        inventory::submit! {
            StrategyMethodDescriptor {
                id: $id,
                key: $key,
                label: $label,
                layer: $layer,
                parent: $parent,
                variant: $variant,
                overlays: $overlays,
                required_features: $features,
                immutable_overrides: $overrides,
                supported_ablations: $ablations,
                implementation: $implementation,
            }
        }
    };
}

register_method!(
    "M0",
    "m0",
    "m0_fixed",
    MethodLayer::Signal,
    None,
    SimulationPolicyVariant::M0Fixed,
    &[],
    &[],
    &[],
    &[],
    "strategy::anchor_policy"
);
register_method!(
    "M1",
    "m1",
    "m1_adaptive_risk",
    MethodLayer::Signal,
    None,
    SimulationPolicyVariant::M1AdaptiveRisk,
    &[],
    &[],
    &[],
    &[],
    "strategy::signal_policy"
);
register_method!(
    "M2",
    "m2",
    "m2_microstructure",
    MethodLayer::Microstructure,
    Some("M1"),
    SimulationPolicyVariant::M2Microstructure,
    &[],
    &[],
    &[],
    &[],
    "simulation::runtime::microstructure"
);
register_method!(
    "M3",
    "m3",
    "m3_fill_aware",
    MethodLayer::Fill,
    Some("M2"),
    SimulationPolicyVariant::M3FillAware,
    &[],
    &[],
    &[],
    &[],
    "simulation::runtime::fill_model"
);
register_method!(
    "M4",
    "m4",
    "m4_statistical",
    MethodLayer::Signal,
    Some("M3"),
    SimulationPolicyVariant::M4Statistical,
    &[],
    &[],
    &[],
    &[],
    "simulation::runtime::statistical_policy"
);
register_method!(
    "M5",
    "m5",
    "m5_robust",
    MethodLayer::Risk,
    Some("M4"),
    SimulationPolicyVariant::M5Robust,
    &[],
    &[],
    &[],
    &[],
    "simulation::runtime::robust_policy"
);
register_method!(
    "M6",
    "m6",
    "m6_dynamic_capital",
    MethodLayer::Capital,
    Some("M5"),
    SimulationPolicyVariant::M6DynamicCapital,
    &[],
    &[],
    &[],
    &[],
    "simulation::runtime::dynamic_capital"
);
register_method!(
    "M7",
    "m7",
    "m7_evidence_gated",
    MethodLayer::Evidence,
    Some("M6"),
    SimulationPolicyVariant::M7EvidenceGated,
    &[],
    &[],
    &[],
    &[],
    "simulation::runtime::evidence_gate"
);
register_method!(
    "M8",
    "m8",
    "m8_funding_aware",
    MethodLayer::Funding,
    Some("M7"),
    SimulationPolicyVariant::M8FundingAware,
    &["funding_controller"],
    &["funding_rate_state", "funding_schedule"],
    &["funding_entry_policy"],
    &["funding"],
    "simulation::runtime::funding_controller"
);
register_method!(
    "M9",
    "m9",
    "m9_deadline_causal_dro_mpc",
    MethodLayer::Risk,
    Some("M8"),
    SimulationPolicyVariant::M9DeadlineCausalDroMpc,
    &[
        "causal_residual",
        "joint_fill_markout_exit",
        "deadline_mpc",
        "distributionally_robust_optimizer"
    ],
    &[
        "versioned_fair_value",
        "deadline_exit_plan",
        "joint_outcome_forecast",
        "robust_risk_certificate"
    ],
    &[],
    &[],
    "strategy::m9"
);

pub fn all() -> Vec<&'static StrategyMethodDescriptor> {
    let mut values = inventory::iter::<StrategyMethodDescriptor>
        .into_iter()
        .collect::<Vec<_>>();
    values.sort_by_key(|descriptor| descriptor.id);
    values
}

pub fn resolve(key: &str, ablations: &[String]) -> Result<SimulationPolicyVariant, &'static str> {
    let normalized = key.trim();
    let descriptor = all()
        .into_iter()
        .find(|descriptor| {
            descriptor.id.eq_ignore_ascii_case(normalized)
                || descriptor.key.eq_ignore_ascii_case(normalized)
                || descriptor.label.eq_ignore_ascii_case(normalized)
        })
        .ok_or("unknown strategy method")?;

    if ablations.iter().any(|ablation| {
        !descriptor
            .supported_ablations
            .iter()
            .any(|supported| supported.eq_ignore_ascii_case(ablation.trim()))
    }) {
        return Err("unsupported strategy method ablation");
    }

    if descriptor.key == "m8"
        && ablations
            .iter()
            .any(|ablation| ablation.eq_ignore_ascii_case("funding"))
    {
        return Ok(SimulationPolicyVariant::M7EvidenceGated);
    }

    Ok(descriptor.variant)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_unique_and_resolves_aliases() {
        let values = all();
        assert_eq!(values.len(), 10);
        assert_eq!(values[0].id, "M0");
        assert_eq!(
            resolve("M9", &[]).unwrap(),
            SimulationPolicyVariant::M9DeadlineCausalDroMpc
        );
        assert_eq!(
            resolve("m8_funding_aware", &["funding".to_owned()]).unwrap(),
            SimulationPolicyVariant::M7EvidenceGated
        );
    }

    #[test]
    fn unsupported_ablations_fail_closed() {
        assert_eq!(
            resolve("m1", &["funding".to_owned()]),
            Err("unsupported strategy method ablation")
        );
    }
}
