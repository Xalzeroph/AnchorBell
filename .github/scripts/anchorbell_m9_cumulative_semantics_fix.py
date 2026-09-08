from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
text = RUNTIME.read_text(encoding="utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global text
    if old in text:
        text = text.replace(old, new, 1)
        return
    if new in text:
        return
    raise SystemExit(f"missing anchor: {label}")


# Capability semantics must be cumulative. M9 is documented as M8 plus the
# deadline/causal/DRO-MPC decision layer, so it cannot bypass M7 evidence gates
# or M8 funding economics merely because a match expression forgot the new enum.
replace_once(
    '''    fn uses_evidence_gate(self) -> bool {
        matches!(self, Self::M7EvidenceGated | Self::M8FundingAware)
    }
''',
    '''    fn uses_evidence_gate(self) -> bool {
        self >= Self::M7EvidenceGated
    }

    fn uses_funding_controller(self) -> bool {
        self >= Self::M8FundingAware
    }
''',
    "cumulative capability helpers",
)

replace_once(
    '''    fn funding_controller_active(&self) -> bool {
        self.strategy_variant == SimulationPolicyVariant::M8FundingAware
            && self.funding_controller_enabled
    }
''',
    '''    fn funding_controller_active(&self) -> bool {
        self.strategy_variant.uses_funding_controller() && self.funding_controller_enabled
    }
''',
    "funding controller cumulative activation",
)

replace_once(
    '''        if self.strategy_variant == SimulationPolicyVariant::M8FundingAware
            && !self.funding_controller_enabled
        {
''',
    '''        if self.strategy_variant.uses_funding_controller() && !self.funding_controller_enabled {
''',
    "funding ablation cumulative semantics",
)

replace_once(
    '''    if variant != SimulationPolicyVariant::M8FundingAware {
        return funding_entry_allowed(state, now_ms);
    }
''',
    '''    if !variant.uses_funding_controller() {
        return funding_entry_allowed(state, now_ms);
    }
''',
    "funding entry cumulative semantics",
)

replace_once(
    '''                    funding_action: if self.strategy_variant
                        == SimulationPolicyVariant::M8FundingAware
                        && !self.funding_controller_enabled
                    {
''',
    '''                    funding_action: if self.strategy_variant.uses_funding_controller()
                        && !self.funding_controller_enabled
                    {
''',
    "funding metrics cumulative semantics",
)

replace_once(
    '''        || (!config.funding_controller_enabled
            && config.strategy_variant != SimulationPolicyVariant::M8FundingAware)
''',
    '''        || (!config.funding_controller_enabled
            && !config.strategy_variant.uses_funding_controller())
''',
    "replay cumulative funding capability",
)

# M9 has its own action optimizer, so the generic M7 gate is not reached by the
# ordinary M0-M8 branch. Enforce inherited M7 evidence admissibility explicitly
# before M9 is allowed to manufacture new risk. Reduce-only/risk-state paths
# remain outside this guard and therefore can always flatten.
old_m9 = '''            } else if strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc {
                let (intent, reduce_only, reason) = m9_intent_for_state(
                    state,
                    timestamp_ms,
                    max_position,
                    requested_quantity,
                    max_mark_index_gap_bps,
                    fee_ppm,
                    quantity_scale,
                );
                (intent, reduce_only, state.working.is_some(), reason)
            } else {
'''
new_m9 = '''            } else if strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc {
                let m9_threshold = adaptive_threshold_for_variant(
                    state,
                    strategy_variant,
                    timestamp_ms,
                    fee_ppm,
                )
                .threshold
                .map(|threshold| scale_threshold_non_fee(threshold, self.threshold_scale_ppm));
                let inherited_required_pico_bps = m9_threshold
                    .and_then(|value| value.required_pico_bps())
                    .map(|required| required.saturating_sub(state.adaptive_relief_pico_bps))
                    .unwrap_or(0);
                if !m7_entry_admissible(state, inherited_required_pico_bps) && state.position == 0 {
                    (None, false, state.working.is_some(), "m7_evidence_gate")
                } else {
                    let (intent, reduce_only, reason) = m9_intent_for_state(
                        state,
                        timestamp_ms,
                        max_position,
                        requested_quantity,
                        max_mark_index_gap_bps,
                        fee_ppm,
                        quantity_scale,
                    );
                    (intent, reduce_only, state.working.is_some(), reason)
                }
            } else {
'''
if old_m9 in text:
    text = text.replace(old_m9, new_m9, 1)
elif new_m9 not in text:
    raise SystemExit("missing anchor: M9 inherited evidence gate")

# Regression: adding M9 must never silently remove a capability from M8/M7.
if "m9_inherits_m7_and_m8_capabilities" not in text:
    test_anchor = '''    #[test]
    fn dynamic_allocation_deadband_ignores_sub_percent_noise() {
'''
    test = '''    #[test]
    fn m9_inherits_m7_and_m8_capabilities() {
        assert!(SimulationPolicyVariant::M7EvidenceGated.uses_evidence_gate());
        assert!(SimulationPolicyVariant::M8FundingAware.uses_evidence_gate());
        assert!(SimulationPolicyVariant::M9DeadlineCausalDroMpc.uses_evidence_gate());
        assert!(!SimulationPolicyVariant::M7EvidenceGated.uses_funding_controller());
        assert!(SimulationPolicyVariant::M8FundingAware.uses_funding_controller());
        assert!(SimulationPolicyVariant::M9DeadlineCausalDroMpc.uses_funding_controller());

        let m9 = engine().with_strategy_variant(SimulationPolicyVariant::M9DeadlineCausalDroMpc);
        assert!(m9.funding_controller_active());
        let ablated = m9.with_funding_controller_enabled(false);
        assert!(!ablated.funding_controller_active());
    }

'''
    if test_anchor not in text:
        raise SystemExit("missing anchor: M9 cumulative test")
    text = text.replace(test_anchor, test + test_anchor, 1)

RUNTIME.write_text(text, encoding="utf-8")
print("M9 cumulative strategy semantics repair complete")
