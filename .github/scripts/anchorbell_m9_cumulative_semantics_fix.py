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

baseline = '''            } else if strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc {
                let (intent, reduce_only, reason) = m9_intent_for_state(
                    state,
                    timestamp_ms,
                    max_position,
                    requested_quantity,
                    self.max_mark_index_gap_bps,
                    self.fee_ppm,
                );
                (intent, reduce_only, state.working.is_some(), reason)
            } else {
'''
weak = '''            } else if strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc {
                let inherited_required_pico_bps = dynamic_threshold_for(
                    state,
                    strategy_variant,
                    self.strategy.entry_threshold_bps,
                    self.fee_ppm,
                    requested_quantity,
                    max_position,
                    timestamp_ms,
                )
                .map(|value| scale_threshold_non_fee(value, self.threshold_scale_ppm))
                .and_then(|value| value.required_pico_bps())
                .map(|value| value.saturating_sub(state.adaptive_relief_pico_bps))
                .unwrap_or(0);
                if state.position == 0 && !m7_entry_admissible(state, inherited_required_pico_bps) {
                    (None, false, state.working.is_some(), "m7_evidence_gate")
                } else {
                    let (intent, reduce_only, reason) = m9_intent_for_state(
                        state,
                        timestamp_ms,
                        max_position,
                        requested_quantity,
                        self.max_mark_index_gap_bps,
                        self.fee_ppm,
                    );
                    (intent, reduce_only, state.working.is_some(), reason)
                }
            } else {
'''
strong = '''            } else if strategy_variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc {
                let (intent, reduce_only, reason) = m9_intent_for_state(
                    state,
                    timestamp_ms,
                    max_position,
                    requested_quantity,
                    self.max_mark_index_gap_bps,
                    self.fee_ppm,
                );
                let evidence_ok = reduce_only
                    || dynamic_threshold_for(
                        state,
                        strategy_variant,
                        self.strategy.entry_threshold_bps,
                        self.fee_ppm,
                        requested_quantity,
                        max_position,
                        timestamp_ms,
                    )
                    .map(|value| scale_threshold_non_fee(value, self.threshold_scale_ppm))
                    .and_then(|value| value.required_pico_bps())
                    .is_some_and(|value| {
                        m7_entry_admissible(
                            state,
                            value.saturating_sub(state.adaptive_relief_pico_bps),
                        )
                    });
                if intent.is_some() && !evidence_ok {
                    (None, false, state.working.is_some(), "m7_evidence_gate")
                } else {
                    (intent, reduce_only, state.working.is_some(), reason)
                }
            } else {
'''
if weak in text:
    text = text.replace(weak, strong, 1)
elif baseline in text:
    text = text.replace(baseline, strong, 1)
elif strong not in text:
    raise SystemExit("missing anchor: M9 inherited evidence gate")

if "m9_inherits_m7_and_m8_capabilities" not in text:
    test_anchor = '''    #[test]
    fn dynamic_allocation_deadband_ignores_sub_percent_noise() {
'''
    test = '''    #[test]
    fn m9_inherits_m7_and_m8_capabilities() {
        let variant = SimulationPolicyVariant::M9DeadlineCausalDroMpc;
        assert!(variant.uses_evidence_gate());
        assert!(variant.uses_funding_controller());
        assert!(engine().with_strategy_variant(variant).funding_controller_active());
    }

'''
    if test_anchor not in text:
        raise SystemExit("missing anchor: M9 cumulative test")
    text = text.replace(test_anchor, test + test_anchor, 1)

RUNTIME.write_text(text, encoding="utf-8")
print("M9 cumulative strategy semantics repair complete")
