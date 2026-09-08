from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count == 0:
        if new in text:
            print(f"already patched: {path}")
            return
        raise SystemExit(f"anchor not found in {path}: {old[:160]!r}")
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}")
    p.write_text(text.replace(old, new, 1))
    print(f"patched: {path}")


# M8_no_funding must remain the same M8 strategy identity. Ablations are
# capabilities, not a request to silently substitute an earlier strategy.
replace_once(
    "engine/src/simulation/experiment_plan.rs",
    '''                let funding_disabled = experiment\n                    .ablations\n                    .iter()\n                    .any(|ablation| ablation == "funding");\n                let variant = match experiment.strategy.as_str() {''',
    '''                let variant = match experiment.strategy.as_str() {''',
)
replace_once(
    "engine/src/simulation/experiment_plan.rs",
    '''                    "m7" => SimulationPolicyVariant::M7EvidenceGated,\n                    "m8" if funding_disabled => SimulationPolicyVariant::M7EvidenceGated,\n                    "m8" => SimulationPolicyVariant::M8FundingAware,''',
    '''                    "m7" => SimulationPolicyVariant::M7EvidenceGated,\n                    "m8" => SimulationPolicyVariant::M8FundingAware,''',
)
replace_once(
    "engine/src/simulation/experiment_plan.rs",
    '''        assert_eq!(*variant, SimulationPolicyVariant::M7EvidenceGated);''',
    '''        assert_eq!(*variant, SimulationPolicyVariant::M8FundingAware);''',
)

# Make cumulative strategy semantics explicit. M8 is M7 + funding; M9 owns a
# separate decision path and therefore does not run the M7 admission gate.
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    fn uses_dynamic_capital(self) -> bool {\n        self >= Self::M6DynamicCapital\n    }''',
    '''    fn uses_dynamic_capital(self) -> bool {\n        self >= Self::M6DynamicCapital\n    }\n\n    fn uses_evidence_gate(self) -> bool {\n        matches!(self, Self::M7EvidenceGated | Self::M8FundingAware)\n    }''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    quote_reprice_min_interval_ms: u64,\n    live_risk_gates: bool,\n    threshold_scale_ppm: i64,''',
    '''    quote_reprice_min_interval_ms: u64,\n    live_risk_gates: bool,\n    funding_controller_enabled: bool,\n    threshold_scale_ppm: i64,''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''            quote_reprice_min_interval_ms: 0,\n            live_risk_gates: false,\n            threshold_scale_ppm: 1_000_000,''',
    '''            quote_reprice_min_interval_ms: 0,\n            live_risk_gates: false,\n            funding_controller_enabled: true,\n            threshold_scale_ppm: 1_000_000,''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''    pub fn with_live_risk_gates(mut self) -> Self {\n        self.live_risk_gates = true;\n        self\n    }''',
    '''    pub fn with_live_risk_gates(mut self) -> Self {\n        self.live_risk_gates = true;\n        self\n    }\n\n    pub fn with_funding_controller_enabled(mut self, enabled: bool) -> Self {\n        self.funding_controller_enabled = enabled;\n        self\n    }\n\n    fn funding_controller_active(&self) -> bool {\n        self.strategy_variant == SimulationPolicyVariant::M8FundingAware\n            && self.funding_controller_enabled\n    }\n\n    fn funding_entry_allowed_for_strategy(\n        &self,\n        state: &SimulationSymbolState,\n        now_ms: u64,\n    ) -> bool {\n        if self.strategy_variant == SimulationPolicyVariant::M8FundingAware\n            && !self.funding_controller_enabled\n        {\n            funding_entry_allowed(state, now_ms)\n        } else {\n            funding_entry_allowed_variant(\n                state,\n                now_ms,\n                self.strategy_variant,\n                self.fee_ppm,\n            )\n        }\n    }''',
)

# Dynamic capital eligibility must respect the exact same funding semantics as
# the order-admission path.
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                    && (!self.live_risk_gates\n                        || funding_entry_allowed_variant(\n                            state,\n                            timestamp_ms,\n                            self.strategy_variant,\n                            self.fee_ppm,\n                        ));''',
    '''                    && (!self.live_risk_gates\n                        || self.funding_entry_allowed_for_strategy(state, timestamp_ms));''',
)

# Metrics/risk-state projection uses the capability flag instead of assuming
# every M8 ledger has its funding controller active.
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                let funding_allowed = !self.live_risk_gates\n                    || if self.strategy_variant == SimulationPolicyVariant::M8FundingAware {\n                        funding_overlay.allow_base_strategy\n                    } else {\n                        funding_entry_allowed_variant(\n                            state,\n                            self.last_event_at_ms,\n                            self.strategy_variant,\n                            self.fee_ppm,\n                        )\n                    };''',
    '''                let funding_controller_active = self.funding_controller_active();\n                let funding_allowed = !self.live_risk_gates\n                    || if funding_controller_active {\n                        funding_overlay.allow_base_strategy\n                    } else {\n                        self.funding_entry_allowed_for_strategy(state, self.last_event_at_ms)\n                    };''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                } else if !funding_known {\n                    SimulationRiskState::HaltFundingMetadata\n                } else if self.strategy_variant == SimulationPolicyVariant::M8FundingAware {''',
    '''                } else if !funding_known {\n                    SimulationRiskState::HaltFundingMetadata\n                } else if funding_controller_active {''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                    funding_flatten_deadline_ms: (self.strategy_variant\n                        != SimulationPolicyVariant::M8FundingAware)\n                        .then(|| funding_flatten_deadline(state.next_funding_time_ms))\n                        .flatten(),\n                    funding_action: format!("{:?}", funding_decision.action),\n                    funding_carry_bps: funding_decision.funding_carry_bps,\n                    funding_net_edge_bps: funding_decision.net_edge_bps,''',
    '''                    funding_flatten_deadline_ms: (!funding_controller_active)\n                        .then(|| funding_flatten_deadline(state.next_funding_time_ms))\n                        .flatten(),\n                    funding_action: if self.strategy_variant\n                        == SimulationPolicyVariant::M8FundingAware\n                        && !self.funding_controller_enabled\n                    {\n                        "Ablated".to_owned()\n                    } else {\n                        format!("{:?}", funding_decision.action)\n                    },\n                    funding_carry_bps: if funding_controller_active {\n                        funding_decision.funding_carry_bps\n                    } else {\n                        0\n                    },\n                    funding_net_edge_bps: if funding_controller_active {\n                        funding_decision.net_edge_bps\n                    } else {\n                        0\n                    },''',
)

# Runtime order decisions: disable only the funding overlay. Settlement and the
# generic pre-funding flatten safety remain active, so the ablation is economic
# and strategy-causal rather than an unrealistic no-funding universe.
replace_once(
    "engine/src/simulation/runtime.rs",
    '''            let funding_decision = (self.strategy_variant\n                == SimulationPolicyVariant::M8FundingAware)\n                .then(|| m8_funding_decision(state, timestamp_ms, max_position, self.fee_ppm));''',
    '''            let funding_controller_active = self.funding_controller_active();\n            let funding_decision = funding_controller_active\n                .then(|| m8_funding_decision(state, timestamp_ms, max_position, self.fee_ppm));''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                    || {\n                        funding_entry_allowed_variant(\n                            state,\n                            timestamp_ms,\n                            self.strategy_variant,\n                            self.fee_ppm,\n                        )\n                    },''',
    '''                    || self.funding_entry_allowed_for_strategy(state, timestamp_ms),''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                    let m7_blocked = strategy_variant == SimulationPolicyVariant::M7EvidenceGated\n                        && !m7_entry_admissible(state, m7_required_pico_bps);''',
    '''                    let m7_blocked = strategy_variant.uses_evidence_gate()\n                        && !m7_entry_admissible(state, m7_required_pico_bps);''',
)
replace_once(
    "engine/src/simulation/runtime.rs",
    '''                    } else if strategy_variant == SimulationPolicyVariant::M7EvidenceGated {\n                        "m7_evidence_gate"''',
    '''                    } else if strategy_variant.uses_evidence_gate() {\n                        "m7_evidence_gate"''',
)

# Batch construction applies ablations as engine capabilities while preserving
# the strategy variant identity in manifests and metrics.
replace_once(
    "engine/src/simulation_batch.rs",
    '''    .with_live_risk_gates()\n    .with_strategy_variant(spec.variant)\n    .with_quote_reprice_min_interval_ms(config.quote_reprice_min_interval_ms)''',
    '''    .with_live_risk_gates()\n    .with_strategy_variant(spec.variant)\n    .with_funding_controller_enabled(\n        !spec.ablations.iter().any(|ablation| ablation == "funding"),\n    )\n    .with_quote_reprice_min_interval_ms(config.quote_reprice_min_interval_ms)''',
)
