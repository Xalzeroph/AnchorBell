from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count == 0:
        if new in text:
            print(f"already patched: {path}")
            return
        raise SystemExit(f"anchor not found in {path}: {old[:180]!r}")
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}")
    p.write_text(text.replace(old, new, 1))
    print(f"patched: {path}")


runtime = "engine/src/simulation/runtime.rs"

# M6 must not convert one-minute estimator noise into fee-generating turnover.
replace_once(
    runtime,
    '''    fn refresh_dynamic_allocations(&mut self, timestamp_ms: u64) -> Vec<SimulationRecord> {''',
    '''    const DYNAMIC_ALLOCATION_MIN_REFRESH_MS: u64 = 5 * 60 * 1_000;\n    const DYNAMIC_ALLOCATION_REBALANCE_DEADBAND_BPS: i64 = 100;\n    const DYNAMIC_PERFORMANCE_MIN_FILLS: u64 = 10;\n\n    fn allocation_budget_change_bps(old_budget: i64, new_budget: i64, total_capital: i64) -> i64 {\n        if total_capital <= 0 {\n            return i64::MAX;\n        }\n        let delta = i128::from(new_budget)\n            .saturating_sub(i128::from(old_budget))\n            .abs();\n        (delta.saturating_mul(10_000) / i128::from(total_capital))\n            .clamp(0, i128::from(i64::MAX)) as i64\n    }\n\n    fn refresh_dynamic_allocations(&mut self, timestamp_ms: u64) -> Vec<SimulationRecord> {''',
)
replace_once(
    runtime,
    '''                && timestamp_ms.saturating_sub(self.last_dynamic_capital_update_ms)\n                    < self.dynamic_capital_refresh_ms)''',
    '''                && timestamp_ms.saturating_sub(self.last_dynamic_capital_update_ms)\n                    < self\n                        .dynamic_capital_refresh_ms\n                        .max(Self::DYNAMIC_ALLOCATION_MIN_REFRESH_MS))''',
)
replace_once(
    runtime,
    '''        let risk_inputs = self\n            .states''',
    '''        let total_capital = self.capital_usdt_ticks.unwrap_or(0);\n        if total_capital <= 0 {\n            return Vec::new();\n        }\n        let fallback_budget = total_capital / i64::try_from(self.states.len()).unwrap_or(1).max(1);\n        let risk_inputs = self\n            .states''',
)
replace_once(
    runtime,
    '''                let risk_bps = 1_i64\n                    .saturating_add(state.ewma_abs_return_bps.saturating_mul(3))\n                    .saturating_add(state.ewma_spread_bps)\n                    .saturating_add(gap_bps / 2)\n                    .saturating_add(tail_bps / 2)\n                    .max(1);''',
    '''                let allocated_capital = self\n                    .position_allocations\n                    .get(symbol)\n                    .map(|allocation| allocation.budget_usdt_ticks)\n                    .filter(|budget| *budget > 0)\n                    .unwrap_or(fallback_budget.max(1));\n                let net_pnl_ticks = state\n                    .market_pnl_ticks\n                    .saturating_add(state.strategy_pnl_ticks)\n                    .saturating_add(state.funding_pnl_ticks)\n                    .saturating_sub(state.fees_ticks);\n                let post_fee_loss_bps = if state.fills >= Self::DYNAMIC_PERFORMANCE_MIN_FILLS\n                    && net_pnl_ticks < 0\n                {\n                    (i128::from(net_pnl_ticks)\n                        .abs()\n                        .saturating_mul(10_000)\n                        / i128::from(allocated_capital))\n                    .clamp(0, 250) as i64\n                } else {\n                    0\n                };\n                let fee_drag_bps = if state.fills >= Self::DYNAMIC_PERFORMANCE_MIN_FILLS {\n                    (i128::from(state.fees_ticks.max(0))\n                        .saturating_mul(10_000)\n                        / i128::from(allocated_capital))\n                    .clamp(0, 100) as i64\n                } else {\n                    0\n                };\n                let adverse_markout_bps =\n                    pico_bps_to_bps(state.ewma_adverse_markout_pico_bps).clamp(0, 100);\n                let risk_bps = 1_i64\n                    .saturating_add(state.ewma_abs_return_bps.saturating_mul(3))\n                    .saturating_add(state.ewma_spread_bps)\n                    .saturating_add(gap_bps / 2)\n                    .saturating_add(tail_bps / 2)\n                    .saturating_add(adverse_markout_bps.saturating_mul(2))\n                    .saturating_add(fee_drag_bps)\n                    .saturating_add(post_fee_loss_bps)\n                    .max(1);''',
)
replace_once(
    runtime,
    '''        let total_capital = self.capital_usdt_ticks.unwrap_or(0);\n        if total_capital <= 0 {\n            return Vec::new();\n        }\n        let symbols = self.states.keys().cloned().collect::<Vec<_>>();''',
    '''        let symbols = self.states.keys().cloned().collect::<Vec<_>>();''',
)
replace_once(
    runtime,
    '''        let changed_symbols = symbols\n            .iter()\n            .filter(|symbol| {\n                let old = self.position_allocations.get(*symbol);\n                let candidate = allocations.get(*symbol);\n                old.map(|allocation| {\n                    candidate.is_some_and(|candidate| {\n                        allocation.budget_usdt_ticks != candidate.budget_usdt_ticks\n                            || allocation.max_position != candidate.max_position\n                    })\n                })\n                .unwrap_or(true)\n            })\n            .cloned()\n            .collect::<Vec<_>>();''',
    '''        let changed_symbols = symbols\n            .iter()\n            .filter(|symbol| {\n                let old = self.position_allocations.get(*symbol);\n                let candidate = allocations.get(*symbol);\n                match (old, candidate) {\n                    (Some(old), Some(candidate)) => {\n                        let budget_change_bps = Self::allocation_budget_change_bps(\n                            old.budget_usdt_ticks,\n                            candidate.budget_usdt_ticks,\n                            total_capital,\n                        );\n                        let absolute_position = self.states[*symbol]\n                            .position\n                            .checked_abs()\n                            .unwrap_or(i64::MAX);\n                        budget_change_bps >= Self::DYNAMIC_ALLOCATION_REBALANCE_DEADBAND_BPS\n                            || candidate.max_position < absolute_position\n                    }\n                    _ => true,\n                }\n            })\n            .cloned()\n            .collect::<Vec<_>>();''',
)

# Fill diagnostics must classify wins after the maker fee, while keeping gross
# execution alpha and fees separately auditable.
replace_once(
    runtime,
    '''    let execution_alpha = state.mark_price_ticks.map(|mark| match side {\n        Side::Buy => i128::from(mark) - i128::from(price_ticks),\n        Side::Sell => i128::from(price_ticks) - i128::from(mark),\n    });\n    if let Some(alpha) = execution_alpha {\n        let alpha_ticks = alpha * i128::from(quantity) / quantity_scale_multiplier(quantity_scale);\n        let alpha_ticks = clamp_i128(alpha_ticks);\n        state.strategy_pnl_ticks = state.strategy_pnl_ticks.saturating_add(alpha_ticks);\n        if alpha_ticks > 0 {\n            state.winning_fills = state.winning_fills.saturating_add(1);\n        } else if alpha_ticks < 0 {\n            state.losing_fills = state.losing_fills.saturating_add(1);\n        }\n    }''',
    '''    let notional = i128::from(price_ticks).abs() * i128::from(quantity).abs();\n    let fill_fee_ticks = clamp_i128(\n        notional * i128::from(fee_ppm) / 1_000_000 / quantity_scale_multiplier(quantity_scale),\n    );\n    let execution_alpha = state.mark_price_ticks.map(|mark| match side {\n        Side::Buy => i128::from(mark) - i128::from(price_ticks),\n        Side::Sell => i128::from(price_ticks) - i128::from(mark),\n    });\n    if let Some(alpha) = execution_alpha {\n        let alpha_ticks = alpha * i128::from(quantity) / quantity_scale_multiplier(quantity_scale);\n        let alpha_ticks = clamp_i128(alpha_ticks);\n        state.strategy_pnl_ticks = state.strategy_pnl_ticks.saturating_add(alpha_ticks);\n        let fee_adjusted_alpha = alpha_ticks.saturating_sub(fill_fee_ticks);\n        if fee_adjusted_alpha > 0 {\n            state.winning_fills = state.winning_fills.saturating_add(1);\n        } else if fee_adjusted_alpha < 0 {\n            state.losing_fills = state.losing_fills.saturating_add(1);\n        }\n    }''',
)
replace_once(
    runtime,
    '''    let notional = i128::from(price_ticks).abs() * i128::from(quantity).abs();\n    state.fees_ticks = state.fees_ticks.saturating_add(clamp_i128(\n        notional * i128::from(fee_ppm) / 1_000_000 / quantity_scale_multiplier(quantity_scale),\n    ));''',
    '''    state.fees_ticks = state.fees_ticks.saturating_add(fill_fee_ticks);''',
)

# A multi-challenger in-sample matrix must never self-promote by summing all
# challenger fills/PnL. Promotion is candidate-level and requires OOS/stress
# evidence via oos_validation.
validation = "engine/src/analytics_validation.rs"
replace_once(
    validation,
    '''pub fn evaluate_simulation_promotion(input: SimulationPromotionInput) -> SimulationPromotionGate {\n    const MINIMUM_FILLS: u64 = 100;\n    let integrity_passed = input.ledger_count > 0 && input.records_dropped == 0;''',
    '''pub fn evaluate_simulation_promotion(input: SimulationPromotionInput) -> SimulationPromotionGate {\n    const MINIMUM_FILLS: u64 = 100;\n    if input.ledger_count > 1 {\n        return SimulationPromotionGate {\n            methodology_id: "anchorbell-automatic-promotion-v2".to_owned(),\n            minimum_fills: MINIMUM_FILLS,\n            integrity_passed: input.records_dropped == 0,\n            evidence_sufficient: false,\n            economic_passed: false,\n            survival_passed: input.valuation_incomplete_ledgers == 0\n                && input.non_flat_ledgers == 0,\n            verdict: ValidationVerdict::Indeterminate,\n            reason: "candidate_level_oos_validation_required".to_owned(),\n        };\n    }\n    let integrity_passed = input.ledger_count > 0 && input.records_dropped == 0;''',
)
replace_once(
    validation,
    '''        methodology_id: "anchorbell-automatic-promotion-v1".to_owned(),''',
    '''        methodology_id: "anchorbell-automatic-promotion-v2".to_owned(),''',
)
replace_once(
    validation,
    '''    fn incomplete_verdict_is_indeterminate() {''',
    '''    fn multi_ledger_matrix_requires_candidate_level_oos_validation() {\n        let gate = evaluate_simulation_promotion(SimulationPromotionInput {\n            ledger_count: 10,\n            orders: 1_000,\n            fills: 500,\n            records_dropped: 0,\n            valuation_incomplete_ledgers: 0,\n            non_flat_ledgers: 0,\n            total_net_pnl_ticks: 1_000_000,\n        });\n        assert_eq!(gate.verdict, ValidationVerdict::Indeterminate);\n        assert_eq!(gate.reason, "candidate_level_oos_validation_required");\n        assert!(!gate.economic_passed);\n    }\n\n    #[test]\n    fn incomplete_verdict_is_indeterminate() {''',
)

# Pure deadband regression tests ensure sub-1% noise cannot trigger a rebalance.
replace_once(
    runtime,
    '''    fn simulation_replay_rejects_out_of_order_events() {''',
    '''    fn dynamic_allocation_deadband_ignores_sub_percent_noise() {\n        assert_eq!(SimulationEngine::allocation_budget_change_bps(2_000, 2_050, 10_000), 50);\n        assert_eq!(SimulationEngine::allocation_budget_change_bps(2_000, 2_200, 10_000), 200);\n        assert!(\n            SimulationEngine::allocation_budget_change_bps(2_000, 2_050, 10_000)\n                < SimulationEngine::DYNAMIC_ALLOCATION_REBALANCE_DEADBAND_BPS\n        );\n    }\n\n    #[test]\n    fn simulation_replay_rejects_out_of_order_events() {''',
)
