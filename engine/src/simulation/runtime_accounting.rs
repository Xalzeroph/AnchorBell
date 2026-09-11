use super::*;

impl SimulationEngine {
    pub fn summary(&self) -> SimulationSummary {
        let mut summary = self.accounting_summary();
        summary.gate_rejections = self.gate_rejections.clone();
        summary.gate_rejection_records = self.gate_rejection_records.iter().cloned().collect();
        summary
    }

    // The per-event risk path needs balances, not copies of diagnostic strings.
    pub(super) fn accounting_summary(&self) -> SimulationSummary {
        let mut current_absolute_position = 0_i64;
        let mut realized_pnl_ticks = 0_i64;
        let mut unrealized_pnl_ticks = 0_i64;
        let mut market_pnl_ticks = 0_i64;
        let mut strategy_pnl_ticks = 0_i64;
        let mut funding_pnl_ticks = 0_i64;
        let mut fees_ticks = 0_i64;
        let mut working_orders = 0_u64;
        let mut unrealized_valuation_complete = true;
        for state in self.states.values() {
            current_absolute_position = current_absolute_position
                .saturating_add(state.position.checked_abs().unwrap_or(i64::MAX));
            realized_pnl_ticks = realized_pnl_ticks.saturating_add(state.realized_pnl_ticks);
            market_pnl_ticks = market_pnl_ticks.saturating_add(state.market_pnl_ticks);
            strategy_pnl_ticks = strategy_pnl_ticks.saturating_add(state.strategy_pnl_ticks);
            funding_pnl_ticks = funding_pnl_ticks.saturating_add(state.funding_pnl_ticks);
            fees_ticks = fees_ticks.saturating_add(state.fees_ticks);
            working_orders += u64::from(state.working.is_some());
            if state.position != 0 {
                match unrealized_pnl(state, self.quantity_scale) {
                    Some(pnl) => unrealized_pnl_ticks = unrealized_pnl_ticks.saturating_add(pnl),
                    None => unrealized_valuation_complete = false,
                }
            }
        }
        let flat_at_end = current_absolute_position == 0 && working_orders == 0;
        SimulationSummary {
            event_count: self.event_count,
            order_count: self.order_count,
            fill_count: self.fill_count,
            filled_quantity: self.filled_quantity,
            rejected_entries: self.rejected_entries,
            gate_rejections: BTreeMap::new(),
            gate_rejection_records: Vec::new(),
            gate_rejection_records_truncated: self.gate_rejection_records_truncated,
            realized_pnl_ticks,
            unrealized_pnl_ticks,
            market_pnl_ticks,
            strategy_pnl_ticks,
            funding_pnl_ticks,
            gross_pnl_ticks: market_pnl_ticks
                .saturating_add(strategy_pnl_ticks)
                .saturating_add(funding_pnl_ticks),
            fees_ticks,
            net_pnl_ticks: market_pnl_ticks
                .saturating_add(strategy_pnl_ticks)
                .saturating_add(funding_pnl_ticks)
                .saturating_sub(fees_ticks),
            maker_fee_ppm: self.fee_ppm,
            taker_fee_ppm: self.emergency_policy.taker_fee_ppm,
            unrealized_valuation_complete,
            current_absolute_position,
            portfolio_inventory_imbalance_bps: self.portfolio_inventory_imbalance_bps(),
            peak_absolute_position: self.peak_absolute_position,
            working_orders,
            flat_at_end,
        }
    }
}
