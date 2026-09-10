//! Bounded diagnostics and allocation-free portfolio aggregation.

use super::{unrealized_pnl, SimulationEngine};
use serde::Serialize;
use std::collections::BTreeMap;

const RECENT_REJECTION_LIMIT: usize = 1024;

#[derive(Debug, Clone, Serialize)]
pub struct SimulationSummary {
    pub event_count: u64,
    pub order_count: u64,
    pub fill_count: u64,
    pub filled_quantity: i64,
    pub rejected_entries: u64,
    /// Rejections partitioned by the owning layer (strategy/risk/execution).
    pub gate_rejections: BTreeMap<String, u64>,
    /// Recent diagnostic window; cumulative counts above cover the full run.
    pub gate_rejection_records: Vec<GateRejectionRecord>,
    pub gate_rejection_records_truncated: bool,
    pub realized_pnl_ticks: i64,
    pub unrealized_pnl_ticks: i64,
    pub market_pnl_ticks: i64,
    pub strategy_pnl_ticks: i64,
    pub funding_pnl_ticks: i64,
    pub gross_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub net_pnl_ticks: i64,
    pub maker_fee_ppm: i64,
    pub taker_fee_ppm: i64,
    pub unrealized_valuation_complete: bool,
    pub current_absolute_position: i64,
    pub peak_absolute_position: i64,
    pub working_orders: u64,
    pub flat_at_end: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GateRejectionRecord {
    pub reason: String,
    pub symbol: String,
    pub market: String,
    pub method: String,
    pub source: String,
    pub threshold: Option<i64>,
    pub observed_value: Option<i64>,
    pub timestamp_ms: u64,
}

impl SimulationEngine {
    pub(super) fn reject_entry(&mut self, owner: &str) {
        self.rejected_entries = self.rejected_entries.saturating_add(1);
        let count = self.gate_rejections.entry(owner.to_owned()).or_default();
        *count = count.saturating_add(1);
    }

    pub(super) fn reject_entry_structured(
        &mut self,
        symbol: &str,
        reason: &str,
        source: &str,
        threshold: Option<i64>,
        observed_value: Option<i64>,
        timestamp_ms: u64,
    ) {
        self.reject_entry(reason);
        if self.gate_rejection_records.len() == RECENT_REJECTION_LIMIT {
            self.gate_rejection_records.pop_front();
            self.gate_rejection_records_truncated = true;
        }
        self.gate_rejection_records.push_back(GateRejectionRecord {
            reason: reason.to_owned(),
            symbol: symbol.to_owned(),
            market: self.market_id.clone(),
            method: self.method_id.clone(),
            source: source.to_owned(),
            threshold,
            observed_value,
            timestamp_ms,
        });
    }

    pub(super) fn accounting_summary(&self) -> SimulationSummary {
        self.summary_with_diagnostics(false)
    }

    pub fn summary(&self) -> SimulationSummary {
        self.summary_with_diagnostics(true)
    }

    fn summary_with_diagnostics(&self, diagnostics: bool) -> SimulationSummary {
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
            gate_rejections: if diagnostics {
                self.gate_rejections.clone()
            } else {
                Default::default()
            },
            gate_rejection_records: if diagnostics {
                self.gate_rejection_records.iter().cloned().collect()
            } else {
                Vec::new()
            },
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
            peak_absolute_position: self.peak_absolute_position,
            working_orders,
            flat_at_end,
        }
    }
}
