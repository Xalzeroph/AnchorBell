//! Auditable replay configuration kept outside the simulation runtime state machine.

use std::collections::BTreeMap;

use crate::{execution::EmergencyExecutionPolicy, strategy::CalibrationState};

use super::runtime::SimulationPolicyVariant;

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub price_scale: u32,
    pub quantity_scale: u32,
    pub entry_threshold_bps: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub max_mark_index_gap_bps: i64,
    pub max_anchor_age_ms: u64,
    pub fee_ppm: i64,
    pub emergency_execution: EmergencyExecutionPolicy,
    pub fee_schedule_source: String,
    pub realism: crate::backtest::realism::RealisticFillModel,
    pub strategy_variant: SimulationPolicyVariant,
    pub threshold_scale_ppm: i64,
    pub quote_reprice_min_interval_ms: u64,
    pub dynamic_capital_refresh_ms: u64,
    pub live_risk_gates: bool,
    pub funding_controller_enabled: bool,
    pub capital_usdt_ticks: Option<i64>,
    pub portfolio_drawdown_limits_bps: Option<(i64, i64)>,
    pub calibration_updates_enabled: bool,
    pub calibration_seeds: BTreeMap<String, CalibrationState>,
}
