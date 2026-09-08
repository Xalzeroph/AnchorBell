use super::{SimulationEngine, SimulationSymbolState};

pub(super) struct DecisionAuditContext<'a> {
    pub(super) engine: &'a SimulationEngine,
    pub(super) symbol: &'a str,
    pub(super) state: &'a SimulationSymbolState,
    pub(super) timestamp_ms: u64,
    pub(super) decision_id: u64,
    pub(super) outcome: &'a str,
    pub(super) final_gate: &'a str,
    pub(super) max_position: i64,
    pub(super) requested_quantity: i64,
}
