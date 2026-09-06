use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AuditKind {
    MarketAccepted,
    MarketRejected,
    Decision,
    RiskRejected,
    OrderIntent,
    ExchangeAcknowledgement,
    Lifecycle,
    Recovery,
    Halt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub kind: AuditKind,
    pub symbol: Option<String>,
    pub reason: Option<String>,
    pub correlation_id: Option<String>,
}

impl AuditRecord {
    pub fn new(sequence: u64, timestamp_ms: u64, kind: AuditKind) -> Self {
        Self {
            sequence,
            timestamp_ms,
            kind,
            symbol: None,
            reason: None,
            correlation_id: None,
        }
    }

    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }
}

/// Versioned, venue-neutral decision audit shared by live, replay, and simulation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionGateAudit {
    pub name: String,
    pub passed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionAudit {
    pub schema_version: u16,
    pub decision_id: u64,
    pub exchange_event_time_ms: u64,
    pub received_at_ms: u64,
    pub outcome: String,
    pub final_gate: String,
    pub gates: Vec<DecisionGateAudit>,
    pub book_bid_ticks: Option<i64>,
    pub book_ask_ticks: Option<i64>,
    pub book_bid_quantity: Option<i64>,
    pub book_ask_quantity: Option<i64>,
    pub mark_ticks: Option<i64>,
    pub index_ticks: Option<i64>,
    pub anchor_ticks: i64,
    pub position: i64,
    pub mark_age_ms: Option<u64>,
    pub anchor_age_ms: Option<u64>,
    pub threshold_status: String,
    pub threshold_pico_bps: Option<i64>,
    pub fair_value_ticks: Option<i64>,
    pub liquidity_ratio_bps: Option<i64>,
}

/// Stable schema version for decision/order/fill correlation records.
pub const DECISION_AUDIT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Default)]
pub struct AuditSequence {
    next: u64,
}

impl AuditSequence {
    pub fn next_value(&mut self) -> u64 {
        let value = self.next;
        self.next = self.next.saturating_add(1);
        value
    }

    pub fn record(&mut self, timestamp_ms: u64, kind: AuditKind) -> AuditRecord {
        AuditRecord::new(self.next_value(), timestamp_ms, kind)
    }
}

pub fn redact_secret(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    "[REDACTED]".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_sequence_is_monotonic() {
        let mut sequence = AuditSequence::default();
        assert_eq!(sequence.record(10, AuditKind::Decision).sequence, 0);
        assert_eq!(sequence.record(11, AuditKind::Lifecycle).sequence, 1);
    }

    #[test]
    fn secret_redaction_never_returns_input() {
        assert_eq!(redact_secret("api-secret"), "[REDACTED]");
        assert_eq!(redact_secret(""), "");
    }
}
