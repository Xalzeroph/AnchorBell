use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub event_id: String,
    pub event_type: String,
    pub event_at_ms: u64,
    pub payload_digest: String,
    pub previous_digest: String,
    pub event_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerError {
    EmptyEventId,
    EmptyEventType,
    InvalidEventTime,
    NonMonotonicTime,
    DuplicateEventId,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceLedger {
    pub events: Vec<LedgerEvent>,
    pub head_digest: String,
}

impl EvidenceLedger {
    pub fn append(
        &mut self,
        event_id: impl Into<String>,
        event_type: impl Into<String>,
        at_ms: u64,
        payload: &[u8],
    ) -> Result<(), LedgerError> {
        let event_id = event_id.into();
        let event_type = event_type.into();
        if event_id.is_empty() {
            return Err(LedgerError::EmptyEventId);
        }
        if event_type.is_empty() {
            return Err(LedgerError::EmptyEventType);
        }
        if at_ms == 0 {
            return Err(LedgerError::InvalidEventTime);
        }
        if self.events.iter().any(|event| event.event_id == event_id) {
            return Err(LedgerError::DuplicateEventId);
        }
        if self
            .events
            .last()
            .is_some_and(|last| at_ms < last.event_at_ms)
        {
            return Err(LedgerError::NonMonotonicTime);
        }

        let payload_digest = digest(payload);
        let previous_digest = self.head_digest.clone();
        let event_digest = digest(
            format!(
                "{}|{}|{}|{}|{}",
                previous_digest, event_id, event_type, at_ms, payload_digest
            )
            .as_bytes(),
        );
        self.events.push(LedgerEvent {
            event_id,
            event_type,
            event_at_ms: at_ms,
            payload_digest,
            previous_digest,
            event_digest: event_digest.clone(),
        });
        self.head_digest = event_digest;
        Ok(())
    }

    pub fn verify(&self) -> bool {
        let mut previous = String::new();
        let mut previous_at_ms = 0_u64;
        let mut event_ids = BTreeSet::new();
        for event in &self.events {
            if event.event_id.is_empty()
                || event.event_type.is_empty()
                || event.event_at_ms == 0
                || event.event_at_ms < previous_at_ms
                || !event_ids.insert(event.event_id.as_str())
                || event.previous_digest != previous
            {
                return false;
            }
            let expected = digest(
                format!(
                    "{}|{}|{}|{}|{}",
                    event.previous_digest,
                    event.event_id,
                    event.event_type,
                    event.event_at_ms,
                    event.payload_digest
                )
                .as_bytes(),
            );
            if event.event_digest != expected {
                return false;
            }
            previous = event.event_digest.clone();
            previous_at_ms = event.event_at_ms;
        }
        self.head_digest == previous
    }
}

fn digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_detects_event_or_chain_tampering() {
        let mut ledger = EvidenceLedger::default();
        ledger.append("one", "frame", 1, b"a").unwrap();
        ledger.append("two", "frame", 2, b"b").unwrap();
        assert!(ledger.verify());
        ledger.events[0].event_type = "tampered".into();
        assert!(!ledger.verify());
    }

    #[test]
    fn ledger_rejects_invalid_time_and_order() {
        let mut ledger = EvidenceLedger::default();
        assert_eq!(
            ledger.append("one", "frame", 0, b"a"),
            Err(LedgerError::InvalidEventTime)
        );
        ledger.append("one", "frame", 2, b"a").unwrap();
        assert_eq!(
            ledger.append("two", "frame", 1, b"b"),
            Err(LedgerError::NonMonotonicTime)
        );
    }

    #[test]
    fn ledger_allows_same_event_time_when_ids_are_unique() {
        let mut ledger = EvidenceLedger::default();
        ledger
            .append("frame-100-0", "evidence_frame", 100, b"a")
            .unwrap();
        ledger
            .append("decision-100-1", "decision", 100, b"b")
            .unwrap();
        assert!(ledger.verify());
        assert_eq!(ledger.events.len(), 2);
    }

    #[test]
    fn ledger_rejects_semantically_invalid_but_hash_consistent_history() {
        let mut ledger = EvidenceLedger::default();
        ledger.append("one", "frame", 2, b"a").unwrap();
        ledger.append("two", "frame", 3, b"b").unwrap();
        ledger.events[1].event_at_ms = 1;
        assert!(!ledger.verify());
    }

    #[test]
    fn ledger_rejects_duplicate_event_ids() {
        let mut ledger = EvidenceLedger::default();
        ledger.append("one", "frame", 1, b"a").unwrap();
        assert_eq!(
            ledger.append("one", "frame", 2, b"b"),
            Err(LedgerError::DuplicateEventId)
        );
        assert!(ledger.verify());
        assert_eq!(ledger.events.len(), 1);
    }
}
