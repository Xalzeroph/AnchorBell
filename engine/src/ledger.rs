use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub event_id: String,
    pub event_type: String,
    pub event_at_ms: u64,
    pub payload_digest: String,
    pub previous_digest: String,
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
    ) {
        let mut h = Sha256::new();
        h.update(self.head_digest.as_bytes());
        h.update(payload);
        let digest = hex::encode(h.finalize());
        self.events.push(LedgerEvent {
            event_id: event_id.into(),
            event_type: event_type.into(),
            event_at_ms: at_ms,
            payload_digest: digest.clone(),
            previous_digest: self.head_digest.clone(),
        });
        self.head_digest = digest;
    }
    pub fn verify(&self) -> bool {
        self.events
            .windows(2)
            .all(|pair| pair[1].previous_digest == pair[0].payload_digest)
    }
}
