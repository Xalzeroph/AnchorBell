use crate::model::EvidenceFrame;
use crate::{
    evolution::StrategyArtifact,
    ledger::EvidenceLedger,
    policy::{Decision, DecisionEngine},
};

pub struct TradingEngine {
    pub decision: DecisionEngine,
    pub champion: StrategyArtifact,
    pub ledger: EvidenceLedger,
}

impl TradingEngine {
    pub fn evaluate(&mut self, frame: &EvidenceFrame) -> Decision {
        let payload = serde_json::to_vec(frame).unwrap_or_default();
        self.ledger.append(
            format!("frame-{}", frame.now_ms),
            "evidence_frame",
            frame.now_ms,
            &payload,
        );
        self.decision.decide(frame)
    }
}
