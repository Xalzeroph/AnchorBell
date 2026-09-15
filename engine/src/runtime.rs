use crate::{
    calibration::{
        CalibrationError, CalibrationObservation, CalibrationSnapshot, CalibrationState,
    },
    evolution::StrategyArtifact,
    ledger::{EvidenceLedger, LedgerError},
    model::EvidenceFrame,
    policy::{Decision, DecisionEngine},
};
use serde_json::Error as SerializationError;

#[derive(Debug)]
pub enum RuntimeError {
    Calibration(CalibrationError),
    CalibrationMismatch,
    EvidenceSerialization(SerializationError),
    Ledger(LedgerError),
}

pub struct TradingEngine {
    pub decision: DecisionEngine,
    pub champion: StrategyArtifact,
    pub ledger: EvidenceLedger,
    pub calibration: CalibrationState,
}

impl TradingEngine {
    pub fn new(
        decision: DecisionEngine,
        champion: StrategyArtifact,
        ledger: EvidenceLedger,
        calibration: CalibrationState,
    ) -> Self {
        Self {
            decision,
            champion,
            ledger,
            calibration,
        }
    }

    pub fn observe_calibration(
        &mut self,
        observation: CalibrationObservation,
    ) -> Result<(), CalibrationError> {
        self.calibration.observe(observation)
    }

    pub fn calibration_snapshot(&self) -> Result<CalibrationSnapshot, CalibrationError> {
        self.calibration.decision_snapshot()
    }

    pub fn save_calibration(&self) -> Result<String, CalibrationError> {
        self.calibration.encode_json()
    }

    pub fn load_calibration(&mut self, encoded: &str) -> Result<(), CalibrationError> {
        self.calibration = CalibrationState::decode_json(encoded)?;
        Ok(())
    }

    pub fn evaluate(&mut self, frame: &EvidenceFrame) -> Result<Decision, RuntimeError> {
        let expected_calibration = self
            .calibration
            .decision_snapshot()
            .map_err(RuntimeError::Calibration)?;
        if expected_calibration != frame.calibration {
            return Err(RuntimeError::CalibrationMismatch);
        }
        let payload = serde_json::to_vec(frame).map_err(RuntimeError::EvidenceSerialization)?;
        self.ledger
            .append(
                format!("frame-{}", frame.now_ms),
                "evidence_frame",
                frame.now_ms,
                &payload,
            )
            .map_err(RuntimeError::Ledger)?;
        Ok(self.decision.decide(frame))
    }
}
