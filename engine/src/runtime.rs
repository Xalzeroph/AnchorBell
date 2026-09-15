use crate::{
    calibration::{
        CalibrationError, CalibrationObservation, CalibrationSnapshot, CalibrationState,
    },
    calibration_store::{CalibrationStoreError, FileCalibrationStore},
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
    StrategyIdentityMismatch,
    LedgerCorrupt,
    CalibrationStore(CalibrationStoreError),
    EvidenceSerialization(SerializationError),
    Ledger(LedgerError),
    EventSequenceExhausted,
}

pub struct TradingEngine {
    pub decision: DecisionEngine,
    pub champion: StrategyArtifact,
    pub ledger: EvidenceLedger,
    pub calibration: CalibrationState,
    pub calibration_store: Option<FileCalibrationStore>,
    next_event_sequence: u64,
}

impl TradingEngine {
    pub fn new(
        decision: DecisionEngine,
        champion: StrategyArtifact,
        ledger: EvidenceLedger,
        calibration: CalibrationState,
    ) -> Self {
        Self::from_parts(decision, champion, ledger, calibration, None)
    }

    pub fn new_with_calibration_store(
        decision: DecisionEngine,
        champion: StrategyArtifact,
        ledger: EvidenceLedger,
        calibration: CalibrationState,
        calibration_store: FileCalibrationStore,
    ) -> Self {
        Self::from_parts(
            decision,
            champion,
            ledger,
            calibration,
            Some(calibration_store),
        )
    }

    fn from_parts(
        decision: DecisionEngine,
        champion: StrategyArtifact,
        ledger: EvidenceLedger,
        calibration: CalibrationState,
        calibration_store: Option<FileCalibrationStore>,
    ) -> Self {
        Self {
            decision,
            champion,
            next_event_sequence: ledger.events.len() as u64,
            ledger,
            calibration,
            calibration_store,
        }
    }

    pub fn observe_calibration(
        &mut self,
        observation: CalibrationObservation,
    ) -> Result<(), RuntimeError> {
        let previous = self.calibration.clone();
        self.calibration
            .observe(observation)
            .map_err(RuntimeError::Calibration)?;
        let persistence_result = self
            .calibration_store
            .as_ref()
            .map(|store| store.save(&self.calibration));
        if let Some(Err(error)) = persistence_result {
            self.calibration = previous;
            return Err(RuntimeError::CalibrationStore(error));
        }
        Ok(())
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

    pub fn save_calibration_to_store(
        &self,
        store: &FileCalibrationStore,
    ) -> Result<(), CalibrationStoreError> {
        store.save(&self.calibration)
    }

    pub fn load_calibration_from_store(
        &mut self,
        store: &FileCalibrationStore,
        instrument: &str,
    ) -> Result<(), CalibrationStoreError> {
        self.calibration = store.load_or_cold_start(instrument)?;
        Ok(())
    }

    fn next_event_id(&mut self, kind: &str, at_ms: u64) -> Result<String, RuntimeError> {
        let sequence = self
            .next_event_sequence
            .checked_add(1)
            .ok_or(RuntimeError::EventSequenceExhausted)?;
        let event_id = format!(
            "{kind}-{at_ms}-{}-{}",
            self.next_event_sequence, self.ledger.head_digest
        );
        self.next_event_sequence = sequence;
        Ok(event_id)
    }

    pub fn evaluate(&mut self, frame: &EvidenceFrame) -> Result<Decision, RuntimeError> {
        if !self.ledger.verify() {
            return Err(RuntimeError::LedgerCorrupt);
        }
        if self.champion.version.trim().is_empty()
            || self.champion.version != self.champion.plan.version
            || self.champion.version != self.decision.plan.version
            || !self.champion.plan.is_legal()
            || !self.decision.plan.is_legal()
        {
            return Err(RuntimeError::StrategyIdentityMismatch);
        }
        let expected_calibration = self
            .calibration
            .decision_snapshot()
            .map_err(RuntimeError::Calibration)?;
        if expected_calibration != frame.calibration {
            return Err(RuntimeError::CalibrationMismatch);
        }

        let frame_payload =
            serde_json::to_vec(frame).map_err(RuntimeError::EvidenceSerialization)?;
        let frame_event_id = self.next_event_id("frame", frame.now_ms)?;
        self.ledger
            .append(
                frame_event_id,
                "evidence_frame",
                frame.now_ms,
                &frame_payload,
            )
            .map_err(RuntimeError::Ledger)?;

        let decision = self.decision.decide(frame);
        let decision_payload =
            serde_json::to_vec(&decision).map_err(RuntimeError::EvidenceSerialization)?;
        let decision_event_id = self.next_event_id("decision", frame.now_ms)?;
        self.ledger
            .append(
                decision_event_id,
                "decision",
                frame.now_ms,
                &decision_payload,
            )
            .map_err(RuntimeError::Ledger)?;
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        calibration::CalibrationObservation,
        evolution::StrategyArtifact,
        model::Side,
        policy::{PolicyLimits, StrategyPlan},
    };
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn temporary_path() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "anchorbell-runtime-calibration-{}-{nonce}-{sequence}.json",
            std::process::id()
        ))
    }

    fn engine(store: FileCalibrationStore) -> TradingEngine {
        let plan = StrategyPlan::anchor_closed_maker("plan-v1");
        TradingEngine::new_with_calibration_store(
            DecisionEngine {
                plan: plan.clone(),
                limits: PolicyLimits {
                    requested_quantity: 1,
                },
            },
            StrategyArtifact {
                version: "plan-v1".into(),
                plan,
                evidence_digest: "a".repeat(64),
                sealed_set_digest: "b".repeat(64),
                effective_sample_size: 30,
                robust_value_pico_bps: 1,
                max_drawdown_pico_bps: 0,
                simplicity_score: 1,
            },
            EvidenceLedger::default(),
            CalibrationState::new("BTCUSDT").unwrap(),
            store,
        )
    }

    #[test]
    fn configured_runtime_persists_each_calibration_observation() {
        let path = temporary_path();
        let store = FileCalibrationStore::new(&path);
        let mut engine = engine(store);
        engine
            .observe_calibration(CalibrationObservation {
                event_at_ms: 1,
                side: Side::Buy,
                attempted_quantity: 10,
                filled_quantity: 5,
                markout_pico_bps: None,
            })
            .unwrap();
        let loaded = FileCalibrationStore::new(&path).load("BTCUSDT").unwrap();
        assert_eq!(loaded, engine.calibration);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn persistence_failure_rolls_back_the_in_memory_observation() {
        let path = temporary_path();
        fs::create_dir_all(&path).unwrap();
        let initial = CalibrationState::new("BTCUSDT").unwrap();
        let mut engine = engine(FileCalibrationStore::new(&path));
        let result = engine.observe_calibration(CalibrationObservation {
            event_at_ms: 1,
            side: Side::Buy,
            attempted_quantity: 10,
            filled_quantity: 5,
            markout_pico_bps: None,
        });
        assert!(matches!(result, Err(RuntimeError::CalibrationStore(_))));
        assert_eq!(engine.calibration, initial);
        fs::remove_dir_all(path).unwrap();
    }
}
