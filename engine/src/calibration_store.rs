use crate::calibration::{CalibrationError, CalibrationState};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum CalibrationStoreError {
    Io(io::Error),
    Calibration(CalibrationError),
    InstrumentMismatch,
}

#[derive(Debug)]
pub struct FileCalibrationStore {
    path: PathBuf,
}

impl FileCalibrationStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self, instrument: &str) -> Result<CalibrationState, CalibrationStoreError> {
        let encoded = fs::read_to_string(&self.path).map_err(CalibrationStoreError::Io)?;
        let state =
            CalibrationState::decode_json(&encoded).map_err(CalibrationStoreError::Calibration)?;
        if state.instrument != instrument {
            return Err(CalibrationStoreError::InstrumentMismatch);
        }
        Ok(state)
    }

    pub fn load_or_cold_start(
        &self,
        instrument: &str,
    ) -> Result<CalibrationState, CalibrationStoreError> {
        match self.load(instrument) {
            Ok(state) => Ok(state),
            Err(CalibrationStoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                CalibrationState::new(instrument).map_err(CalibrationStoreError::Calibration)
            }
            Err(error) => Err(error),
        }
    }

    pub fn save(&self, state: &CalibrationState) -> Result<(), CalibrationStoreError> {
        let encoded = state
            .encode_json()
            .map_err(CalibrationStoreError::Calibration)?;
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(CalibrationStoreError::Io)?;
        }
        let temporary_path = self.path.with_extension("tmp");
        let result = (|| {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary_path)
                .map_err(CalibrationStoreError::Io)?;
            file.write_all(encoded.as_bytes())
                .map_err(CalibrationStoreError::Io)?;
            file.sync_all().map_err(CalibrationStoreError::Io)?;
            drop(file);
            fs::rename(&temporary_path, &self.path).map_err(CalibrationStoreError::Io)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn temporary_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "anchorbell-calibration-{}-{nonce}-{sequence}.json",
            std::process::id()
        ))
    }

    fn observation(at_ms: u64) -> crate::calibration::CalibrationObservation {
        crate::calibration::CalibrationObservation {
            event_at_ms: at_ms,
            side: Side::Buy,
            attempted_quantity: 10,
            filled_quantity: 5,
            markout_pico_bps: None,
        }
    }

    #[test]
    fn missing_store_starts_from_empty_cold_state() {
        let path = temporary_path();
        let store = FileCalibrationStore::new(&path);
        let state = store.load_or_cold_start("BTCUSDT").unwrap();
        assert_eq!(
            state.phase(),
            crate::calibration::CalibrationPhase::ColdStart
        );
        assert!(!path.exists());
    }

    #[test]
    fn save_and_load_requires_replay_validated_identity() {
        let path = temporary_path();
        let store = FileCalibrationStore::new(&path);
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        state.observe(observation(1)).unwrap();
        store.save(&state).unwrap();
        assert_eq!(store.load("BTCUSDT").unwrap(), state);
        assert!(matches!(
            store.load("ETHUSDT"),
            Err(CalibrationStoreError::InstrumentMismatch)
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn malformed_store_is_not_replaced_by_cold_start() {
        let path = temporary_path();
        fs::write(&path, b"{}").unwrap();
        let store = FileCalibrationStore::new(&path);
        assert!(matches!(
            store.load_or_cold_start("BTCUSDT"),
            Err(CalibrationStoreError::Calibration(
                CalibrationError::InvalidSnapshot
            ))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn save_replaces_existing_state_atomically() {
        let path = temporary_path();
        let store = FileCalibrationStore::new(&path);
        let state = CalibrationState::new("BTCUSDT").unwrap();
        store.save(&state).unwrap();
        assert!(path.exists());
        assert!(!path.with_extension("tmp").exists());
        fs::remove_file(path).unwrap();
    }
}
