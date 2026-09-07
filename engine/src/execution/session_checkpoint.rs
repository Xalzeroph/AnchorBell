use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const SESSION_CHECKPOINT_SCHEMA_VERSION: u16 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionCheckpoint {
    pub schema_version: u16,
    pub session_id: String,
    pub environment: String,
    pub symbol: String,
    pub last_event_at_ms: u64,
    /// Signed net position. Independent ledgers may offset one another.
    pub position_ticks: i64,
    /// Sum of absolute positions across the checkpoint scope.
    pub gross_position_ticks: i64,
    /// Per-ledger positions keyed by strategy_label::symbol.
    pub portfolio_positions: BTreeMap<String, i64>,
    pub working_order_ids: Vec<String>,
    pub risk_stopped: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("checkpoint encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("unsupported checkpoint schema version: {0}")]
    UnsupportedSchema(u16),
    #[error("checkpoint session id is empty")]
    EmptySessionId,
    #[error("checkpoint gross position does not match its position detail")]
    GrossPositionMismatch,
}

impl SessionCheckpoint {
    pub fn new(
        session_id: impl Into<String>,
        environment: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SESSION_CHECKPOINT_SCHEMA_VERSION,
            session_id: session_id.into(),
            environment: environment.into(),
            symbol: symbol.into(),
            last_event_at_ms: 0,
            position_ticks: 0,
            gross_position_ticks: 0,
            portfolio_positions: BTreeMap::new(),
            working_order_ids: Vec::new(),
            risk_stopped: true,
        }
    }

    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.schema_version != SESSION_CHECKPOINT_SCHEMA_VERSION {
            return Err(CheckpointError::UnsupportedSchema(self.schema_version));
        }
        if self.session_id.trim().is_empty() {
            return Err(CheckpointError::EmptySessionId);
        }
        if self.gross_position_ticks < 0 {
            return Err(CheckpointError::GrossPositionMismatch);
        }
        let computed_net = self
            .portfolio_positions
            .values()
            .copied()
            .fold(0_i64, i64::saturating_add);
        if computed_net != self.position_ticks {
            return Err(CheckpointError::GrossPositionMismatch);
        }
        let computed_gross = self
            .portfolio_positions
            .values()
            .map(|position| position.checked_abs().unwrap_or(i64::MAX))
            .fold(0_i64, i64::saturating_add);
        let expected_gross = if self.symbol == "PORTFOLIO" {
            computed_gross
        } else {
            self.position_ticks.checked_abs().unwrap_or(i64::MAX)
        };
        if self.gross_position_ticks != expected_gross {
            return Err(CheckpointError::GrossPositionMismatch);
        }
        Ok(())
    }
    pub fn write_atomic(&self, path: &Path) -> Result<(), CheckpointError> {
        self.validate()?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let temp_path = temporary_path(path);
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&temp_path, bytes)?;
        replace_file(&temp_path, path)?;
        Ok(())
    }

    pub fn read(path: &Path) -> Result<Self, CheckpointError> {
        let checkpoint: Self = serde_json::from_slice(&fs::read(path)?)?;
        checkpoint.validate()?;
        Ok(checkpoint)
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let mut temp = path.as_os_str().to_owned();
    temp.push(format!(".{nonce}.tmp"));
    PathBuf::from(temp)
}

fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };
        let source = source
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let target = target
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                target.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        fs::rename(source, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_round_trips_and_restarts_risk_stopped() {
        let path = std::env::temp_dir().join(format!(
            "anchorbell-checkpoint-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut checkpoint = SessionCheckpoint::new("session-1", "testnet", "BTCUSDT");
        checkpoint.position_ticks = -12;
        checkpoint.gross_position_ticks = 12;
        checkpoint.portfolio_positions.insert("BTCUSDT".into(), -12);
        checkpoint.working_order_ids = vec!["client-7".into()];
        checkpoint.write_atomic(&path).unwrap();
        let restored = SessionCheckpoint::read(&path).unwrap();
        assert_eq!(restored, checkpoint);
        assert!(restored.risk_stopped);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn portfolio_checkpoint_keeps_offsetting_positions_visible() {
        let mut checkpoint = SessionCheckpoint::new("session-1", "simulation", "PORTFOLIO");
        checkpoint
            .portfolio_positions
            .insert("M1::BTCUSDT".into(), 12);
        checkpoint
            .portfolio_positions
            .insert("M2::BTCUSDT".into(), -12);
        checkpoint.position_ticks = 0;
        checkpoint.gross_position_ticks = 24;
        assert!(checkpoint.validate().is_ok());
    }

    #[test]
    fn invalid_schema_is_rejected_before_restore() {
        let mut checkpoint = SessionCheckpoint::new("session-1", "testnet", "BTCUSDT");
        checkpoint.schema_version = 99;
        assert!(matches!(
            checkpoint.validate(),
            Err(CheckpointError::UnsupportedSchema(99))
        ));
    }
}
