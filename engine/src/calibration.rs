use crate::model::Side;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

pub const CALIBRATION_SCHEMA_VERSION: u16 = 2;
pub const CALIBRATION_MODEL_VERSION: &str = "anchorbell-conditional-outcome-v2";
pub const MIN_EFFECTIVE_SAMPLES: usize = 30;
const MIN_DIRECTIONAL_SAMPLES: usize = MIN_EFFECTIVE_SAMPLES / 2;
const WINDOW_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectionalCalibration {
    pub attempts: u64,
    pub fills: u64,
    pub fill_probability_bps: u16,
    pub adverse_markout_pico_bps: i64,
    pub robust_lower_pico_bps: i64,
}

impl DirectionalCalibration {
    fn cold_start() -> Self {
        Self {
            attempts: 0,
            fills: 0,
            fill_probability_bps: 0,
            adverse_markout_pico_bps: 0,
            robust_lower_pico_bps: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CalibrationPhase {
    ColdStart,
    Validating,
    Admitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationObservation {
    pub event_at_ms: u64,
    pub side: Side,
    pub attempted_quantity: i64,
    pub filled_quantity: i64,
    pub markout_pico_bps: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryAdmission {
    pub admitted_at_ms: u64,
    pub effective_sample_size: u64,
    pub train_robust_lower_pico_bps: i64,
    pub validation_median_pico_bps: i64,
    pub validation_robust_lower_pico_bps: i64,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryQualityReport {
    pub total_observations: u64,
    pub complete_observations: u64,
    pub buy_complete_observations: u64,
    pub sell_complete_observations: u64,
    pub validation_observations: u64,
    pub chronology_valid: bool,
    pub replay_valid: bool,
    pub train_robust_lower_pico_bps: Option<i64>,
    pub validation_median_pico_bps: Option<i64>,
    pub validation_robust_lower_pico_bps: Option<i64>,
    pub admissible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationState {
    pub schema_version: u16,
    pub model_version: String,
    pub instrument: String,
    pub observations: VecDeque<CalibrationObservation>,
    pub first_event_at_ms: u64,
    pub last_event_at_ms: u64,
    pub admission: Option<HistoryAdmission>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationSnapshot {
    pub schema_version: u16,
    pub model_version: String,
    pub instrument: String,
    pub phase: CalibrationPhase,
    pub effective_sample_size: u64,
    pub buy: DirectionalCalibration,
    pub sell: DirectionalCalibration,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationError {
    EmptyInstrument,
    InvalidObservation,
    EventTimeRegression,
    SchemaMismatch,
    ModelMismatch,
    InsufficientHistory,
    HistoryNotAdmitted,
    InvalidSnapshot,
}

impl CalibrationState {
    pub fn new(instrument: impl Into<String>) -> Result<Self, CalibrationError> {
        let instrument = instrument.into();
        if instrument.trim().is_empty() {
            return Err(CalibrationError::EmptyInstrument);
        }
        Ok(Self {
            schema_version: CALIBRATION_SCHEMA_VERSION,
            model_version: CALIBRATION_MODEL_VERSION.into(),
            instrument,
            observations: VecDeque::new(),
            first_event_at_ms: 0,
            last_event_at_ms: 0,
            admission: None,
        })
    }

    pub fn observe(&mut self, observation: CalibrationObservation) -> Result<(), CalibrationError> {
        self.validate_identity()?;
        if observation.event_at_ms == 0
            || observation.attempted_quantity <= 0
            || observation.filled_quantity < 0
            || observation.filled_quantity > observation.attempted_quantity
            || observation.markout_pico_bps.is_some_and(|v| v == i64::MIN)
        {
            return Err(CalibrationError::InvalidObservation);
        }
        if self.last_event_at_ms != 0 && observation.event_at_ms < self.last_event_at_ms {
            return Err(CalibrationError::EventTimeRegression);
        }
        if self.first_event_at_ms == 0 {
            self.first_event_at_ms = observation.event_at_ms;
        }
        self.last_event_at_ms = observation.event_at_ms;
        self.observations.push_back(observation);
        while self.observations.len() > WINDOW_CAPACITY {
            self.observations.pop_front();
        }
        self.refresh_admission();
        Ok(())
    }

    pub fn phase(&self) -> CalibrationPhase {
        if self.admission.is_some() {
            CalibrationPhase::Admitted
        } else if self.observations.is_empty() {
            CalibrationPhase::ColdStart
        } else {
            CalibrationPhase::Validating
        }
    }

    pub fn history_quality(&self) -> Result<HistoryQualityReport, CalibrationError> {
        self.validate_identity()?;
        let chronology_valid = self
            .observations
            .iter()
            .zip(self.observations.iter().skip(1))
            .all(|(previous, current)| previous.event_at_ms <= current.event_at_ms);
        let completed: Vec<(Side, i64)> = self
            .observations
            .iter()
            .filter_map(|observation| {
                observation
                    .markout_pico_bps
                    .map(|markout| (observation.side, markout))
            })
            .collect();

        let split = completed.len().saturating_mul(2) / 3;
        let train_values: Vec<i64> = completed[..split]
            .iter()
            .map(|(_, markout)| *markout)
            .collect();
        let validation_values: Vec<i64> = completed[split..]
            .iter()
            .map(|(_, markout)| *markout)
            .collect();
        let train = robust_stats(&train_values);
        let validation = robust_stats(&validation_values);
        let buy_complete_observations = completed
            .iter()
            .filter(|(side, _)| *side == Side::Buy)
            .count() as u64;
        let sell_complete_observations = completed
            .iter()
            .filter(|(side, _)| *side == Side::Sell)
            .count() as u64;
        let replay_valid = chronology_valid
            && self.first_event_at_ms
                == self
                    .observations
                    .front()
                    .map_or(0, |observation| observation.event_at_ms)
            && self.last_event_at_ms
                == self
                    .observations
                    .back()
                    .map_or(0, |observation| observation.event_at_ms);

        let admissible =
            completed.len() >= MIN_EFFECTIVE_SAMPLES
                && buy_complete_observations >= MIN_DIRECTIONAL_SAMPLES as u64
                && sell_complete_observations >= MIN_DIRECTIONAL_SAMPLES as u64
                && split > 0
                && validation.is_some()
                && chronology_valid
                && replay_valid
                && validation.zip(train).is_some_and(
                    |((_, validation_lower), (_, train_lower))| validation_lower >= train_lower,
                );

        Ok(HistoryQualityReport {
            total_observations: self.observations.len() as u64,
            complete_observations: completed.len() as u64,
            buy_complete_observations,
            sell_complete_observations,
            validation_observations: completed.len().saturating_sub(split) as u64,
            chronology_valid,
            replay_valid,
            train_robust_lower_pico_bps: train.map(|(_, lower)| lower),
            validation_median_pico_bps: validation.map(|(median, _)| median),
            validation_robust_lower_pico_bps: validation.map(|(_, lower)| lower),
            admissible,
        })
    }

    pub fn decision_snapshot(&self) -> Result<CalibrationSnapshot, CalibrationError> {
        self.validate_identity()?;
        if let Some(admission) = &self.admission {
            return self.admitted_snapshot(admission);
        }
        Ok(self.cold_start_snapshot())
    }

    pub fn snapshot(&self) -> Result<CalibrationSnapshot, CalibrationError> {
        self.validate_identity()?;
        let admission = self
            .admission
            .as_ref()
            .ok_or(CalibrationError::HistoryNotAdmitted)?;
        self.admitted_snapshot(admission)
    }

    pub fn replay(&self) -> Result<Self, CalibrationError> {
        self.validate_identity()?;
        let mut replayed = Self::new(self.instrument.clone())?;
        for observation in &self.observations {
            replayed.observe(*observation)?;
        }
        Ok(replayed)
    }

    pub fn encode_json(&self) -> Result<String, CalibrationError> {
        self.validate_identity()?;
        serde_json::to_string(self).map_err(|_| CalibrationError::InvalidSnapshot)
    }

    pub fn decode_json(encoded: &str) -> Result<Self, CalibrationError> {
        let state: Self =
            serde_json::from_str(encoded).map_err(|_| CalibrationError::InvalidSnapshot)?;
        state.validate_identity()?;
        if state.replay()? != state {
            return Err(CalibrationError::InvalidSnapshot);
        }
        Ok(state)
    }

    fn refresh_admission(&mut self) {
        let Ok(report) = self.history_quality() else {
            self.admission = None;
            return;
        };
        if !report.admissible {
            self.admission = None;
            return;
        }
        let (Some(train_lower), Some(validation_median), Some(validation_lower)) = (
            report.train_robust_lower_pico_bps,
            report.validation_median_pico_bps,
            report.validation_robust_lower_pico_bps,
        ) else {
            self.admission = None;
            return;
        };
        self.admission = Some(HistoryAdmission {
            admitted_at_ms: self.last_event_at_ms,
            effective_sample_size: report.complete_observations,
            train_robust_lower_pico_bps: train_lower,
            validation_median_pico_bps: validation_median,
            validation_robust_lower_pico_bps: validation_lower,
            evidence_digest: digest_observations(&self.observations),
        });
    }

    fn admitted_snapshot(
        &self,
        admission: &HistoryAdmission,
    ) -> Result<CalibrationSnapshot, CalibrationError> {
        let buy = self
            .directional_calibration(Side::Buy)
            .ok_or(CalibrationError::InsufficientHistory)?;
        let sell = self
            .directional_calibration(Side::Sell)
            .ok_or(CalibrationError::InsufficientHistory)?;
        if admission.effective_sample_size < MIN_EFFECTIVE_SAMPLES as u64 {
            return Err(CalibrationError::InsufficientHistory);
        }
        let snapshot = CalibrationSnapshot {
            schema_version: self.schema_version,
            model_version: self.model_version.clone(),
            instrument: self.instrument.clone(),
            phase: CalibrationPhase::Admitted,
            effective_sample_size: admission.effective_sample_size,
            buy,
            sell,
            window_start_ms: self.first_event_at_ms,
            window_end_ms: self.last_event_at_ms,
            evidence_digest: admission.evidence_digest.clone(),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn directional_calibration(&self, side: Side) -> Option<DirectionalCalibration> {
        let observations: Vec<&CalibrationObservation> = self
            .observations
            .iter()
            .filter(|observation| observation.side == side)
            .collect();
        let attempts = observations.iter().try_fold(0_u64, |sum, observation| {
            sum.checked_add(observation.attempted_quantity as u64)
        })?;
        let fills = observations.iter().try_fold(0_u64, |sum, observation| {
            sum.checked_add(observation.filled_quantity as u64)
        })?;
        let markouts: Vec<i64> = observations
            .iter()
            .filter_map(|observation| observation.markout_pico_bps)
            .collect();
        let (median, robust_lower) = robust_stats(&markouts)?;
        Some(DirectionalCalibration {
            attempts,
            fills,
            fill_probability_bps: (fills.saturating_mul(10_000) / attempts).min(10_000) as u16,
            adverse_markout_pico_bps: median,
            robust_lower_pico_bps: robust_lower,
        })
    }

    fn cold_start_snapshot(&self) -> CalibrationSnapshot {
        CalibrationSnapshot {
            schema_version: self.schema_version,
            model_version: self.model_version.clone(),
            instrument: self.instrument.clone(),
            phase: CalibrationPhase::ColdStart,
            effective_sample_size: 0,
            buy: DirectionalCalibration::cold_start(),
            sell: DirectionalCalibration::cold_start(),
            window_start_ms: 0,
            window_end_ms: 0,
            evidence_digest: digest_bytes(
                format!("cold-start:{}:{}", self.model_version, self.instrument).as_bytes(),
            ),
        }
    }

    fn validate_identity(&self) -> Result<(), CalibrationError> {
        if self.schema_version != CALIBRATION_SCHEMA_VERSION {
            return Err(CalibrationError::SchemaMismatch);
        }
        if self.model_version != CALIBRATION_MODEL_VERSION {
            return Err(CalibrationError::ModelMismatch);
        }
        if self.instrument.trim().is_empty() {
            return Err(CalibrationError::EmptyInstrument);
        }
        Ok(())
    }
}

impl CalibrationSnapshot {
    pub fn robust_executable_value(&self, side: Side, path_value_pico_bps: i64) -> i64 {
        if self.phase != CalibrationPhase::Admitted {
            return path_value_pico_bps;
        }
        let directional = match side {
            Side::Buy => self.buy,
            Side::Sell => self.sell,
        };
        let fill_adjusted = crate::model::floor_div_positive(
            i128::from(path_value_pico_bps) * i128::from(directional.fill_probability_bps),
            10_000,
        )
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or_else(|| {
            if path_value_pico_bps.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        });
        fill_adjusted.saturating_add(directional.robust_lower_pico_bps)
    }

    pub fn validate(&self) -> Result<(), CalibrationError> {
        if self.schema_version != CALIBRATION_SCHEMA_VERSION
            || self.model_version != CALIBRATION_MODEL_VERSION
        {
            return Err(CalibrationError::SchemaMismatch);
        }
        if self.instrument.trim().is_empty() || self.evidence_digest.is_empty() {
            return Err(CalibrationError::InvalidSnapshot);
        }
        match self.phase {
            CalibrationPhase::ColdStart => {
                if self.effective_sample_size != 0
                    || self.buy != DirectionalCalibration::cold_start()
                    || self.sell != DirectionalCalibration::cold_start()
                {
                    return Err(CalibrationError::InvalidSnapshot);
                }
            }
            CalibrationPhase::Validating => return Err(CalibrationError::InvalidSnapshot),
            CalibrationPhase::Admitted => {
                let directional_valid = [self.buy, self.sell].iter().all(|value| {
                    value.attempts > 0
                        && value.fills <= value.attempts
                        && value.fill_probability_bps <= 10_000
                });
                if self.effective_sample_size < MIN_EFFECTIVE_SAMPLES as u64
                    || !directional_valid
                    || self.window_end_ms < self.window_start_ms
                {
                    return Err(CalibrationError::InvalidSnapshot);
                }
            }
        }
        Ok(())
    }
}

fn robust_stats(values: &[i64]) -> Option<(i64, i64)> {
    if values.is_empty() {
        return None;
    }
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let median = ordered[(ordered.len() - 1) / 2];
    let mut deviations: Vec<i64> = ordered
        .iter()
        .map(|value| value.saturating_sub(median).abs())
        .collect();
    deviations.sort_unstable();
    let mad = deviations[(deviations.len() - 1) / 2];
    Some((median, median.saturating_sub(mad.saturating_mul(3))))
}

fn digest_observations(observations: &VecDeque<CalibrationObservation>) -> String {
    let bytes = serde_json::to_vec(observations).unwrap_or_default();
    digest_bytes(&bytes)
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(at: u64, side: Side, markout: Option<i64>) -> CalibrationObservation {
        CalibrationObservation {
            event_at_ms: at,
            side,
            attempted_quantity: 10,
            filled_quantity: 5,
            markout_pico_bps: markout,
        }
    }

    #[test]
    fn cold_start_uses_no_history() {
        let state = CalibrationState::new("BTCUSDT").unwrap();
        let snapshot = state.decision_snapshot().unwrap();
        assert_eq!(snapshot.phase, CalibrationPhase::ColdStart);
        assert_eq!(snapshot.robust_executable_value(Side::Buy, 123), 123);
        assert!(snapshot.validate().is_ok());
    }

    #[test]
    fn history_shortage_is_not_admitted() {
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        for i in 0..(MIN_EFFECTIVE_SAMPLES - 1) {
            state
                .observe(observation(
                    i as u64 + 1,
                    if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    Some(20_000_000_000),
                ))
                .unwrap();
        }
        assert_eq!(state.snapshot(), Err(CalibrationError::HistoryNotAdmitted));
        assert_eq!(state.phase(), CalibrationPhase::Validating);
    }

    #[test]
    fn history_is_admitted_only_after_holdout_validation() {
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        for i in 0..MIN_EFFECTIVE_SAMPLES {
            state
                .observe(observation(
                    i as u64 + 1,
                    if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    Some(if i % 2 == 0 {
                        20_000_000_000
                    } else {
                        -10_000_000_000
                    }),
                ))
                .unwrap();
        }
        let report = state.history_quality().unwrap();
        assert!(report.admissible);
        assert_eq!(state.phase(), CalibrationPhase::Admitted);
        let snapshot = state.snapshot().unwrap();
        assert!(snapshot.validate().is_ok());
        assert_eq!(snapshot.buy.robust_lower_pico_bps, 20_000_000_000);
        assert_eq!(snapshot.sell.robust_lower_pico_bps, -10_000_000_000);
    }

    #[test]
    fn chronological_holdout_rejects_future_regime_break() {
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        for i in 0..MIN_EFFECTIVE_SAMPLES {
            let markout = if i < 20 {
                20_000_000_000
            } else {
                -20_000_000_000
            };
            state
                .observe(observation(
                    i as u64 + 1,
                    if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    Some(markout),
                ))
                .unwrap();
        }
        let report = state.history_quality().unwrap();
        assert!(!report.admissible);
        assert!(
            report.train_robust_lower_pico_bps.expect("train statistic")
                > report
                    .validation_robust_lower_pico_bps
                    .expect("validation statistic")
        );
    }

    #[test]
    fn incomplete_history_is_kept_but_not_admitted() {
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        for i in 0..MIN_EFFECTIVE_SAMPLES {
            state
                .observe(observation(i as u64 + 1, Side::Sell, None))
                .unwrap();
        }
        assert_eq!(state.observations.len(), MIN_EFFECTIVE_SAMPLES);
        assert_eq!(state.phase(), CalibrationPhase::Validating);
        assert_eq!(state.snapshot(), Err(CalibrationError::HistoryNotAdmitted));
    }

    #[test]
    fn persisted_state_round_trips_only_after_replay_validation() {
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        for i in 0..MIN_EFFECTIVE_SAMPLES {
            state
                .observe(observation(i as u64 + 1, Side::Sell, Some(20_000_000_000)))
                .unwrap();
        }
        let encoded = state.encode_json().unwrap();
        assert_eq!(CalibrationState::decode_json(&encoded).unwrap(), state);
    }

    #[test]
    fn event_time_regression_is_rejected() {
        let mut state = CalibrationState::new("BTCUSDT").unwrap();
        state.observe(observation(10, Side::Sell, Some(1))).unwrap();
        assert_eq!(
            state.observe(observation(9, Side::Sell, Some(1))),
            Err(CalibrationError::EventTimeRegression)
        );
    }
}
