use super::m9::M9Calibration;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const CALIBRATION_SCHEMA_VERSION: u32 = 2;
pub const CALIBRATION_MODEL_VERSION: &str = "m9-data-driven-calibration-v2";
const ROLLING_WINDOW_CAPACITY: usize = 4096;
/// Common effective-sample scale used in calibration reports. Individual
/// evidence streams have different natural frequencies, so readiness is based
/// on component-specific floors rather than the raw minimum count.
const MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE: u64 = 30;
const MIN_MARKET_SAMPLES: u64 = 30;
const MIN_REVERSION_SAMPLES: u64 = 8;
const MIN_ORDER_LIFECYCLE_SAMPLES: u64 = 30;
const MIN_FILL_PARTICIPATION_SAMPLES: u64 = 10;
const MIN_MARKOUT_SAMPLES: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalibrationStatus {
    InsufficientHistory,
    Calibrated,
}

impl CalibrationStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::InsufficientHistory => "insufficient_history",
            Self::Calibrated => "calibrated",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationParameter {
    pub parameter_name: String,
    pub value: i64,
    pub unit: String,
    pub estimator: String,
    pub sample_count: u64,
    pub effective_sample_size: u64,
    pub window_start_event_time_ms: u64,
    pub window_end_event_time_ms: u64,
    pub uncertainty: i64,
    pub source_streams: Vec<String>,
    pub model_version: String,
    pub snapshot_event_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationState {
    pub instrument: String,
    pub first_event_time_ms: u64,
    pub last_event_time_ms: u64,
    pub orders_placed: u64,
    pub completed_orders: u64,
    pub fill_events: u64,
    pub reversion_events: u64,
    pub return_abs_pico_bps: VecDeque<i64>,
    pub spread_pico_bps: VecDeque<i64>,
    pub residual_abs_pico_bps: VecDeque<i64>,
    pub reversion_half_life_ms: VecDeque<u64>,
    pub order_wait_ms: VecDeque<u64>,
    pub fill_participation_bps: VecDeque<i64>,
    pub adverse_markout_pico_bps: VecDeque<i64>,
    pub last_residual_abs_pico_bps: Option<i64>,
    pub last_residual_event_time_ms: Option<u64>,
}

impl CalibrationState {
    pub fn new(instrument: impl Into<String>) -> Self {
        Self {
            instrument: instrument.into(),
            first_event_time_ms: 0,
            last_event_time_ms: 0,
            orders_placed: 0,
            completed_orders: 0,
            fill_events: 0,
            reversion_events: 0,
            return_abs_pico_bps: VecDeque::new(),
            spread_pico_bps: VecDeque::new(),
            residual_abs_pico_bps: VecDeque::new(),
            reversion_half_life_ms: VecDeque::new(),
            order_wait_ms: VecDeque::new(),
            fill_participation_bps: VecDeque::new(),
            adverse_markout_pico_bps: VecDeque::new(),
            last_residual_abs_pico_bps: None,
            last_residual_event_time_ms: None,
        }
    }

    fn touch(&mut self, time: u64) {
        if time == 0 {
            return;
        }
        if self.first_event_time_ms == 0 {
            self.first_event_time_ms = time;
        }
        self.last_event_time_ms = self.last_event_time_ms.max(time);
    }

    fn push<T>(samples: &mut VecDeque<T>, value: T) {
        samples.push_back(value);
        while samples.len() > ROLLING_WINDOW_CAPACITY {
            samples.pop_front();
        }
    }

    pub fn observe_market(
        &mut self,
        time: u64,
        ret: Option<i64>,
        spread: Option<i64>,
        residual: Option<i64>,
    ) {
        self.touch(time);
        if let Some(value) = ret.filter(|value| *value >= 0) {
            Self::push(&mut self.return_abs_pico_bps, value);
        }
        if let Some(value) = spread.filter(|value| *value >= 0) {
            Self::push(&mut self.spread_pico_bps, value);
        }
        let Some(residual) = residual else { return };
        let absolute = residual.unsigned_abs().min(i64::MAX as u64) as i64;
        if let (Some(previous), Some(previous_time)) = (
            self.last_residual_abs_pico_bps,
            self.last_residual_event_time_ms,
        ) {
            if time > previous_time && absolute.saturating_mul(2) <= previous {
                Self::push(
                    &mut self.reversion_half_life_ms,
                    time.saturating_sub(previous_time),
                );
                self.reversion_events = self.reversion_events.saturating_add(1);
            }
        }
        Self::push(&mut self.residual_abs_pico_bps, absolute);
        self.last_residual_abs_pico_bps = Some(absolute);
        self.last_residual_event_time_ms = Some(time);
    }

    pub fn observe_order_placed(&mut self, time: u64) {
        self.touch(time);
        self.orders_placed = self.orders_placed.saturating_add(1);
    }

    pub fn observe_fill(&mut self, time: u64, placed_at: u64, quantity: i64, displayed_depth: i64) {
        self.touch(time);
        self.fill_events = self.fill_events.saturating_add(1);
        if displayed_depth > 0 && quantity > 0 {
            let participation = (i128::from(quantity) * 10_000 / i128::from(displayed_depth))
                .clamp(0, 10_000) as i64;
            Self::push(&mut self.fill_participation_bps, participation);
        }
        if time >= placed_at {
            Self::push(&mut self.order_wait_ms, time.saturating_sub(placed_at));
        }
    }

    pub fn observe_order_terminal(&mut self, time: u64, placed_at: u64) {
        self.touch(time);
        self.completed_orders = self.completed_orders.saturating_add(1);
        if time >= placed_at {
            Self::push(&mut self.order_wait_ms, time.saturating_sub(placed_at));
        }
    }

    pub fn observe_markout(&mut self, time: u64, markout: i64) {
        self.touch(time);
        Self::push(&mut self.adverse_markout_pico_bps, markout.max(0));
    }

    pub fn snapshot(&self, fee_pico_bps: i64) -> CalibrationSnapshot {
        CalibrationSnapshot::from_state(self, fee_pico_bps)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationSnapshot {
    pub schema_version: u32,
    pub model_version: String,
    pub instrument: String,
    pub status: CalibrationStatus,
    pub reason: String,
    pub created_at_event_time_ms: u64,
    pub window_start_event_time_ms: u64,
    pub window_end_event_time_ms: u64,
    pub effective_sample_size: u64,
    pub calibration: Option<M9Calibration>,
    pub parameters: Vec<CalibrationParameter>,
    pub state: CalibrationState,
}

impl CalibrationSnapshot {
    fn from_state(state: &CalibrationState, fee_pico_bps: i64) -> Self {
        let residual = robust_stats(&state.residual_abs_pico_bps);
        let spread = robust_stats(&state.spread_pico_bps);
        let markout = robust_stats(&state.adverse_markout_pico_bps);
        let half_life = median_u64(&state.reversion_half_life_ms);
        let fill_horizon = median_u64(&state.order_wait_ms);
        let participation = median(&state.fill_participation_bps);
        let residual_count = state.residual_abs_pico_bps.len() as u64;
        let market_samples = (state.return_abs_pico_bps.len() as u64)
            .min(state.spread_pico_bps.len() as u64)
            .min(residual_count);
        let reversion_samples = state.reversion_half_life_ms.len() as u64;
        let order_lifecycle_samples = state.order_wait_ms.len() as u64;
        let fill_participation_samples = state.fill_participation_bps.len() as u64;
        let markout_samples = state.adverse_markout_pico_bps.len() as u64;
        let scaled = |count: u64, required: u64| {
            count.saturating_mul(MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE) / required.max(1)
        };
        let effective = [
            scaled(market_samples, MIN_MARKET_SAMPLES),
            scaled(reversion_samples, MIN_REVERSION_SAMPLES),
            scaled(order_lifecycle_samples, MIN_ORDER_LIFECYCLE_SAMPLES),
            scaled(fill_participation_samples, MIN_FILL_PARTICIPATION_SAMPLES),
            scaled(markout_samples, MIN_MARKOUT_SAMPLES),
        ]
        .into_iter()
        .min()
        .unwrap_or(0);
        let component_history_ready = market_samples >= MIN_MARKET_SAMPLES
            && reversion_samples >= MIN_REVERSION_SAMPLES
            && order_lifecycle_samples >= MIN_ORDER_LIFECYCLE_SAMPLES
            && fill_participation_samples >= MIN_FILL_PARTICIPATION_SAMPLES
            && markout_samples >= MIN_MARKOUT_SAMPLES;
        let mut missing = Vec::new();
        if residual.is_none() {
            missing.push("residual");
        }
        if spread.is_none() {
            missing.push("spread");
        }
        if markout.is_none() {
            missing.push("markout");
        }
        if half_life.is_none() {
            missing.push("half_life");
        }
        if fill_horizon.is_none() {
            missing.push("fill_horizon");
        }
        if participation.is_none() {
            missing.push("fill_participation");
        }
        if state.fill_events == 0 {
            missing.push("fill_events");
        }
        if market_samples < MIN_MARKET_SAMPLES {
            missing.push("market_samples");
        }
        if reversion_samples < MIN_REVERSION_SAMPLES {
            missing.push("reversion_samples");
        }
        if order_lifecycle_samples < MIN_ORDER_LIFECYCLE_SAMPLES {
            missing.push("order_lifecycle_samples");
        }
        if fill_participation_samples < MIN_FILL_PARTICIPATION_SAMPLES {
            missing.push("fill_participation_samples");
        }
        if markout_samples < MIN_MARKOUT_SAMPLES {
            missing.push("markout_samples");
        }
        if !component_history_ready {
            missing.push("effective_sample_size");
        }
        let residual_mad = residual.map(|(_, mad)| mad).unwrap_or(0);
        let spread_center = spread.map(|(center, _)| center).unwrap_or(0);
        let markout_center = markout.map(|(center, _)| center).unwrap_or(0);
        let uncertainty_pico = residual_mad
            .saturating_add(spread.map(|(_, mad)| mad).unwrap_or(0))
            .saturating_add(markout.map(|(_, mad)| mad).unwrap_or(0));
        let denominator = state.orders_placed.saturating_add(state.completed_orders);
        let fill_hazard = if denominator > 0 {
            (i128::from(state.fill_events) * 10_000 / i128::from(denominator)).clamp(0, 10_000)
                as i64
        } else {
            0
        };
        let null_weight = if residual_count > 0 {
            (10_000_i128 - i128::from(state.reversion_events) * 10_000 / i128::from(residual_count))
                .clamp(0, 10_000) as i64
        } else {
            10_000
        };
        let fee = fee_pico_bps.max(0);
        let min_residual = residual_mad
            .saturating_add(spread_center)
            .saturating_add(markout_center)
            .saturating_add(fee);
        let min_lcb = uncertainty_pico
            .saturating_add(markout_center)
            .saturating_add(spread_center)
            .saturating_add(fee);
        let calibration = match (
            half_life,
            fill_horizon,
            participation,
            residual,
            spread,
            markout,
        ) {
            (
                Some(half_life_ms),
                Some(fill_horizon_ms),
                Some(exit_participation_bps),
                Some(_),
                Some(_),
                Some(_),
            ) if fill_hazard > 0 && component_history_ready => {
                let value = M9Calibration {
                    half_life_ms,
                    exit_lead_ms: fill_horizon_ms,
                    fill_horizon_ms,
                    fill_hazard_bps: fill_hazard,
                    null_rw_min_weight_bps: null_weight,
                    uncertainty_bps: pico_to_bps_ceil(uncertainty_pico),
                    adverse_selection_bps: pico_to_bps_ceil(markout_center),
                    exit_participation_bps: exit_participation_bps.clamp(0, 10_000),
                    min_residual_pico_bps: min_residual,
                    min_robust_lcb_pico_bps: min_lcb,
                };
                value.valid().then_some(value)
            }
            _ => None,
        };
        let mut parameters = Vec::new();
        let mut add =
            |name: &str, value: i64, unit: &str, count: u64, uncertainty: i64, streams: &[&str]| {
                parameters.push(CalibrationParameter {
                    parameter_name: name.to_owned(),
                    value,
                    unit: unit.to_owned(),
                    estimator: "rolling_median_mad".to_owned(),
                    sample_count: count,
                    effective_sample_size: effective,
                    window_start_event_time_ms: state.first_event_time_ms,
                    window_end_event_time_ms: state.last_event_time_ms,
                    uncertainty,
                    source_streams: streams.iter().map(|stream| (*stream).to_owned()).collect(),
                    model_version: CALIBRATION_MODEL_VERSION.to_owned(),
                    snapshot_event_time_ms: state.last_event_time_ms,
                });
            };
        add(
            "half_life",
            half_life.unwrap_or(0) as i64,
            "ms",
            state.reversion_half_life_ms.len() as u64,
            0,
            &["residual"],
        );
        add(
            "exit_lead",
            fill_horizon.unwrap_or(0) as i64,
            "ms",
            state.order_wait_ms.len() as u64,
            0,
            &["order_lifecycle"],
        );
        add(
            "fill_horizon",
            fill_horizon.unwrap_or(0) as i64,
            "ms",
            state.order_wait_ms.len() as u64,
            0,
            &["order_lifecycle"],
        );
        add(
            "fill_hazard",
            fill_hazard,
            "probability_bps",
            state.fill_events,
            0,
            &["order_lifecycle"],
        );
        add(
            "null_random_walk_weight",
            null_weight,
            "bps",
            residual_count,
            0,
            &["residual"],
        );
        add(
            "uncertainty",
            pico_to_bps_ceil(uncertainty_pico),
            "bps",
            residual_count,
            pico_to_bps_ceil(uncertainty_pico),
            &["residual", "spread", "markout"],
        );
        add(
            "adverse_selection",
            pico_to_bps_ceil(markout_center),
            "bps",
            state.adverse_markout_pico_bps.len() as u64,
            0,
            &["markout"],
        );
        add(
            "exit_participation",
            participation.unwrap_or(0),
            "probability_bps",
            state.fill_participation_bps.len() as u64,
            0,
            &["depth", "fills"],
        );
        add(
            "min_residual",
            min_residual,
            "pico_bps",
            residual_count,
            uncertainty_pico,
            &["residual", "spread", "markout", "fee"],
        );
        add(
            "min_robust_lcb",
            min_lcb,
            "pico_bps",
            residual_count,
            uncertainty_pico,
            &["residual", "spread", "markout", "fee"],
        );
        let status = if calibration.is_some() {
            CalibrationStatus::Calibrated
        } else {
            CalibrationStatus::InsufficientHistory
        };
        let reason = if calibration.is_some() {
            "calibration_available".to_owned()
        } else {
            format!("insufficient_history:{}", missing.join(","))
        };
        Self {
            schema_version: CALIBRATION_SCHEMA_VERSION,
            model_version: CALIBRATION_MODEL_VERSION.to_owned(),
            instrument: state.instrument.clone(),
            status,
            reason,
            created_at_event_time_ms: state.last_event_time_ms,
            window_start_event_time_ms: state.first_event_time_ms,
            window_end_event_time_ms: state.last_event_time_ms,
            effective_sample_size: effective,
            calibration,
            parameters,
            state: state.clone(),
        }
    }

    pub fn replay(&self) -> Result<CalibrationState, String> {
        if self.schema_version != CALIBRATION_SCHEMA_VERSION {
            return Err("calibration_schema_version_mismatch".to_owned());
        }
        if self.model_version != CALIBRATION_MODEL_VERSION {
            return Err("calibration_model_version_mismatch".to_owned());
        }
        if self.instrument != self.state.instrument {
            return Err("calibration_instrument_mismatch".to_owned());
        }
        Ok(self.state.clone())
    }
}

fn median(samples: &VecDeque<i64>) -> Option<i64> {
    if samples.is_empty() {
        return None;
    }
    let mut values = samples.iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    Some(values[(values.len() - 1) / 2])
}
fn median_u64(samples: &VecDeque<u64>) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut values = samples.iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    Some(values[(values.len() - 1) / 2])
}
fn robust_stats(samples: &VecDeque<i64>) -> Option<(i64, i64)> {
    let center = median(samples)?;
    let deviations = samples
        .iter()
        .map(|value| value.saturating_sub(center).abs())
        .collect::<VecDeque<_>>();
    Some((center, median(&deviations)?))
}
fn pico_to_bps_ceil(value: i64) -> i64 {
    if value <= 0 {
        return 0;
    }
    ((i128::from(value) + 1_000_000_000_000 - 1) / 1_000_000_000_000).clamp(0, i128::from(i64::MAX))
        as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insufficient_history_is_explicit_and_replayable() {
        let state = CalibrationState::new("TESTUSDT");
        let snapshot = state.snapshot(2_000_000_000_000);
        assert_eq!(snapshot.status, CalibrationStatus::InsufficientHistory);
        assert!(snapshot.reason.starts_with("insufficient_history:"));
        assert_eq!(snapshot.replay().unwrap().instrument, "TESTUSDT");
    }
    #[test]
    fn calibrated_snapshot_uses_observed_values() {
        let mut state = CalibrationState::new("TESTUSDT");
        for time in 1..=61 {
            state.observe_market(
                time,
                Some(2_000_000_000_000),
                Some(1_000_000_000_000),
                Some(if time % 2 == 0 {
                    4_000_000_000_000
                } else {
                    8_000_000_000_000
                }),
            );
        }
        for index in 0..30 {
            let placed_at = 100 + index * 3;
            state.observe_order_placed(placed_at);
            state.observe_fill(placed_at + 1, placed_at, 10, 100);
            state.observe_order_terminal(placed_at + 1, placed_at);
            state.observe_markout(placed_at + 2, 1_000_000_000_000);
        }
        let snapshot = state.snapshot(2_000_000_000_000);
        assert_eq!(snapshot.status, CalibrationStatus::Calibrated);
        assert_eq!(snapshot.calibration.unwrap().fill_horizon_ms, 1);
    }

    #[test]
    fn heterogeneous_stream_rates_can_calibrate_safely() {
        let mut state = CalibrationState::new("TESTUSDT");
        for time in 1..=30 {
            state.observe_market(
                time,
                Some(2_000_000_000_000),
                Some(1_000_000_000_000),
                Some(if time % 2 == 0 {
                    4_000_000_000_000
                } else {
                    8_000_000_000_000
                }),
            );
        }
        for index in 0..30 {
            let placed_at = 100 + index * 3;
            state.observe_order_placed(placed_at);
            if index < 10 {
                state.observe_fill(placed_at + 1, placed_at, 10, 100);
                state.observe_markout(placed_at + 2, 1_000_000_000_000);
            }
            state.observe_order_terminal(placed_at + 1, placed_at);
        }
        let snapshot = state.snapshot(2_000_000_000_000);
        assert_eq!(snapshot.status, CalibrationStatus::Calibrated);
        assert!(snapshot.effective_sample_size >= MIN_CALIBRATION_EFFECTIVE_SAMPLE_SIZE);
    }

    #[test]
    fn one_fill_is_not_enough_to_calibrate() {
        let mut state = CalibrationState::new("TESTUSDT");
        for time in 1..=5 {
            state.observe_market(
                time,
                Some(2_000_000_000_000),
                Some(1_000_000_000_000),
                Some(8_000_000_000_000),
            );
        }
        state.observe_order_placed(5);
        state.observe_fill(6, 5, 10, 100);
        state.observe_order_terminal(6, 5);
        state.observe_markout(7, 1_000_000_000_000);
        let snapshot = state.snapshot(2_000_000_000_000);
        assert_eq!(snapshot.status, CalibrationStatus::InsufficientHistory);
        assert!(snapshot.reason.contains("effective_sample_size"));
    }
}
