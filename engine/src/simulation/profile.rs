use std::{collections::BTreeSet, fs, path::{Path, PathBuf}, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    execution::BinanceEnvironment,
    simulation::experiment_plan::ExperimentPlan,
};

pub const SIMULATION_STRATEGY_PROFILE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SimulationExecutionConfig {
    pub entry_threshold_bps: i64,
    pub threshold_scale_ppm: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub max_mark_index_gap_bps: i64,
    pub max_anchor_age_ms: u64,
    pub fee_ppm: i64,
    pub quantity_scale: u32,
    pub price_scale: u32,
    pub max_subscriptions_per_shard: usize,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub metrics_refresh_ms: u64,
    pub index_anchor_refresh_ms: u64,
    pub anchor_kline_interval: String,
    pub anchor_kline_lookback_ms: u64,
    pub anchor_kline_limit: usize,
    pub fx_refresh_ms: u64,
    pub fx_max_age_ms: u64,
    pub queue_ahead: i64,
    pub trade_through: i64,
    pub market_to_decision_ms: u64,
    pub decision_to_exchange_ms: u64,
    pub cancel_to_exchange_ms: u64,
    pub quote_reprice_min_interval_ms: u64,
    pub dynamic_capital_refresh_ms: u64,
    pub depth_snapshot_limit: usize,
    pub checkpoint_interval_ms: u64,
}

impl SimulationExecutionConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.entry_threshold_bps < 0 {
            return Err("execution.entry_threshold_bps must be non-negative".to_owned());
        }
        if !(1..=2_000_000).contains(&self.threshold_scale_ppm) {
            return Err("execution.threshold_scale_ppm must be in 1..=2000000".to_owned());
        }
        if self.max_position <= 0 || self.requested_quantity <= 0 {
            return Err("execution position and order quantities must be positive".to_owned());
        }
        if self.max_mark_index_gap_bps < 0 || self.fee_ppm < 0 {
            return Err("execution mark/index gap and fee must be non-negative".to_owned());
        }
        if self.quantity_scale > 18 || self.price_scale > 18 {
            return Err("execution price/quantity scale cannot exceed 18".to_owned());
        }
        if self.max_subscriptions_per_shard == 0
            || self.connect_timeout_ms == 0
            || self.read_timeout_ms == 0
            || self.metrics_refresh_ms < 250
            || self.index_anchor_refresh_ms == 0
            || self.anchor_kline_interval.trim().is_empty()
            || self.anchor_kline_lookback_ms == 0
            || self.anchor_kline_limit == 0
            || self.fx_refresh_ms == 0
            || self.fx_max_age_ms == 0
            || self.depth_snapshot_limit == 0
            || self.checkpoint_interval_ms == 0
        {
            return Err("execution timing/capacity values must be non-zero and metrics_refresh_ms >= 250".to_owned());
        }
        if self.queue_ahead < 0 || self.trade_through < 0 {
            return Err("execution queue realism values must be non-negative".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SimulationStrategyProfile {
    pub schema_version: u16,
    pub policy_id: String,
    pub environment: String,
    pub index_anchors: bool,
    pub symbols: Vec<String>,
    pub output_root: PathBuf,
    /// Decimal USDT string. Keeping money textual avoids JSON floating-point drift.
    pub capital_usdt: String,
    pub duration_secs: u64,
    #[serde(default)]
    pub fold_id: Option<String>,
    #[serde(default)]
    pub stress_profile: Option<String>,
    pub m9_calibration_source_label: String,
    pub experiment_plan: ExperimentPlan,
    pub execution: SimulationExecutionConfig,
}

impl SimulationStrategyProfile {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read strategy profile {}: {error}", path.display()))?;
        let profile: Self = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid strategy profile {}: {error}", path.display()))?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn environment_value(&self) -> Result<BinanceEnvironment, String> {
        BinanceEnvironment::from_str(self.environment.trim())
            .map_err(|_| format!("unsupported strategy profile environment: {}", self.environment))
    }

    pub fn capital_usdt_ticks(&self) -> Result<i64, String> {
        parse_decimal(&self.capital_usdt, 8)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != SIMULATION_STRATEGY_PROFILE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported strategy profile schema_version {}; expected {}",
                self.schema_version, SIMULATION_STRATEGY_PROFILE_SCHEMA_VERSION
            ));
        }
        if self.policy_id.trim().is_empty() {
            return Err("strategy profile policy_id cannot be empty".to_owned());
        }
        self.environment_value()?;
        if !self.index_anchors {
            return Err("strategy profile must enable index_anchors for live-like batch simulation".to_owned());
        }
        if self.symbols.is_empty() {
            return Err("strategy profile symbols cannot be empty".to_owned());
        }
        let mut symbols = BTreeSet::new();
        for symbol in &self.symbols {
            let normalized = symbol.trim().to_ascii_uppercase();
            if normalized.is_empty() || normalized != *symbol {
                return Err("strategy profile symbols must be non-empty uppercase canonical symbols".to_owned());
            }
            if !symbols.insert(normalized) {
                return Err("strategy profile symbols must be unique".to_owned());
            }
        }
        if self.output_root.as_os_str().is_empty() {
            return Err("strategy profile output_root cannot be empty".to_owned());
        }
        if self.capital_usdt_ticks()? <= 0 {
            return Err("strategy profile capital_usdt must be positive".to_owned());
        }
        if self.fold_id.as_ref().is_some_and(|value| value.trim().is_empty()) {
            return Err("strategy profile fold_id cannot be empty".to_owned());
        }
        if self.fold_id.is_some() && self.duration_secs == 0 {
            return Err("strategy profile fold_id requires finite duration_secs".to_owned());
        }
        if self.stress_profile.is_some() && self.fold_id.is_none() {
            return Err("strategy profile stress_profile requires fold_id".to_owned());
        }
        self.experiment_plan
            .runtime_specs_with_ablations()
            .map_err(|error| format!("invalid strategy profile experiment_plan: {error}"))?;
        let source_label = self.m9_calibration_source_label.trim();
        if source_label.is_empty() {
            return Err("strategy profile m9_calibration_source_label cannot be empty".to_owned());
        }
        let source = self
            .experiment_plan
            .experiments
            .iter()
            .find(|experiment| experiment.label == source_label)
            .ok_or_else(|| "m9 calibration source must name a ledger in experiment_plan".to_owned())?;
        if source.strategy == "m9" {
            return Err("m9 calibration source cannot be an M9 ledger".to_owned());
        }
        let has_m9 = self
            .experiment_plan
            .experiments
            .iter()
            .any(|experiment| experiment.strategy == "m9");
        if has_m9 && !matches!(source.strategy.as_str(), "m3" | "m4" | "m5" | "m6" | "m7" | "m8") {
            return Err("m9 calibration source must be a fill-aware M3-M8 ledger".to_owned());
        }
        self.execution.validate()
    }
}

fn parse_decimal(value: &str, scale: u32) -> Result<i64, String> {
    let value = value.trim();
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    let fraction_len = fraction.len() as u32;
    if parts.next().is_some()
        || whole.is_empty()
        || fraction.len() > scale as usize
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("capital_usdt must be a positive decimal string".to_owned());
    }
    let unit = 10_i128.pow(scale);
    let whole = whole
        .parse::<i128>()
        .map_err(|_| "capital_usdt overflows".to_owned())?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| "capital_usdt overflows".to_owned())?
    };
    let scaled = whole
        .checked_mul(unit)
        .and_then(|base| base.checked_add(fraction_value * 10_i128.pow(scale - fraction_len)))
        .ok_or_else(|| "capital_usdt overflows".to_owned())?;
    i64::try_from(scaled).map_err(|_| "capital_usdt overflows".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> SimulationStrategyProfile {
        SimulationStrategyProfile {
            schema_version: SIMULATION_STRATEGY_PROFILE_SCHEMA_VERSION,
            policy_id: "test-policy".to_owned(),
            environment: "production".to_owned(),
            index_anchors: true,
            symbols: vec!["CXMTUSDT".to_owned()],
            output_root: PathBuf::from("target/simulation-profile-test"),
            capital_usdt: "1500".to_owned(),
            duration_secs: 0,
            fold_id: None,
            stress_profile: None,
            m9_calibration_source_label: "F3_m3".to_owned(),
            experiment_plan: ExperimentPlan::m1_to_m9(),
            execution: SimulationExecutionConfig {
                entry_threshold_bps: 5,
                threshold_scale_ppm: 700_000,
                max_position: 10_000_000,
                requested_quantity: 1_000_000,
                max_mark_index_gap_bps: 50,
                max_anchor_age_ms: 120_000,
                fee_ppm: 200,
                quantity_scale: 8,
                price_scale: 8,
                max_subscriptions_per_shard: 64,
                connect_timeout_ms: 5_000,
                read_timeout_ms: 15_000,
                metrics_refresh_ms: 1_000,
                index_anchor_refresh_ms: 60_000,
                anchor_kline_interval: "1m".into(),
                anchor_kline_lookback_ms: 86_400_000,
                anchor_kline_limit: 1_500,
                fx_refresh_ms: 30_000,
                fx_max_age_ms: 120_000,
                queue_ahead: 0,
                trade_through: 0,
                market_to_decision_ms: 0,
                decision_to_exchange_ms: 0,
                cancel_to_exchange_ms: 0,
                quote_reprice_min_interval_ms: 750,
                dynamic_capital_refresh_ms: 60_000,
                depth_snapshot_limit: 100,
                checkpoint_interval_ms: 5_000,
            },
        }
    }

    #[test]
    fn profile_validates_and_preserves_exact_capital_ticks() {
        let profile = profile();
        profile.validate().unwrap();
        assert_eq!(profile.capital_usdt_ticks().unwrap(), 150_000_000_000);
    }

    #[test]
    fn m9_source_must_exist_and_be_fill_aware() {
        let mut profile = profile();
        profile.m9_calibration_source_label = "F1_m1".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "missing".to_owned();
        assert!(profile.validate().is_err());
    }

    #[test]
    fn stress_profile_requires_a_finite_fold() {
        let mut profile = profile();
        profile.stress_profile = Some("synthetic_fee2x_latency_50_100_150_v1".to_owned());
        assert!(profile.validate().is_err());
        profile.fold_id = Some("fold-1".to_owned());
        profile.duration_secs = 3_600;
        assert!(profile.validate().is_ok());
    }
}
