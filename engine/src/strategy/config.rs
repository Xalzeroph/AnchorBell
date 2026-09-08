//! Versioned, typed strategy/runtime configuration.
use crate::execution::BinanceEnvironment;
use crate::simulation::experiment_plan::ExperimentSpec;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

pub const STRATEGY_PROFILE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyProfile {
    pub schema_version: u16,
    pub policy_id: String,
    pub environment: BinanceEnvironment,
    pub index_anchors: bool,
    pub symbols: Vec<String>,
    pub output_root: String,
    pub capital_usdt: String,
    pub duration_secs: u64,
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
    pub m9_calibration_source_label: String,
    pub experiments: Vec<ExperimentSpec>,
}

impl StrategyProfile {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read strategy profile {}: {error}", path.display()))?;
        let profile: Self = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid strategy profile {}: {error}", path.display()))?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != STRATEGY_PROFILE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported strategy profile schema {}",
                self.schema_version
            ));
        }
        if self.policy_id.trim().is_empty()
            || self.symbols.is_empty()
            || self.output_root.trim().is_empty()
            || self.capital_usdt.trim().is_empty()
            || self.m9_calibration_source_label.trim().is_empty()
        {
            return Err(
                "strategy profile identity, symbols, output, and capital are required".into(),
            );
        }
        if self.entry_threshold_bps < 0
            || self.threshold_scale_ppm <= 0
            || self.max_position <= 0
            || self.requested_quantity <= 0
            || self.max_mark_index_gap_bps < 0
            || self.fee_ppm < 0
            || self.quantity_scale > 18
            || self.price_scale > 18
            || self.max_subscriptions_per_shard == 0
            || self.experiments.is_empty()
        {
            return Err("strategy profile contains invalid numeric or experiment values".into());
        }
        Ok(())
    }

    pub fn experiment_plan(
        &self,
    ) -> Result<crate::simulation::experiment_plan::ExperimentPlan, String> {
        crate::simulation::experiment_plan::ExperimentPlan::from_specs(self.experiments.clone())
            .map_err(str::to_owned)
    }

    pub fn capital_usdt_ticks(&self) -> Result<i64, String> {
        let unsigned = self.capital_usdt.trim();
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if whole.is_empty()
            || fraction.len() > 8
            || !whole.chars().all(|c| c.is_ascii_digit())
            || !fraction.chars().all(|c| c.is_ascii_digit())
        {
            return Err("capital_usdt must be a non-negative decimal with at most 8 places".into());
        }
        let whole_ticks = whole
            .parse::<i64>()
            .map_err(|_| "capital_usdt is too large")?;
        let mut fraction_text = fraction.to_owned();
        while fraction_text.len() < 8 {
            fraction_text.push('0');
        }
        let fraction_ticks = fraction_text
            .parse::<i64>()
            .map_err(|_| "invalid capital_usdt")?;
        whole_ticks
            .checked_mul(100_000_000)
            .and_then(|value| value.checked_add(fraction_ticks))
            .ok_or_else(|| "capital_usdt is too large".into())
    }
}
