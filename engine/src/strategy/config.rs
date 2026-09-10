//! Versioned, typed strategy/runtime configuration.
use crate::execution::{BinanceEnvironment, EmergencyExecutionPolicy};
use crate::market::AssetClass;
use crate::simulation::experiment_plan::ExperimentSpec;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fs, path::Path};

pub const STRATEGY_PROFILE_SCHEMA_VERSION: u16 = 1;

fn default_market_event_queue_capacity() -> usize {
    1_048_576
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeScheduleConfig {
    pub maker_fee_ppm: i64,
    pub taker_fee_ppm: i64,
    pub source: String,
    pub account_tier: String,
    pub bnb_discount: bool,
    pub effective_from_ms: u64,
    pub effective_until_ms: Option<u64>,
}

impl FeeScheduleConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.maker_fee_ppm < 0
            || self.taker_fee_ppm < 0
            || self.source.trim().is_empty()
            || self.account_tier.trim().is_empty()
            || self.effective_from_ms == 0
            || self
                .effective_until_ms
                .is_some_and(|until| until <= self.effective_from_ms)
        {
            return Err("invalid fee schedule configuration");
        }
        Ok(())
    }

    pub fn validate_at(&self, now_ms: u64) -> Result<(), &'static str> {
        self.validate()?;
        if self.effective_from_ms > now_ms
            || self.effective_until_ms.is_some_and(|until| now_ms >= until)
        {
            return Err("fee schedule is not effective at the current timestamp");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyProfile {
    pub schema_version: u16,
    pub policy_id: String,
    pub default_strategy_variant: String,
    pub market_id: String,
    pub experiment_plan_id: String,
    pub universe_id: String,
    pub asset_class: AssetClass,
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
    pub funding_lead_ms: u64,
    pub fee_ppm: i64,
    pub fee_schedule: FeeScheduleConfig,
    pub quantity_scale: u32,
    pub price_scale: u32,
    pub max_subscriptions_per_shard: usize,
    #[serde(default = "default_market_event_queue_capacity")]
    pub market_event_queue_capacity: usize,
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
    pub emergency_execution: EmergencyExecutionPolicy,
    pub dynamic_capital_refresh_ms: u64,
    pub depth_snapshot_limit: usize,
    pub checkpoint_interval_ms: u64,
    pub max_stale_ms: u64,
    pub run_registry_heartbeat_ms: u64,
    pub runtime_audit_path: String,
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
            || self.experiment_plan_id.trim().is_empty()
            || self.default_strategy_variant.trim().is_empty()
            || self.market_id.trim().is_empty()
            || self.universe_id.trim().is_empty()
            || self.asset_class == AssetClass::Unknown
            || self.symbols.is_empty()
            || self.output_root.trim().is_empty()
            || self.capital_usdt.trim().is_empty()
            || self.m9_calibration_source_label.trim().is_empty()
        {
            return Err(
                "strategy profile identity, symbols, output, and capital are required".into(),
            );
        }
        let mut symbols = BTreeSet::new();
        if self.symbols.iter().any(|symbol| {
            let normalized = symbol.trim().to_ascii_uppercase();
            normalized.is_empty() || !symbols.insert(normalized)
        }) {
            return Err("strategy profile symbols must be non-empty and unique".into());
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
            || self.market_event_queue_capacity == 0
            || self.experiments.is_empty()
            || self.checkpoint_interval_ms == 0
            || self.max_stale_ms == 0
            || self.run_registry_heartbeat_ms == 0
            || self.runtime_audit_path.trim().is_empty()
            || self.anchor_kline_interval.trim().is_empty()
            || self.anchor_kline_lookback_ms == 0
            || self.anchor_kline_limit == 0
        {
            return Err("strategy profile contains invalid numeric or experiment values".into());
        }
        self.emergency_execution.validate().map_err(str::to_owned)?;
        self.fee_schedule.validate().map_err(str::to_owned)?;
        if self.fee_ppm != self.fee_schedule.maker_fee_ppm
            || self.emergency_execution.taker_fee_ppm != self.fee_schedule.taker_fee_ppm
        {
            return Err("fee_ppm and emergency taker fee must match fee_schedule".into());
        }
        self.experiment_plan()
            .map_err(|error| format!("invalid experiment plan: {error}"))?;
        Ok(())
    }

    pub fn experiment_plan(
        &self,
    ) -> Result<crate::simulation::experiment_plan::ExperimentPlan, String> {
        crate::simulation::experiment_plan::ExperimentPlan::from_specs(
            self.experiment_plan_id.clone(),
            self.experiments.clone(),
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_profile_is_typed_and_resolves_every_method() {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/anchorbell-simulation.json");
        let profile = StrategyProfile::load(path).unwrap();
        let plan = profile.experiment_plan().unwrap();
        assert_eq!(plan.experiments.len(), 1);
        assert!(plan.runtime_specs_with_ablations().is_ok());
    }

    #[test]
    fn duplicate_symbols_are_rejected_before_runtime() {
        let profile = StrategyProfile {
            schema_version: STRATEGY_PROFILE_SCHEMA_VERSION,
            policy_id: "test".into(),
            default_strategy_variant: "m4".into(),
            market_id: "binance_usdm_tradfi_perpetual".into(),
            experiment_plan_id: "test".into(),
            universe_id: "test".into(),
            asset_class: AssetClass::OrdinaryEquity,
            environment: BinanceEnvironment::Production,
            index_anchors: true,
            symbols: vec!["CXMTUSDT".into(), "cxmtusdt".into()],
            output_root: "target/test".into(),
            capital_usdt: "1".into(),
            duration_secs: 1,
            entry_threshold_bps: 1,
            threshold_scale_ppm: 1,
            max_position: 1,
            requested_quantity: 1,
            max_mark_index_gap_bps: 1,
            max_anchor_age_ms: 1,
            funding_lead_ms: 1,
            fee_ppm: 1,
            fee_schedule: FeeScheduleConfig {
                maker_fee_ppm: 1,
                taker_fee_ppm: 400,
                source: "test".into(),
                account_tier: "test".into(),
                bnb_discount: false,
                effective_from_ms: 1,
                effective_until_ms: None,
            },
            quantity_scale: 1,
            price_scale: 1,
            max_subscriptions_per_shard: 1,
            market_event_queue_capacity: 1_048_576,
            connect_timeout_ms: 1,
            read_timeout_ms: 1,
            metrics_refresh_ms: 1,
            index_anchor_refresh_ms: 1,
            anchor_kline_interval: "1m".into(),
            anchor_kline_lookback_ms: 86_400_000,
            anchor_kline_limit: 1_500,
            fx_refresh_ms: 1,
            fx_max_age_ms: 1,
            queue_ahead: 0,
            trade_through: 0,
            market_to_decision_ms: 0,
            decision_to_exchange_ms: 0,
            cancel_to_exchange_ms: 0,
            quote_reprice_min_interval_ms: 1,
            emergency_execution: EmergencyExecutionPolicy::default(),
            dynamic_capital_refresh_ms: 1,
            depth_snapshot_limit: 1,
            checkpoint_interval_ms: 1,
            max_stale_ms: 1,
            run_registry_heartbeat_ms: 1,
            runtime_audit_path: "target/test-audit.jsonl".into(),
            m9_calibration_source_label: "F3_m3".into(),
            experiments: vec![ExperimentSpec {
                label: "M1".into(),
                strategy: "m1".into(),
                ablations: vec![],
                role: crate::simulation::experiment_plan::ExperimentRole::Incremental,
                parent_experiment_id: None,
                execution_overlay: "maker_only".into(),
                evidence_policy: "oos_required".into(),
            }],
        };
        assert!(profile.validate().is_err());
    }
}
