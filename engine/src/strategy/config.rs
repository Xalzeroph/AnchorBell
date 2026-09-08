use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::{
    execution::BinanceEnvironment,
    simulation::experiment_plan::{ExperimentPlan, ExperimentSpec},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StrategyProfile {
    pub schema_version: u16,
    pub policy_id: String,
    pub environment: String,
    pub index_anchors: bool,
    pub symbols: Vec<String>,
    pub output_root: PathBuf,
    pub capital_usdt: String,
    pub duration_secs: u64,
    pub entry_threshold_bps: i64,
    pub threshold_scale_ppm: i64,
    #[serde(default)]
    pub portfolio_drawdown_soft_limit_bps: i64,
    #[serde(default)]
    pub portfolio_drawdown_hard_limit_bps: i64,
    pub max_position: i64,
    pub requested_quantity: i64,
    pub max_mark_index_gap_bps: i64,
    pub max_anchor_age_ms: u64,
    pub fee_ppm: i64,
    pub quantity_scale: u32,
    pub price_scale: u32,
    pub queue_ahead: i64,
    pub trade_through: i64,
    pub market_to_decision_ms: u64,
    pub decision_to_exchange_ms: u64,
    pub cancel_to_exchange_ms: u64,
    pub quote_reprice_min_interval_ms: u64,
    pub dynamic_capital_refresh_ms: u64,
    pub depth_snapshot_limit: usize,
    pub checkpoint_interval_ms: u64,
    pub max_subscriptions_per_shard: usize,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub metrics_refresh_ms: u64,
    pub index_anchor_refresh_ms: u64,
    pub fx_refresh_ms: u64,
    pub fx_max_age_ms: u64,
    pub m9_calibration_source_label: String,
    #[serde(default)]
    pub validation_fold_id: Option<String>,
    #[serde(default)]
    pub validation_stress_profile: Option<String>,
    pub experiments: Vec<ExperimentSpec>,
}

impl StrategyProfile {
    pub const SCHEMA_VERSION: u16 = 2;

    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read strategy profile {}: {error}", path.display()))?;
        let profile = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| format!("invalid strategy profile {}: {error}", path.display()))?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn environment(&self) -> Result<BinanceEnvironment, String> {
        BinanceEnvironment::from_str(&self.environment)
            .map_err(|_| format!("unsupported profile environment {}", self.environment))
    }

    pub fn capital_usdt_ticks(&self) -> Result<i64, String> {
        parse_positive_decimal_ticks(&self.capital_usdt, 8)
    }

    pub fn experiment_plan(&self) -> Result<ExperimentPlan, String> {
        let plan = ExperimentPlan {
            schema_version: ExperimentPlan::SCHEMA_VERSION,
            plan_id: format!("strategy-profile:{}", self.policy_id),
            experiments: self.experiments.clone(),
        };
        plan.runtime_specs_with_ablations()
            .map_err(|error| format!("invalid profile experiment plan: {error}"))?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(format!(
                "unsupported strategy profile schema {}; expected {}",
                self.schema_version,
                Self::SCHEMA_VERSION
            ));
        }
        if self.policy_id.trim().is_empty() {
            return Err("strategy profile policy_id cannot be empty".to_owned());
        }
        self.environment()?;
        if !self.index_anchors {
            return Err("strategy profile must use live index anchors".to_owned());
        }
        if self.symbols.is_empty() {
            return Err("strategy profile symbols cannot be empty".to_owned());
        }
        let mut symbols = BTreeSet::new();
        for symbol in &self.symbols {
            let normalized = symbol.trim().to_ascii_uppercase();
            if normalized.is_empty()
                || !normalized.bytes().all(|byte| byte.is_ascii_alphanumeric())
                || !symbols.insert(normalized)
            {
                return Err("strategy profile symbols must be unique ASCII symbols".to_owned());
            }
        }
        if self.output_root.as_os_str().is_empty() || self.capital_usdt_ticks()? <= 0 {
            return Err("strategy profile requires output_root and positive capital".to_owned());
        }
        if self.entry_threshold_bps < 0
            || !(1..=1_000_000).contains(&self.threshold_scale_ppm)
            || self.max_position <= 0
            || self.requested_quantity <= 0
            || self.max_mark_index_gap_bps < 0
            || self.fee_ppm < 0
            || self.quantity_scale > 18
            || self.price_scale > 18
            || self.queue_ahead < 0
            || self.trade_through < 0
            || self.depth_snapshot_limit == 0
            || self.checkpoint_interval_ms == 0
            || self.max_subscriptions_per_shard == 0
            || self.connect_timeout_ms == 0
            || self.read_timeout_ms == 0
            || self.metrics_refresh_ms == 0
            || self.fx_refresh_ms == 0
            || self.fx_max_age_ms == 0
            || self.dynamic_capital_refresh_ms == 0
        {
            return Err("strategy profile contains invalid runtime limits".to_owned());
        }
        if self.index_anchor_refresh_ms == 0 {
            return Err("strategy profile index-anchor refresh must be enabled".to_owned());
        }
        let drawdown_disabled = self.portfolio_drawdown_soft_limit_bps == 0
            && self.portfolio_drawdown_hard_limit_bps == 0;
        let drawdown_valid = self.portfolio_drawdown_soft_limit_bps > 0
            && self.portfolio_drawdown_hard_limit_bps > self.portfolio_drawdown_soft_limit_bps
            && self.portfolio_drawdown_hard_limit_bps <= 10_000;
        if !drawdown_disabled && !drawdown_valid {
            return Err(
                "portfolio drawdown limits require 0/0 or 0 < soft < hard <= 10000 bps".to_owned(),
            );
        }
        if self.experiments.is_empty() {
            return Err("strategy profile experiments cannot be empty".to_owned());
        }
        let plan = self.experiment_plan()?;
        for experiment in &plan.experiments {
            let mut ablations = BTreeSet::new();
            for ablation in &experiment.ablations {
                if !ablations.insert(ablation.as_str()) {
                    return Err(format!(
                        "duplicate ablation {ablation} for {}",
                        experiment.label
                    ));
                }
                if ablation != "funding" || experiment.strategy != "m8" {
                    return Err(format!(
                        "unsupported ablation {ablation} for strategy {}",
                        experiment.strategy
                    ));
                }
            }
        }
        if self.m9_calibration_source_label.trim().is_empty() {
            return Err("m9_calibration_source_label cannot be empty".to_owned());
        }
        if plan
            .experiments
            .iter()
            .any(|experiment| experiment.strategy == "m9")
        {
            let source = plan
                .experiments
                .iter()
                .find(|experiment| experiment.label == self.m9_calibration_source_label)
                .ok_or_else(|| "M9 calibration source is not present in experiments".to_owned())?;
            if !matches!(
                source.strategy.as_str(),
                "m3" | "m4" | "m5" | "m6" | "m7" | "m8"
            ) || !source.ablations.is_empty()
            {
                return Err(
                    "M9 calibration source must be an unablated fill-aware M3-M8 candidate"
                        .to_owned(),
                );
            }
        }
        match (
            self.validation_fold_id.as_deref(),
            self.validation_stress_profile.as_deref(),
        ) {
            (None, Some(_)) => {
                return Err("validation stress profile requires validation_fold_id".to_owned())
            }
            (Some(fold_id), _) if fold_id.trim().is_empty() => {
                return Err("validation_fold_id cannot be empty".to_owned())
            }
            (Some(_), _) if self.duration_secs == 0 => {
                return Err("validation folds require finite duration_secs".to_owned())
            }
            _ => {}
        }
        Ok(())
    }
}

fn parse_positive_decimal_ticks(value: &str, scale: u32) -> Result<i64, String> {
    let value = value.trim();
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty()
        || fraction.len() > scale as usize
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("capital_usdt must be a positive decimal".to_owned());
    }
    let unit = 10_i128.pow(scale);
    let whole = whole
        .parse::<i128>()
        .map_err(|_| "capital_usdt overflows".to_owned())?;
    let fraction_ticks = if fraction.is_empty() {
        0
    } else {
        let fraction_value = fraction
            .parse::<i128>()
            .map_err(|_| "capital_usdt overflows".to_owned())?;
        fraction_value
            .checked_mul(10_i128.pow(scale - fraction.len() as u32))
            .ok_or_else(|| "capital_usdt overflows".to_owned())?
    };
    let ticks = whole
        .checked_mul(unit)
        .and_then(|value| value.checked_add(fraction_ticks))
        .ok_or_else(|| "capital_usdt overflows".to_owned())?;
    let ticks = i64::try_from(ticks).map_err(|_| "capital_usdt overflows".to_owned())?;
    if ticks <= 0 {
        return Err("capital_usdt must be positive".to_owned());
    }
    Ok(ticks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shipped_profile() -> StrategyProfile {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/anchorbell-simulation.json");
        StrategyProfile::load(&path).unwrap()
    }

    #[test]
    fn shipped_profile_is_valid_and_preserves_the_true_m8_ablation() {
        let profile = shipped_profile();
        assert_eq!(profile.schema_version, StrategyProfile::SCHEMA_VERSION);
        assert_eq!(profile.capital_usdt_ticks().unwrap(), 1500 * 100_000_000);
        let no_funding = profile
            .experiments
            .iter()
            .find(|experiment| experiment.label == "M8_no_funding")
            .unwrap();
        assert_eq!(no_funding.strategy, "m8");
        assert_eq!(no_funding.ablations, vec!["funding".to_owned()]);
    }

    #[test]
    fn portfolio_drawdown_limits_are_backward_compatible_and_validated() {
        let mut profile = shipped_profile();
        assert_eq!(profile.portfolio_drawdown_soft_limit_bps, 0);
        assert_eq!(profile.portfolio_drawdown_hard_limit_bps, 0);
        profile.portfolio_drawdown_soft_limit_bps = 500;
        assert!(profile.validate().is_err());
        profile.portfolio_drawdown_hard_limit_bps = 1_000;
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn funding_ablation_cannot_be_mislabelled_as_m7() {
        let mut profile = shipped_profile();
        profile
            .experiments
            .iter_mut()
            .find(|experiment| experiment.label == "M8_no_funding")
            .unwrap()
            .strategy = "m7".to_owned();
        assert!(profile.validate().is_err());
    }

    #[test]
    fn m9_source_rejects_pre_fill_aware_candidates() {
        let mut profile = shipped_profile();
        profile.m9_calibration_source_label = "F1_m1".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "F2_m2".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "F3_m3".to_owned();
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn m9_source_must_resolve_to_an_unablated_non_m9_candidate() {
        let mut profile = shipped_profile();
        profile.m9_calibration_source_label = "M9_full".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "missing".to_owned();
        assert!(profile.validate().is_err());
    }
}
