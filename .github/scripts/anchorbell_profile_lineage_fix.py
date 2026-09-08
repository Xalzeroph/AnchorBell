from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


# 1) Strategy-profile layer: an M9 warm-start source must actually own the
# fill-aware calibration components. M1/M2 are not valid calibration parents.
config_path = ROOT / "engine" / "src" / "strategy" / "config.rs"
config = config_path.read_text(encoding="utf-8")
old = '''            if source.strategy == "m9" || !source.ablations.is_empty() {
                return Err(
                    "M9 calibration source must be a non-M9, non-ablated candidate".to_owned(),
                );
            }
'''
new = '''            if !matches!(source.strategy.as_str(), "m3" | "m4" | "m5" | "m6" | "m7" | "m8")
                || !source.ablations.is_empty()
            {
                return Err(
                    "M9 calibration source must be an unablated fill-aware M3-M8 candidate"
                        .to_owned(),
                );
            }
'''
config = replace_once(config, old, new, "profile M9 fill-aware source")

if "m9_source_rejects_pre_fill_aware_candidates" not in config:
    anchor = '''    #[test]
    fn m9_source_must_resolve_to_an_unablated_non_m9_candidate() {
'''
    test = '''    #[test]
    fn m9_source_rejects_pre_fill_aware_candidates() {
        let mut profile = shipped_profile();
        profile.m9_calibration_source_label = "F1_m1".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "F2_m2".to_owned();
        assert!(profile.validate().is_err());
        profile.m9_calibration_source_label = "F3_m3".to_owned();
        assert!(profile.validate().is_ok());
    }

'''
    config = replace_once(config, anchor, test + anchor, "profile M9 source regression test")
config_path.write_text(config, encoding="utf-8")


# 2) Batch layer repeats the invariant so programmatic callers cannot bypass
# StrategyProfile validation.
batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")
old = '''        if source.variant == SimulationPolicyVariant::M9DeadlineCausalDroMpc
            || !source.ablations.is_empty()
        {
            return Err(SimulationError::InvalidConfig(
                "M9 calibration source must be non-M9 and non-ablated",
            ));
        }
'''
new = '''        if source.variant < SimulationPolicyVariant::M3FillAware
            || source.variant > SimulationPolicyVariant::M8FundingAware
            || !source.ablations.is_empty()
        {
            return Err(SimulationError::InvalidConfig(
                "M9 calibration source must be an unablated fill-aware M3-M8 ledger",
            ));
        }
'''
batch = replace_once(batch, old, new, "batch M9 fill-aware source")

# Parameter lineage must change whenever the candidate matrix or any execution,
# sizing, freshness, transport, persistence, validation, or evidence assumption
# changes. This prevents incomparable runs from sharing a parameter identity.
old = '''    let parameter_material = serde_json::json!({
        "policy_id": config.policy_id,
        "m9_calibration_source_label": config.m9_calibration_source_label,
        "entry_threshold_bps": config.entry_threshold_bps,
        "threshold_scale_ppm": config.threshold_scale_ppm,
        "fee_ppm": config.fee_ppm,
        "queue_ahead": config.queue_ahead,
        "trade_through": config.trade_through,
        "market_to_decision_ms": config.market_to_decision_ms,
        "decision_to_exchange_ms": config.decision_to_exchange_ms,
        "cancel_to_exchange_ms": config.cancel_to_exchange_ms,
        "dynamic_capital_refresh_ms": config.dynamic_capital_refresh_ms,
        "depth_snapshot_limit": config.depth_snapshot_limit,
        "duration_secs": config.duration_secs,
        "risk_history_window_ms": RISK_HISTORY_WINDOW_MS,
    });
'''
new = '''    let parameter_material = serde_json::json!({
        "policy_id": config.policy_id,
        "m9_calibration_source_label": config.m9_calibration_source_label,
        "specs": config.specs.iter().map(|spec| serde_json::json!({
            "label": spec.label,
            "strategy_variant": spec.variant.label(),
            "ablations": spec.ablations,
        })).collect::<Vec<_>>(),
        "entry_threshold_bps": config.entry_threshold_bps,
        "threshold_scale_ppm": config.threshold_scale_ppm,
        "max_position": config.max_position,
        "requested_quantity": config.requested_quantity,
        "max_mark_index_gap_bps": config.max_mark_index_gap_bps,
        "max_anchor_age_ms": config.max_anchor_age_ms,
        "fee_ppm": config.fee_ppm,
        "quantity_scale": config.quantity_scale,
        "price_scale": config.price_scale,
        "max_subscriptions_per_shard": config.max_subscriptions_per_shard,
        "connect_timeout_ms": config.connect_timeout_ms,
        "read_timeout_ms": config.read_timeout_ms,
        "metrics_refresh_ms": config.metrics_refresh_ms,
        "index_anchor_refresh_ms": config.index_anchor_refresh_ms,
        "fx_refresh_ms": config.fx_refresh_ms,
        "fx_max_age_ms": config.fx_max_age_ms,
        "queue_ahead": config.queue_ahead,
        "trade_through": config.trade_through,
        "market_to_decision_ms": config.market_to_decision_ms,
        "decision_to_exchange_ms": config.decision_to_exchange_ms,
        "cancel_to_exchange_ms": config.cancel_to_exchange_ms,
        "quote_reprice_min_interval_ms": config.quote_reprice_min_interval_ms,
        "dynamic_capital_refresh_ms": config.dynamic_capital_refresh_ms,
        "depth_snapshot_limit": config.depth_snapshot_limit,
        "checkpoint_interval_ms": config.checkpoint_interval_ms,
        "duration_secs": config.duration_secs,
        "validation_fold_id": config.validation_fold_id,
        "validation_stress_profile": config.validation_stress_profile,
        "risk_history_window_ms": RISK_HISTORY_WINDOW_MS,
        "evidence": config.evidence,
    });
'''
batch = replace_once(batch, old, new, "complete parameter lineage")

# Make the exact matrix visible in the manifest as one structured object as well
# as the existing parallel diagnostic arrays.
old = '''        "m9_calibration_source_label": config.m9_calibration_source_label,
        "created_at_ms": manifest_created_at_ms,
'''
new = '''        "m9_calibration_source_label": config.m9_calibration_source_label,
        "experiment_specs": config.specs.iter().map(|spec| serde_json::json!({
            "label": spec.label,
            "strategy_variant": spec.variant.label(),
            "ablations": spec.ablations,
        })).collect::<Vec<_>>(),
        "created_at_ms": manifest_created_at_ms,
'''
batch = replace_once(batch, old, new, "structured experiment manifest")

batch_path.write_text(batch, encoding="utf-8")
print("profile lineage and M9 calibration-source audit repair complete")
