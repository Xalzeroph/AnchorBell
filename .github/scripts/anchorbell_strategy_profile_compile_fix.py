from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
path = ROOT / "engine" / "src" / "simulation_batch.rs"
text = path.read_text(encoding="utf-8")

# The production initializer was migrated by the profile transformer; migrate
# the stress-profile unit fixture as well so tests compile against the same
# provenance contract.
old = '''        let mut config = SimulationBatchConfig {\n            policy_id: "test".to_owned(),\n            environment: BinanceEnvironment::Testnet,\n'''
new = '''        let mut config = SimulationBatchConfig {\n            policy_id: "test".to_owned(),\n            experiment_plan_id: "test-plan".to_owned(),\n            m9_calibration_source_label: "F3_m3".to_owned(),\n            environment: BinanceEnvironment::Testnet,\n'''
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("missing stress-profile SimulationBatchConfig fixture")

# The fixture does not exercise orchestration validation, but the declared M9
# calibration source should still exist in specs to keep future test reuse sane.
old_specs = '''            output_root: PathBuf::from("target/test-stress"),\n            specs: vec![],\n            max_subscriptions_per_shard: 1,\n'''
new_specs = '''            output_root: PathBuf::from("target/test-stress"),\n            specs: vec![SimulationBatchSpec {\n                label: "F3_m3".to_owned(),\n                variant: SimulationPolicyVariant::M3FillAware,\n                ablations: vec![],\n            }],\n            max_subscriptions_per_shard: 1,\n'''
if old_specs in text:
    text = text.replace(old_specs, new_specs, 1)
elif new_specs not in text:
    raise SystemExit("missing stress-profile fixture specs")

# Remove a duplicated JSON key introduced by the first transformer. Keeping
# parameter lineage canonical matters because this object is hashed.
duplicate = '''        "checkpoint_interval_ms": config.checkpoint_interval_ms,\n        "entry_threshold_bps": config.entry_threshold_bps,\n        "threshold_scale_ppm": config.threshold_scale_ppm,\n'''
canonical = '''        "checkpoint_interval_ms": config.checkpoint_interval_ms,\n        "threshold_scale_ppm": config.threshold_scale_ppm,\n'''
if duplicate in text:
    text = text.replace(duplicate, canonical, 1)
elif canonical not in text:
    raise SystemExit("missing parameter lineage checkpoint anchor")

path.write_text(text, encoding="utf-8")
print("strategy-profile fixture migration complete")
