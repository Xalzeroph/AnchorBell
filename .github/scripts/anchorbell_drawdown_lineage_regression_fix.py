from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "engine" / "src" / "simulation_batch.rs"
text = PATH.read_text(encoding="utf-8")

if (
    'fn portfolio_drawdown_lineage(' in text
    and 'fn parameter_material(' in text
    and 'fn parameter_digest(' in text
    and 'portfolio_drawdown_changes_parameter_digest_and_lineage' in text
):
    print("drawdown lineage regression already present")
    raise SystemExit(0)

# Extract the production parameter JSON into a reusable helper so tests exercise
# the exact material that run() hashes.
start_marker = '    let parameter_material = serde_json::json!({\n'
end_marker = '    let parameter_bytes = serde_json::to_vec(&parameter_material)\n'
start = text.find(start_marker)
end = text.find(end_marker, start)
if start < 0 or end < 0:
    raise SystemExit("cannot locate parameter material block")

json_start = start + len('    let parameter_material = ')
json_end = text.find('    });\n', json_start)
if json_end < 0 or json_end > end:
    raise SystemExit("cannot locate parameter material terminator")
json_expr = text[json_start:json_end + len('    })')]

# Deduplicate the direct drawdown keys and replace them with one canonical nested
# lineage object. The same helper will also be written to run-manifest.json.
pair = (
    '        "portfolio_drawdown_soft_limit_bps": config.portfolio_drawdown_soft_limit_bps,\n'
    '        "portfolio_drawdown_hard_limit_bps": config.portfolio_drawdown_hard_limit_bps,\n'
)
count = json_expr.count(pair)
if count != 2:
    raise SystemExit(f"expected two duplicate drawdown pairs, found {count}")
json_expr = json_expr.replace(pair, '        "portfolio_drawdown": portfolio_drawdown_lineage(config),\n', 1)
json_expr = json_expr.replace(pair, '', 1)

# Normalize indentation from the in-run expression to a top-level helper body.
json_expr = '\n'.join(line[4:] if line.startswith('    ') else line for line in json_expr.splitlines())
helper = f'''fn portfolio_drawdown_lineage(config: &SimulationBatchConfig) -> serde_json::Value {{
    serde_json::json!({{
        "soft_limit_bps": config.portfolio_drawdown_soft_limit_bps,
        "hard_limit_bps": config.portfolio_drawdown_hard_limit_bps,
    }})
}}

fn parameter_material(config: &SimulationBatchConfig) -> serde_json::Value {{
    {json_expr}
}}

fn parameter_digest(config: &SimulationBatchConfig) -> Result<String, SimulationError> {{
    let bytes = serde_json::to_vec(&parameter_material(config))
        .map_err(|_| SimulationError::InvalidConfig("cannot encode parameter digest"))?;
    Ok(format!("sha256:{{}}", hex::encode(Sha256::digest(bytes))))
}}

'''
insert_at = text.find('pub async fn run(')
if insert_at < 0:
    raise SystemExit("cannot locate run()")
text = text[:insert_at] + helper + text[insert_at:]

# Replace the original inline material + hash block with the production helper.
start = text.find(start_marker, insert_at + len(helper))
if start < 0:
    raise SystemExit("cannot relocate inline parameter material")
end_marker2 = '    let data_material = serde_json::json!({\n'
end = text.find(end_marker2, start)
if end < 0:
    raise SystemExit("cannot locate data material after parameter digest")
text = text[:start] + '    let parameter_digest = parameter_digest(&config)?;\n' + text[end:]

# Put the same canonical object in the top-level run manifest.
manifest_anchor = '        "policy_id": config.policy_id,\n        "m9_calibration_source_label": config.m9_calibration_source_label,\n'
manifest_replacement = (
    '        "policy_id": config.policy_id,\n'
    '        "portfolio_drawdown": portfolio_drawdown_lineage(&config),\n'
    '        "m9_calibration_source_label": config.m9_calibration_source_label,\n'
)
if manifest_anchor not in text:
    raise SystemExit("cannot locate manifest policy lineage anchor")
text = text.replace(manifest_anchor, manifest_replacement, 1)

# Reuse the existing comprehensive fixture instead of duplicating a large config.
fixture_start = text.find('    #[test]\n    fn execution_adverse_stress_profile_changes_economics_and_latency() {\n        let mut config = SimulationBatchConfig {')
if fixture_start < 0:
    raise SystemExit("cannot locate simulation batch test fixture")
struct_start = text.find('        let mut config = SimulationBatchConfig {', fixture_start)
struct_end = text.find('        };\n        apply_validation_stress_profile', struct_start)
if struct_end < 0:
    raise SystemExit("cannot locate simulation batch fixture end")
struct_literal = text[struct_start + len('        let mut config = '):struct_end + len('        }')]
fixture_helper = '    fn test_config() -> SimulationBatchConfig {\n        ' + struct_literal.replace('\n', '\n        ') + '\n    }\n\n'
text = text[:fixture_start] + fixture_helper + text[fixture_start:]
text = text.replace(
    '    fn execution_adverse_stress_profile_changes_economics_and_latency() {\n        let mut config = SimulationBatchConfig {' + struct_literal.split('SimulationBatchConfig {',1)[1] + ';\n',
    '    fn execution_adverse_stress_profile_changes_economics_and_latency() {\n        let mut config = test_config();\n',
    1,
)
# The exact replacement above is intentionally strict; fall back to bounded slice if formatting differed.
remaining_start = text.find('    fn execution_adverse_stress_profile_changes_economics_and_latency() {\n        let mut config = SimulationBatchConfig {')
if remaining_start >= 0:
    s = text.find('        let mut config = SimulationBatchConfig {', remaining_start)
    e = text.find('        };\n        apply_validation_stress_profile', s)
    if e < 0:
        raise SystemExit("cannot replace duplicated fixture")
    text = text[:s] + '        let mut config = test_config();\n' + text[e + len('        };\n'):]

new_test_anchor = '    #[test]\n    fn candidate_identity_normalizes_ablation_order_and_ignores_label() {'
new_test = '''    #[test]
    fn portfolio_drawdown_changes_parameter_digest_and_lineage() {
        let mut config = test_config();
        config.validation_stress_profile = None;
        config.validation_fold_id = None;
        let disabled_digest = parameter_digest(&config).unwrap();
        let disabled = portfolio_drawdown_lineage(&config);
        assert_eq!(disabled["soft_limit_bps"], 0);
        assert_eq!(disabled["hard_limit_bps"], 0);

        config.portfolio_drawdown_soft_limit_bps = 500;
        config.portfolio_drawdown_hard_limit_bps = 1_000;
        let enabled_digest = parameter_digest(&config).unwrap();
        let enabled = portfolio_drawdown_lineage(&config);
        assert_ne!(disabled_digest, enabled_digest);
        assert_eq!(enabled["soft_limit_bps"], 500);
        assert_eq!(enabled["hard_limit_bps"], 1_000);
    }

    #[test]
    fn candidate_identity_normalizes_ablation_order_and_ignores_label() {'''
if new_test_anchor not in text:
    raise SystemExit("cannot locate test insertion anchor")
text = text.replace(new_test_anchor, new_test, 1)

# Tests need the production helper functions in scope.
use_old = '''        apply_validation_stress_profile, calibration_key, candidate_identity,
        SimulationBatchConfig, SimulationBatchSpec,
'''
use_new = '''        apply_validation_stress_profile, calibration_key, candidate_identity, parameter_digest,
        portfolio_drawdown_lineage, SimulationBatchConfig, SimulationBatchSpec,
'''
if use_old not in text:
    raise SystemExit("cannot locate test imports")
text = text.replace(use_old, use_new, 1)

PATH.write_text(text, encoding="utf-8")
print("drawdown lineage/digest regression transform applied")
