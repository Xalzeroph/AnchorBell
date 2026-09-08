from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
SIMULATION = ROOT / "engine" / "src" / "simulation.rs"
OUT = ROOT / "engine" / "src" / "simulation" / "replay_config.rs"

runtime = RUNTIME.read_text(encoding="utf-8")
simulation = SIMULATION.read_text(encoding="utf-8")

if OUT.exists() and "pub use super::replay_config::ReplayConfig;" in runtime:
    print("ReplayConfig already extracted")
    raise SystemExit(0)

start_marker = "#[derive(Debug, Clone)]\npub struct ReplayConfig {"
end_marker = "#[derive(Debug, Clone, Serialize)]\npub struct ReplayEvaluation"
start = runtime.find(start_marker)
end = runtime.find(end_marker, start)
if start < 0 or end < 0:
    raise SystemExit("cannot locate ReplayConfig extraction boundaries")

block = runtime[start:end].rstrip() + "\n"
runtime = runtime[:start] + "pub use super::replay_config::ReplayConfig;\n\n" + runtime[end:]

module = '''//! Auditable replay configuration kept outside the simulation runtime state machine.\n\nuse std::collections::BTreeMap;\n\nuse crate::strategy::CalibrationState;\n\nuse super::runtime::SimulationPolicyVariant;\n\n''' + block

module_anchor = '#[path = "simulation/replay.rs"]\npub mod replay;\n'
module_line = module_anchor + '#[path = "simulation/replay_config.rs"]\npub mod replay_config;\n'
if '#[path = "simulation/replay_config.rs"]' not in simulation:
    if module_anchor not in simulation:
        raise SystemExit("cannot locate simulation replay module anchor")
    simulation = simulation.replace(module_anchor, module_line, 1)

RUNTIME.write_text(runtime, encoding="utf-8")
SIMULATION.write_text(simulation, encoding="utf-8")
OUT.write_text(module, encoding="utf-8")
print(
    "ReplayConfig extracted: "
    f"runtime={len(runtime.encode('utf-8'))}, config={len(module.encode('utf-8'))} bytes"
)
