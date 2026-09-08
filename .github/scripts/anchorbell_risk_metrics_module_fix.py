from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
SIMULATION = ROOT / "engine" / "src" / "simulation.rs"
TARGET = ROOT / "engine" / "src" / "simulation" / "risk_metrics.rs"


def brace_block(text: str, start: int) -> tuple[int, int]:
    open_brace = text.index("{", start)
    depth = 0
    for index in range(open_brace, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                while end < len(text) and text[end] in " \t":
                    end += 1
                if end < len(text) and text[end] == "\n":
                    end += 1
                return start, end
    raise SystemExit("unterminated Rust item")


runtime = RUNTIME.read_text(encoding="utf-8")
simulation = SIMULATION.read_text(encoding="utf-8")

runtime_import = (
    "pub use super::risk_metrics::RiskMetrics;\n"
    "use super::risk_metrics::{calculate_risk_metrics, RISK_SAMPLE_INTERVAL_MS};\n"
)
module_decl = '#[path = "simulation/risk_metrics.rs"]\npub mod risk_metrics;\n'

# Idempotent validation path for future diagnostic pushes.
if TARGET.exists() and runtime_import in runtime and module_decl in simulation:
    forbidden = [
        "pub struct RiskMetrics {",
        "fn calculate_risk_metrics(points:",
        "const MIN_RISK_RETURN_SAMPLES",
        "const RISK_SAMPLE_INTERVAL_MS",
    ]
    if any(token in runtime for token in forbidden):
        raise SystemExit("risk-metrics extraction is only partially applied")
    print("risk metrics already extracted")
    raise SystemExit(0)

if TARGET.exists():
    raise SystemExit("risk_metrics.rs exists without the expected runtime wiring")

struct_token = "pub struct RiskMetrics {"
struct_pos = runtime.find(struct_token)
if struct_pos < 0:
    raise SystemExit("missing RiskMetrics struct")
derive_pos = runtime.rfind("#[derive(", 0, struct_pos)
if derive_pos < 0:
    raise SystemExit("missing RiskMetrics derive")
struct_start, struct_end = brace_block(runtime, derive_pos)
struct_block = runtime[struct_start:struct_end]
runtime = runtime[:struct_start] + runtime[struct_end:]

function_token = "fn calculate_risk_metrics(points:"
function_pos = runtime.find(function_token)
if function_pos < 0:
    raise SystemExit("missing calculate_risk_metrics")
function_start, function_end = brace_block(runtime, function_pos)
function_block = runtime[function_start:function_end]
runtime = runtime[:function_start] + runtime[function_end:]

interval_line = "const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;\n"
minimum_line = "const MIN_RISK_RETURN_SAMPLES: usize = 30;\n"
for line, label in ((interval_line, "risk sample interval"), (minimum_line, "minimum risk samples")):
    if runtime.count(line) != 1:
        raise SystemExit(f"expected one {label} constant")
    runtime = runtime.replace(line, "", 1)

insert_after = "use tokio::sync::mpsc;\n\n"
if runtime.count(insert_after) != 1:
    raise SystemExit("missing runtime import insertion point")
runtime = runtime.replace(insert_after, insert_after + runtime_import + "\n", 1)

simulation_anchor = '#[path = "simulation/replay.rs"]\npub mod replay;\n'
if module_decl not in simulation:
    if simulation.count(simulation_anchor) != 1:
        raise SystemExit("missing simulation module insertion point")
    simulation = simulation.replace(simulation_anchor, simulation_anchor + module_decl, 1)

function_block = function_block.replace(
    "fn calculate_risk_metrics(points:",
    "pub(crate) fn calculate_risk_metrics(points:",
    1,
)
target = (
    "//! Risk statistics for simulation and deterministic replay.\n"
    "//!\n"
    "//! Kept outside the execution runtime so statistical accounting can evolve\n"
    "//! without expanding the latency-sensitive simulation orchestration module.\n\n"
    "use serde::Serialize;\n\n"
    "pub(crate) const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;\n"
    "const MIN_RISK_RETURN_SAMPLES: usize = 30;\n\n"
    + struct_block
    + "\n"
    + function_block
)

RUNTIME.write_text(runtime, encoding="utf-8")
SIMULATION.write_text(simulation, encoding="utf-8")
TARGET.write_text(target, encoding="utf-8")
print(
    "simulation risk metrics extracted: "
    f"runtime={len(runtime.encode('utf-8'))} bytes, "
    f"risk_metrics={len(target.encode('utf-8'))} bytes"
)
