from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
strategy = ROOT / "engine" / "src" / "strategy"
execution = ROOT / "engine" / "src" / "execution"
# Exchange I/O belongs to the execution adapter. Strategy remains pure and
# communicates through typed intents and snapshots only.
production_roots = [strategy]

# Runtime internals are crate-private implementation details. External
# binaries must consume the root facade so module extraction cannot silently
# expand the public API.
runtime_mod = ROOT / "engine" / "src" / "runtime" / "mod.rs"
runtime_text = runtime_mod.read_text(encoding="utf-8")
for module_name in (
    "audit",
    "channels",
    "control_plane",
    "event_envelope",
    "event_loop",
    "health_reporter",
    "io",
    "reference_authority",
    "run_registry",
    "supervisor",
):
    if re.search(rf"^pub\s+mod\s+{module_name}\s*;", runtime_text, re.MULTILINE):
        raise SystemExit(f"runtime implementation module leaked publicly: {runtime_mod}:{module_name}")
for facade_name in (
    "AuditSink",
    "RuntimeControlPlane",
    "EventEnvelope",
    "RuntimeHealthReporter",
    "load_index_anchor_set",
    "RunRegistry",
    "RuntimeHandles",
):
    if not re.search(rf"pub\s+use[\s\S]*\b{facade_name}\b", runtime_text):
        raise SystemExit(f"runtime facade export missing: {runtime_mod}:{facade_name}")


forbidden_exchange_io = re.compile(
    r"tokio_tungstenite|reqwest|TcpStream|BinanceRestClient|"
    r"BinanceOrderWebSocket|std::net"
)
for path in [p for root in production_roots for p in root.rglob("*.rs")]:
    text = path.read_text(encoding="utf-8", errors="replace")
    match = forbidden_exchange_io.search(text)
    if match:
        raise SystemExit(f"strategy exchange I/O: {path}:{match.group(0)}")

analytics = ROOT / "engine" / "src" / "analytics.rs"
analytics_text = analytics.read_text(encoding="utf-8")
if re.search(r"crate::execution|crate::market::live|tokio_tungstenite|reqwest", analytics_text):
    raise SystemExit(f"analytics execution coupling: {analytics}")
decision_execution = [p for root in (strategy, execution) for p in root.rglob("*.rs")]
legacy_boundary = re.compile(r"crate::(analytics_evidence|analytics_validation|analytics)")
for path in decision_execution:
    text = path.read_text(encoding="utf-8", errors="replace")
    if legacy_boundary.search(text):
        raise SystemExit(f"decision/execution analytics coupling: {path}")

live = ROOT / "engine" / "src" / "bin" / "anchorbell_live.rs"
live_text = live.read_text(encoding="utf-8")
if re.search(r"::simulation::|use anchorbell_engine::simulation\s*::", live_text):
    raise SystemExit(f"live runtime imports simulation facade: {live}")

authority = ROOT / "engine" / "src" / "runtime" / "reference_authority.rs"
# The internal loader is implemented in the simulation engine and exposed only
# through reference_authority. Keep both the current module path and the legacy
# pre-refactor path explicit so this gate follows source layout changes without
# weakening the single-authority invariant for any other caller.
authority_implementation = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
legacy_authority_implementation = ROOT / "engine" / "src" / "simulation_runtime.rs"
for path in (ROOT / "engine" / "src").rglob("*.rs"):
    if path in {authority, authority_implementation, legacy_authority_implementation}:
        continue
    text = path.read_text(encoding="utf-8", errors="replace")
    if "load_index_anchor_set_internal" in text:
        raise SystemExit(f"anchor authority bypass: {path}")

web = ROOT / "engine" / "web"
for path in web.rglob("*"):
    if path.is_file():
        text = path.read_text(encoding="utf-8", errors="replace")
        for term in ("paper", "PAPER", "Paper", "market_legacy_exports", "validation_methods"):
            if term in text:
                raise SystemExit(f"forbidden production vocabulary: {path}:{term}")

platform = ROOT / "engine" / "src" / "platform.rs"
platform_text = platform.read_text(encoding="utf-8")
if "descriptor.layer != PlatformLayer::Control" in platform_text:
    raise SystemExit(f"control layer bypasses strict topology validation: {platform}")
for obsolete in ("control.registry", "control.recovery", "control.console"):
    for path in (ROOT / "engine", ROOT / "docs", ROOT / "scripts"):
        for candidate in path.rglob("*"):
            if candidate == Path(__file__):
                continue
            if candidate.is_file() and candidate.suffix in {".rs", ".md", ".py", ".yml", ".yaml"}:
                if obsolete in candidate.read_text(encoding="utf-8", errors="replace"):
                    raise SystemExit(f"obsolete system identity: {candidate}:{obsolete}")

for path in (ROOT / "engine" / "src" / "bin").glob("*.rs"):
    text = path.read_text(encoding="utf-8", errors="replace")
    if re.search(r"RuntimeHealthReporter[\s\S]{0,400}\.start\(\s*&\[", text):
        raise SystemExit(f"entrypoint owns a manual health system list: {path}")

print("ARCHITECTURE_GATE_PASS")
