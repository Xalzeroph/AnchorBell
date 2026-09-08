from pathlib import Path

root = Path(__file__).resolve().parents[2]
path = root / "engine" / "src" / "simulation" / "runtime.rs"
text = path.read_text(encoding="utf-8")
old = '''//! Shared simulation-trading and replay execution engine.
//!
//! The simulation path consumes the same Binance bookTicker, markPrice, and
//! aggregate-trade events as the live adapter.  A passive order is filled only
//! when a public aggregate trade is at the order price and its aggressor side
//! is compatible with the order.  No bar-only shortcut is used here.
'''
new = '''//! Shared causal maker-only simulation/replay execution engine; see architecture docs.
'''
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("runtime module-doc anchor not found")
path.write_text(text, encoding="utf-8")
print(f"runtime module docs compacted: {len(text.encode('utf-8'))} bytes")
