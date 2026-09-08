from pathlib import Path

path = Path(__file__).resolve().parents[2] / "engine" / "src" / "simulation_batch.rs"
text = path.read_text(encoding="utf-8")
old = "risk_history: VecDeque::with_capacity(RISK_HISTORY_CAPACITY),"
new = "risk_history: VecDeque::new(),"
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("risk history allocation anchor missing")
path.write_text(text, encoding="utf-8")
print("risk history allocation ownership repair complete")
