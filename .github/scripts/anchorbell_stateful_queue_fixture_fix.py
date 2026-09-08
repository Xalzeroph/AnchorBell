from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
text = RUNTIME.read_text(encoding="utf-8")

# The queue transformer emits Rust raw byte strings. Quotes inside a raw string
# must not be backslash-escaped; otherwise parse_market_message sees literal
# backslashes and returns InvalidJson. Normalize only raw JSON literals that
# actually contain escaped quotes, leaving all existing valid fixtures untouched.
def normalize_raw_json(match: re.Match[str]) -> str:
    body = match.group(1)
    if '\\"' not in body:
        return match.group(0)
    return 'br#"' + body.replace('\\"', '"') + '"#'

text = re.sub(r'br#"(.*?)"#', normalize_raw_json, text, flags=re.DOTALL)

# Stateful FIFO no longer calls MakerQuote/RealisticFillModel::evaluate_after_latency
# from runtime::on_agg_trade; keep the import surface honest.
text = text.replace(
    '    backtest::{MakerQuote, TopOfBook},\n',
    '    backtest::TopOfBook,\n',
)

RUNTIME.write_text(text, encoding="utf-8")
print("stateful queue regression fixtures normalized")
