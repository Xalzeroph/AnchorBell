from pathlib import Path

path = Path(__file__).resolve().parents[2] / "engine" / "src" / "bin" / "anchorbell_simulation_batch.rs"
text = path.read_text(encoding="utf-8")

replacements = [
    ("execution::{BinanceEnvironment, SessionCheckpoint}", "execution::SessionCheckpoint"),
    ("parse_args().unwrap_or_else(fail)", "parse_args().unwrap_or_else(|error| fail(error))"),
    (
        "StrategyProfile::load(&args.strategy_profile).unwrap_or_else(fail)",
        "StrategyProfile::load(&args.strategy_profile).unwrap_or_else(|error| fail(error))",
    ),
    ("profile.environment().unwrap_or_else(fail)", "profile.environment().unwrap_or_else(|error| fail(error))"),
    (
        "profile.capital_usdt_ticks().unwrap_or_else(fail)",
        "profile.capital_usdt_ticks().unwrap_or_else(|error| fail(error))",
    ),
    (
        "profile.experiment_plan().unwrap_or_else(fail)",
        "profile.experiment_plan().unwrap_or_else(|error| fail(error))",
    ),
    (
        "claim_single_simulation_batch_instance().unwrap_or_else(fail)",
        "claim_single_simulation_batch_instance().unwrap_or_else(|error| fail(error))",
    ),
]

for old, new in replacements:
    if old in text:
        text = text.replace(old, new, 1)
    elif new not in text:
        raise SystemExit(f"missing compile-fix anchor: {old}")

path.write_text(text, encoding="utf-8")
print("strategy-profile Rust 1.98 inference repair complete")
