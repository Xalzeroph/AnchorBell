from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BATCH = ROOT / "engine" / "src" / "simulation_batch.rs"
REPLAY = ROOT / "engine" / "src" / "simulation" / "replay_config.rs"

batch = BATCH.read_text(encoding="utf-8")
replay = REPLAY.read_text(encoding="utf-8")

old = '''            position_allocations: Some(BTreeMap::new()),
            output_root: PathBuf::from("target/test-stress"),'''
new = '''            position_allocations: Some(BTreeMap::new()),
            portfolio_drawdown_soft_limit_bps: 0,
            portfolio_drawdown_hard_limit_bps: 0,
            output_root: PathBuf::from("target/test-stress"),'''
if old in batch:
    batch = batch.replace(old, new, 1)
elif new not in batch:
    raise SystemExit("missing anchor: stress SimulationBatchConfig fixture")

replay = replay.replace(
    'use crate::{backtest::realism::RealisticFillModel, strategy::CalibrationState};',
    'use crate::strategy::CalibrationState;',
)

BATCH.write_text(batch, encoding="utf-8")
REPLAY.write_text(replay, encoding="utf-8")
print("drawdown fixture and replay import cleanup applied")
