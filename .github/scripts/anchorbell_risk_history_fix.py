from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        print(f"already repaired: {label}")
        return text
    raise SystemExit(f"missing anchor: {label}")


# Risk statistics must use one coherent observation window. Store independent
# 30-second points rather than one point per metrics tick, retain seven days,
# and normalize all return/drawdown statistics to the window's first PnL.
runtime_path = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
runtime = runtime_path.read_text(encoding="utf-8")

old_constants = '''const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_HISTORY_CAPACITY: usize = 7_201;
const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;
const MIN_RISK_RETURN_SAMPLES: usize = 30;
'''
new_constants = '''const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;
const RISK_HISTORY_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const RISK_HISTORY_CAPACITY: usize =
    (RISK_HISTORY_RETENTION_MS / RISK_SAMPLE_INTERVAL_MS) as usize + 2;
const MIN_RISK_RETURN_SAMPLES: usize = 30;
'''
runtime = replace_once(runtime, old_constants, new_constants, "runtime risk constants")

helper_anchor = '''fn calculate_risk_metrics(points: &[(u64, i64)], capital_ticks: i64) -> RiskMetrics {
'''
helper = '''fn push_risk_history_point(
    history: &mut VecDeque<PerformancePoint>,
    point: PerformancePoint,
    force_endpoint: bool,
) {
    if history
        .back()
        .is_some_and(|last| last.observed_at_ms == point.observed_at_ms)
    {
        history.pop_back();
        history.push_back(point);
    } else if force_endpoint
        || history.back().is_none_or(|last| {
            point.observed_at_ms.saturating_sub(last.observed_at_ms) >= RISK_SAMPLE_INTERVAL_MS
        })
    {
        history.push_back(point);
    }
    while history.len() > RISK_HISTORY_CAPACITY {
        history.pop_front();
    }
}

'''
if "fn push_risk_history_point(" not in runtime:
    runtime = replace_once(runtime, helper_anchor, helper + helper_anchor, "runtime risk history helper")

old_tick = '''                        performance_history.push_back(point.clone());
                        risk_history.push_back(point);
                        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
                            performance_history.pop_front();
                        }
                        while risk_history.len() > RISK_HISTORY_CAPACITY {
                            risk_history.pop_front();
                        }
'''
new_tick = '''                        performance_history.push_back(point.clone());
                        push_risk_history_point(&mut risk_history, point, false);
                        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
                            performance_history.pop_front();
                        }
'''
runtime = replace_once(runtime, old_tick, new_tick, "runtime periodic risk sampling")

old_final = '''        performance_history.push_back(point.clone());
        risk_history.push_back(point);
        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
            performance_history.pop_front();
        }
        while risk_history.len() > RISK_HISTORY_CAPACITY {
            risk_history.pop_front();
        }
'''
new_final = '''        performance_history.push_back(point.clone());
        push_risk_history_point(&mut risk_history, point, true);
        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
            performance_history.pop_front();
        }
'''
runtime = replace_once(runtime, old_final, new_final, "runtime final risk endpoint")

old_baseline = '''    let capital = capital_ticks.max(1) as f64;
    let total_return_pct = points
        .last()
        .map(|(_, pnl)| (*pnl as f64 / capital) * 100.0)
        .unwrap_or(0.0);
    let mut max_drawdown_pct = 0.0_f64;
    let mut peak_equity = 1.0_f64;
    for (_, pnl) in points {
        let equity = 1.0 + (*pnl as f64 / capital);
'''
new_baseline = '''    let capital = capital_ticks.max(1) as f64;
    let baseline_pnl_ticks = points.first().map(|(_, pnl)| *pnl).unwrap_or(0);
    let total_return_pct = points
        .last()
        .map(|(_, pnl)| (pnl.saturating_sub(baseline_pnl_ticks) as f64 / capital) * 100.0)
        .unwrap_or(0.0);
    let mut max_drawdown_pct = 0.0_f64;
    let mut peak_equity = 1.0_f64;
    for (_, pnl) in points {
        let window_pnl_ticks = pnl.saturating_sub(baseline_pnl_ticks);
        let equity = 1.0 + (window_pnl_ticks as f64 / capital);
'''
runtime = replace_once(runtime, old_baseline, new_baseline, "risk metric rolling baseline")

# Add deterministic regressions without depending on live market fixtures.
if "risk_history_uses_independent_sampling_and_forced_endpoint" not in runtime:
    test_anchor = '''    #[test]
    fn summary_includes_mark_to_market_for_open_position() {
'''
    tests = '''    #[test]
    fn risk_history_uses_independent_sampling_and_forced_endpoint() {
        let point = |observed_at_ms, net_pnl_ticks| PerformancePoint {
            observed_at_ms,
            market_pnl_ticks: net_pnl_ticks,
            strategy_pnl_ticks: 0,
            funding_pnl_ticks: 0,
            fees_ticks: 0,
            gross_pnl_ticks: net_pnl_ticks,
            net_pnl_ticks,
            current_absolute_position: 0,
            symbols: Vec::new(),
        };
        let mut history = VecDeque::new();
        push_risk_history_point(&mut history, point(0, 100), false);
        push_risk_history_point(&mut history, point(1_000, 101), false);
        assert_eq!(history.len(), 1);
        push_risk_history_point(&mut history, point(30_000, 102), false);
        assert_eq!(history.len(), 2);
        push_risk_history_point(&mut history, point(31_000, 103), true);
        assert_eq!(history.len(), 3);
        assert_eq!(history.back().unwrap().net_pnl_ticks, 103);
    }

    #[test]
    fn rolling_risk_metrics_normalize_to_window_start() {
        let points = vec![(0, 1_000), (30_000, 1_100), (60_000, 1_200)];
        let metrics = calculate_risk_metrics(&points, 10_000);
        assert!((metrics.total_return_pct - 2.0).abs() < 1e-9);
        assert!(metrics.max_drawdown_pct.abs() < 1e-9);
    }

'''
    runtime = replace_once(runtime, test_anchor, tests + test_anchor, "runtime risk regression tests")

runtime_path.write_text(runtime, encoding="utf-8")


# Batch has its own risk-history deque. Keep the exact same 30-second/7-day
# semantics and reject validation folds that cannot be fully retained.
batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")

old_batch_constants = '''const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_HISTORY_CAPACITY: usize = 7_201;
'''
new_batch_constants = '''const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;
const RISK_HISTORY_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const RISK_HISTORY_CAPACITY: usize =
    (RISK_HISTORY_RETENTION_MS / RISK_SAMPLE_INTERVAL_MS) as usize + 2;
const MAX_VALIDATION_FOLD_DURATION_SECS: u64 = RISK_HISTORY_RETENTION_MS / 1_000;
'''
batch = replace_once(batch, old_batch_constants, new_batch_constants, "batch risk constants")

batch_helper_anchor = '''fn now_ms() -> u64 {
'''
batch_helper = '''fn push_batch_risk_history_point(
    history: &mut VecDeque<PerformancePoint>,
    point: PerformancePoint,
    force_endpoint: bool,
) {
    if history
        .back()
        .is_some_and(|last| last.observed_at_ms == point.observed_at_ms)
    {
        history.pop_back();
        history.push_back(point);
    } else if force_endpoint
        || history.back().is_none_or(|last| {
            point.observed_at_ms.saturating_sub(last.observed_at_ms) >= RISK_SAMPLE_INTERVAL_MS
        })
    {
        history.push_back(point);
    }
    while history.len() > RISK_HISTORY_CAPACITY {
        history.pop_front();
    }
}

'''
if "fn push_batch_risk_history_point(" not in batch:
    batch = replace_once(batch, batch_helper_anchor, batch_helper + batch_helper_anchor, "batch risk history helper")

old_batch_tick = '''                        ledger.history.push_back(point.clone());
                        ledger.risk_history.push_back(point);
                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
                            ledger.history.pop_front();
                        }
                        while ledger.risk_history.len() > RISK_HISTORY_CAPACITY {
                            ledger.risk_history.pop_front();
                        }
'''
new_batch_tick = '''                        ledger.history.push_back(point.clone());
                        push_batch_risk_history_point(&mut ledger.risk_history, point, false);
                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
                            ledger.history.pop_front();
                        }
'''
batch = replace_once(batch, old_batch_tick, new_batch_tick, "batch periodic risk sampling")

old_batch_final = '''        ledger.history.push_back(point.clone());
        ledger.risk_history.push_back(point);
        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
            ledger.history.pop_front();
        }
        while ledger.risk_history.len() > RISK_HISTORY_CAPACITY {
            ledger.risk_history.pop_front();
        }
'''
new_batch_final = '''        ledger.history.push_back(point.clone());
        push_batch_risk_history_point(&mut ledger.risk_history, point, true);
        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
            ledger.history.pop_front();
        }
'''
batch = replace_once(batch, old_batch_final, new_batch_final, "batch final risk endpoint")

old_duration = '''        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.position_allocations.is_none() {
'''
new_duration = '''        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.duration_secs > MAX_VALIDATION_FOLD_DURATION_SECS {
            return Err(SimulationError::InvalidConfig(
                "validation fold exceeds retained risk-history window",
            ));
        }
        if config.position_allocations.is_none() {
'''
batch = replace_once(batch, old_duration, new_duration, "validation fold risk-window bound")

batch_path.write_text(batch, encoding="utf-8")
print("rolling risk history repair complete")
