from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


runtime_path = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
runtime = runtime_path.read_text(encoding="utf-8")

old_constants = '''const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_HISTORY_CAPACITY: usize = 7_201;
const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;
const MIN_RISK_RETURN_SAMPLES: usize = 30;
'''
new_constants = '''const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;
pub(crate) const RISK_HISTORY_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const RISK_HISTORY_CAPACITY: usize =
    (RISK_HISTORY_WINDOW_MS / RISK_SAMPLE_INTERVAL_MS) as usize + 2;
const MIN_RISK_RETURN_SAMPLES: usize = 30;

/// Store statistically independent risk samples instead of every metrics tick.
/// `force_final` replaces a sub-interval tail sample so shutdown/fold-end PnL is
/// reflected without creating a spuriously tiny return interval.
pub(crate) fn append_risk_history_sample(
    history: &mut VecDeque<PerformancePoint>,
    point: PerformancePoint,
    force_final: bool,
) {
    match history.back() {
        None => history.push_back(point),
        Some(last)
            if point.observed_at_ms.saturating_sub(last.observed_at_ms)
                >= RISK_SAMPLE_INTERVAL_MS =>
        {
            history.push_back(point);
        }
        Some(_) if force_final => {
            if let Some(last) = history.back_mut() {
                *last = point;
            }
        }
        Some(_) => {}
    }
    while history.len() > RISK_HISTORY_CAPACITY {
        history.pop_front();
    }
}
'''
runtime = replace_once(runtime, old_constants, new_constants, "runtime risk constants")

periodic_old = '''                        performance_history.push_back(point.clone());
                        risk_history.push_back(point);
                        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
                            performance_history.pop_front();
                        }
                        while risk_history.len() > RISK_HISTORY_CAPACITY {
                            risk_history.pop_front();
                        }
'''
periodic_new = '''                        performance_history.push_back(point.clone());
                        append_risk_history_sample(&mut risk_history, point, false);
                        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
                            performance_history.pop_front();
                        }
'''
runtime = replace_once(runtime, periodic_old, periodic_new, "runtime periodic risk sampling")

final_old = '''        performance_history.push_back(point.clone());
        risk_history.push_back(point);
        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
            performance_history.pop_front();
        }
        while risk_history.len() > RISK_HISTORY_CAPACITY {
            risk_history.pop_front();
        }
'''
final_new = '''        performance_history.push_back(point.clone());
        append_risk_history_sample(&mut risk_history, point, true);
        while performance_history.len() > DISPLAY_HISTORY_CAPACITY {
            performance_history.pop_front();
        }
'''
runtime = replace_once(runtime, final_old, final_new, "runtime final risk sampling")

return_old = '''    let capital = capital_ticks.max(1) as f64;
    let total_return_pct = points
        .last()
        .map(|(_, pnl)| (*pnl as f64 / capital) * 100.0)
        .unwrap_or(0.0);
    let mut max_drawdown_pct = 0.0_f64;
    let mut peak_equity = 1.0_f64;
    for (_, pnl) in points {
        let equity = 1.0 + (*pnl as f64 / capital);
'''
return_new = '''    let capital = capital_ticks.max(1) as f64;
    let window_start_pnl = points.first().map(|(_, pnl)| *pnl).unwrap_or(0);
    let total_return_pct = points
        .last()
        .map(|(_, pnl)| (pnl.saturating_sub(window_start_pnl) as f64 / capital) * 100.0)
        .unwrap_or(0.0);
    let mut max_drawdown_pct = 0.0_f64;
    let mut peak_equity = 1.0_f64;
    for (_, pnl) in points {
        let window_pnl = pnl.saturating_sub(window_start_pnl);
        let equity = 1.0 + (window_pnl as f64 / capital);
'''
runtime = replace_once(runtime, return_old, return_new, "window-relative return and drawdown")

interval_old = '''        let dt = (pair[1].0.saturating_sub(pair[0].0) as f64 / 1_000.0).max(0.001);
        observed_seconds += dt;
        returns.push((pair[1].1.saturating_sub(pair[0].1)) as f64 / capital);
'''
interval_new = '''        let dt_ms = pair[1].0.saturating_sub(pair[0].0).max(1);
        let dt = dt_ms as f64 / 1_000.0;
        observed_seconds += dt;
        let interval_scale = RISK_SAMPLE_INTERVAL_MS as f64 / dt_ms as f64;
        returns.push(
            (pair[1].1.saturating_sub(pair[0].1)) as f64 / capital * interval_scale,
        );
'''
runtime = replace_once(runtime, interval_old, interval_new, "interval-normalized returns")

annual_old = '''    let annualization = if sample_count > 0 && observed_seconds > 0.0 {
        (365.0 * 24.0 * 60.0 * 60.0 / (observed_seconds / sample_count as f64)).sqrt()
    } else {
        0.0
    };
'''
annual_new = '''    let annualization = if sample_count > 0 {
        (365.0 * 24.0 * 60.0 * 60.0 / (RISK_SAMPLE_INTERVAL_MS as f64 / 1_000.0)).sqrt()
    } else {
        0.0
    };
'''
runtime = replace_once(runtime, annual_old, annual_new, "fixed-interval annualization")

if "risk_metrics_are_relative_to_the_retained_window" not in runtime:
    test_anchor = '''fn event_symbol(event: &BinanceMarketEvent) -> &str {
'''
    tests = '''#[cfg(test)]
mod risk_window_regression_tests {
    use super::*;

    fn point(timestamp_ms: u64, pnl: i64) -> PerformancePoint {
        PerformancePoint {
            observed_at_ms: timestamp_ms,
            market_pnl_ticks: pnl,
            strategy_pnl_ticks: 0,
            funding_pnl_ticks: 0,
            fees_ticks: 0,
            gross_pnl_ticks: pnl,
            net_pnl_ticks: pnl,
            current_absolute_position: 0,
            symbols: Vec::new(),
        }
    }

    #[test]
    fn risk_history_is_downsampled_and_keeps_the_fold_end() {
        let mut history = VecDeque::new();
        append_risk_history_sample(&mut history, point(0, 100), false);
        append_risk_history_sample(&mut history, point(1_000, 101), false);
        append_risk_history_sample(&mut history, point(30_000, 102), false);
        append_risk_history_sample(&mut history, point(31_000, 103), true);
        assert_eq!(history.len(), 2);
        assert_eq!(history.front().unwrap().observed_at_ms, 0);
        assert_eq!(history.back().unwrap().observed_at_ms, 31_000);
        assert_eq!(history.back().unwrap().net_pnl_ticks, 103);
    }

    #[test]
    fn risk_metrics_are_relative_to_the_retained_window() {
        let metrics = calculate_risk_metrics(&[(0, 1_000), (30_000, 1_100)], 10_000);
        assert!((metrics.total_return_pct - 1.0).abs() < 1e-9);
        assert!((metrics.max_drawdown_pct - 0.0).abs() < 1e-9);
    }
}

'''
    runtime = replace_once(runtime, test_anchor, tests + test_anchor, "risk regression tests")

runtime_path.write_text(runtime, encoding="utf-8")


batch_path = ROOT / "engine" / "src" / "simulation_batch.rs"
batch = batch_path.read_text(encoding="utf-8")

import_old = '''    simulation::engine::{
        AnchorSnapshot, PerformancePoint, PositionAllocation, RiskMetrics, SimulationEngine,
        SimulationError, SimulationPolicyVariant, SimulationSummary,
    },
'''
import_new = '''    simulation::engine::{
        append_risk_history_sample, AnchorSnapshot, PerformancePoint, PositionAllocation,
        RiskMetrics, SimulationEngine, SimulationError, SimulationPolicyVariant,
        SimulationSummary, RISK_HISTORY_WINDOW_MS,
    },
'''
batch = replace_once(batch, import_old, import_new, "batch risk helper import")

batch_constants_old = '''const M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";
const DISPLAY_HISTORY_CAPACITY: usize = 900;
const RISK_HISTORY_CAPACITY: usize = 7_201;
'''
batch_constants_new = '''const M9_CALIBRATION_SOURCE_LABEL: &str = "F3_m3";
const DISPLAY_HISTORY_CAPACITY: usize = 900;
'''
batch = replace_once(batch, batch_constants_old, batch_constants_new, "batch risk constants")

batch_periodic_old = '''                        ledger.history.push_back(point.clone());
                        ledger.risk_history.push_back(point);
                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
                            ledger.history.pop_front();
                        }
                        while ledger.risk_history.len() > RISK_HISTORY_CAPACITY {
                            ledger.risk_history.pop_front();
                        }
'''
batch_periodic_new = '''                        ledger.history.push_back(point.clone());
                        append_risk_history_sample(&mut ledger.risk_history, point, false);
                        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
                            ledger.history.pop_front();
                        }
'''
batch = replace_once(batch, batch_periodic_old, batch_periodic_new, "batch periodic risk sampling")

batch_final_old = '''        ledger.history.push_back(point.clone());
        ledger.risk_history.push_back(point);
        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
            ledger.history.pop_front();
        }
        while ledger.risk_history.len() > RISK_HISTORY_CAPACITY {
            ledger.risk_history.pop_front();
        }
'''
batch_final_new = '''        ledger.history.push_back(point.clone());
        append_risk_history_sample(&mut ledger.risk_history, point, true);
        while ledger.history.len() > DISPLAY_HISTORY_CAPACITY {
            ledger.history.pop_front();
        }
'''
batch = replace_once(batch, batch_final_old, batch_final_new, "batch final risk sampling")

validation_old = '''        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.position_allocations.is_none() {
'''
validation_new = '''        if config.duration_secs == 0 {
            return Err(SimulationError::InvalidConfig(
                "validation folds require a finite duration",
            ));
        }
        if config.duration_secs.saturating_mul(1_000) > RISK_HISTORY_WINDOW_MS {
            return Err(SimulationError::InvalidConfig(
                "validation fold exceeds the complete risk-history window",
            ));
        }
        if config.position_allocations.is_none() {
'''
batch = replace_once(batch, validation_old, validation_new, "validation risk-window bound")

manifest_old = '''        "depth_snapshot_limit": config.depth_snapshot_limit,
        "duration_secs": config.duration_secs,
'''
manifest_new = '''        "depth_snapshot_limit": config.depth_snapshot_limit,
        "duration_secs": config.duration_secs,
        "risk_history_window_ms": RISK_HISTORY_WINDOW_MS,
'''
batch = replace_once(batch, manifest_old, manifest_new, "risk window lineage")

batch_path.write_text(batch, encoding="utf-8")
print("coherent risk-window repair complete")
