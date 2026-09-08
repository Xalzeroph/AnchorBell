//! Risk statistics for simulation and deterministic replay.
//!
//! Kept outside the execution runtime so statistical accounting can evolve
//! without expanding the latency-sensitive simulation orchestration module.

use serde::Serialize;

pub(crate) const RISK_SAMPLE_INTERVAL_MS: u64 = 30_000;
const MIN_RISK_RETURN_SAMPLES: usize = 30;

#[derive(Debug, Clone, Serialize)]
pub struct RiskMetrics {
    pub status: String,
    pub sample_count: usize,
    pub observed_seconds: f64,
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub win_rate_pct: f64,
    pub average_return_bps: f64,
    pub profit_factor: Option<f64>,
    /// Annualized Sharpe; null until enough independent history exists.
    pub sharpe_ratio: Option<f64>,
    /// Annualized Sortino; null until enough independent history exists.
    pub sortino_ratio: Option<f64>,
}

pub(crate) fn calculate_risk_metrics(points: &[(u64, i64)], capital_ticks: i64) -> RiskMetrics {
    let capital = capital_ticks.max(1) as f64;
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
        peak_equity = peak_equity.max(equity);
        if peak_equity > 0.0 {
            max_drawdown_pct = max_drawdown_pct.max((peak_equity - equity) / peak_equity * 100.0);
        }
    }

    let mut sampled_points = Vec::with_capacity(points.len());
    for point in points.iter().copied() {
        if sampled_points.last().is_none_or(|last: &(u64, i64)| {
            point.0.saturating_sub(last.0) >= RISK_SAMPLE_INTERVAL_MS
        }) {
            sampled_points.push(point);
        }
    }
    let mut returns = Vec::with_capacity(sampled_points.len().saturating_sub(1));
    let mut observed_seconds = 0.0_f64;
    for pair in sampled_points.windows(2) {
        let dt_ms = pair[1].0.saturating_sub(pair[0].0).max(1);
        let dt = dt_ms as f64 / 1_000.0;
        observed_seconds += dt;
        let interval_scale = RISK_SAMPLE_INTERVAL_MS as f64 / dt_ms as f64;
        returns.push((pair[1].1.saturating_sub(pair[0].1)) as f64 / capital * interval_scale);
    }
    let sample_count = returns.len();
    let mean = if sample_count == 0 {
        0.0
    } else {
        returns.iter().sum::<f64>() / sample_count as f64
    };
    let variance = if sample_count > 1 {
        returns
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (sample_count - 1) as f64
    } else {
        0.0
    };
    let standard_deviation = variance.sqrt();
    let downside_deviation = if sample_count == 0 {
        0.0
    } else {
        (returns
            .iter()
            .map(|value| if *value < 0.0 { value.powi(2) } else { 0.0 })
            .sum::<f64>()
            / sample_count as f64)
            .sqrt()
    };
    let annualization = if sample_count > 0 {
        (365.0 * 24.0 * 60.0 * 60.0 / (RISK_SAMPLE_INTERVAL_MS as f64 / 1_000.0)).sqrt()
    } else {
        0.0
    };
    let positive = returns.iter().filter(|value| **value > 0.0).count();
    let gross_profit = returns.iter().filter(|value| **value > 0.0).sum::<f64>();
    let gross_loss = returns
        .iter()
        .filter(|value| **value < 0.0)
        .map(|value| value.abs())
        .sum::<f64>();

    RiskMetrics {
        status: if sample_count >= MIN_RISK_RETURN_SAMPLES {
            "ok".to_owned()
        } else {
            "insufficient_history".to_owned()
        },
        sample_count,
        observed_seconds,
        total_return_pct,
        max_drawdown_pct,
        win_rate_pct: if sample_count == 0 {
            0.0
        } else {
            positive as f64 / sample_count as f64 * 100.0
        },
        average_return_bps: mean * 10_000.0,
        profit_factor: (gross_loss > 0.0).then_some(gross_profit / gross_loss),
        sharpe_ratio: (sample_count >= MIN_RISK_RETURN_SAMPLES && standard_deviation > 0.0)
            .then_some(mean / standard_deviation * annualization),
        sortino_ratio: (sample_count >= MIN_RISK_RETURN_SAMPLES && downside_deviation > 0.0)
            .then_some(mean / downside_deviation * annualization),
    }
}
