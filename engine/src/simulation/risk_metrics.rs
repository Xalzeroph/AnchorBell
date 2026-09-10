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
    /// Effective independent sample count after accounting for serial
    /// correlation in interval returns.
    pub effective_sample_count: f64,
    /// Mean of the worst 5% interval returns, expressed in bps.
    pub expected_shortfall_5pct_bps: Option<f64>,
    /// Worst single sampled interval return, expressed in bps.
    pub max_interval_loss_bps: Option<f64>,
    pub profit_factor: Option<f64>,
    /// Annualized Sharpe; null until enough independent history exists.
    pub sharpe_ratio: Option<f64>,
    /// Annualized Sortino; null until enough independent history exists.
    pub sortino_ratio: Option<f64>,
    /// Annualized return divided by maximum drawdown.
    pub calmar_ratio: Option<f64>,
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
    let long_run_variance_multiplier = hac_variance_multiplier(&returns);
    let effective_sample_count = if sample_count == 0 {
        0.0
    } else {
        sample_count as f64 / long_run_variance_multiplier
    };
    let hac_standard_deviation = (variance * long_run_variance_multiplier).max(0.0).sqrt();
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
    let expected_shortfall_5pct_bps = if returns.is_empty() {
        None
    } else {
        let mut tail = returns.clone();
        tail.sort_by(|left, right| left.total_cmp(right));
        let tail_count = ((tail.len() as f64 * 0.05).ceil() as usize).max(1);
        Some(
            tail[..tail_count.min(tail.len())].iter().sum::<f64>()
                / tail_count.min(tail.len()) as f64
                * 10_000.0,
        )
    };
    let max_interval_loss_bps = returns
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .min_by(|left, right| left.total_cmp(right))
        .map(|value| value * 10_000.0);
    let annualized_return = if observed_seconds > 0.0 {
        let years = observed_seconds / (365.0 * 24.0 * 60.0 * 60.0);
        if years > 0.0 && total_return_pct > -100.0 {
            ((1.0 + total_return_pct / 100.0).powf(1.0 / years) - 1.0).max(-1.0)
        } else {
            0.0
        }
    } else {
        0.0
    };
    let history_sufficient = effective_sample_count >= MIN_RISK_RETURN_SAMPLES as f64;
    let calmar_ratio = (history_sufficient && max_drawdown_pct > 0.0)
        .then_some(annualized_return / (max_drawdown_pct / 100.0));

    RiskMetrics {
        status: if history_sufficient {
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
        effective_sample_count,
        expected_shortfall_5pct_bps,
        max_interval_loss_bps,
        profit_factor: (gross_loss > 0.0).then_some(gross_profit / gross_loss),
        sharpe_ratio: (history_sufficient && hac_standard_deviation > 0.0)
            .then_some(mean / hac_standard_deviation * annualization),
        sortino_ratio: (history_sufficient && downside_deviation > 0.0)
            .then_some(mean / downside_deviation * annualization),
        calmar_ratio,
    }
}

/// Bartlett-kernel long-run variance inflation. This is a small online-safe
/// HAC estimator: it discounts higher lags instead of treating event-driven
/// interval returns as iid observations.
fn hac_variance_multiplier(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 1.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let centered = returns.iter().map(|value| value - mean).collect::<Vec<_>>();
    let variance =
        centered.iter().map(|value| value * value).sum::<f64>() / (returns.len() - 1) as f64;
    if variance <= f64::EPSILON {
        return 1.0;
    }
    let bandwidth = ((returns.len() as f64).sqrt() as usize)
        .max(1)
        .min(20)
        .min(returns.len() - 1);
    let mut multiplier = 1.0_f64;
    for lag in 1..=bandwidth {
        let covariance = centered[lag..]
            .iter()
            .zip(&centered[..returns.len() - lag])
            .map(|(left, right)| left * right)
            .sum::<f64>()
            / returns.len() as f64;
        let autocorrelation = covariance / variance;
        let kernel = 1.0 - lag as f64 / (bandwidth as f64 + 1.0);
        multiplier += 2.0 * kernel * autocorrelation;
    }
    multiplier.clamp(1.0, returns.len() as f64)
}

#[cfg(test)]
mod tests {
    use super::{calculate_risk_metrics, hac_variance_multiplier};

    #[test]
    fn hac_penalty_reduces_effective_samples_for_persistent_returns() {
        let independent = [1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
        let persistent = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5];
        assert_eq!(hac_variance_multiplier(&independent), 1.0);
        assert!(hac_variance_multiplier(&persistent) > 1.0);
    }

    #[test]
    fn risk_metrics_expose_tail_loss_and_effective_sample_count() {
        let points = (0..40)
            .map(|index| {
                let pnl = if index == 39 { -100 } else { index as i64 };
                (index as u64 * 30_000, pnl)
            })
            .collect::<Vec<_>>();
        let metrics = calculate_risk_metrics(&points, 10_000);
        assert!(metrics
            .expected_shortfall_5pct_bps
            .is_some_and(|value| value < 0.0));
        assert!(metrics
            .max_interval_loss_bps
            .is_some_and(|value| value < 0.0));
        assert!(metrics.effective_sample_count <= metrics.sample_count as f64);
        assert!(metrics.calmar_ratio.is_some());
    }
}
