//! Causal portfolio drawdown state machine for simulation and replay.
//!
//! Drawdown is measured against the stable configured capital base, never a
//! dynamically reweighted symbol budget. A hard breach latches until restart;
//! soft breaches can recover as marked-to-market equity recovers.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PortfolioDrawdownAction {
    Trading,
    ReduceOnly,
    HardStop,
}

impl PortfolioDrawdownAction {
    pub fn label(self) -> &'static str {
        match self {
            Self::Trading => "trading",
            Self::ReduceOnly => "portfolio_drawdown_reduce_only",
            Self::HardStop => "portfolio_drawdown_hard_stop",
        }
    }

    pub fn blocks_new_risk(self) -> bool {
        self != Self::Trading
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioDrawdownSnapshot {
    pub capital_ticks: i64,
    pub soft_limit_bps: i64,
    pub hard_limit_bps: i64,
    pub peak_pnl_ticks: i64,
    pub current_mark_to_market_pnl_ticks: Option<i64>,
    pub drawdown_bps: i64,
    pub hard_stopped: bool,
    pub action: &'static str,
}

#[derive(Debug, Clone)]
pub struct PortfolioDrawdownGuard {
    capital_ticks: i64,
    soft_limit_bps: i64,
    hard_limit_bps: i64,
    peak_pnl_ticks: i64,
    current_mark_to_market_pnl_ticks: Option<i64>,
    drawdown_bps: i64,
    hard_stopped: bool,
    action: PortfolioDrawdownAction,
}

impl PortfolioDrawdownGuard {
    /// Zero/zero explicitly disables the governor. Otherwise both limits must
    /// be positive, ordered, and bounded by 100% of the stable capital base.
    pub fn new(
        capital_ticks: i64,
        soft_limit_bps: i64,
        hard_limit_bps: i64,
    ) -> Result<Option<Self>, &'static str> {
        if soft_limit_bps == 0 && hard_limit_bps == 0 {
            return Ok(None);
        }
        if capital_ticks <= 0
            || soft_limit_bps <= 0
            || hard_limit_bps <= soft_limit_bps
            || hard_limit_bps > 10_000
        {
            return Err("portfolio drawdown limits require positive capital and 0 < soft < hard <= 10000 bps");
        }
        Ok(Some(Self {
            capital_ticks,
            soft_limit_bps,
            hard_limit_bps,
            peak_pnl_ticks: 0,
            current_mark_to_market_pnl_ticks: Some(0),
            drawdown_bps: 0,
            hard_stopped: false,
            action: PortfolioDrawdownAction::Trading,
        }))
    }

    /// Observe a causal marked-to-market portfolio PnL. Missing valuation is
    /// fail-closed to reduce-only, but it does not poison the historical peak.
    pub fn observe(&mut self, mark_to_market_pnl_ticks: Option<i64>) -> PortfolioDrawdownAction {
        if self.hard_stopped {
            self.action = PortfolioDrawdownAction::HardStop;
            return self.action;
        }
        let Some(pnl) = mark_to_market_pnl_ticks else {
            self.current_mark_to_market_pnl_ticks = None;
            self.action = PortfolioDrawdownAction::ReduceOnly;
            return self.action;
        };
        self.current_mark_to_market_pnl_ticks = Some(pnl);
        self.peak_pnl_ticks = self.peak_pnl_ticks.max(pnl);
        let loss_from_peak = self.peak_pnl_ticks.saturating_sub(pnl).max(0);
        self.drawdown_bps = (i128::from(loss_from_peak).saturating_mul(10_000)
            / i128::from(self.capital_ticks))
        .clamp(0, i128::from(i64::MAX)) as i64;
        self.action = if self.drawdown_bps >= self.hard_limit_bps {
            self.hard_stopped = true;
            PortfolioDrawdownAction::HardStop
        } else if self.drawdown_bps >= self.soft_limit_bps {
            PortfolioDrawdownAction::ReduceOnly
        } else {
            PortfolioDrawdownAction::Trading
        };
        self.action
    }

    pub fn observe_optional(guard: Option<&mut Self>, pnl: Option<i64>) -> PortfolioDrawdownAction {
        guard.map_or(PortfolioDrawdownAction::Trading, |guard| guard.observe(pnl))
    }

    pub fn metric_labels<'a>(
        guard: Option<&Self>,
        risk: &'a str,
        reason: &'a str,
    ) -> (&'a str, &'a str) {
        guard
            .filter(|guard| guard.action.blocks_new_risk())
            .map_or((risk, reason), |guard| {
                let label = guard.action.label();
                (label, label)
            })
    }

    pub fn snapshot(&self) -> PortfolioDrawdownSnapshot {
        PortfolioDrawdownSnapshot {
            capital_ticks: self.capital_ticks,
            soft_limit_bps: self.soft_limit_bps,
            hard_limit_bps: self.hard_limit_bps,
            peak_pnl_ticks: self.peak_pnl_ticks,
            current_mark_to_market_pnl_ticks: self.current_mark_to_market_pnl_ticks,
            drawdown_bps: self.drawdown_bps,
            hard_stopped: self.hard_stopped,
            action: self.action.label(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_guard_requires_explicit_zero_pair() {
        assert!(PortfolioDrawdownGuard::new(1_000, 0, 0).unwrap().is_none());
        assert!(PortfolioDrawdownGuard::new(1_000, 0, 100).is_err());
        assert!(PortfolioDrawdownGuard::new(1_000, 100, 100).is_err());
        assert!(PortfolioDrawdownGuard::new(1_000, 200, 100).is_err());
    }

    #[test]
    fn soft_drawdown_recovers_but_hard_stop_latches() {
        let mut guard = PortfolioDrawdownGuard::new(100_000, 100, 200)
            .unwrap()
            .unwrap();
        assert_eq!(guard.observe(Some(2_000)), PortfolioDrawdownAction::Trading);
        assert_eq!(
            guard.observe(Some(1_000)),
            PortfolioDrawdownAction::ReduceOnly
        );
        assert_eq!(guard.snapshot().drawdown_bps, 100);
        assert_eq!(guard.observe(Some(1_500)), PortfolioDrawdownAction::Trading);
        assert_eq!(guard.observe(Some(0)), PortfolioDrawdownAction::HardStop);
        assert!(guard.snapshot().hard_stopped);
        assert_eq!(
            guard.observe(Some(2_500)),
            PortfolioDrawdownAction::HardStop
        );
    }

    #[test]
    fn missing_mark_to_market_is_fail_closed_without_moving_peak() {
        let mut guard = PortfolioDrawdownGuard::new(100_000, 100, 200)
            .unwrap()
            .unwrap();
        guard.observe(Some(1_500));
        assert_eq!(guard.observe(None), PortfolioDrawdownAction::ReduceOnly);
        assert_eq!(guard.snapshot().peak_pnl_ticks, 1_500);
        assert_eq!(guard.observe(Some(1_500)), PortfolioDrawdownAction::Trading);
    }
}
