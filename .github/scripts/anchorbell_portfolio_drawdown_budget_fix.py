from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


runtime = RUNTIME.read_text(encoding="utf-8")

runtime = replace_once(
    runtime,
    '''    pub fn with_portfolio_drawdown_limits_bps(
        mut self,
        capital_ticks: i64,
        soft_limit_bps: i64,
        hard_limit_bps: i64,
    ) -> Result<Self, SimulationError> {
        if self
            .capital_usdt_ticks
            .is_some_and(|existing| existing != capital_ticks)
        {
            return Err(SimulationError::InvalidConfig(
                "portfolio drawdown capital must match allocated capital",
            ));
        }
        self.portfolio_drawdown_guard = PortfolioDrawdownGuard::new(
            capital_ticks,
            soft_limit_bps,
            hard_limit_bps,
        )
        .map_err(SimulationError::InvalidConfig)?;
        self.capital_usdt_ticks = Some(capital_ticks);
        Ok(self)
    }

''',
    '''    pub fn with_portfolio_drawdown_limits_bps(
        mut self,
        capital: i64,
        soft: i64,
        hard: i64,
    ) -> Result<Self, SimulationError> {
        if self.capital_usdt_ticks.is_some_and(|v| v != capital) {
            return Err(SimulationError::InvalidConfig("drawdown capital mismatch"));
        }
        self.portfolio_drawdown_guard =
            PortfolioDrawdownGuard::new(capital, soft, hard).map_err(SimulationError::InvalidConfig)?;
        self.capital_usdt_ticks = Some(capital);
        Ok(self)
    }

''',
    "compact drawdown builder",
)

runtime = replace_once(
    runtime,
    '''    fn observe_portfolio_drawdown(&mut self) -> PortfolioDrawdownAction {
        let summary = self.summary();
        let mark_to_market_pnl = summary.unrealized_valuation_complete.then(|| {
            summary
                .net_pnl_ticks
                .saturating_add(summary.unrealized_pnl_ticks)
        });
        self.portfolio_drawdown_guard.as_mut().map_or(
            PortfolioDrawdownAction::Trading,
            |guard| guard.observe(mark_to_market_pnl),
        )
    }

    pub fn portfolio_drawdown_snapshot(&self) -> Option<PortfolioDrawdownSnapshot> {
        self.portfolio_drawdown_guard
            .as_ref()
            .map(PortfolioDrawdownGuard::snapshot)
    }

''',
    '''    fn observe_portfolio_drawdown(&mut self) -> PortfolioDrawdownAction {
        let summary = self.summary();
        self.portfolio_drawdown_guard.as_mut().map_or(
            PortfolioDrawdownAction::Trading,
            |guard| {
                guard.observe(summary.unrealized_valuation_complete.then(|| {
                    summary
                        .net_pnl_ticks
                        .saturating_add(summary.unrealized_pnl_ticks)
                }))
            },
        )
    }

''',
    "compact drawdown observer",
)

runtime = replace_once(
    runtime,
    '''            portfolio_drawdown: self.portfolio_drawdown_snapshot(),''',
    '''            portfolio_drawdown: self
                .portfolio_drawdown_guard
                .as_ref()
                .map(PortfolioDrawdownGuard::snapshot),''',
    "inline drawdown snapshot",
)

RUNTIME.write_text(runtime, encoding="utf-8")
print(f"drawdown runtime glue compacted: runtime={len(runtime.encode('utf-8'))} bytes")
