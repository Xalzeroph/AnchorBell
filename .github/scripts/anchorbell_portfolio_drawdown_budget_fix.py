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

runtime = runtime.replace(
    '"portfolio drawdown capital must match allocated capital"',
    '"portfolio drawdown capital mismatch"',
    1,
)

RUNTIME.write_text(runtime, encoding="utf-8")
print(f"drawdown runtime glue compacted: runtime={len(runtime.encode('utf-8'))} bytes")
