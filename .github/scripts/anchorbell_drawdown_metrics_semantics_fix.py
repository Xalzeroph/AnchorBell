from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
GUARD = ROOT / "engine" / "src" / "simulation" / "portfolio_guard.rs"

runtime = RUNTIME.read_text(encoding="utf-8")
guard = GUARD.read_text(encoding="utf-8")

old_guard = '''    pub fn snapshot(&self) -> PortfolioDrawdownSnapshot {
'''
new_guard = '''    pub fn observe_optional(guard: Option<&mut Self>, pnl: Option<i64>) -> PortfolioDrawdownAction {
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
'''
if 'pub fn observe_optional(' not in guard:
    if old_guard not in guard:
        raise SystemExit("missing anchor: portfolio guard helpers")
    guard = guard.replace(old_guard, new_guard, 1)

verbose_observe = '''    fn observe_portfolio_drawdown(&mut self) -> PortfolioDrawdownAction {
        let summary = self.summary();
        self.portfolio_drawdown_guard
            .as_mut()
            .map_or(PortfolioDrawdownAction::Trading, |guard| {
                guard.observe(summary.unrealized_valuation_complete.then(|| {
                    summary
                        .net_pnl_ticks
                        .saturating_add(summary.unrealized_pnl_ticks)
                }))
            })
    }
'''
double_count_observe = '''    fn observe_portfolio_drawdown(&mut self) -> PortfolioDrawdownAction {
        let s = self.summary();
        PortfolioDrawdownGuard::observe_optional(
            self.portfolio_drawdown_guard.as_mut(),
            s.unrealized_valuation_complete
                .then(|| s.net_pnl_ticks.saturating_add(s.unrealized_pnl_ticks)),
        )
    }
'''
canonical_observe = '''    fn observe_portfolio_drawdown(&mut self) -> PortfolioDrawdownAction {
        let s = self.summary();
        PortfolioDrawdownGuard::observe_optional(
            self.portfolio_drawdown_guard.as_mut(),
            s.unrealized_valuation_complete.then_some(s.net_pnl_ticks),
        )
    }
'''
if canonical_observe not in runtime:
    if double_count_observe in runtime:
        runtime = runtime.replace(double_count_observe, canonical_observe, 1)
    elif verbose_observe in runtime:
        runtime = runtime.replace(verbose_observe, canonical_observe, 1)
    else:
        raise SystemExit("missing anchor: canonical portfolio PnL observe")

old_fields = '''                    risk_state: risk_state.label().to_owned(),
                    entry_block_reason: entry_block_reason.to_owned(),
'''
new_fields = '''                    risk_state: labels.0.to_owned(),
                    entry_block_reason: labels.1.to_owned(),
'''
if new_fields not in runtime:
    if old_fields not in runtime:
        raise SystemExit("missing anchor: metrics risk fields")
    marker = '''                SymbolMetrics {
'''
    insert = '''                let labels = PortfolioDrawdownGuard::metric_labels(
                    self.portfolio_drawdown_guard.as_ref(),
                    risk_state.label(),
                    entry_block_reason,
                );

                SymbolMetrics {
'''
    if marker not in runtime:
        raise SystemExit("missing anchor: symbol metrics construction")
    runtime = runtime.replace(marker, insert, 1)
    runtime = runtime.replace(old_fields, new_fields, 1)

GUARD.write_text(guard, encoding="utf-8")
RUNTIME.write_text(runtime, encoding="utf-8")
print("portfolio drawdown metrics semantics repair applied")
