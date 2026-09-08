from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
GUARD = ROOT / "engine" / "src" / "simulation" / "portfolio_guard.rs"

runtime = RUNTIME.read_text(encoding="utf-8")
guard = GUARD.read_text(encoding="utf-8")

old_guard = '''    pub fn snapshot(&self) -> PortfolioDrawdownSnapshot {
'''
new_guard = '''    pub fn metric_labels<'a>(
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
if new_guard not in guard:
    if old_guard not in guard:
        raise SystemExit("missing anchor: portfolio guard metric labels")
    guard = guard.replace(old_guard, new_guard, 1)

old_risk = '''                let risk_state = if !equity_entry_allowed {
'''
if old_risk not in runtime and 'let portfolio_labels = PortfolioDrawdownGuard::metric_labels(' not in runtime:
    raise SystemExit("missing anchor: metrics risk state")

old_fields = '''                    risk_state: risk_state.label().to_owned(),
                    entry_block_reason: entry_block_reason.to_owned(),
'''
new_fields = '''                    risk_state: portfolio_labels.0.to_owned(),
                    entry_block_reason: portfolio_labels.1.to_owned(),
'''
if new_fields not in runtime:
    if old_fields not in runtime:
        raise SystemExit("missing anchor: metrics risk fields")
    marker = '''                SymbolMetrics {
'''
    insert = '''                let portfolio_labels = PortfolioDrawdownGuard::metric_labels(
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
