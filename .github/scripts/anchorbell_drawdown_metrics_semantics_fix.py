from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
GUARD = ROOT / "engine" / "src" / "simulation" / "portfolio_guard.rs"

runtime = RUNTIME.read_text(encoding="utf-8")
guard = GUARD.read_text(encoding="utf-8")

old_guard = '''    pub fn snapshot(&self) -> PortfolioDrawdownSnapshot {
'''
new_guard = '''    pub fn action(&self) -> PortfolioDrawdownAction {
        self.action
    }

    pub fn snapshot(&self) -> PortfolioDrawdownSnapshot {
'''
if new_guard not in guard:
    if old_guard not in guard:
        raise SystemExit("missing anchor: portfolio guard action accessor")
    guard = guard.replace(old_guard, new_guard, 1)

old_risk = '''                let risk_state = if !equity_entry_allowed {
'''
new_risk = '''                let portfolio_action = self
                    .portfolio_drawdown_guard
                    .as_ref()
                    .map_or(PortfolioDrawdownAction::Trading, PortfolioDrawdownGuard::action);
                let risk_state = if !equity_entry_allowed {
'''
if new_risk not in runtime:
    if old_risk not in runtime:
        raise SystemExit("missing anchor: metrics risk state")
    runtime = runtime.replace(old_risk, new_risk, 1)

old_fields = '''                    risk_state: risk_state.label().to_owned(),
                    entry_block_reason: entry_block_reason.to_owned(),
'''
new_fields = '''                    risk_state: if portfolio_action.blocks_new_risk() {
                        portfolio_action.label()
                    } else {
                        risk_state.label()
                    }
                    .to_owned(),
                    entry_block_reason: if portfolio_action.blocks_new_risk() {
                        portfolio_action.label()
                    } else {
                        entry_block_reason
                    }
                    .to_owned(),
'''
if new_fields not in runtime:
    if old_fields not in runtime:
        raise SystemExit("missing anchor: metrics risk fields")
    runtime = runtime.replace(old_fields, new_fields, 1)

GUARD.write_text(guard, encoding="utf-8")
RUNTIME.write_text(runtime, encoding="utf-8")
print("portfolio drawdown metrics semantics repair applied")
