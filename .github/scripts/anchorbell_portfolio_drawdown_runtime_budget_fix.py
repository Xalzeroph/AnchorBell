from pathlib import Path

path = Path(__file__).resolve().parents[2] / "engine" / "src" / "simulation" / "runtime.rs"
text = path.read_text(encoding="utf-8")

blocks = [
    '''    #[serde(skip_serializing_if = "Option::is_none")]
    pub portfolio_drawdown: Option<PortfolioDrawdownSnapshot>,
''',
    '''    pub fn portfolio_drawdown_snapshot(&self) -> Option<PortfolioDrawdownSnapshot> {
        self.portfolio_drawdown_guard
            .as_ref()
            .map(PortfolioDrawdownGuard::snapshot)
    }

''',
    '''            portfolio_drawdown: self.portfolio_drawdown_snapshot(),
''',
]
for block in blocks:
    text = text.replace(block, "", 1)

text = text.replace(
    '''use super::portfolio_guard::{
    PortfolioDrawdownAction, PortfolioDrawdownGuard, PortfolioDrawdownSnapshot,
};''',
    '''use super::portfolio_guard::{PortfolioDrawdownAction, PortfolioDrawdownGuard};''',
    1,
)
path.write_text(text, encoding="utf-8")
print(f"drawdown runtime budget trim complete: {len(text.encode('utf-8'))} bytes")
