from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SIMULATION = ROOT / "engine" / "src" / "simulation.rs"
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    if new in text:
        return text
    raise SystemExit(f"missing anchor: {label}")


simulation = SIMULATION.read_text(encoding="utf-8")
runtime = RUNTIME.read_text(encoding="utf-8")

# The validated integration may already be committed in its source-budget compacted
# form. Treat that state as complete instead of trying to recreate the pre-compact
# method body byte-for-byte.
if (
    'pub mod portfolio_guard;' in simulation
    and 'portfolio_drawdown_guard: Option<PortfolioDrawdownGuard>' in runtime
    and 'pub fn with_portfolio_drawdown_limits_bps(' in runtime
    and 'let portfolio_drawdown_action = if strategy_variant.uses_tail_guard()' in runtime
    and 'portfolio_drawdown_action.blocks_new_risk()' in runtime
    and 'pub portfolio_drawdown: Option<PortfolioDrawdownSnapshot>' in runtime
):
    print("portfolio drawdown runtime integration already present")
    raise SystemExit(0)

simulation = replace_once(
    simulation,
    '#[path = "simulation/orchestration.rs"]\npub mod orchestration;\n',
    '#[path = "simulation/orchestration.rs"]\npub mod orchestration;\n#[path = "simulation/portfolio_guard.rs"]\npub mod portfolio_guard;\n',
    "portfolio guard module",
)

runtime = replace_once(
    runtime,
    'pub use super::risk_metrics::RiskMetrics;\nuse super::risk_metrics::{calculate_risk_metrics, RISK_SAMPLE_INTERVAL_MS};\n',
    'use super::portfolio_guard::{\n    PortfolioDrawdownAction, PortfolioDrawdownGuard, PortfolioDrawdownSnapshot,\n};\npub use super::risk_metrics::RiskMetrics;\nuse super::risk_metrics::{calculate_risk_metrics, RISK_SAMPLE_INTERVAL_MS};\n',
    "portfolio guard imports",
)

runtime = replace_once(
    runtime,
    '''    pub risk_metrics: Option<RiskMetrics>,
    pub calendar_snapshot: String,''',
    '''    pub risk_metrics: Option<RiskMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub portfolio_drawdown: Option<PortfolioDrawdownSnapshot>,
    pub calendar_snapshot: String,''',
    "metrics drawdown snapshot",
)

runtime = replace_once(
    runtime,
    '''    funding_controller_enabled: bool,
    threshold_scale_ppm: i64,
    position_allocations: BTreeMap<String, PositionAllocation>,''',
    '''    funding_controller_enabled: bool,
    threshold_scale_ppm: i64,
    portfolio_drawdown_guard: Option<PortfolioDrawdownGuard>,
    position_allocations: BTreeMap<String, PositionAllocation>,''',
    "engine drawdown state",
)

runtime = replace_once(
    runtime,
    '''            funding_controller_enabled: true,
            threshold_scale_ppm: 1_000_000,
            position_allocations,''',
    '''            funding_controller_enabled: true,
            threshold_scale_ppm: 1_000_000,
            portfolio_drawdown_guard: None,
            position_allocations,''',
    "engine drawdown default",
)

runtime = replace_once(
    runtime,
    '''    pub fn with_threshold_scale_ppm(mut self, scale_ppm: i64) -> Self {
        self.threshold_scale_ppm = scale_ppm.clamp(0, 1_000_000);
        self
    }

    pub fn with_strategy_variant''',
    '''    pub fn with_threshold_scale_ppm(mut self, scale_ppm: i64) -> Self {
        self.threshold_scale_ppm = scale_ppm.clamp(0, 1_000_000);
        self
    }

    pub fn with_portfolio_drawdown_limits_bps(
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

    fn observe_portfolio_drawdown(&mut self) -> PortfolioDrawdownAction {
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

    pub fn with_strategy_variant''',
    "engine drawdown methods",
)

runtime = replace_once(
    runtime,
    '''        let requested_quantity = allocation
            .map(|allocation| allocation.requested_quantity)
            .unwrap_or(self.requested_quantity);
        let strategy_variant = self.strategy_variant;
        self.update_adaptive_threshold_controller(''',
    '''        let requested_quantity = allocation
            .map(|allocation| allocation.requested_quantity)
            .unwrap_or(self.requested_quantity);
        let strategy_variant = self.strategy_variant;
        let portfolio_drawdown_action = if strategy_variant.uses_tail_guard() {
            self.observe_portfolio_drawdown()
        } else {
            PortfolioDrawdownAction::Trading
        };
        self.update_adaptive_threshold_controller(''',
    "rebalance drawdown observation",
)

runtime = replace_once(
    runtime,
    '''            let funding_reduce_only = funding_overlay
                .as_ref()
                .is_some_and(|overlay| overlay.reduce_only);
            let entries_allowed = session_allowed && funding_allowed;
            let tail_reduce_only = strategy_variant.uses_tail_guard() && m5_tail_reduce_only(state);
            if !entries_allowed || tail_reduce_only {
                let should_reduce = position_requires_reduction(
                    state.position,
                    session_allowed,
                    funding_allowed,
                    funding_reduce_only,
                    tail_reduce_only,
                );
                if !should_reduce {
                    (
                        None,
                        true,
                        state.working.is_some(),
                        entry_restriction_reason(state.position, session_allowed, funding_allowed),
                    )
                } else {''',
    '''            let funding_reduce_only = funding_overlay
                .as_ref()
                .is_some_and(|overlay| overlay.reduce_only);
            let portfolio_reduce_only = portfolio_drawdown_action.blocks_new_risk();
            let entries_allowed = session_allowed && funding_allowed && !portfolio_reduce_only;
            let tail_reduce_only = strategy_variant.uses_tail_guard() && m5_tail_reduce_only(state);
            if !entries_allowed || tail_reduce_only {
                let should_reduce = (portfolio_reduce_only && state.position != 0)
                    || position_requires_reduction(
                        state.position,
                        session_allowed,
                        funding_allowed,
                        funding_reduce_only,
                        tail_reduce_only,
                    );
                if !should_reduce {
                    (
                        None,
                        true,
                        state.working.is_some(),
                        if portfolio_reduce_only {
                            portfolio_drawdown_action.label()
                        } else {
                            entry_restriction_reason(
                                state.position,
                                session_allowed,
                                funding_allowed,
                            )
                        },
                    )
                } else {''',
    "drawdown entry and flatten gate",
)

runtime = replace_once(
    runtime,
    '''            history: Vec::new(),
            risk_metrics: None,
            calendar_snapshot: "sse-hkex-2026".to_owned(),''',
    '''            history: Vec::new(),
            risk_metrics: None,
            portfolio_drawdown: self.portfolio_drawdown_snapshot(),
            calendar_snapshot: "sse-hkex-2026".to_owned(),''',
    "metrics drawdown output",
)

SIMULATION.write_text(simulation, encoding="utf-8")
RUNTIME.write_text(runtime, encoding="utf-8")
print(
    "portfolio drawdown runtime integration applied: "
    f"runtime={len(runtime.encode('utf-8'))} bytes"
)
