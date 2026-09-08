from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
text = RUNTIME.read_text(encoding="utf-8")

old = '''                let allocated_capital = self
                    .position_allocations
                    .get(symbol)
                    .map(|allocation| allocation.budget_usdt_ticks)
                    .filter(|budget| *budget > 0)
                    .unwrap_or(fallback_budget.max(1));
                let net_pnl_ticks = state
                    .market_pnl_ticks
                    .saturating_add(state.strategy_pnl_ticks)
                    .saturating_add(state.funding_pnl_ticks)
                    .saturating_sub(state.fees_ticks);
                let post_fee_loss_bps =
                    if state.fills >= Self::DYNAMIC_PERFORMANCE_MIN_FILLS && net_pnl_ticks < 0 {
                        (i128::from(net_pnl_ticks).abs().saturating_mul(10_000)
                            / i128::from(allocated_capital))
                        .clamp(0, 250) as i64
                    } else {
                        0
                    };
                let fee_drag_bps = if state.fills >= Self::DYNAMIC_PERFORMANCE_MIN_FILLS {
                    (i128::from(state.fees_ticks.max(0)).saturating_mul(10_000)
                        / i128::from(allocated_capital))
                    .clamp(0, 100) as i64
                } else {
                    0
                };
'''
new = '''                // Performance penalties must use a stable denominator. Using the
                // current dynamic allocation creates a positive-feedback loop: reducing a
                // budget makes the same historical loss look larger and forces another cut.
                let performance_budget = fallback_budget.max(1);
                let gross_pnl_ticks = state
                    .market_pnl_ticks
                    .saturating_add(state.strategy_pnl_ticks)
                    .saturating_add(state.funding_pnl_ticks);
                let net_pnl_ticks = gross_pnl_ticks.saturating_sub(state.fees_ticks);
                let post_fee_loss_bps = Self::stable_post_fee_loss_bps(
                    net_pnl_ticks,
                    state.fills,
                    performance_budget,
                );
                let fee_drag_bps = Self::fee_efficiency_penalty_bps(
                    gross_pnl_ticks,
                    state.fees_ticks,
                    state.fills,
                );
'''
if old not in text:
    if new not in text:
        raise SystemExit("allocator performance anchor not found")
else:
    text = text.replace(old, new, 1)

anchor = '''    fn allocation_budget_change_bps(old_budget: i64, new_budget: i64, total_capital: i64) -> i64 {
        if total_capital <= 0 {
            return i64::MAX;
        }
        let delta = i128::from(new_budget)
            .saturating_sub(i128::from(old_budget))
            .abs();
        (delta.saturating_mul(10_000) / i128::from(total_capital)).clamp(0, i128::from(i64::MAX))
            as i64
    }

'''
helpers = '''    fn stable_post_fee_loss_bps(net_pnl_ticks: i64, fills: u64, baseline_budget: i64) -> i64 {
        if fills < Self::DYNAMIC_PERFORMANCE_MIN_FILLS || net_pnl_ticks >= 0 || baseline_budget <= 0 {
            return 0;
        }
        (i128::from(net_pnl_ticks)
            .abs()
            .saturating_mul(10_000)
            / i128::from(baseline_budget))
        .clamp(0, 250) as i64
    }

    /// Penalize fee inefficiency without making the penalty grow merely because
    /// a simulation has been running longer. Fees are already included in net
    /// PnL; this bounded term measures how much positive gross edge they consume.
    fn fee_efficiency_penalty_bps(gross_pnl_ticks: i64, fees_ticks: i64, fills: u64) -> i64 {
        if fills < Self::DYNAMIC_PERFORMANCE_MIN_FILLS || gross_pnl_ticks <= 0 || fees_ticks <= 0 {
            return 0;
        }
        (i128::from(fees_ticks)
            .saturating_mul(100)
            / i128::from(gross_pnl_ticks))
        .clamp(0, 100) as i64
    }

'''
if "fn stable_post_fee_loss_bps" not in text:
    if anchor not in text:
        raise SystemExit("allocator helper anchor not found")
    text = text.replace(anchor, anchor + helpers, 1)

# Regression tests: scoring cannot worsen merely because the allocator itself
# reduced the current budget, and fee pressure is duration-stable when edge and
# fees scale proportionally.
test_anchor = '''    fn dynamic_allocation_deadband_ignores_sub_percent_noise() {
'''
if "allocator_performance_penalty_uses_stable_scale" not in text:
    tests = '''    #[test]
    fn allocator_performance_penalty_uses_stable_scale() {
        assert_eq!(SimulationEngine::stable_post_fee_loss_bps(-50, 10, 10_000), 50);
        assert_eq!(SimulationEngine::stable_post_fee_loss_bps(-50, 10, 20_000), 25);
        assert_eq!(SimulationEngine::stable_post_fee_loss_bps(-50, 9, 10_000), 0);
    }

    #[test]
    fn fee_efficiency_penalty_is_ratio_based_not_runtime_accumulation() {
        let short = SimulationEngine::fee_efficiency_penalty_bps(1_000, 200, 10);
        let long = SimulationEngine::fee_efficiency_penalty_bps(10_000, 2_000, 100);
        assert_eq!(short, 20);
        assert_eq!(short, long);
        assert_eq!(SimulationEngine::fee_efficiency_penalty_bps(-1, 200, 100), 0);
    }

    #[test]
'''
    if test_anchor not in text:
        raise SystemExit("allocator test anchor not found")
    text = text.replace(test_anchor, tests + test_anchor, 1)

RUNTIME.write_text(text, encoding="utf-8")
print("stable allocator feedback repair complete")
