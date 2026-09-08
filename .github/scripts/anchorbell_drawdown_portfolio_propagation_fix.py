from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"

runtime = RUNTIME.read_text(encoding="utf-8")

old_variant_gate = '''        let strategy_variant = self.strategy_variant;
        let portfolio_drawdown_action = if strategy_variant.uses_tail_guard() {
            self.observe_portfolio_drawdown()
        } else {
            PortfolioDrawdownAction::Trading
        };
        self.update_adaptive_threshold_controller('''
new_variant_gate = '''        let strategy_variant = self.strategy_variant;
        let portfolio_drawdown_action = self.observe_portfolio_drawdown();
        self.update_adaptive_threshold_controller('''
if old_variant_gate in runtime:
    runtime = runtime.replace(old_variant_gate, new_variant_gate, 1)
elif new_variant_gate not in runtime:
    raise SystemExit("missing anchor: variant-independent drawdown gate")

old_event_tail = '''        records.extend(match event {
            BinanceMarketEvent::BookTicker(ticker) => self.on_book_ticker(ticker),
            BinanceMarketEvent::MarkPrice(mark) => self.on_mark_price(mark, received_at_ms),
            BinanceMarketEvent::AggTrade(trade) => self.on_agg_trade(trade),
            BinanceMarketEvent::DepthUpdate(depth) => self.on_depth_update(depth),
        });
        records
    }
'''
new_event_tail = '''        records.extend(match event {
            BinanceMarketEvent::BookTicker(ticker) => self.on_book_ticker(ticker),
            BinanceMarketEvent::MarkPrice(mark) => self.on_mark_price(mark, received_at_ms),
            BinanceMarketEvent::AggTrade(trade) => self.on_agg_trade(trade),
            BinanceMarketEvent::DepthUpdate(depth) => self.on_depth_update(depth),
        });
        if self.observe_portfolio_drawdown().blocks_new_risk() {
            let source = event_symbol(event);
            let source_rebalanced = matches!(
                event,
                BinanceMarketEvent::BookTicker(_) | BinanceMarketEvent::MarkPrice(_)
            );
            for symbol in self.states.keys().cloned().collect::<Vec<_>>() {
                if !source_rebalanced || !symbol.eq_ignore_ascii_case(source) {
                    records.extend(self.rebalance_symbol(&symbol, self.last_event_at_ms));
                }
            }
        }
        records
    }
'''
if old_event_tail in runtime:
    runtime = runtime.replace(old_event_tail, new_event_tail, 1)
elif new_event_tail not in runtime:
    raise SystemExit("missing anchor: portfolio drawdown event propagation")

RUNTIME.write_text(runtime, encoding="utf-8")
print("portfolio drawdown propagation repair applied")
