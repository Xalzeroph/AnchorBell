from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "engine" / "src" / "simulation" / "runtime.rs"
text = RUNTIME.read_text(encoding="utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global text
    if old in text:
        text = text.replace(old, new, 1)
        return
    if new in text:
        return
    raise SystemExit(f"missing anchor: {label}")


replace_once(
    '''    /// Quantity resting ahead of this order when it reached the exchange.\n    queue_ahead_quantity: i64,\n    /// Absolute distance from the contemporaneous mid, in basis points.\n''',
    '''    /// Initial modeled quantity resting ahead of this order. When local depth is\n    /// seeded this includes the observed quantity at our price plus any explicit\n    /// synthetic queue/trade-through stress.\n    queue_ahead_quantity: i64,\n    /// Stateful queue barrier still requiring compatible aggressor volume before\n    /// this maker order may fill. This can only decrease after exchange arrival.\n    queue_ahead_remaining: i64,\n    /// Absolute distance from the contemporaneous mid, in basis points.\n''',
    "working-order queue state",
)

replace_once(
    '''        let queue_ahead_quantity = if state.local_book.is_valid() {\n            state\n                .local_book\n                .quantity_at(intent.side == Side::Buy, intent.price)\n        } else {\n            match intent.side {\n                Side::Buy => book.bid_quantity.max(0),\n                Side::Sell => book.ask_quantity.max(0),\n            }\n        };\n        state.working = Some(WorkingOrder {\n''',
    '''        // A new maker order joins behind the observable resting quantity at its\n        // price. With no seeded local depth (legacy/simple replay), only explicit\n        // synthetic queue assumptions are applied; we do not pretend top-of-book\n        // size is a fully reconstructed FIFO queue.\n        let observed_queue_ahead = if state.local_book.is_valid() {\n            state\n                .local_book\n                .quantity_at(intent.side == Side::Buy, intent.price)\n                .max(0)\n        } else {\n            0\n        };\n        let queue_ahead_quantity = observed_queue_ahead\n            .saturating_add(self.realism.queue.visible_ahead.max(0))\n            .saturating_add(self.realism.queue.trade_through.max(0));\n        state.working = Some(WorkingOrder {\n''',
    "placement queue composition",
)

replace_once(
    '''            queue_ahead_quantity,\n            quote_distance_bps,\n''',
    '''            queue_ahead_quantity,\n            queue_ahead_remaining: queue_ahead_quantity,\n            quote_distance_bps,\n''',
    "placement queue remaining initialization",
)

old_fill = '''            let fill_quantity = realism.evaluate_after_latency(\n                MakerQuote {\n                    side: order.side,\n                    price_ticks: order.price_ticks,\n                    quantity: order.remaining_quantity,\n                },\n                book,\n                trade.quantity.0,\n            );\n            let quantity = match fill_quantity {\n                crate::backtest::FillDecision::Fill { quantity } => quantity,\n                crate::backtest::FillDecision::NoFill => 0,\n            };\n            if quantity <= 0 {\n                return Vec::new();\n            }\n            let mut updated_order = order;\n            updated_order.remaining_quantity -= quantity;\n            state.working = (updated_order.remaining_quantity > 0).then_some(updated_order);\n'''
new_fill = '''            // Consume FIFO queue state cumulatively across compatible trades. The\n            // previous model compared each aggregate trade independently against a\n            // fixed global queue threshold and ignored the observed per-order queue.\n            let mut updated_order = order;\n            let aggressed_quantity = trade.quantity.0.max(0);\n            let consumed_ahead = aggressed_quantity.min(updated_order.queue_ahead_remaining.max(0));\n            updated_order.queue_ahead_remaining = updated_order\n                .queue_ahead_remaining\n                .saturating_sub(consumed_ahead);\n            let executable_quantity = aggressed_quantity.saturating_sub(consumed_ahead);\n            if executable_quantity <= 0 {\n                state.working = Some(updated_order);\n                return Vec::new();\n            }\n            // Queue and trade-through assumptions were incorporated once at order\n            // placement. After they are exhausted, cap the fill by currently\n            // displayed depth and remaining maker quantity without double-counting.\n            let displayed_depth = match order.side {\n                Side::Buy => book.bid_quantity,\n                Side::Sell => book.ask_quantity,\n            };\n            let quantity = executable_quantity\n                .min(displayed_depth.max(0))\n                .min(updated_order.remaining_quantity)\n                .max(0);\n            if quantity <= 0 {\n                state.working = Some(updated_order);\n                return Vec::new();\n            }\n            updated_order.remaining_quantity =\n                updated_order.remaining_quantity.saturating_sub(quantity);\n            state.working = (updated_order.remaining_quantity > 0).then_some(updated_order);\n'''
replace_once(old_fill, new_fill, "stateful queue fill path")

# `realism` is no longer needed inside on_agg_trade after queue/latency state is
# materialized on the working order.
replace_once(
    '''        let quantity_scale = self.quantity_scale;\n        let realism = self.realism;\n        let (quantity, order) = {\n''',
    '''        let quantity_scale = self.quantity_scale;\n        let (quantity, order) = {\n''',
    "remove stale realism copy",
)

replace_once(
    '''                fill_model: "local_depth_when_seeded_else_top_of_book_plus_aggregate_trade_queue"\n                    .to_owned(),\n''',
    '''                fill_model: "stateful_fifo_observed_depth_plus_synthetic_queue_then_aggregate_trade"\n                    .to_owned(),\n''',
    "fill-model audit label",
)

# Add regression that would fail under the old per-trade/global-queue model.
if "observed_queue_is_consumed_across_multiple_compatible_trades" not in text:
    anchor = '''    #[test]\n    fn seeded_local_depth_caps_fill_at_the_order_price() {\n'''
    test = '''    #[test]\n    fn observed_queue_is_consumed_across_multiple_compatible_trades() {\n        let mut engine = engine();\n        engine\n            .load_depth_snapshot("CXMTUSDT", 10, &[(98, 5)], &[(99, 10)])\n            .unwrap();\n        feed(\n            &mut engine,\n            br#"{\\"e\\":\\"markPriceUpdate\\",\\"E\\":1,\\"s\\":\\"CXMTUSDT\\",\\"p\\":\\"100\\",\\"i\\":\\"100\\",\\"T\\":600000,\\"r\\":\\"0\\"}"#,\n        );\n        let placed = feed(\n            &mut engine,\n            br#"{\\"e\\":\\"bookTicker\\",\\"u\\":1,\\"E\\":2,\\"T\\":2,\\"s\\":\\"CXMTUSDT\\",\\"b\\":\\"98\\",\\"B\\":\\"5\\",\\"a\\":\\"99\\",\\"A\\":\\"10\\"}"#,\n        );\n        let order = placed\n            .iter()\n            .find(|record| record.kind == "order_placed")\n            .unwrap();\n        assert_eq!(order.queue_ahead_quantity, Some(5));\n\n        let first = feed(\n            &mut engine,\n            br#"{\\"e\\":\\"aggTrade\\",\\"E\\":3,\\"s\\":\\"CXMTUSDT\\",\\"a\\":1,\\"p\\":\\"98\\",\\"q\\":\\"3\\",\\"T\\":3,\\"m\\":true}"#,\n        );\n        assert!(first.is_empty());\n        assert_eq!(engine.summary().current_absolute_position, 0);\n\n        let second = feed(\n            &mut engine,\n            br#"{\\"e\\":\\"aggTrade\\",\\"E\\":4,\\"s\\":\\"CXMTUSDT\\",\\"a\\":2,\\"p\\":\\"98\\",\\"q\\":\\"3\\",\\"T\\":4,\\"m\\":true}"#,\n        );\n        assert_eq!(second.len(), 1);\n        assert_eq!(second[0].quantity, Some(1));\n        assert_eq!(engine.summary().current_absolute_position, 1);\n    }\n\n'''
    replace_once(anchor, test + anchor, "stateful observed queue regression")

if "latency_rejected_trade_does_not_consume_queue" not in text:
    anchor = '''    #[test]\n    fn exchange_arrival_uses_local_receipt_time_not_exchange_event_time() {\n'''
    test = '''    #[test]\n    fn latency_rejected_trade_does_not_consume_queue() {\n        let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {\n            queue: crate::backtest::realism::QueueModel {\n                visible_ahead: 2,\n                trade_through: 0,\n            },\n            latency: crate::backtest::realism::LatencyModel {\n                market_to_decision_ms: 5,\n                decision_to_exchange_ms: 5,\n                cancel_to_exchange_ms: 0,\n            },\n        });\n        feed(\n            &mut engine,\n            br#"{\\"e\\":\\"markPriceUpdate\\",\\"E\\":1,\\"s\\":\\"CXMTUSDT\\",\\"p\\":\\"100\\",\\"i\\":\\"100\\",\\"T\\":600000,\\"r\\":\\"0\\"}"#,\n        );\n        feed(\n            &mut engine,\n            br#"{\\"e\\":\\"bookTicker\\",\\"u\\":1,\\"E\\":2,\\"T\\":2,\\"s\\":\\"CXMTUSDT\\",\\"b\\":\\"98\\",\\"B\\":\\"10\\",\\"a\\":\\"99\\",\\"A\\":\\"10\\"}"#,\n        );\n        let early = feed(\n            &mut engine,\n            br#"{\\"e\\":\\"aggTrade\\",\\"E\\":4,\\"s\\":\\"CXMTUSDT\\",\\"a\\":1,\\"p\\":\\"98\\",\\"q\\":\\"2\\",\\"T\\":4,\\"m\\":true}"#,\n        );\n        assert!(early.is_empty());\n        let at_exchange = feed(\n            &mut engine,\n            br#"{\\"e\\":\\"aggTrade\\",\\"E\\":12,\\"s\\":\\"CXMTUSDT\\",\\"a\\":2,\\"p\\":\\"98\\",\\"q\\":\\"2\\",\\"T\\":12,\\"m\\":true}"#,\n        );\n        assert!(at_exchange.is_empty());\n        let through = feed(\n            &mut engine,\n            br#"{\\"e\\":\\"aggTrade\\",\\"E\\":13,\\"s\\":\\"CXMTUSDT\\",\\"a\\":3,\\"p\\":\\"98\\",\\"q\\":\\"1\\",\\"T\\":13,\\"m\\":true}"#,\n        );\n        assert_eq!(through[0].quantity, Some(1));\n    }\n\n'''
    replace_once(anchor, test + anchor, "latency queue regression")

RUNTIME.write_text(text, encoding="utf-8")
print("stateful maker queue repair complete")
