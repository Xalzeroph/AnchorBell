from pathlib import Path

path = Path(__file__).resolve().parents[2] / "engine" / "src" / "simulation" / "runtime.rs"
text = path.read_text(encoding="utf-8")
text = text.replace("backtest::{MakerQuote, TopOfBook}", "backtest::TopOfBook", 1)


def replace_test(name: str, next_name: str, replacement: str) -> None:
    global text
    start_marker = f"    #[test]\n    fn {name}() {{\n"
    end_marker = f"    #[test]\n    fn {next_name}() {{\n"
    start = text.find(start_marker)
    end = text.find(end_marker, start + 1)
    if start < 0 or end < 0:
        if replacement in text:
            return
        raise SystemExit(f"cannot locate test block {name}")
    text = text[:start] + replacement + "\n" + text[end:]


replace_test(
    "latency_rejected_trade_does_not_consume_queue",
    "exchange_arrival_uses_local_receipt_time_not_exchange_event_time",
    '''    #[test]
    fn latency_rejected_trade_does_not_consume_queue() {
        let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {
            queue: crate::backtest::realism::QueueModel {
                visible_ahead: 2,
                trade_through: 0,
            },
            latency: crate::backtest::realism::LatencyModel {
                market_to_decision_ms: 5,
                decision_to_exchange_ms: 5,
                cancel_to_exchange_ms: 0,
            },
        });
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
        let early = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":1,"p":"98","q":"2","T":4,"m":true}"#,
        );
        assert!(early.is_empty());
        let at_exchange = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":12,"s":"CXMTUSDT","a":2,"p":"98","q":"2","T":12,"m":true}"#,
        );
        assert!(at_exchange.is_empty());
        let through = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":13,"s":"CXMTUSDT","a":3,"p":"98","q":"1","T":13,"m":true}"#,
        );
        assert_eq!(through[0].quantity, Some(1));
    }
''',
)

replace_test(
    "observed_queue_is_consumed_across_multiple_compatible_trades",
    "seeded_local_depth_caps_fill_at_the_order_price",
    '''    #[test]
    fn observed_queue_is_consumed_across_multiple_compatible_trades() {
        let mut engine = engine();
        engine
            .load_depth_snapshot("CXMTUSDT", 10, &[(98, 5)], &[(99, 10)])
            .unwrap();
        feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
        let placed = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"5","a":"99","A":"10"}"#,
        );
        let order = placed
            .iter()
            .find(|record| record.kind == "order_placed")
            .unwrap();
        assert_eq!(order.queue_ahead_quantity, Some(5));

        let first = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
        );
        assert!(first.is_empty());
        assert_eq!(engine.summary().current_absolute_position, 0);

        let second = feed(
            &mut engine,
            br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":4,"m":true}"#,
        );
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].quantity, Some(1));
        assert_eq!(engine.summary().current_absolute_position, 1);
    }
''',
)

path.write_text(text, encoding="utf-8")
print("stateful queue regression fixture repair complete")
