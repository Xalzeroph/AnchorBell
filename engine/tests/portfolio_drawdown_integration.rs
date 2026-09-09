use std::collections::BTreeMap;

use anchorbell_engine::{
    execution::EmergencyExecutionPolicy,
    market::binance::parse_market_message,
    simulation::{AnchorSnapshot, SimulationEngine, SimulationPolicyVariant},
};

fn event(json: &str) -> anchorbell_engine::market::binance::BinanceMarketEvent {
    parse_market_message(json.as_bytes(), 0, 0).expect("valid market fixture")
}

fn anchors() -> BTreeMap<String, AnchorSnapshot> {
    ["CXMTUSDT", "UNITREEUSDT"]
        .into_iter()
        .map(|symbol| {
            (
                symbol.to_owned(),
                AnchorSnapshot {
                    close_price_ticks: 100,
                    observed_at_ms: 0,
                    valid_until_ms: 0,
                },
            )
        })
        .collect()
}

#[test]
fn portfolio_drawdown_uses_canonical_net_pnl_and_propagates_cross_symbol() {
    let mut engine = SimulationEngine::new(
        anchors(),
        100,
        100,
        10,
        20,
        0,
        0,
        0,
        EmergencyExecutionPolicy {
            max_slippage_bps: 25,
            max_participation_bps: 5_000,
            minimum_maker_confidence_bps: 7_000,
            cooldown_ms: 1_000,
            safety_buffer_ms: 1_000,
            taker_fee_ppm: 400,
            urgency_cost_bps_per_second: 2,
            deadline_penalty_bps: 100,
            cost_margin_bps: 1,
        },
    )
    .unwrap()
    .with_strategy_variant(SimulationPolicyVariant::M0Fixed)
    .with_portfolio_drawdown_limits_bps(100, 1_000, 9_000)
    .unwrap();

    for symbol in ["CXMTUSDT", "UNITREEUSDT"] {
        let mark = format!(
            r#"{{"e":"markPriceUpdate","E":1,"s":"{symbol}","p":"100","i":"100","r":"0","T":600000}}"#
        );
        engine.on_event(event(&mark));
    }

    let a_quote = engine.on_event(event(
        r#"{"e":"bookTicker","E":2,"T":2,"u":1,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
    ));
    assert!(a_quote
        .iter()
        .any(|record| { record.symbol == "CXMTUSDT" && record.kind == "order_placed" }));

    let b_quote = engine.on_event(event(
        r#"{"e":"bookTicker","E":2,"T":2,"u":1,"s":"UNITREEUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
    ));
    assert!(b_quote
        .iter()
        .any(|record| { record.symbol == "UNITREEUSDT" && record.kind == "order_placed" }));

    let fill = engine.on_event(event(
        r#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"10","T":3,"m":true}"#,
    ));
    assert!(fill
        .iter()
        .any(|record| record.symbol == "CXMTUSDT" && record.kind == "fill"));

    // The adverse mark moves A from +20 ticks of execution alpha to -80 ticks
    // of canonical net PnL. The portfolio peak-to-current loss is therefore
    // 100 ticks on a 100-tick capital base and must hard-stop immediately.
    let breach = engine.on_event(event(
        r#"{"e":"markPriceUpdate","E":4,"s":"CXMTUSDT","p":"90","i":"90","r":"0","T":600000}"#,
    ));

    // B received no new market event. Its opening maker quote must still be
    // canceled in the same A event cycle by portfolio-level propagation.
    assert!(breach
        .iter()
        .any(|record| { record.symbol == "UNITREEUSDT" && record.kind == "order_canceled" }));

    let snapshot = engine.metrics_snapshot(4, 4);
    let drawdown = snapshot
        .portfolio_drawdown
        .expect("configured drawdown governor must be observable");
    assert_eq!(drawdown.action, "portfolio_drawdown_hard_stop");
    assert_eq!(
        drawdown.current_mark_to_market_pnl_ticks,
        Some(snapshot.summary.net_pnl_ticks)
    );
    assert_ne!(
        snapshot.summary.net_pnl_ticks,
        snapshot
            .summary
            .net_pnl_ticks
            .saturating_add(snapshot.summary.unrealized_pnl_ticks),
        "fixture must distinguish canonical net PnL from the old double-counted value"
    );

    let b_metrics = snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol == "UNITREEUSDT")
        .unwrap();
    assert_eq!(b_metrics.risk_state, "portfolio_drawdown_hard_stop");
    assert_eq!(b_metrics.entry_block_reason, "portfolio_drawdown_hard_stop");
}
