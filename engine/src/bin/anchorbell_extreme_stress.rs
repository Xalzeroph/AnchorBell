use std::time::Instant;

use anchorbell_engine::strategy::StrategyProfile;

use anchorbell_engine::execution::{
    parse_user_data_message, ExecutionSupervisor, GateDecision, OrderIntent, SupervisorConfig,
};
use serde_json::json;

fn main() {
    let start = Instant::now();
    let symbols = StrategyProfile::load("config/anchorbell-simulation.json")
        .expect("simulation strategy profile must be valid")
        .symbols;
    let mut supervisor = ExecutionSupervisor::new(SupervisorConfig::default(), symbols.clone())
        .expect("default supervisor configuration is valid");

    for symbol in &symbols {
        supervisor
            .observe_symbol(symbol, 1, 1, true, true, true, u64::MAX, 0)
            .expect("fixed symbol must be accepted");
    }
    supervisor
        .reconciliation_clean()
        .expect("initial reconciliation must complete");

    let intent = OrderIntent::maker_buy(7, 100, 1);
    let mut allowed = 0_u64;
    let mut rejected = 0_u64;
    for tick in 1..=1_000_000_u64 {
        let timestamp = tick + 1;
        supervisor
            .observe_symbol(
                "CXMTUSDT",
                timestamp,
                timestamp,
                true,
                true,
                true,
                u64::MAX,
                0,
            )
            .expect("target symbol remains available");
        match supervisor.evaluate("CXMTUSDT", intent, timestamp) {
            GateDecision::Allow => allowed += 1,
            _ => rejected += 1,
        }
    }

    let order = br#"{"e":"ORDER_TRADE_UPDATE","E":100,"T":101,"o":{"s":"CXMTUSDT","c":"anchorbell-stress","i":7,"S":"BUY","o":"LIMIT","f":"GTX","X":"NEW","x":"NEW","z":"0","l":"0","ap":"0","R":false}}"#;
    for _ in 0..100_000 {
        let event = parse_user_data_message(order).expect("valid order event");
        supervisor
            .on_user_data(event)
            .expect("valid order event must be accepted");
    }

    let malformed = br#"{"e":"ORDER_TRADE_UPDATE","E":100,"T":101,"o":{"s":"BTCUSDT","c":"bad","i":7,"S":"BUY","o":"MARKET","f":"GTC","X":"NEW","x":"NEW","z":"0","l":"0","ap":"0","R":false}}"#;
    let malformed_event = parse_user_data_message(malformed).expect("wire event remains parseable");
    let malformed_result = supervisor.on_user_data(malformed_event);
    let output = json!({
        "event": "extreme_stress_complete",
        "gate_iterations": 1_000_000_u64,
        "user_events": 100_000_u64,
        "allowed": allowed,
        "rejected": rejected,
        "tracked_orders": supervisor.tracked_order_count(),
        "malformed_event_rejected": malformed_result.is_err(),
        "state_after_malformed": format!("{:?}", supervisor.state()),
        "elapsed_ms": start.elapsed().as_millis(),
        "state": format!("{:?}", supervisor.state()),
        "expected_symbols": symbols.len(),
    });
    println!("{}", output);
    assert_eq!(allowed, 1_000_000);
    assert_eq!(rejected, 0);
    assert_eq!(supervisor.tracked_order_count(), 1);
    assert!(malformed_result.is_err());
    assert_eq!(format!("{:?}", supervisor.state()), "Halted");
}
