use super::{entry_restriction_reason, position_requires_reduction};

#[test]
fn flat_risk_gate_does_not_enter_maker_exit_path() {
    assert!(!position_requires_reduction(0, false, true, false, false));
    assert_eq!(
        entry_restriction_reason(0, false, true),
        "equity_session_open"
    );
    assert_eq!(
        entry_restriction_reason(0, true, false),
        "funding_entry_blocked"
    );
    assert!(position_requires_reduction(10, false, true, false, false));
}

use super::*;
use crate::market::binance::parse_market_message;

fn anchors() -> BTreeMap<String, AnchorSnapshot> {
    [(
        "CXMTUSDT".to_owned(),
        AnchorSnapshot {
            close_price_ticks: 100,
            observed_at_ms: 0,
            valid_until_ms: 0,
        },
    )]
    .into_iter()
    .collect()
}

fn engine() -> SimulationEngine {
    SimulationEngine::new(
        anchors(),
        100,
        100,
        10,
        20,
        0,
        0,
        0,
        EmergencyExecutionPolicy::default(),
    )
    .unwrap()
    .with_strategy_variant(SimulationPolicyVariant::M0Fixed)
    .with_funding_intervals([("CXMTUSDT".to_owned(), 8)].into_iter().collect())
    .with_funding_lead_ms(5 * 60 * 1_000)
}

fn feed(engine: &mut SimulationEngine, raw: &[u8]) -> Vec<SimulationRecord> {
    let event = parse_market_message(raw, 0, 0).unwrap();
    engine.on_event(event)
}

#[test]
fn replay_realism_applies_queue_and_entry_latency() {
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
    let too_early = feed(
        &mut engine,
        br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":4,"m":true}"#,
    );
    assert!(too_early.is_empty());
    let queued = feed(
        &mut engine,
        br#"{"e":"aggTrade","E":12,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":12,"m":true}"#,
    );
    assert_eq!(queued[0].quantity, Some(1));
    assert_eq!(engine.summary().current_absolute_position, 1);
}

#[test]
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

#[test]
fn exchange_arrival_uses_local_receipt_time_not_exchange_event_time() {
    let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {
        queue: crate::backtest::realism::QueueModel::default(),
        latency: crate::backtest::realism::LatencyModel {
            market_to_decision_ms: 5,
            decision_to_exchange_ms: 5,
            cancel_to_exchange_ms: 0,
        },
    });
    let mark = parse_market_message(
        br#"{"e":"markPriceUpdate","E":100,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        0,
        0,
    )
    .unwrap();
    engine.on_event_at_ref(&mark, 100);
    let book = parse_market_message(
            br#"{"e":"bookTicker","u":1,"E":1000,"T":1000,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
            0,
            0,
        )
        .unwrap();
    engine.on_event_at_ref(&book, 3_000);
    let too_early = parse_market_message(
        br#"{"e":"aggTrade","E":2050,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":2050,"m":true}"#,
        0,
        0,
    )
    .unwrap();
    assert!(engine.on_event_at_ref(&too_early, 3_050).is_empty());
    assert_eq!(engine.summary().current_absolute_position, 0);
    let available = parse_market_message(
        br#"{"e":"aggTrade","E":3020,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":3020,"m":true}"#,
        0,
        0,
    )
    .unwrap();
    assert_eq!(engine.on_event_at_ref(&available, 3_020).len(), 1);
    assert_eq!(engine.summary().current_absolute_position, 3);
}

#[test]
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

#[test]
fn seeded_local_depth_caps_fill_at_the_order_price() {
    let mut engine = engine();
    engine
        .load_depth_snapshot("CXMTUSDT", 10, &[(98, 1)], &[(99, 10)])
        .unwrap();
    feed(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    feed(
            &mut engine,
            br#"{"e":"depthUpdate","E":3,"T":3,"s":"CXMTUSDT","U":11,"u":11,"pu":10,"b":[["98","1"]],"a":[]}"#,
        );
    let filled = feed(
        &mut engine,
        br#"{"e":"aggTrade","E":12,"s":"CXMTUSDT","a":1,"p":"98","q":"5","T":12,"m":true}"#,
    );
    assert_eq!(filled[0].quantity, Some(1));
}

#[test]
fn simulation_order_fills_only_on_compatible_aggressor_at_exact_price() {
    let mut engine = engine();
    feed(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    let placed = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    assert!(placed.iter().any(|record| record.kind == "decision"));
    assert!(placed.iter().any(|record| record.kind == "order_placed"));
    let wrong_side = feed(
        &mut engine,
        br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":false}"#,
    );
    assert!(wrong_side.is_empty());
    let filled = feed(
        &mut engine,
        br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":2,"p":"98","q":"3","T":4,"m":true}"#,
    );
    assert_eq!(filled.len(), 1);
    assert_eq!(filled[0].quantity, Some(3));
    assert_eq!(engine.summary().current_absolute_position, 3);
}

#[test]
fn funding_deadline_cancels_new_risk_before_settlement() {
    let mut engine = engine().with_live_risk_gates();
    feed(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    let placed = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    assert!(placed.iter().any(|record| record.kind == "order_placed"));
    feed(
            &mut engine,
            br#"{"e":"markPriceUpdate","E":299999,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        );
    let canceled = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":300001,"T":300001,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    assert!(canceled.iter().any(|record| record.kind == "decision"));
    assert!(canceled
        .iter()
        .any(|record| record.kind == "order_canceled"));
    assert_eq!(engine.summary().working_orders, 0);
}

#[test]
fn summary_includes_mark_to_market_for_open_position() {
    let mut engine = engine();
    feed(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    feed(
        &mut engine,
        br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
    );
    let summary = engine.summary();
    assert_eq!(summary.current_absolute_position, 3);
    assert_eq!(summary.unrealized_pnl_ticks, 6);
    assert_eq!(summary.net_pnl_ticks, 6);
    assert!(summary.unrealized_valuation_complete);
    assert!(!summary.flat_at_end);
}

#[test]
fn quantity_precision_is_applied_to_mark_to_market_pnl() {
    let mut engine = SimulationEngine::new(
        anchors(),
        100,
        1_000,
        300,
        20,
        0,
        0,
        2,
        EmergencyExecutionPolicy::default(),
    )
    .unwrap()
    .with_strategy_variant(SimulationPolicyVariant::M0Fixed);
    let feed_scaled = |engine: &mut SimulationEngine, raw: &[u8]| {
        let event = parse_market_message(raw, 0, 2).unwrap();
        engine.on_event(event)
    };
    feed_scaled(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    feed_scaled(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    feed_scaled(
        &mut engine,
        br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
    );
    let summary = engine.summary();
    assert_eq!(summary.current_absolute_position, 300);
    assert_eq!(summary.unrealized_pnl_ticks, 6);
}

#[test]
fn replay_cancels_working_quotes_at_window_end_without_faking_a_fill() {
    let path = std::env::temp_dir().join("anchorbell-simulation-replay-eof.jsonl");
    std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1000,\"r\":\"0\"}\n{\"e\":\"bookTicker\",\"u\":1,\"E\":2,\"T\":2,\"s\":\"CXMTUSDT\",\"b\":\"98\",\"B\":\"10\",\"a\":\"99\",\"A\":\"10\"}\n",
        )
        .unwrap();
    let result = replay_jsonl(
        &path,
        None,
        anchors(),
        0,
        0,
        100,
        100,
        10,
        20,
        0,
        0,
        EmergencyExecutionPolicy::default(),
    )
    .unwrap();
    assert_eq!(result.order_count, 1);
    assert_eq!(result.fill_count, 0);
    assert_eq!(result.working_orders, 0);
    assert!(result.flat_at_end);
    let _ = std::fs::remove_file(path);
}

#[test]
fn simulation_replay_rejects_symbols_without_configured_state() {
    let path = std::env::temp_dir().join("anchorbell-simulation-replay-symbol.jsonl");
    std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"XYZUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1000,\"r\":\"0\"}\n",
        )
        .unwrap();
    let result = replay_jsonl(
        &path,
        None,
        anchors(),
        0,
        0,
        100,
        100,
        10,
        20,
        0,
        0,
        EmergencyExecutionPolicy::default(),
    );
    assert!(matches!(
        result,
        Err(SimulationError::ReplaySymbolNotConfigured(symbol)) if symbol == "XYZUSDT"
    ));
    let _ = std::fs::remove_file(path);
}

#[test]
fn allocator_performance_penalty_uses_stable_scale() {
    assert_eq!(
        SimulationEngine::stable_post_fee_loss_bps(-50, 10, 10_000),
        50
    );
    assert_eq!(
        SimulationEngine::stable_post_fee_loss_bps(-50, 10, 20_000),
        25
    );
    assert_eq!(
        SimulationEngine::stable_post_fee_loss_bps(-50, 9, 10_000),
        0
    );
}

#[test]
fn fee_efficiency_penalty_is_ratio_based_not_runtime_accumulation() {
    let short = SimulationEngine::fee_efficiency_penalty_bps(1_000, 200, 10);
    let long = SimulationEngine::fee_efficiency_penalty_bps(10_000, 2_000, 100);
    assert_eq!(short, 20);
    assert_eq!(short, long);
    assert_eq!(
        SimulationEngine::fee_efficiency_penalty_bps(-1, 200, 100),
        0
    );
}

#[test]
fn m9_inherits_m7_and_m8_capabilities() {
    let variant = SimulationPolicyVariant::M9DeadlineCausalDroMpc;
    assert!(variant.uses_evidence_gate());
    assert!(variant.uses_funding_controller());
    assert!(engine()
        .with_strategy_variant(variant)
        .funding_controller_active());
}

#[test]
fn dynamic_allocation_deadband_ignores_sub_percent_noise() {
    assert_eq!(
        SimulationEngine::allocation_budget_change_bps(2_000, 2_050, 10_000),
        50
    );
    assert_eq!(
        SimulationEngine::allocation_budget_change_bps(2_000, 2_200, 10_000),
        200
    );
    assert!(
        SimulationEngine::allocation_budget_change_bps(2_000, 2_050, 10_000)
            < SimulationEngine::DYNAMIC_ALLOCATION_REBALANCE_DEADBAND_BPS
    );
}

#[test]
fn simulation_replay_rejects_out_of_order_events() {
    let path = std::env::temp_dir().join("anchorbell-simulation-replay-order.jsonl");
    std::fs::write(
            &path,
            "{\"e\":\"markPriceUpdate\",\"E\":2,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":2,\"r\":\"0\"}\n{\"e\":\"markPriceUpdate\",\"E\":1,\"s\":\"CXMTUSDT\",\"p\":\"100\",\"i\":\"100\",\"T\":1,\"r\":\"0\"}\n",
        )
        .unwrap();
    let result = replay_jsonl(
        &path,
        None,
        anchors(),
        0,
        0,
        100,
        100,
        10,
        20,
        0,
        0,
        EmergencyExecutionPolicy::default(),
    );
    assert!(matches!(
        result,
        Err(SimulationError::ReplayOutOfOrder { .. })
    ));
    let _ = std::fs::remove_file(path);
}

#[test]
fn simulation_marks_beijing_overnight_as_closed_and_usable() {
    let overnight = 1_788_377_934_321_u64;
    let previous_close = overnight - 12 * 60 * 60 * 1_000;
    let next_close = overnight + 12 * 60 * 60 * 1_000;
    assert_eq!(calendar_state_for("CXMTUSDT", overnight), "closed");
    assert!(simulation_session_allows_entry("CXMTUSDT", overnight));
    assert!(simulation_anchor_usable(
        "CXMTUSDT",
        previous_close,
        overnight
    ));
    assert!(!simulation_anchor_usable(
        "CXMTUSDT",
        previous_close,
        next_close
    ));
    assert!(anchor_refresh_allowed("CXMTUSDT", overnight));
}

#[test]
fn next_equity_pre_open_uses_region_specific_session_boundary() {
    let timestamp_ms = 1_788_742_182_000_u64;
    assert_eq!(
        next_equity_pre_open_at_ms("CXMTUSDT", timestamp_ms),
        Some(1_788_743_700_000)
    );
    assert_eq!(
        next_equity_pre_open_at_ms("MINIMAXUSDT", timestamp_ms),
        Some(1_788_742_800_000)
    );
}

#[test]
fn simulation_allows_static_anchor_entries_on_weekends() {
    // 2026-09-05 12:00 Asia/Shanghai (Saturday), within the supported
    // 2026 calendar snapshot.
    let saturday_midday = 1_788_580_800_000_u64;
    assert_eq!(calendar_state_for("CXMTUSDT", saturday_midday), "weekend");
    assert!(simulation_session_allows_entry("CXMTUSDT", saturday_midday));
    assert!(anchor_refresh_allowed("CXMTUSDT", saturday_midday));
    assert!(simulation_anchor_usable(
        "CXMTUSDT",
        saturday_midday,
        saturday_midday + 60 * 60 * 1_000
    ));
}

#[test]
fn capital_allocation_respects_fixed_and_weighted_modes() {
    let anchors = [
        (
            "CXMTUSDT".to_owned(),
            AnchorSnapshot {
                close_price_ticks: 100 * 100_000_000,
                observed_at_ms: 0,
                valid_until_ms: 0,
            },
        ),
        (
            "UNITREEUSDT".to_owned(),
            AnchorSnapshot {
                close_price_ticks: 200 * 100_000_000,
                observed_at_ms: 0,
                valid_until_ms: 0,
            },
        ),
    ]
    .into_iter()
    .collect();
    let modes = [(
        "CXMTUSDT".to_owned(),
        PositionMode::FixedUsdt(2 * 100_000_000),
    )]
    .into_iter()
    .collect();
    let allocations = allocate_positions(&anchors, 10 * 100_000_000, &modes, 8).unwrap();
    assert_eq!(allocations["CXMTUSDT"].budget_usdt_ticks, 2 * 100_000_000);
    assert_eq!(
        allocations["UNITREEUSDT"].budget_usdt_ticks,
        8 * 100_000_000
    );
    assert_eq!(
        allocations
            .values()
            .map(|allocation| allocation.budget_usdt_ticks)
            .sum::<i64>(),
        10 * 100_000_000
    );
    assert_eq!(allocations["CXMTUSDT"].requested_quantity, 2_000_000);
    assert_eq!(allocations["UNITREEUSDT"].requested_quantity, 4_000_000);
}

#[test]
fn simulation_allows_midday_break_anchor_but_not_open_anchor() {
    let midday_break = 1_788_408_258_130_u64;
    let morning_open = 1_788_404_658_130_u64;
    assert!(anchor_reference_allowed("CXMTUSDT", midday_break));
    assert!(anchor_reference_allowed("HK0625USDT", midday_break));
    assert!(!anchor_reference_allowed("CXMTUSDT", morning_open));
    assert!(!anchor_reference_allowed("HK0625USDT", morning_open));
}

#[test]
fn cancel_latency_keeps_order_fillable_until_exchange_ack() {
    let mut engine = engine().with_realism(crate::backtest::realism::RealisticFillModel {
        queue: crate::backtest::realism::QueueModel::default(),
        latency: crate::backtest::realism::LatencyModel {
            market_to_decision_ms: 0,
            decision_to_exchange_ms: 0,
            cancel_to_exchange_ms: 5,
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
    let in_flight = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":2,"E":3,"T":3,"s":"CXMTUSDT","b":"97","B":"10","a":"99","A":"10"}"#,
        );
    assert!(in_flight.iter().any(|record| record.kind == "decision"));
    assert!(!in_flight
        .iter()
        .any(|record| record.kind == "order_canceled"));
    assert_eq!(engine.summary().working_orders, 1);
    let filled = feed(
        &mut engine,
        br#"{"e":"aggTrade","E":4,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":4,"m":true}"#,
    );
    assert_eq!(filled.len(), 1);
    assert_eq!(engine.summary().current_absolute_position, 3);
    let acknowledged = feed(
            &mut engine,
            br#"{"e":"bookTicker","u":3,"E":8,"T":8,"s":"CXMTUSDT","b":"97","B":"10","a":"99","A":"10"}"#,
        );
    assert!(acknowledged
        .iter()
        .any(|record| record.kind == "order_canceled"));
}

#[test]
fn micro_ewma_preserves_sub_basis_point_samples() {
    assert_eq!(ewma_micro(0, 250_000), 250_000);
    assert_eq!(ewma_micro(1_000_000, 500_000), 850_000);
    assert_eq!(micro_bps_to_bps(499_999), 0);
    assert_eq!(micro_bps_to_bps(500_000), 1);
}

#[test]
fn adaptive_relief_never_removes_hard_cost_or_deadline_risk() {
    let base = AdaptiveThreshold::from_components(5, 4, 3, 2, 7, 6, 5, 4, 3, 2, 8, 1).unwrap();
    let hard_cost_micro =
        (base.floor_bps + base.cost_bps + base.deadline_risk_bps) * MICRO_BPS_SCALE;
    let relaxed_required = base
        .required_micro_bps()
        .unwrap()
        .saturating_sub(3 * MICRO_BPS_SCALE);
    assert!(relaxed_required >= hard_cost_micro);
    assert!(relaxed_required < base.required_micro_bps().unwrap());
}

#[test]
fn edge_micro_bps_preserves_fractional_basis_points() {
    let edge = edge_micro_bps(100_001, 100_000).unwrap();
    assert_eq!(edge, 100_000);
    assert_eq!(micro_bps_to_bps(edge), 0);
    assert!(edge_micro_bps(100_000, 100_001).unwrap() < 0);
}

#[test]
fn exchange_clock_freshness_ignores_local_receipt_clock() {
    let mut engine = engine();
    let mark = parse_market_message(
        br#"{"e":"markPriceUpdate","E":100,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
        0,
        0,
    )
    .unwrap();
    engine.on_event_at_ref(&mark, 1_000_000);
    let book = parse_market_message(
            br#"{"e":"bookTicker","u":1,"E":200,"T":200,"s":"CXMTUSDT","b":"98","B":"100","a":"99","A":"100"}"#,
            0,
            0,
        )
        .unwrap();
    engine.on_event_at_ref(&book, 1);
    let metrics = engine.metrics_snapshot(200, 1).symbols[0].clone();
    assert_eq!(metrics.data_quality, DataQualityStatus::Fresh);
    assert_eq!(metrics.mark_age_ms, Some(100));
}

#[test]
fn liquidity_controls_are_continuous_and_monotonic() {
    assert_eq!(liquidity_ratio_bps(10, 100, 100), 1_000);
    assert_eq!(liquidity_penalty_bps(10, 100, 100), 0);
    assert!(liquidity_penalty_bps(50, 100, 100) > liquidity_penalty_bps(25, 100, 100));
    assert!(fill_probability_bps(10, 100, 100) > fill_probability_bps(50, 100, 100));
    assert_eq!(liquidity_adjusted_quantity(100, 100, 100), 100);
}

#[test]
fn tail_quantity_scaling_is_continuous_monotone_and_bounded() {
    let requested = 10_000;
    assert_eq!(m5_scaled_quantity(0, requested), requested);
    assert_eq!(
        m5_scaled_quantity(M5_TAIL_CAUTION_BPS, requested),
        requested
    );
    assert!(m5_scaled_quantity(36, requested) < requested);
    assert!(m5_scaled_quantity(36, requested) > m5_scaled_quantity(59, requested));
    assert_eq!(
        m5_scaled_quantity(M5_TAIL_REDUCE_ONLY_BPS, requested),
        2_500
    );
    assert_eq!(m5_scaled_quantity(M5_TAIL_HALT_BPS, requested), 0);
    for stress in M5_TAIL_CAUTION_BPS..=M5_TAIL_HALT_BPS {
        let quantity = m5_scaled_quantity(stress, requested);
        assert!((0..=requested).contains(&quantity));
        if stress < M5_TAIL_HALT_BPS {
            assert!(quantity > 0);
        }
    }
}

#[test]
fn threshold_status_explains_warmup_and_uses_a_conservative_prior() {
    let mut engine = engine().with_strategy_variant(SimulationPolicyVariant::M7EvidenceGated);
    let initial = engine.metrics_snapshot(1, 1).symbols[0].clone();
    assert_eq!(initial.threshold_status, "warming_up");
    assert_eq!(initial.threshold_missing_component.as_deref(), Some("book"));
    feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    let missing_mark = engine.metrics_snapshot(2, 2).symbols[0].clone();
    assert_eq!(missing_mark.threshold_status, "insufficient_data");
    assert_eq!(
        missing_mark.threshold_missing_component.as_deref(),
        Some("mark_price")
    );
    feed(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":3,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    let warmed = engine.metrics_snapshot(3, 3).symbols[0].clone();
    assert_eq!(warmed.threshold_status, "warming_up");
    assert!(warmed.threshold_prior_used);
    assert!(warmed.threshold.is_some());
}

#[test]
fn shutdown_uses_bounded_reduce_only_flatten_and_reports_flat() {
    let mut engine = engine();
    feed(
        &mut engine,
        br#"{"e":"markPriceUpdate","E":1,"s":"CXMTUSDT","p":"100","i":"100","T":600000,"r":"0"}"#,
    );
    feed(
            &mut engine,
            br#"{"e":"bookTicker","u":1,"E":2,"T":2,"s":"CXMTUSDT","b":"98","B":"10","a":"99","A":"10"}"#,
        );
    feed(
        &mut engine,
        br#"{"e":"aggTrade","E":3,"s":"CXMTUSDT","a":1,"p":"98","q":"3","T":3,"m":true}"#,
    );
    assert_eq!(engine.summary().current_absolute_position, 3);
    let settlement = engine.shutdown(4, "test shutdown");
    assert!(settlement.flatten_requested);
    assert_eq!(settlement.summary.current_absolute_position, 0);
    assert_eq!(settlement.settlement_status, "flat");
    assert!(settlement
        .records
        .iter()
        .any(|record| record.kind == "fill"));
}

#[cfg(test)]
mod walkforward_regression_tests {
    use super::*;

    #[test]
    fn calibration_seed_must_strictly_precede_oos_replay() {
        let mut seed = CalibrationState::new("TEST");
        seed.last_event_time_ms = 100;
        let seeds = BTreeMap::from([("TEST".to_owned(), seed)]);
        assert!(matches!(
            validate_calibration_seed_horizon(&seeds, 100),
            Err(SimulationError::CalibrationSeedNotPrior { .. })
        ));
        assert!(matches!(
            validate_calibration_seed_horizon(&seeds, 99),
            Err(SimulationError::CalibrationSeedNotPrior { .. })
        ));
        assert!(validate_calibration_seed_horizon(&seeds, 101).is_ok());
    }

    #[test]
    fn core_v1_capabilities_are_explicit_and_funding_ablation_is_isolated() {
        assert!(SimulationPolicyVariant::CoreV1.uses_tail_guard());
        assert!(SimulationPolicyVariant::CoreV1.uses_evidence_gate());
        assert!(!SimulationPolicyVariant::CoreV1.uses_microstructure());
        assert!(!SimulationPolicyVariant::CoreV1.uses_fill_gate());
        assert!(!SimulationPolicyVariant::CoreV1.uses_statistical_term());
        assert!(!SimulationPolicyVariant::CoreV1.uses_dynamic_capital());
        assert!(!SimulationPolicyVariant::CoreV1.uses_funding_controller());

        assert!(SimulationPolicyVariant::M8FundingAware.uses_funding_controller());
        assert!(SimulationPolicyVariant::M8FundingDisabled.uses_tail_guard());
        assert!(SimulationPolicyVariant::M8FundingDisabled.uses_evidence_gate());
        assert!(!SimulationPolicyVariant::M8FundingDisabled.uses_funding_controller());
    }
}
