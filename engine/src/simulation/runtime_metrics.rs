use super::*;

impl SimulationEngine {
    pub fn metrics_snapshot(
        &self,
        observed_at_ms: u64,
        last_received_at_ms: u64,
    ) -> MetricsSnapshot {
        let symbols = self
            .states
            .iter()
            .map(|(symbol, state)| {
                let (requested_quantity, max_position) = self
                    .position_allocations
                    .get(symbol)
                    .map(|allocation| (allocation.requested_quantity, allocation.max_position))
                    .unwrap_or((self.requested_quantity, self.max_position));
                let quote_quantity = state
                    .book
                    .map(|book| {
                        liquidity_adjusted_quantity(
                            requested_quantity,
                            book.bid_quantity,
                            book.ask_quantity,
                        )
                    })
                    .unwrap_or(requested_quantity);
                let (bid_price_ticks, ask_price_ticks) = state
                    .book
                    .map(|book| (Some(book.bid_price_ticks), Some(book.ask_price_ticks)))
                    .unwrap_or((None, None));
                let fair_value = fair_value_for_state(state);
                let threshold_diagnostic = dynamic_threshold_diagnostic_for(
                    state,
                    self.strategy_variant,
                    self.strategy.entry_threshold_bps,
                    self.fee_ppm,
                    quote_quantity,
                    max_position,
                    self.last_event_at_ms,
                );
                let threshold = threshold_diagnostic
                    .threshold
                    .map(|threshold| scale_threshold_non_fee(threshold, self.threshold_scale_ppm));
                let calendar_state = calendar_state_for(symbol, self.last_event_at_ms);
                let data_quality =
                    data_quality_for(state, self.last_event_at_ms, self.max_mark_index_gap_bps);
                let equity_entry_allowed = !self.live_risk_gates
                    || simulation_session_allows_entry(symbol, self.last_event_at_ms);
                let funding_known = !self.live_risk_gates
                    || (state.next_funding_time_ms > self.last_event_at_ms
                        && state.latest_funding_rate_e8.is_some());
                let anchor_allowed = state
                    .anchor
                    .valid_at(self.last_event_at_ms, self.max_anchor_age_ms)
                    && (!self.live_risk_gates
                        || state.anchor.observed_at_ms == 0
                        || simulation_anchor_usable(
                            symbol,
                            state.anchor.observed_at_ms,
                            self.last_event_at_ms,
                        ));
                let funding_decision =
                    m8_funding_decision(state, self.last_event_at_ms, max_position, self.fee_ppm);
                let funding_overlay = evaluate_funding_overlay(
                    funding_decision.action,
                    if state.latest_funding_rate_e8.is_some() {
                        crate::m8::FundingRateStatus::Observed
                    } else {
                        crate::m8::FundingRateStatus::Missing
                    },
                    funding_decision.funding_carry_bps,
                    state.position,
                );
                let funding_controller_active = self.funding_controller_active();
                let funding_allowed = !self.live_risk_gates
                    || if funding_controller_active {
                        funding_overlay.allow_base_strategy
                    } else {
                        self.funding_entry_allowed_for_strategy(state, self.last_event_at_ms)
                    };
                let risk_state = if !equity_entry_allowed {
                    SimulationRiskState::ReduceOnlyEquitySession
                } else if !matches!(data_quality, DataQualityStatus::Fresh) {
                    SimulationRiskState::HaltMarketData
                } else if !anchor_allowed {
                    SimulationRiskState::HaltAnchor
                } else if self.symbol_drawdown_hard_bps > 0
                    && self.symbol_drawdown_bps(symbol) >= self.symbol_drawdown_hard_bps
                {
                    SimulationRiskState::ReduceOnlySymbolDrawdown
                } else if self.strategy_variant.uses_tail_guard() && m5_tail_reduce_only(state) {
                    SimulationRiskState::ReduceOnlyTailRisk
                } else if !funding_known {
                    SimulationRiskState::HaltFundingMetadata
                } else if funding_controller_active {
                    match funding_overlay.state {
                        crate::risk::FundingRiskState::ReduceOnly => {
                            SimulationRiskState::ReduceOnlyFundingRisk
                        }
                        crate::risk::FundingRiskState::Adverse => {
                            SimulationRiskState::NoEntryFunding
                        }
                        crate::risk::FundingRiskState::Halt => {
                            SimulationRiskState::HaltFundingMetadata
                        }
                        crate::risk::FundingRiskState::Neutral
                        | crate::risk::FundingRiskState::Favorable => SimulationRiskState::Trading,
                    }
                } else if !funding_allowed {
                    SimulationRiskState::ReduceOnlyFundingDeadline
                } else {
                    SimulationRiskState::Trading
                };
                let anchor_age_ms = (state.anchor.observed_at_ms > 0).then(|| {
                    self.last_event_at_ms
                        .saturating_sub(state.anchor.observed_at_ms)
                });
                // Signal age is measured entirely on the exchange clock.
                // Receipt timestamps remain transport telemetry only.
                let mark_age_ms = (state.last_mark_time_ms > 0).then(|| {
                    self.last_event_at_ms
                        .saturating_sub(state.last_mark_time_ms)
                });
                let book_age_ms = (state.last_book_event_at_ms > 0).then(|| {
                    self.last_event_at_ms
                        .saturating_sub(state.last_book_event_at_ms)
                });
                let reference_ticks = fair_value
                    .map(|estimate| estimate.price.0)
                    .unwrap_or(state.anchor.close_price_ticks);
                let buy_edge_pico_bps =
                    bid_price_ticks.and_then(|price| edge_pico_bps(reference_ticks, price));
                let sell_edge_pico_bps =
                    ask_price_ticks.and_then(|price| edge_pico_bps(price, reference_ticks));
                let buy_edge_bps = buy_edge_pico_bps.map(pico_bps_to_bps);
                let sell_edge_bps = sell_edge_pico_bps.map(pico_bps_to_bps);
                let buy_edge_micro_bps = buy_edge_pico_bps.map(pico_bps_to_micro);
                let sell_edge_micro_bps = sell_edge_pico_bps.map(pico_bps_to_micro);
                let adverse_markout_upper_pico_bps = conservative_adverse_markout_pico_bps(state);
                let entry_block_reason = entry_block_reason_for(
                    state,
                    risk_state,
                    threshold,
                    threshold_diagnostic.status,
                    state.adaptive_relief_pico_bps,
                    buy_edge_pico_bps,
                    sell_edge_pico_bps,
                );

                let labels = PortfolioDrawdownGuard::metric_labels(
                    self.portfolio_drawdown_guard.as_ref(),
                    risk_state.label(),
                    entry_block_reason,
                );
                let symbol_drawdown_bps = self.symbol_drawdown_bps(symbol);

                SymbolMetrics {
                    symbol: symbol.clone(),
                    position_mode: self
                        .position_allocations
                        .get(symbol)
                        .map(|allocation| allocation.mode.clone()),
                    allocated_capital_usdt_ticks: self
                        .position_allocations
                        .get(symbol)
                        .filter(|allocation| allocation.budget_usdt_ticks > 0)
                        .map(|allocation| allocation.budget_usdt_ticks),
                    allocated_capital_usdt: self
                        .position_allocations
                        .get(symbol)
                        .filter(|allocation| allocation.budget_usdt_ticks > 0)
                        .map(|allocation| {
                            crate::execution::binance_wire::format_ticks(
                                allocation.budget_usdt_ticks,
                                self.price_scale,
                            )
                        }),
                    target_quantity: self
                        .position_allocations
                        .get(symbol)
                        .map(|allocation| allocation.requested_quantity),
                    target_quantity_units: self.position_allocations.get(symbol).map(
                        |allocation| {
                            crate::execution::binance_wire::format_ticks(
                                allocation.requested_quantity,
                                self.quantity_scale,
                            )
                        },
                    ),
                    position_notional_usdt_ticks: state.mark_price_ticks.map(|price| {
                        clamp_i128(
                            i128::from(price.abs()) * i128::from(state.position.abs())
                                / quantity_scale_multiplier(self.quantity_scale),
                        )
                    }),
                    position_notional_usdt: state.mark_price_ticks.map(|price| {
                        let notional_ticks = clamp_i128(
                            i128::from(price.abs()) * i128::from(state.position.abs())
                                / quantity_scale_multiplier(self.quantity_scale),
                        );
                        crate::execution::binance_wire::format_ticks(
                            notional_ticks,
                            self.price_scale,
                        )
                    }),
                    position: state.position,
                    fills: state.fills,
                    winning_fills: state.winning_fills,
                    losing_fills: state.losing_fills,
                    realized_pnl_ticks: state.realized_pnl_ticks,
                    unrealized_pnl_ticks: unrealized_pnl(state, self.quantity_scale).unwrap_or(0),
                    market_pnl_ticks: state.market_pnl_ticks,
                    strategy_pnl_ticks: state.strategy_pnl_ticks,
                    funding_pnl_ticks: state.funding_pnl_ticks,
                    fees_ticks: state.fees_ticks,
                    net_pnl_ticks: state
                        .market_pnl_ticks
                        .saturating_add(state.strategy_pnl_ticks)
                        .saturating_add(state.funding_pnl_ticks)
                        .saturating_sub(state.fees_ticks),
                    symbol_drawdown_bps,
                    risk_metrics: None,
                    anchor_age_ms,
                    anchor_final_close: state.anchor.observed_at_ms == 0
                        || anchor_refresh_allowed(symbol, state.anchor.observed_at_ms),
                    calendar_state: calendar_state.to_owned(),
                    next_funding_time_ms: state.next_funding_time_ms,
                    latest_funding_rate_e8: state.latest_funding_rate_e8,
                    funding_flatten_deadline_ms: (!funding_controller_active)
                        .then(|| {
                            funding_flatten_deadline(
                                state.next_funding_time_ms,
                                self.funding_lead_ms,
                            )
                        })
                        .flatten(),
                    funding_action: if self.strategy_variant.uses_funding_controller()
                        && !self.funding_controller_enabled
                    {
                        "Ablated".to_owned()
                    } else {
                        format!("{:?}", funding_decision.action)
                    },
                    funding_carry_bps: if funding_controller_active {
                        funding_decision.funding_carry_bps
                    } else {
                        0
                    },
                    funding_net_edge_bps: if funding_controller_active {
                        funding_decision.net_edge_bps
                    } else {
                        0
                    },
                    risk_state: labels.0.to_owned(),
                    entry_block_reason: labels.1.to_owned(),
                    data_quality,
                    mark_age_ms,
                    book_age_ms,
                    bid_price_ticks,
                    ask_price_ticks,
                    anchor_price_ticks: state.anchor.close_price_ticks,
                    mark_price_ticks: state.mark_price_ticks,
                    index_price_ticks: state.index_price_ticks,
                    ewma_abs_return_bps: state.ewma_abs_return_bps,
                    ewma_spread_bps: state.ewma_spread_bps,
                    ewma_abs_return_micro_bps: state.ewma_abs_return_micro_bps,
                    ewma_spread_micro_bps: state.ewma_spread_micro_bps,
                    ewma_abs_return_pico_bps: state.ewma_abs_return_pico_bps,
                    ewma_spread_pico_bps: state.ewma_spread_pico_bps,
                    ewma_signed_return_pico_bps: state.ewma_signed_return_pico_bps,
                    ewma_signed_residual_pico_bps: state.ewma_signed_residual_pico_bps,
                    ewma_residual_drift_pico_bps: state.ewma_residual_drift_pico_bps,
                    ewma_signed_residual_drift_pico_bps: state.ewma_signed_residual_drift_pico_bps,
                    ewma_residual_curvature_pico_bps: state.ewma_residual_curvature_pico_bps,
                    ewma_residual_persistence_ppm: state.ewma_residual_persistence_ppm,
                    residual_regime_risk_pico_bps: residual_regime_risk_pico_bps(state, Side::Buy)
                        .max(residual_regime_risk_pico_bps(state, Side::Sell)),
                    residual_regime_scale_ppm: residual_regime_scale_ppm(state),
                    trend_persistence_bps: trend_persistence_bps(state),
                    buy_trend_conflict_pico_bps: trend_conflict_pico_bps(state, Side::Buy),
                    sell_trend_conflict_pico_bps: trend_conflict_pico_bps(state, Side::Sell),
                    buy_market_trend_conflict_pico_bps: self
                        .market_trend_conflict_pico_bps(symbol, Side::Buy),
                    sell_market_trend_conflict_pico_bps: self
                        .market_trend_conflict_pico_bps(symbol, Side::Sell),
                    reversion_evidence_lower_bps: reversion_evidence_lower_bps(state),
                    reversion_evidence_scale_ppm: reversion_evidence_scale_ppm(state),
                    ewma_adverse_markout_bps: pico_bps_to_bps(state.ewma_adverse_markout_pico_bps),
                    ewma_adverse_markout_micro_bps: pico_bps_to_micro(
                        state.ewma_adverse_markout_pico_bps,
                    ),
                    ewma_adverse_markout_pico_bps: state.ewma_adverse_markout_pico_bps,
                    adverse_markout_upper_pico_bps,
                    buy_adverse_markout_upper_pico_bps:
                        conservative_adverse_markout_pico_bps_for_side(state, Side::Buy),
                    sell_adverse_markout_upper_pico_bps:
                        conservative_adverse_markout_pico_bps_for_side(state, Side::Sell),
                    evaluated_markouts: state.evaluated_markouts,
                    adverse_markouts: state.adverse_markouts,
                    adaptive_relief_bps: state.adaptive_relief_bps,
                    adaptive_relief_micro_bps: state.adaptive_relief_micro_bps,
                    adaptive_relief_pico_bps: state.adaptive_relief_pico_bps,
                    buy_edge_bps,
                    sell_edge_bps,
                    buy_edge_micro_bps,
                    sell_edge_micro_bps,
                    buy_edge_pico_bps,
                    sell_edge_pico_bps,
                    liquidity_ratio_bps: state.book.map(|book| {
                        liquidity_ratio_bps(quote_quantity, book.bid_quantity, book.ask_quantity)
                    }),
                    liquidity_penalty_bps: state.book.map(|book| {
                        liquidity_penalty_bps(quote_quantity, book.bid_quantity, book.ask_quantity)
                    }),
                    liquidity_fill_probability_bps: state.book.map(|book| {
                        fill_probability_bps(quote_quantity, book.bid_quantity, book.ask_quantity)
                    }),
                    empirical_fill_probability_lcb_bps: empirical_fill_probability_lcb_bps(state),
                    buy_empirical_fill_probability_lcb_bps:
                        empirical_fill_probability_lcb_bps_for_side(state, Side::Buy),
                    sell_empirical_fill_probability_lcb_bps:
                        empirical_fill_probability_lcb_bps_for_side(state, Side::Sell),
                    fair_value_ticks: fair_value.map(|estimate| estimate.price.0),
                    fair_value_confidence_bps: fair_value.map(|estimate| estimate.confidence_bps),
                    market_regime: fair_value.map(|estimate| estimate.regime.label().to_owned()),
                    threshold_status: threshold_diagnostic.status.label().to_owned(),
                    threshold_prior_used: threshold_diagnostic.prior_used,
                    threshold_missing_component: threshold_diagnostic
                        .missing_component
                        .map(str::to_owned),
                    m9_calibration: state
                        .calibration
                        .snapshot(ppm_to_pico_bps(self.fee_ppm.saturating_mul(2))),
                    threshold: threshold.map(threshold_metrics),
                }
            })
            .collect();
        MetricsSnapshot {
            observed_at_ms,
            strategy_variant: self.strategy_variant.label().to_owned(),
            last_market_event_at_ms: self.last_event_at_ms,
            last_received_at_ms,
            summary: self.summary(),
            symbols,
            history: Vec::new(),
            risk_metrics: None,
            portfolio_drawdown: self
                .portfolio_drawdown_guard
                .as_ref()
                .map(PortfolioDrawdownGuard::snapshot),
            calendar_snapshot: "sse-hkex-2026".to_owned(),
            maker_fee_source: self.fee_schedule_source.clone(),
            taker_fee_source: self.fee_schedule_source.clone(),
            funding_model: "m8_exact_mark_settlement_plus_strategy_funding_controller".to_owned(),
            capital_usdt_ticks: self.capital_usdt_ticks,
            capital_usdt: self.capital_usdt_ticks.map(|capital| {
                crate::execution::binance_wire::format_ticks(capital, self.price_scale)
            }),
            model_assumptions: ModelAssumptions {
                fill_model:
                    "stateful_fifo_observed_depth_plus_synthetic_queue_then_aggregate_trade"
                        .to_owned(),
                queue_ahead: self.realism.queue.visible_ahead,
                trade_through: self.realism.queue.trade_through,
                market_to_decision_ms: self.realism.latency.market_to_decision_ms,
                decision_to_exchange_ms: self.realism.latency.decision_to_exchange_ms,
                cancel_to_exchange_ms: self.realism.latency.cancel_to_exchange_ms,
            },
        }
    }
}
