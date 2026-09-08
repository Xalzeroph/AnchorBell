use std::collections::{BTreeMap, BTreeSet};

use crate::observability::{DecisionAudit, DecisionGateAudit, DECISION_AUDIT_SCHEMA_VERSION};

use super::{
    recovery::{RecoveryEvent, RecoveryMachine, RecoveryState},
    OrderIntent, SessionCheckpoint, Side, UserDataEvent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorState {
    Synchronizing,
    Healthy,
    RiskStopped,
    Flattening,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateReason {
    UnknownSymbol,
    NotHealthy,
    NonMakerIntent,
    InvalidIntent,
    MarketStale,
    FxStale,
    AnchorUnavailable,
    EquitySessionOpen,
    FundingUnknown,
    FundingWindow,
    PositionLimit,
    ResidualExposure,
    UnknownRemoteState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    Allow,
    NoAction(GateReason),
    Halt(GateReason),
    Flatten(GateReason),
}

impl GateReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::UnknownSymbol => "unknown_symbol",
            Self::NotHealthy => "not_healthy",
            Self::NonMakerIntent => "non_maker_intent",
            Self::InvalidIntent => "invalid_intent",
            Self::MarketStale => "market_stale",
            Self::FxStale => "fx_stale",
            Self::AnchorUnavailable => "anchor_unavailable",
            Self::EquitySessionOpen => "equity_session_open",
            Self::FundingUnknown => "funding_unknown",
            Self::FundingWindow => "funding_window",
            Self::PositionLimit => "position_limit",
            Self::ResidualExposure => "residual_exposure",
            Self::UnknownRemoteState => "unknown_remote_state",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorConfig {
    pub max_market_age_ms: u64,
    pub max_fx_age_ms: u64,
    pub funding_lead_ms: u64,
    pub max_position: i64,
    pub quantity_scale: u32,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            max_market_age_ms: 5_000,
            max_fx_age_ms: 120_000,
            funding_lead_ms: 300_000,
            max_position: 100,
            quantity_scale: 8,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SymbolState {
    market_at_ms: u64,
    fx_at_ms: u64,
    anchor_ready: bool,
    equity_closed: bool,
    funding_known: bool,
    next_funding_at_ms: u64,
    position: i64,
}

#[derive(Debug)]
pub struct ExecutionSupervisor {
    state: SupervisorState,
    config: SupervisorConfig,
    symbols: BTreeMap<String, SymbolState>,
    tracked_orders: BTreeSet<String>,
    last_user_event_at_ms: u64,
    unknown_remote_state: bool,
    recovery: RecoveryMachine,
    next_decision_id: u64,
}

impl ExecutionSupervisor {
    pub fn new<I, S>(config: SupervisorConfig, symbols: I) -> Result<Self, GateReason>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if config.max_market_age_ms == 0
            || config.max_fx_age_ms == 0
            || config.max_position <= 0
            || config.quantity_scale > 18
        {
            return Err(GateReason::InvalidIntent);
        }
        let mut symbol_states = BTreeMap::new();
        for raw_symbol in symbols {
            let symbol = raw_symbol.as_ref().trim().to_ascii_uppercase();
            if symbol.is_empty()
                || symbol_states
                    .insert(
                        symbol,
                        SymbolState {
                            market_at_ms: 0,
                            fx_at_ms: 0,
                            anchor_ready: false,
                            equity_closed: false,
                            funding_known: false,
                            next_funding_at_ms: 0,
                            position: 0,
                        },
                    )
                    .is_some()
            {
                return Err(GateReason::InvalidIntent);
            }
        }
        if symbol_states.is_empty() {
            return Err(GateReason::InvalidIntent);
        }
        Ok(Self {
            state: SupervisorState::Synchronizing,
            config,
            symbols: symbol_states,
            tracked_orders: BTreeSet::new(),
            last_user_event_at_ms: 0,
            unknown_remote_state: false,
            recovery: RecoveryMachine::new(),
            next_decision_id: 1,
        })
    }

    pub fn state(&self) -> SupervisorState {
        self.state
    }

    pub fn tracked_order_count(&self) -> usize {
        self.tracked_orders.len()
    }

    pub fn recovery_epoch(&self) -> Option<super::recovery::RecoveryEpoch> {
        self.recovery.epoch()
    }

    pub fn observe_event_gap(&mut self) {
        self.recovery.record_gap();
        // The gap is unresolved until the authoritative snapshot and order
        // history have been applied; the risk state itself is the gate.
        self.unknown_remote_state = false;
        self.state = SupervisorState::RiskStopped;
    }

    pub fn adopt_remote_position(&mut self, symbol: &str, position: i64) -> Result<(), GateReason> {
        let key = symbol.trim().to_ascii_uppercase();
        let Some(state) = self.symbols.get_mut(key.as_str()) else {
            return Err(GateReason::UnknownSymbol);
        };
        state.position = position;
        self.recovery.record_external_adjustment();
        Ok(())
    }

    pub fn checkpoint(
        &self,
        session_id: impl Into<String>,
        environment: impl Into<String>,
        symbol: &str,
        last_event_at_ms: u64,
    ) -> Result<SessionCheckpoint, GateReason> {
        let key = symbol.trim().to_ascii_uppercase();
        let Some(state) = self.symbols.get(key.as_str()) else {
            return Err(GateReason::UnknownSymbol);
        };
        let mut checkpoint = SessionCheckpoint::new(session_id, environment, key.clone());
        checkpoint.last_event_at_ms = last_event_at_ms;
        checkpoint.position_ticks = state.position;
        checkpoint.gross_position_ticks = state.position.checked_abs().unwrap_or(i64::MAX);
        checkpoint.portfolio_positions.insert(key, state.position);
        checkpoint.working_order_ids = self.tracked_orders.iter().cloned().collect();
        checkpoint.risk_stopped = self.state != SupervisorState::Healthy;
        Ok(checkpoint)
    }

    pub fn restore_checkpoint(&mut self, checkpoint: &SessionCheckpoint) -> Result<(), GateReason> {
        checkpoint
            .validate()
            .map_err(|_| GateReason::UnknownRemoteState)?;
        let Some(state) = self.symbols.get_mut(checkpoint.symbol.as_str()) else {
            self.state = SupervisorState::Halted;
            return Err(GateReason::UnknownSymbol);
        };
        state.position = checkpoint.position_ticks;
        self.tracked_orders = checkpoint.working_order_ids.iter().cloned().collect();
        self.last_user_event_at_ms = checkpoint.last_event_at_ms;
        self.unknown_remote_state = false;
        // A restored checkpoint is never proof of exchange truth; reconcile first.
        self.recovery.start_epoch(checkpoint.last_event_at_ms);
        self.state = SupervisorState::RiskStopped;
        Ok(())
    }

    pub fn on_disconnect(&mut self) {
        self.state = SupervisorState::RiskStopped;
        self.recovery.start_epoch(self.last_user_event_at_ms);
    }

    pub fn on_reconnect(&mut self) -> Result<(), GateReason> {
        if self.state != SupervisorState::RiskStopped {
            return Err(GateReason::NotHealthy);
        }
        self.recovery
            .apply(RecoveryEvent::ReconnectSucceeded)
            .map_err(|_| GateReason::UnknownRemoteState)?;
        self.state = SupervisorState::Synchronizing;
        Ok(())
    }

    pub fn mark_snapshot_loaded(&mut self, snapshot_at_ms: u64) -> Result<(), GateReason> {
        if self.state != SupervisorState::Synchronizing {
            return Err(GateReason::NotHealthy);
        }
        self.recovery.record_snapshot(snapshot_at_ms);
        self.unknown_remote_state = false;
        self.recovery
            .apply(RecoveryEvent::SnapshotLoaded)
            .map_err(|_| GateReason::UnknownRemoteState)
    }

    pub fn reconciliation_clean(&mut self) -> Result<(), GateReason> {
        if self.state != SupervisorState::Synchronizing || self.unknown_remote_state {
            self.state = SupervisorState::Halted;
            return Err(GateReason::UnknownRemoteState);
        }
        if self.recovery.state() == RecoveryState::Synchronizing {
            self.recovery.mark_reconciled();
            self.recovery
                .apply(RecoveryEvent::ReconciliationClean)
                .map_err(|_| GateReason::UnknownRemoteState)?;
        }
        self.state = SupervisorState::Healthy;
        Ok(())
    }

    pub fn begin_flatten(&mut self) -> Result<(), GateReason> {
        if self.state == SupervisorState::Halted {
            return Err(GateReason::NotHealthy);
        }
        self.state = SupervisorState::Flattening;
        Ok(())
    }

    pub fn confirm_flattened(&mut self) -> Result<(), GateReason> {
        if self.state != SupervisorState::Flattening
            || self.symbols.values().any(|value| value.position != 0)
        {
            self.state = SupervisorState::Halted;
            return Err(GateReason::ResidualExposure);
        }
        self.state = SupervisorState::Healthy;
        Ok(())
    }

    // This explicit observation contract keeps every gate input visible at the call site.
    #[allow(clippy::too_many_arguments)]
    pub fn observe_symbol(
        &mut self,
        symbol: &str,
        market_at_ms: u64,
        fx_at_ms: u64,
        anchor_ready: bool,
        equity_closed: bool,
        funding_known: bool,
        next_funding_at_ms: u64,
        position: i64,
    ) -> Result<(), GateReason> {
        let key = symbol.trim().to_ascii_uppercase();
        let Some(state) = self.symbols.get_mut(key.as_str()) else {
            return Err(GateReason::UnknownSymbol);
        };
        state.market_at_ms = market_at_ms;
        state.fx_at_ms = fx_at_ms;
        state.anchor_ready = anchor_ready;
        state.equity_closed = equity_closed;
        state.funding_known = funding_known;
        state.next_funding_at_ms = next_funding_at_ms;
        state.position = position;
        Ok(())
    }

    pub fn evaluate(&self, symbol: &str, intent: OrderIntent, now_ms: u64) -> GateDecision {
        let key = symbol.trim().to_ascii_uppercase();
        let Some(state) = self.symbols.get(key.as_str()) else {
            return GateDecision::Halt(GateReason::UnknownSymbol);
        };
        if self.state != SupervisorState::Healthy {
            return GateDecision::Halt(GateReason::NotHealthy);
        }
        if intent.symbol == 0 || intent.price <= 0 || intent.quantity <= 0 {
            return GateDecision::Halt(GateReason::InvalidIntent);
        }
        if !intent.post_only {
            return GateDecision::Halt(GateReason::NonMakerIntent);
        }
        if now_ms < state.market_at_ms
            || now_ms.saturating_sub(state.market_at_ms) > self.config.max_market_age_ms
        {
            return GateDecision::Halt(GateReason::MarketStale);
        }
        if now_ms < state.fx_at_ms
            || now_ms.saturating_sub(state.fx_at_ms) > self.config.max_fx_age_ms
        {
            return GateDecision::Halt(GateReason::FxStale);
        }
        if !state.anchor_ready {
            return GateDecision::Halt(GateReason::AnchorUnavailable);
        }
        if !state.equity_closed {
            return GateDecision::NoAction(GateReason::EquitySessionOpen);
        }
        if !state.funding_known || state.next_funding_at_ms == 0 {
            return GateDecision::NoAction(GateReason::FundingUnknown);
        }
        if now_ms.saturating_add(self.config.funding_lead_ms) >= state.next_funding_at_ms {
            return if state.position == 0 {
                GateDecision::NoAction(GateReason::FundingWindow)
            } else {
                GateDecision::Flatten(GateReason::FundingWindow)
            };
        }
        if self.unknown_remote_state {
            return GateDecision::Halt(GateReason::UnknownRemoteState);
        }
        let next_position = match intent.side {
            Side::Buy => i128::from(state.position) + i128::from(intent.quantity),
            Side::Sell => i128::from(state.position) - i128::from(intent.quantity),
        };
        if next_position.abs() > i128::from(self.config.max_position) {
            return GateDecision::NoAction(GateReason::PositionLimit);
        }
        GateDecision::Allow
    }

    /// Evaluate a live intent and emit the same versioned audit contract used
    /// by replay and simulation. The adapter owns transport; this layer owns
    /// gate semantics and correlation identity.
    pub fn evaluate_with_audit(
        &mut self,
        symbol: &str,
        intent: OrderIntent,
        now_ms: u64,
    ) -> (GateDecision, DecisionAudit) {
        let decision_id = self.next_decision_id;
        self.next_decision_id = self.next_decision_id.saturating_add(1);
        let decision = self.evaluate(symbol, intent, now_ms);
        let (outcome, final_gate) = match decision {
            GateDecision::Allow => ("admissible", "allow"),
            GateDecision::NoAction(reason) => ("no_action", reason.label()),
            GateDecision::Halt(reason) => ("halt", reason.label()),
            GateDecision::Flatten(reason) => ("reduce_only", reason.label()),
        };
        let key = symbol.trim().to_ascii_uppercase();
        let state = self.symbols.get(key.as_str()).copied();
        let market_fresh = state.is_some_and(|state| {
            state.market_at_ms > 0
                && now_ms >= state.market_at_ms
                && now_ms.saturating_sub(state.market_at_ms) <= self.config.max_market_age_ms
        });
        let fx_fresh = state.is_some_and(|state| {
            state.fx_at_ms > 0
                && now_ms >= state.fx_at_ms
                && now_ms.saturating_sub(state.fx_at_ms) <= self.config.max_fx_age_ms
        });
        let anchor_ready = state.is_some_and(|state| state.anchor_ready);
        let equity_closed = state.is_some_and(|state| state.equity_closed);
        let funding_known =
            state.is_some_and(|state| state.funding_known && state.next_funding_at_ms > now_ms);
        let position_capacity = state.is_some_and(|state| {
            let next_position = match intent.side {
                Side::Buy => i128::from(state.position) + i128::from(intent.quantity),
                Side::Sell => i128::from(state.position) - i128::from(intent.quantity),
            };
            next_position.abs() <= i128::from(self.config.max_position)
        });
        let gates = vec![
            DecisionGateAudit {
                name: "supervisor_health".to_owned(),
                passed: self.state == SupervisorState::Healthy,
                reason: format!("{:?}", self.state),
            },
            DecisionGateAudit {
                name: "intent_post_only".to_owned(),
                passed: intent.post_only
                    && intent.symbol > 0
                    && intent.price > 0
                    && intent.quantity > 0,
                reason: if intent.post_only {
                    "valid maker intent fields"
                } else {
                    "live intent must be post-only"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "market_freshness".to_owned(),
                passed: market_fresh,
                reason: if market_fresh {
                    "exchange market timestamp within configured age"
                } else {
                    "market timestamp missing or stale"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "fx_freshness".to_owned(),
                passed: fx_fresh,
                reason: if fx_fresh {
                    "exchange FX timestamp within configured age"
                } else {
                    "FX timestamp missing or stale"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "anchor_ready".to_owned(),
                passed: anchor_ready,
                reason: if anchor_ready {
                    "authoritative anchor ready"
                } else {
                    "anchor unavailable"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "equity_session".to_owned(),
                passed: equity_closed,
                reason: if equity_closed {
                    "equity market is closed"
                } else {
                    "equity session open"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "funding".to_owned(),
                passed: funding_known,
                reason: if funding_known {
                    "funding known and outside flatten window"
                } else {
                    "funding unknown or inside flatten window"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "position_capacity".to_owned(),
                passed: position_capacity,
                reason: if position_capacity {
                    "position remains within supervisor limit"
                } else {
                    "position limit exceeded"
                }
                .to_owned(),
            },
            DecisionGateAudit {
                name: "terminal_decision".to_owned(),
                passed: matches!(decision, GateDecision::Allow | GateDecision::Flatten(_)),
                reason: final_gate.to_owned(),
            },
        ];
        (
            decision,
            DecisionAudit {
                schema_version: DECISION_AUDIT_SCHEMA_VERSION,
                decision_id,
                exchange_event_time_ms: now_ms,
                received_at_ms: now_ms,
                outcome: outcome.to_owned(),
                final_gate: final_gate.to_owned(),
                gates,
                book_bid_ticks: None,
                book_ask_ticks: None,
                book_bid_quantity: None,
                book_ask_quantity: None,
                mark_ticks: None,
                index_ticks: None,
                anchor_ticks: 0,
                position: state.map(|state| state.position).unwrap_or(0),
                mark_age_ms: state.map(|state| now_ms.saturating_sub(state.market_at_ms)),
                anchor_age_ms: None,
                threshold_status: "not_applicable_live_supervisor".to_owned(),
                threshold_pico_bps: None,
                fair_value_ticks: None,
                liquidity_ratio_bps: None,
                signal_abs_pico_bps: None,
                adaptive_relief_pico_bps: 0,
                threshold_components_pico_bps: None,
            },
        )
    }

    pub fn on_user_data(&mut self, event: UserDataEvent) -> Result<(), GateReason> {
        match event {
            UserDataEvent::ListenKeyExpired => {
                self.unknown_remote_state = true;
                self.state = SupervisorState::RiskStopped;
                self.recovery.start_epoch(self.last_user_event_at_ms);
                Err(GateReason::UnknownRemoteState)
            }
            UserDataEvent::OrderUpdate(update) => {
                if !self.symbols.contains_key(update.symbol.as_str()) {
                    self.unknown_remote_state = true;
                    self.state = SupervisorState::Halted;
                    return Err(GateReason::UnknownSymbol);
                }
                if update.order_type != "LIMIT" || update.time_in_force != "GTX" {
                    self.unknown_remote_state = true;
                    self.state = SupervisorState::Halted;
                    return Err(GateReason::NonMakerIntent);
                }
                self.tracked_orders.insert(update.client_order_id);
                self.last_user_event_at_ms = update.event_time_ms;
                self.recovery.record_event(update.event_time_ms);
                Ok(())
            }
            UserDataEvent::AccountUpdate(update) => {
                for position in update.positions {
                    let Some(state) = self.symbols.get_mut(position.symbol.as_str()) else {
                        self.unknown_remote_state = true;
                        self.state = SupervisorState::Halted;
                        return Err(GateReason::UnknownSymbol);
                    };
                    let Some(parsed_position) =
                        parse_quantity_ticks(&position.position_amount, self.config.quantity_scale)
                    else {
                        self.unknown_remote_state = true;
                        self.state = SupervisorState::Halted;
                        return Err(GateReason::UnknownRemoteState);
                    };
                    state.position = parsed_position;
                }
                self.last_user_event_at_ms = update.event_time_ms;
                self.recovery.record_event(update.event_time_ms);
                Ok(())
            }
        }
    }
}

fn parse_quantity_ticks(value: &str, scale: u32) -> Option<i64> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > scale as usize
    {
        return None;
    }
    let multiplier = 10_i128.checked_pow(scale)?;
    let whole_value = whole.parse::<i128>().ok()?.checked_mul(multiplier)?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .ok()?
            .checked_mul(10_i128.checked_pow(scale.saturating_sub(fraction.len() as u32))?)?
    };
    let value = whole_value.checked_add(fraction_value)?;
    let signed = if negative { -value } else { value };
    i64::try_from(signed).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_symbols() -> Vec<String> {
        [
            "CXMTUSDT",
            "UNITREEUSDT",
            "GIGADEVUSDT",
            "HK0625USDT",
            "MINIMAXUSDT",
            "ZHIPUUSDT",
            "ZHONGJIUSDT",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    fn supervisor() -> ExecutionSupervisor {
        ExecutionSupervisor::new(SupervisorConfig::default(), test_symbols()).unwrap()
    }

    fn ready(mut value: ExecutionSupervisor) -> ExecutionSupervisor {
        value
            .observe_symbol("CXMTUSDT", 1_000, 1_000, true, true, true, 1_000_000, 0)
            .unwrap();
        value.reconciliation_clean().unwrap();
        value
    }

    #[test]
    fn checkpoint_restore_is_risk_stopped_until_reconciliation() {
        let mut value = ready(supervisor());
        value
            .observe_symbol("CXMTUSDT", 1_000, 1_000, true, true, true, 1_000_000, -7)
            .unwrap();
        let checkpoint = value
            .checkpoint("session-1", "testnet", "CXMTUSDT", 1_001)
            .unwrap();

        let mut restored = supervisor();
        restored.restore_checkpoint(&checkpoint).unwrap();
        assert_eq!(restored.state(), SupervisorState::RiskStopped);
        assert_eq!(restored.tracked_order_count(), 0);
        assert!(restored.on_reconnect().is_ok());
        assert!(restored.reconciliation_clean().is_ok());
        assert_eq!(
            restored.evaluate("CXMTUSDT", OrderIntent::maker_buy(7, 100, 1), 1_001),
            GateDecision::Halt(GateReason::AnchorUnavailable)
        );
    }

    #[test]
    fn configured_symbol_universe_is_normalized_and_explicit() {
        let symbols = test_symbols();
        assert_eq!(symbols.len(), 7);
        assert!(symbols.iter().any(|symbol| symbol == "CXMTUSDT"));
        assert!(symbols.iter().any(|symbol| symbol == "ZHONGJIUSDT"));
        assert!(!symbols.iter().any(|symbol| symbol == "BTCUSDT"));
        assert!(ExecutionSupervisor::new(
            SupervisorConfig::default(),
            [" cxmtusdt ".to_owned(), "UNITREEUSDT".to_owned()],
        )
        .is_ok());
        assert!(
            ExecutionSupervisor::new(SupervisorConfig::default(), Vec::<String>::new()).is_err()
        );
    }

    #[test]
    fn healthy_gate_allows_only_fresh_maker_intent() {
        let value = ready(supervisor());
        let intent = OrderIntent::maker_buy(7, 100, 2);
        assert_eq!(
            value.evaluate("CXMTUSDT", intent, 1_001),
            GateDecision::Allow
        );
        assert_eq!(
            value.evaluate(
                "CXMTUSDT",
                OrderIntent {
                    post_only: false,
                    ..intent
                },
                1_001
            ),
            GateDecision::Halt(GateReason::NonMakerIntent)
        );
    }

    #[test]
    fn stale_and_open_session_fail_closed() {
        let mut value = ready(supervisor());
        assert_eq!(
            value.evaluate("CXMTUSDT", OrderIntent::maker_buy(7, 100, 2), 7_000),
            GateDecision::Halt(GateReason::MarketStale)
        );
        value
            .observe_symbol("CXMTUSDT", 1_000, 1_000, true, false, true, 1_000_000, 0)
            .unwrap();
        assert_eq!(
            value.evaluate("CXMTUSDT", OrderIntent::maker_buy(7, 100, 2), 1_001),
            GateDecision::NoAction(GateReason::EquitySessionOpen)
        );
    }

    #[test]
    fn disconnect_and_listen_key_expiry_stop_risk() {
        let mut value = ready(supervisor());
        value.on_disconnect();
        assert_eq!(value.state(), SupervisorState::RiskStopped);
        assert!(value.on_reconnect().is_ok());
        assert!(value.on_user_data(UserDataEvent::ListenKeyExpired).is_err());
        assert_eq!(value.state(), SupervisorState::RiskStopped);
    }

    #[test]
    fn funding_window_flattens_residual_position() {
        let mut value = ready(supervisor());
        value
            .observe_symbol("CXMTUSDT", 1_000, 1_000, true, true, true, 301_000, 10)
            .unwrap();
        assert_eq!(
            value.evaluate("CXMTUSDT", OrderIntent::maker_buy(7, 100, 1), 1_000),
            GateDecision::Flatten(GateReason::FundingWindow)
        );
    }

    #[test]
    fn parses_signed_decimal_position_at_configured_scale() {
        assert_eq!(parse_quantity_ticks("2.5", 1), Some(25));
        assert_eq!(parse_quantity_ticks("-0.125", 3), Some(-125));
        assert_eq!(parse_quantity_ticks("2.50", 1), None);
        assert_eq!(parse_quantity_ticks("999999999999999999999", 8), None);
    }

    #[test]
    fn invalid_remote_position_halts_instead_of_rounding() {
        let mut value = ready(supervisor());
        let event = UserDataEvent::AccountUpdate(crate::execution::AccountUpdate {
            event_time_ms: 2_000,
            transaction_time_ms: 2_000,
            positions: vec![crate::execution::PositionUpdate {
                symbol: "CXMTUSDT".into(),
                position_amount: "2.000000001".into(),
                entry_price: "1".into(),
                unrealized_profit: "0".into(),
                position_side: "BOTH".into(),
            }],
        });
        assert_eq!(
            value.on_user_data(event),
            Err(GateReason::UnknownRemoteState)
        );
        assert_eq!(value.state(), SupervisorState::Halted);
    }

    #[test]
    fn million_fresh_events_stay_bounded_and_deterministic() {
        let mut value = supervisor();
        for i in 0..1_000_000_u64 {
            value
                .observe_symbol("CXMTUSDT", i + 1, i + 1, true, true, true, i + 1_000_000, 0)
                .unwrap();
        }
        value.reconciliation_clean().unwrap();
        assert_eq!(value.state(), SupervisorState::Healthy);
        assert_eq!(value.tracked_order_count(), 0);
    }
}
