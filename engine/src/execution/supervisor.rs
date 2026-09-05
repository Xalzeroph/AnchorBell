use std::collections::{BTreeMap, BTreeSet};

use super::{OrderIntent, SessionCheckpoint, Side, UserDataEvent};

pub const LIVE_SYMBOLS: [&str; 9] = [
    "CXMTUSDT",
    "UNITREEUSDT",
    "CSOPSAMSUNG2LUSDT",
    "CSOPSKHYNIX2LUSDT",
    "GIGADEVUSDT",
    "HK0625USDT",
    "MINIMAXUSDT",
    "ZHIPUUSDT",
    "ZHONGJIUSDT",
];

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
            max_fx_age_ms: 5_000,
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
    symbols: BTreeMap<&'static str, SymbolState>,
    tracked_orders: BTreeSet<String>,
    last_user_event_at_ms: u64,
    unknown_remote_state: bool,
}

impl ExecutionSupervisor {
    pub fn new(config: SupervisorConfig) -> Result<Self, GateReason> {
        if config.max_market_age_ms == 0
            || config.max_fx_age_ms == 0
            || config.max_position <= 0
            || config.quantity_scale > 18
        {
            return Err(GateReason::InvalidIntent);
        }
        let symbols = LIVE_SYMBOLS
            .into_iter()
            .map(|symbol| {
                (
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
            })
            .collect();
        Ok(Self {
            state: SupervisorState::Synchronizing,
            config,
            symbols,
            tracked_orders: BTreeSet::new(),
            last_user_event_at_ms: 0,
            unknown_remote_state: false,
        })
    }

    pub fn state(&self) -> SupervisorState {
        self.state
    }

    pub fn tracked_order_count(&self) -> usize {
        self.tracked_orders.len()
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
        let mut checkpoint = SessionCheckpoint::new(session_id, environment, key);
        checkpoint.last_event_at_ms = last_event_at_ms;
        checkpoint.position_ticks = state.position;
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
        self.state = SupervisorState::RiskStopped;
        Ok(())
    }

    pub fn on_disconnect(&mut self) {
        self.state = SupervisorState::RiskStopped;
    }

    pub fn on_reconnect(&mut self) -> Result<(), GateReason> {
        if self.state != SupervisorState::RiskStopped {
            return Err(GateReason::NotHealthy);
        }
        self.state = SupervisorState::Synchronizing;
        Ok(())
    }

    pub fn reconciliation_clean(&mut self) -> Result<(), GateReason> {
        if self.state != SupervisorState::Synchronizing || self.unknown_remote_state {
            self.state = SupervisorState::Halted;
            return Err(GateReason::UnknownRemoteState);
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

    /// Checks whether exits may be evaluated without manufacturing an entry intent.
    pub fn evaluate_exit(&self, symbol: &str) -> GateDecision {
        if !self
            .symbols
            .contains_key(symbol.trim().to_ascii_uppercase().as_str())
        {
            return GateDecision::Halt(GateReason::UnknownSymbol);
        }
        if self.unknown_remote_state {
            return GateDecision::Halt(GateReason::UnknownRemoteState);
        }
        match self.state {
            SupervisorState::Healthy | SupervisorState::Flattening => GateDecision::Allow,
            _ => GateDecision::Halt(GateReason::NotHealthy),
        }
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

    pub fn on_user_data(&mut self, event: UserDataEvent) -> Result<(), GateReason> {
        match event {
            UserDataEvent::ListenKeyExpired => {
                self.unknown_remote_state = true;
                self.state = SupervisorState::RiskStopped;
                Err(GateReason::UnknownRemoteState)
            }
            UserDataEvent::OrderUpdate(update) => {
                if !LIVE_SYMBOLS.contains(&update.symbol.as_str()) {
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
        || (fraction.len() > scale as usize
            && fraction.as_bytes()[scale as usize..]
                .iter()
                .any(|byte| *byte != b'0'))
    {
        return None;
    }
    let multiplier = 10_i128.checked_pow(scale)?;
    let whole_value = whole.parse::<i128>().ok()?.checked_mul(multiplier)?;
    let significant_fraction = &fraction[..fraction.len().min(scale as usize)];
    let fraction_value = if significant_fraction.is_empty() {
        0
    } else {
        significant_fraction.parse::<i128>().ok()?.checked_mul(
            10_i128.checked_pow(scale.saturating_sub(significant_fraction.len() as u32))?,
        )?
    };
    let value = whole_value.checked_add(fraction_value)?;
    let signed = if negative { -value } else { value };
    i64::try_from(signed).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn supervisor() -> ExecutionSupervisor {
        ExecutionSupervisor::new(SupervisorConfig::default()).unwrap()
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
    fn exact_nine_symbol_universe_is_fixed() {
        assert_eq!(LIVE_SYMBOLS.len(), 9);
        assert!(LIVE_SYMBOLS.contains(&"CXMTUSDT"));
        assert!(LIVE_SYMBOLS.contains(&"ZHONGJIUSDT"));
        assert!(!LIVE_SYMBOLS.contains(&"BTCUSDT"));
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
        assert_eq!(parse_quantity_ticks("2.50", 1), Some(25));
        assert_eq!(parse_quantity_ticks("2.51", 1), None);
        assert_eq!(parse_quantity_ticks("999999999999999999999", 8), None);
    }

    #[test]
    fn exit_evaluation_runs_without_an_entry_intent_and_while_flattening() {
        let mut value = ready(supervisor());
        assert_eq!(value.evaluate_exit("CXMTUSDT"), GateDecision::Allow);
        value.begin_flatten().unwrap();
        assert_eq!(value.evaluate_exit("CXMTUSDT"), GateDecision::Allow);
        value.on_disconnect();
        assert_eq!(
            value.evaluate_exit("CXMTUSDT"),
            GateDecision::Halt(GateReason::NotHealthy)
        );
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
