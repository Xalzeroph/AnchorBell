use crate::model::{
    CandidateOrder, ModelError, QueueFillEstimate, Side, TimeInForce, ValidatedOrder,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    Intent,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    CancelPending,
    Canceled,
    Rejected,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Authority {
    Local,
    Exchange,
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderEvent {
    pub event_id: String,
    pub order_id: String,
    pub symbol: String,
    pub state: OrderState,
    pub authority: Authority,
    pub event_at_ms: u64,
    pub cumulative_quantity: i64,
    pub traded_through_quantity: Option<i64>,
    pub observed_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub order_id: String,
    pub symbol: String,
    pub side: Side,
    pub quantity: i64,
    pub queue_ahead_quantity: i64,
    pub filled_quantity: i64,
    pub state: OrderState,
    pub last_event_at_ms: u64,
    pub contract_digest: String,
    pub applied_event_ids: BTreeSet<String>,
}

impl OrderRecord {
    pub fn from_validated(id: impl Into<String>, order: &ValidatedOrder) -> Self {
        Self {
            order_id: id.into(),
            symbol: order.order.symbol.clone(),
            side: order.order.side,
            quantity: order.order.quantity.0,
            queue_ahead_quantity: order.queue_ahead_quantity,
            filled_quantity: 0,
            state: OrderState::Intent,
            last_event_at_ms: order.validated_at_ms,
            contract_digest: order.contract_digest.clone(),
            applied_event_ids: BTreeSet::new(),
        }
    }
    pub fn apply(&mut self, event: OrderEvent) -> Result<(), ModelError> {
        if event.event_id.is_empty()
            || self.applied_event_ids.contains(&event.event_id)
            || event.order_id != self.order_id
            || event.symbol != self.symbol
            || event.event_at_ms == 0
            || event.cumulative_quantity < self.filled_quantity
            || event.cumulative_quantity > self.quantity
            || event.event_at_ms < self.last_event_at_ms
            || event.traded_through_quantity.is_some_and(|value| value < 0)
        {
            return Err(ModelError::IncompleteEvidence);
        }
        let recovering_unknown = self.state == OrderState::Unknown
            && event.state != OrderState::Unknown
            && matches!(event.authority, Authority::Exchange | Authority::Replay);
        if event.state != OrderState::Unknown
            && !recovering_unknown
            && !valid_transition(self.state, event.state)
        {
            return Err(ModelError::IncompleteEvidence);
        }
        if matches!(
            event.state,
            OrderState::Filled | OrderState::PartiallyFilled
        ) && event.authority == Authority::Local
        {
            return Err(ModelError::IncompleteEvidence);
        }
        if matches!(event.state, OrderState::Canceled | OrderState::Rejected)
            && !matches!(event.authority, Authority::Exchange | Authority::Replay)
        {
            return Err(ModelError::IncompleteEvidence);
        }
        if event.state == OrderState::PartiallyFilled
            && (event.cumulative_quantity == 0 || event.cumulative_quantity == self.quantity)
        {
            return Err(ModelError::IncompleteEvidence);
        }
        if event.state == OrderState::Rejected && event.cumulative_quantity != 0 {
            return Err(ModelError::IncompleteEvidence);
        }
        if event.state == OrderState::Filled && event.cumulative_quantity != self.quantity {
            return Err(ModelError::IncompleteEvidence);
        }
        if let Some(traded_through_quantity) = event.traded_through_quantity {
            let estimate = QueueFillEstimate {
                queue_ahead_quantity: self.queue_ahead_quantity,
                order_quantity: self.quantity,
                traded_through_quantity,
                observed_latency_ms: event.observed_latency_ms.unwrap_or(0),
            };
            let maximum_causal_fill = estimate
                .causal_fill_quantity()
                .ok_or(ModelError::IncompleteEvidence)?;
            if event.cumulative_quantity > maximum_causal_fill {
                return Err(ModelError::IncompleteEvidence);
            }
        }
        self.state = event.state;
        self.filled_quantity = event.cumulative_quantity;
        self.last_event_at_ms = event.event_at_ms;
        self.applied_event_ids.insert(event.event_id);
        Ok(())
    }
}

fn valid_transition(from: OrderState, to: OrderState) -> bool {
    matches!(
        (from, to),
        (
            OrderState::Intent,
            OrderState::Intent
                | OrderState::Submitted
                | OrderState::Accepted
                | OrderState::PartiallyFilled
                | OrderState::Filled
                | OrderState::Rejected
                | OrderState::Unknown,
        ) | (
            OrderState::Submitted,
            OrderState::Submitted
                | OrderState::Accepted
                | OrderState::PartiallyFilled
                | OrderState::Filled
                | OrderState::CancelPending
                | OrderState::Rejected
                | OrderState::Unknown,
        ) | (
            OrderState::Accepted,
            OrderState::Accepted
                | OrderState::PartiallyFilled
                | OrderState::Filled
                | OrderState::CancelPending
                | OrderState::Rejected
                | OrderState::Unknown,
        ) | (
            OrderState::PartiallyFilled,
            OrderState::PartiallyFilled
                | OrderState::Filled
                | OrderState::CancelPending
                | OrderState::Unknown,
        ) | (
            OrderState::CancelPending,
            OrderState::CancelPending
                | OrderState::PartiallyFilled
                | OrderState::Filled
                | OrderState::Canceled
                | OrderState::Unknown,
        ) | (OrderState::Filled, OrderState::Filled)
            | (OrderState::Canceled, OrderState::Canceled)
            | (OrderState::Rejected, OrderState::Rejected)
            | (OrderState::Unknown, OrderState::Unknown)
    )
}

pub trait ExecutionPort {
    type Error;
    fn submit_maker(&mut self, order: &ValidatedOrder) -> Result<String, Self::Error>;
    fn submit_reduce_only(&mut self, order: &ValidatedOrder) -> Result<String, Self::Error>;
    fn submit_emergency_reduce_only(
        &mut self,
        order: &ValidatedOrder,
    ) -> Result<String, Self::Error>;
    fn cancel(&mut self, order_id: &str) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoUnscopedAggressor;

pub fn assert_maker(order: &CandidateOrder) -> Result<(), NoUnscopedAggressor> {
    if order.time_in_force == TimeInForce::Gtx
        && order.trigger_price.is_none()
        && !order.price_protect
    {
        Ok(())
    } else {
        Err(NoUnscopedAggressor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        CandidateOrder, OrderRoute, PositionSide, PriceTicks, Quantity, WorkingType,
    };

    fn record() -> OrderRecord {
        let validated = ValidatedOrder {
            queue_ahead_quantity: 5,
            order: CandidateOrder {
                symbol: "BTCUSDT".into(),
                side: Side::Buy,
                price: PriceTicks(99),
                quantity: Quantity(3),
                reduce_only: false,
                position_side: PositionSide::Both,
                time_in_force: TimeInForce::Gtx,
                working_type: WorkingType::ContractPrice,
                price_protect: false,
                trigger_price: None,
                close_position: false,
            },
            route: OrderRoute::PassiveMaker,
            contract_digest: "rules".into(),
            validated_at_ms: 1,
        };
        OrderRecord::from_validated("order-1", &validated)
    }

    #[test]
    fn local_authority_cannot_create_a_fill() {
        let mut order = record();
        assert_eq!(
            order.apply(OrderEvent {
                event_id: "local-fill".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Filled,
                authority: Authority::Local,
                event_at_ms: 2,
                cumulative_quantity: 3,
                traded_through_quantity: None,
                observed_latency_ms: None,
            }),
            Err(ModelError::IncompleteEvidence)
        );
    }

    #[test]
    fn exchange_fill_cannot_exceed_causal_queue_throughput() {
        let mut order = record();
        assert!(order
            .apply(OrderEvent {
                event_id: "partial".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::PartiallyFilled,
                authority: Authority::Exchange,
                event_at_ms: 2,
                cumulative_quantity: 1,
                traded_through_quantity: Some(6),
                observed_latency_ms: Some(50),
            })
            .is_ok());
        assert_eq!(
            order.apply(OrderEvent {
                event_id: "overfill".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Filled,
                authority: Authority::Exchange,
                event_at_ms: 3,
                cumulative_quantity: 3,
                traded_through_quantity: Some(6),
                observed_latency_ms: Some(50),
            }),
            Err(ModelError::IncompleteEvidence)
        );
    }

    #[test]
    fn duplicate_event_id_is_rejected_even_after_progress() {
        let mut order = record();
        let event = OrderEvent {
            event_id: "accepted".into(),
            order_id: "order-1".into(),
            symbol: "BTCUSDT".into(),
            state: OrderState::Accepted,
            authority: Authority::Exchange,
            event_at_ms: 2,
            cumulative_quantity: 0,
            traded_through_quantity: None,
            observed_latency_ms: None,
        };
        order.apply(event.clone()).unwrap();
        let mut progressed = event;
        progressed.event_at_ms = 3;
        progressed.state = OrderState::Canceled;
        assert_eq!(order.apply(progressed), Err(ModelError::IncompleteEvidence));
    }

    #[test]
    fn terminal_states_cannot_cross_after_cancellation_or_rejection() {
        let mut canceled = record();
        canceled
            .apply(OrderEvent {
                event_id: "accepted".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Accepted,
                authority: Authority::Exchange,
                event_at_ms: 2,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        canceled
            .apply(OrderEvent {
                event_id: "cancel-pending".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::CancelPending,
                authority: Authority::Local,
                event_at_ms: 3,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        canceled
            .apply(OrderEvent {
                event_id: "canceled".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Canceled,
                authority: Authority::Exchange,
                event_at_ms: 4,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        assert_eq!(
            canceled.apply(OrderEvent {
                event_id: "late-fill".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Filled,
                authority: Authority::Exchange,
                event_at_ms: 5,
                cumulative_quantity: 3,
                traded_through_quantity: Some(8),
                observed_latency_ms: Some(50),
            }),
            Err(ModelError::IncompleteEvidence)
        );
    }

    #[test]
    fn cancel_race_allows_authoritative_fill_before_cancel_confirmation() {
        let mut order = record();
        order
            .apply(OrderEvent {
                event_id: "accepted".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Accepted,
                authority: Authority::Exchange,
                event_at_ms: 2,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        order
            .apply(OrderEvent {
                event_id: "cancel-pending".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::CancelPending,
                authority: Authority::Local,
                event_at_ms: 3,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        order
            .apply(OrderEvent {
                event_id: "partial-race".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::PartiallyFilled,
                authority: Authority::Exchange,
                event_at_ms: 4,
                cumulative_quantity: 1,
                traded_through_quantity: Some(6),
                observed_latency_ms: Some(50),
            })
            .unwrap();
        assert_eq!(order.state, OrderState::PartiallyFilled);
        assert_eq!(order.filled_quantity, 1);
    }

    #[test]
    fn unknown_state_can_be_recovered_by_authoritative_update() {
        let mut order = record();
        order
            .apply(OrderEvent {
                event_id: "unknown".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Unknown,
                authority: Authority::Local,
                event_at_ms: 2,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        order
            .apply(OrderEvent {
                event_id: "accepted".into(),
                order_id: "order-1".into(),
                symbol: "BTCUSDT".into(),
                state: OrderState::Accepted,
                authority: Authority::Exchange,
                event_at_ms: 3,
                cumulative_quantity: 0,
                traded_through_quantity: None,
                observed_latency_ms: None,
            })
            .unwrap();
        assert_eq!(order.state, OrderState::Accepted);
        assert_eq!(order.last_event_at_ms, 3);
    }
}
