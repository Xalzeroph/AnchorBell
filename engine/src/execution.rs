use crate::model::{
    CandidateOrder, ModelError, QueueFillEstimate, Side, TimeInForce, ValidatedOrder,
};
use serde::{Deserialize, Serialize};

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
        }
    }
    pub fn apply(&mut self, event: OrderEvent) -> Result<(), ModelError> {
        if event.event_id.is_empty()
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
            && terminal_rank(event.state) < terminal_rank(self.state)
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
        Ok(())
    }
}

fn terminal_rank(state: OrderState) -> u8 {
    match state {
        OrderState::Intent => 0,
        OrderState::Submitted => 1,
        OrderState::Accepted => 2,
        OrderState::PartiallyFilled => 3,
        OrderState::CancelPending => 4,
        OrderState::Filled | OrderState::Canceled | OrderState::Rejected => 5,
        OrderState::Unknown => 6,
    }
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
