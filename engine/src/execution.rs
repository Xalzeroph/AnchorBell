use crate::model::{CandidateOrder, ModelError, Side, ValidatedOrder};
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub order_id: String,
    pub symbol: String,
    pub side: Side,
    pub quantity: i64,
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
            filled_quantity: 0,
            state: OrderState::Intent,
            last_event_at_ms: order.validated_at_ms,
            contract_digest: order.contract_digest.clone(),
        }
    }
    pub fn apply(&mut self, event: OrderEvent) -> Result<(), ModelError> {
        if event.order_id != self.order_id
            || event.symbol != self.symbol
            || event.cumulative_quantity < self.filled_quantity
            || event.cumulative_quantity > self.quantity
            || event.event_at_ms < self.last_event_at_ms
        {
            return Err(ModelError::IncompleteEvidence);
        }
        if event.state != OrderState::Unknown
            && terminal_rank(event.state) < terminal_rank(self.state)
        {
            return Err(ModelError::IncompleteEvidence);
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
    fn cancel(&mut self, order_id: &str) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoUnscopedAggressor;

pub fn assert_maker(order: &CandidateOrder) -> Result<(), NoUnscopedAggressor> {
    if order.reduce_only {
        Ok(())
    } else {
        Ok(())
    }
}
