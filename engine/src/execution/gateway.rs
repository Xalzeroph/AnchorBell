#[derive(Debug, Clone, Copy)]
pub struct ExchangeOrder {
    pub client_id: u64,
    pub price: i64,
    pub quantity: i64,
    pub post_only: bool,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayResult {
    /// The exchange acknowledged the operation.
    Accepted,
    /// The request was rejected by a local policy boundary.
    Rejected,
    /// No live transport is bound to this gateway.
    Unavailable,
}

pub trait ExecutionGateway {
    fn submit(&self, order: ExchangeOrder) -> GatewayResult;
    fn cancel(&self, client_id: u64) -> GatewayResult;
}

pub struct SimulationGateway;

impl ExecutionGateway for SimulationGateway {
    fn submit(&self, order: ExchangeOrder) -> GatewayResult {
        if order.post_only || (order.reduce_only && !order.post_only) {
            GatewayResult::Accepted
        } else {
            GatewayResult::Rejected
        }
    }

    fn cancel(&self, _client_id: u64) -> GatewayResult {
        GatewayResult::Accepted
    }
}
