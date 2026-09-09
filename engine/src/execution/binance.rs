use super::{BinanceEndpoints, BinanceEnvironment, ExchangeOrder, ExecutionGateway, GatewayResult};

#[derive(Debug, Clone, Copy)]
pub struct BinanceGateway {
    pub environment: BinanceEnvironment,
}

impl BinanceGateway {
    pub fn new(testnet: bool) -> Self {
        let environment = if testnet {
            BinanceEnvironment::Testnet
        } else {
            BinanceEnvironment::Production
        };
        Self { environment }
    }

    pub fn endpoints(&self) -> BinanceEndpoints {
        self.environment.endpoints()
    }
}

impl ExecutionGateway for BinanceGateway {
    fn submit(&self, _order: ExchangeOrder) -> GatewayResult {
        // This synchronous facade has no bound signed transport. It must not
        // manufacture exchange acceptance.
        GatewayResult::Unavailable
    }

    fn cancel(&self, _client_id: u64) -> GatewayResult {
        GatewayResult::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unbound_binance_gateway_never_claims_exchange_acceptance() {
        let gateway = BinanceGateway::new(true);
        let order = ExchangeOrder {
            client_id: 1,
            price: 100,
            quantity: 1,
            post_only: true,
            reduce_only: false,
        };
        assert_eq!(gateway.submit(order), GatewayResult::Unavailable);
        assert_eq!(gateway.cancel(1), GatewayResult::Unavailable);
    }
}
