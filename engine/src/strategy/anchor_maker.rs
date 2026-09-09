use crate::execution::{OrderIntent, Side};

use super::{decide_adaptive_signal, AdaptiveThreshold, SignalDecision, SignalInput};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    BuyMaker,
    SellMaker,
    NoAction,
}

#[derive(Debug, Clone, Copy)]
pub struct AnchorMakerStrategy {
    pub entry_threshold_bps: i64,
    pub exit_threshold_bps: i64,
}

impl AnchorMakerStrategy {
    pub fn new(entry_threshold_bps: i64, exit_threshold_bps: i64) -> Self {
        Self {
            entry_threshold_bps,
            exit_threshold_bps,
        }
    }

    pub fn generate_intent(
        &self,
        symbol: u32,
        bid: i64,
        ask: i64,
        anchor_price: i64,
        quantity: i64,
    ) -> Option<OrderIntent> {
        if symbol == 0
            || bid <= 0
            || ask < bid
            || anchor_price <= 0
            || quantity <= 0
            || self.entry_threshold_bps < 0
        {
            return None;
        }

        let mid = bid + (ask - bid) / 2;
        let deviation_numerator = (i128::from(mid) - i128::from(anchor_price)) * 10_000;
        let threshold_numerator = i128::from(self.entry_threshold_bps) * i128::from(anchor_price);

        if deviation_numerator <= -threshold_numerator {
            Some(OrderIntent {
                symbol,
                side: Side::Buy,
                price: bid,
                quantity,
                post_only: true,
                reduce_only: false,
            })
        } else if deviation_numerator >= threshold_numerator {
            Some(OrderIntent {
                symbol,
                side: Side::Sell,
                price: ask,
                quantity,
                post_only: true,
                reduce_only: false,
            })
        } else {
            None
        }
    }

    /// Routes live, replay, simulation, and backtest callers through the
    /// cost-aware admission contract.
    pub fn generate_adaptive_intent(input: SignalInput) -> Option<OrderIntent> {
        match decide_adaptive_signal(input) {
            SignalDecision::BuyMaker { price, quantity } => Some(OrderIntent {
                symbol: input.symbol,
                side: Side::Buy,
                price: price.0,
                quantity,
                post_only: true,
                reduce_only: false,
            }),
            SignalDecision::SellMaker { price, quantity } => Some(OrderIntent {
                symbol: input.symbol,
                side: Side::Sell,
                price: price.0,
                quantity,
                post_only: true,
                reduce_only: false,
            }),
            SignalDecision::Blocked(_) => None,
        }
    }

    pub fn default_adaptive_threshold(&self, floor_bps: i64) -> Option<AdaptiveThreshold> {
        if floor_bps < 0 {
            return None;
        }
        AdaptiveThreshold::from_components(floor_bps, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strategy() -> AnchorMakerStrategy {
        AnchorMakerStrategy::new(100, 0)
    }

    #[test]
    fn uses_external_anchor_for_maker_decision() {
        let intent = strategy().generate_intent(7, 98_000, 98_100, 100_000, 500);
        assert_eq!(intent, Some(OrderIntent::maker_buy(7, 98_000, 500)));
    }

    #[test]
    fn rejects_invalid_inputs_without_arithmetic_overflow() {
        assert_eq!(
            strategy().generate_intent(0, 98_000, 98_100, 100_000, 500),
            None
        );
        assert_eq!(
            strategy().generate_intent(7, i64::MAX - 100, i64::MAX, i64::MAX, 500),
            None
        );
    }
}
