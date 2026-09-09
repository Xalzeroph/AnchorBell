#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderIntentError {
    InvalidShape,
    UnscopedAggressor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderIntent {
    pub symbol: u32,
    pub side: Side,
    pub price: i64,
    pub quantity: i64,
    pub post_only: bool,
    pub reduce_only: bool,
}

impl OrderIntent {
    pub fn maker_buy(symbol: u32, price: i64, quantity: i64) -> Self {
        Self {
            symbol,
            side: Side::Buy,
            price,
            quantity,
            post_only: true,
            reduce_only: false,
        }
    }
    pub fn maker_sell(symbol: u32, price: i64, quantity: i64) -> Self {
        Self {
            symbol,
            side: Side::Sell,
            price,
            quantity,
            post_only: true,
            reduce_only: false,
        }
    }
    pub fn reduce_only_maker_buy(symbol: u32, price: i64, quantity: i64) -> Self {
        Self {
            symbol,
            side: Side::Buy,
            price,
            quantity,
            post_only: true,
            reduce_only: true,
        }
    }
    pub fn reduce_only_maker_sell(symbol: u32, price: i64, quantity: i64) -> Self {
        Self {
            symbol,
            side: Side::Sell,
            price,
            quantity,
            post_only: true,
            reduce_only: true,
        }
    }
    pub fn emergency_reduce_only_taker(symbol: u32, side: Side, price: i64, quantity: i64) -> Self {
        Self {
            symbol,
            side,
            price,
            quantity,
            post_only: false,
            reduce_only: true,
        }
    }
    /// Validates the complete execution shape at the domain boundary.
    ///
    /// An order may be passive, or it may explicitly reduce existing exposure.
    /// A caller cannot accidentally create an unscoped aggressive intent.
    pub fn validate(&self) -> Result<(), OrderIntentError> {
        if !self.is_admissible_shape() {
            return Err(OrderIntentError::InvalidShape);
        }
        if !self.post_only && !self.reduce_only {
            return Err(OrderIntentError::UnscopedAggressor);
        }
        Ok(())
    }

    pub fn is_admissible_shape(&self) -> bool {
        self.symbol > 0 && self.price > 0 && self.quantity > 0
    }
    pub fn is_emergency_taker(&self) -> bool {
        !self.post_only && self.reduce_only
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_intents_share_one_domain_validator() {
        assert!(OrderIntent::maker_buy(1, 100, 2).validate().is_ok());
        assert!(
            OrderIntent::emergency_reduce_only_taker(1, Side::Sell, 99, 2)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn invalid_intents_fail_closed() {
        assert_eq!(
            (OrderIntent {
                symbol: 1,
                side: Side::Buy,
                price: 100,
                quantity: 2,
                post_only: false,
                reduce_only: false,
            })
            .validate(),
            Err(OrderIntentError::UnscopedAggressor)
        );
        assert_eq!(
            (OrderIntent {
                symbol: 0,
                side: Side::Buy,
                price: 100,
                quantity: 2,
                post_only: true,
                reduce_only: false,
            })
            .validate(),
            Err(OrderIntentError::InvalidShape)
        );
    }
}
