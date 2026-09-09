#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
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
    pub fn is_admissible_shape(&self) -> bool {
        self.symbol > 0 && self.price > 0 && self.quantity > 0
    }
    pub fn is_emergency_taker(&self) -> bool {
        !self.post_only && self.reduce_only
    }
}
