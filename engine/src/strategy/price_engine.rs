const PICO_BPS_SCALE: i128 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy)]
pub struct MakerPriceEngine {
    pub offset_bps: i64,
    pub offset_pico_bps: i64,
}

impl MakerPriceEngine {
    pub fn new(offset_bps: i64) -> Self {
        Self {
            offset_bps,
            offset_pico_bps: 0,
        }
    }

    pub fn new_precise(offset_pico_bps: i64) -> Self {
        Self {
            offset_bps: ((i128::from(offset_pico_bps) + PICO_BPS_SCALE / 2) / PICO_BPS_SCALE)
                .clamp(0, i128::from(i64::MAX)) as i64,
            offset_pico_bps,
        }
    }

    #[inline]
    pub fn buy_price(&self, index_price: i64) -> i64 {
        let offset = if self.offset_pico_bps != 0 {
            i128::from(self.offset_pico_bps)
        } else {
            i128::from(self.offset_bps) * PICO_BPS_SCALE
        };
        scaled_price_precise(index_price, -offset)
    }

    #[inline]
    pub fn sell_price(&self, index_price: i64) -> i64 {
        let offset = if self.offset_pico_bps != 0 {
            i128::from(self.offset_pico_bps)
        } else {
            i128::from(self.offset_bps) * PICO_BPS_SCALE
        };
        scaled_price_precise(index_price, offset)
    }
}

#[inline]
fn scaled_price_precise(index_price: i64, offset_pico_bps: i128) -> i64 {
    let adjustment =
        i128::from(index_price).saturating_mul(offset_pico_bps) / (10_000 * PICO_BPS_SCALE);
    (i128::from(index_price) + adjustment).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(test)]
mod tests {
    use super::MakerPriceEngine;

    #[test]
    fn applies_offset_in_bps() {
        let engine = MakerPriceEngine::new(100);
        assert_eq!(engine.buy_price(100_000), 99_000);
        assert_eq!(engine.sell_price(100_000), 101_000);
    }

    #[test]
    fn handles_extreme_prices_without_overflow() {
        let engine = MakerPriceEngine::new(i64::MAX);
        assert_eq!(engine.buy_price(i64::MAX), i64::MIN);
        assert_eq!(engine.sell_price(i64::MAX), i64::MAX);
    }
}
