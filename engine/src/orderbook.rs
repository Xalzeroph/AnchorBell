use std::collections::BTreeMap;

use crate::market::binance::DepthUpdate;

#[derive(Debug, Default)]
pub struct OrderBook {
    best_bid: i64,
    best_ask: i64,
}

impl OrderBook {
    pub fn update(&mut self, bid: i64, ask: i64) {
        self.best_bid = bid;
        self.best_ask = ask;
    }

    pub fn bid(&self) -> i64 {
        self.best_bid
    }

    pub fn ask(&self) -> i64 {
        self.best_ask
    }

    #[inline]
    pub fn mid(&self) -> i64 {
        self.best_bid + self.best_ask.saturating_sub(self.best_bid) / 2
    }

    #[inline]
    pub fn spread(&self) -> i64 {
        if self.best_ask >= self.best_bid {
            self.best_ask - self.best_bid
        } else {
            0
        }
    }
}

/// Sequence-validated local book built from one REST snapshot plus Binance
/// diff-depth updates. A gap invalidates the book until a fresh snapshot is
/// loaded; callers must not continue matching against a partial book.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocalOrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    last_update_id: Option<u64>,
    valid: bool,
    /// The first diff after a REST snapshot uses Binance's bridge rule
    /// (U <= lastUpdateId + 1 <= u); pu continuity applies thereafter.
    awaiting_first_diff: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthApplyResult {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookError {
    SnapshotRequired,
    SequenceGap {
        expected: u64,
        first: u64,
        previous: Option<u64>,
    },
    InvalidLevel,
    CrossedBook,
}

impl LocalOrderBook {
    /// Applies Binance's REST/WebSocket bridge rule to buffered depth events.
    pub fn load_snapshot_and_replay(
        &mut self,
        last_update_id: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
        buffered: &[DepthUpdate],
    ) -> Result<usize, OrderBookError> {
        self.load_snapshot(last_update_id, bids, asks)?;
        let expected = last_update_id.saturating_add(1);
        let Some(first_index) = buffered.iter().position(|update| {
            (update.first_update_id <= expected && update.final_update_id >= expected)
                // Some Binance TradFi depth streams use non-contiguous U/u
                // ranges. If pu directly names the snapshot ID, it is still
                // an unambiguous first-event bridge.
                || (update.previous_final_update_id == Some(last_update_id)
                    && update.final_update_id > last_update_id)
        }) else {
            self.valid = false;
            return Err(OrderBookError::SequenceGap {
                expected,
                first: buffered.first().map_or(0, |u| u.first_update_id),
                previous: buffered.first().and_then(|u| u.previous_final_update_id),
            });
        };
        self.apply_bridge(&buffered[first_index])?;
        let mut consumed = first_index + 1;
        for update in buffered.iter().skip(consumed) {
            self.apply_diff(update)?;
            consumed += 1;
        }
        Ok(consumed)
    }

    fn apply_bridge(&mut self, update: &DepthUpdate) -> Result<(), OrderBookError> {
        let last = self
            .last_update_id
            .ok_or(OrderBookError::SnapshotRequired)?;
        let expected = last.saturating_add(1);
        let strict_bridge =
            update.first_update_id <= expected && update.final_update_id >= expected;
        let pu_bridge =
            update.previous_final_update_id == Some(last) && update.final_update_id > last;
        if !strict_bridge && !pu_bridge {
            self.valid = false;
            return Err(OrderBookError::SequenceGap {
                expected,
                first: update.first_update_id,
                previous: update.previous_final_update_id,
            });
        }
        for level in &update.bids {
            insert_level(&mut self.bids, level.price.0, level.quantity.0)?;
        }
        for level in &update.asks {
            insert_level(&mut self.asks, level.price.0, level.quantity.0)?;
        }
        validate_crossed(&self.bids, &self.asks)?;
        self.last_update_id = Some(update.final_update_id);
        self.awaiting_first_diff = false;
        Ok(())
    }

    pub fn load_snapshot(
        &mut self,
        last_update_id: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> Result<(), OrderBookError> {
        let mut next_bids = BTreeMap::new();
        let mut next_asks = BTreeMap::new();
        for &(price, quantity) in bids {
            insert_level(&mut next_bids, price, quantity)?;
        }
        for &(price, quantity) in asks {
            insert_level(&mut next_asks, price, quantity)?;
        }
        validate_crossed(&next_bids, &next_asks)?;
        self.bids = next_bids;
        self.asks = next_asks;
        self.last_update_id = Some(last_update_id);
        self.valid = true;
        self.awaiting_first_diff = true;
        Ok(())
    }

    pub fn apply_diff(&mut self, update: &DepthUpdate) -> Result<DepthApplyResult, OrderBookError> {
        let last = self
            .last_update_id
            .ok_or(OrderBookError::SnapshotRequired)?;
        if !self.valid {
            return Err(OrderBookError::SnapshotRequired);
        }
        if update.final_update_id <= last {
            return Ok(DepthApplyResult::Duplicate);
        }
        let expected = last.saturating_add(1);
        // Binance's bridge rule applies only to the first diff after the
        // snapshot. For later diffs, pu is the authoritative continuity
        // check; U may legitimately jump because one event can cover a
        // range of update IDs that were not emitted as separate messages.
        let sequence_ok = if self.awaiting_first_diff {
            update.first_update_id <= expected && update.final_update_id >= expected
        } else {
            update.previous_final_update_id == Some(last)
        };
        if !sequence_ok {
            self.valid = false;
            return Err(OrderBookError::SequenceGap {
                expected,
                first: update.first_update_id,
                previous: update.previous_final_update_id,
            });
        }
        for level in &update.bids {
            insert_level(&mut self.bids, level.price.0, level.quantity.0)?;
        }
        for level in &update.asks {
            insert_level(&mut self.asks, level.price.0, level.quantity.0)?;
        }
        if let Err(error) = validate_crossed(&self.bids, &self.asks) {
            self.valid = false;
            return Err(error);
        }
        self.last_update_id = Some(update.final_update_id);
        self.awaiting_first_diff = false;
        Ok(DepthApplyResult::Applied)
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(&price, &quantity)| (price, quantity))
    }

    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks
            .iter()
            .next()
            .map(|(&price, &quantity)| (price, quantity))
    }

    pub fn quantity_at(&self, bid: bool, price: i64) -> i64 {
        let levels = if bid { &self.bids } else { &self.asks };
        levels.get(&price).copied().unwrap_or(0)
    }

    pub fn depth(&self, bid: bool) -> usize {
        if bid {
            self.bids.len()
        } else {
            self.asks.len()
        }
    }
}

fn insert_level(
    book: &mut BTreeMap<i64, i64>,
    price: i64,
    quantity: i64,
) -> Result<(), OrderBookError> {
    if price <= 0 || quantity < 0 {
        return Err(OrderBookError::InvalidLevel);
    }
    if quantity == 0 {
        book.remove(&price);
    } else {
        book.insert(price, quantity);
    }
    Ok(())
}

fn validate_crossed(
    bids: &BTreeMap<i64, i64>,
    asks: &BTreeMap<i64, i64>,
) -> Result<(), OrderBookError> {
    if bids
        .iter()
        .next_back()
        .zip(asks.iter().next())
        .is_some_and(|((&bid, _), (&ask, _))| bid >= ask)
    {
        return Err(OrderBookError::CrossedBook);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DepthApplyResult, LocalOrderBook, OrderBookError};
    use crate::market::binance::{DepthLevel, DepthUpdate};
    use crate::strategy::{PriceTicks, Quantity};

    fn update(first: u64, final_id: u64, previous: Option<u64>) -> DepthUpdate {
        DepthUpdate {
            symbol: "ABCUSDT".into(),
            event_time_ms: 1,
            transaction_time_ms: 1,
            first_update_id: first,
            final_update_id: final_id,
            previous_final_update_id: previous,
            bids: vec![DepthLevel {
                price: PriceTicks(99),
                quantity: Quantity(4),
            }],
            asks: vec![DepthLevel {
                price: PriceTicks(101),
                quantity: Quantity(5),
            }],
        }
    }

    #[test]
    fn snapshot_replay_uses_last_update_plus_one_bridge_rule() {
        let mut book = LocalOrderBook::default();
        let buffered = [update(11, 12, None), update(13, 14, Some(12))];
        assert_eq!(
            book.load_snapshot_and_replay(10, &[(99, 3)], &[(101, 4)], &buffered),
            Ok(2)
        );
        assert_eq!(book.last_update_id(), Some(14));
    }

    #[test]
    fn snapshot_replay_accepts_pu_bridge_when_u_range_jumps() {
        let mut book = LocalOrderBook::default();
        let buffered = [update(20, 21, Some(10)), update(30, 31, Some(21))];
        assert_eq!(
            book.load_snapshot_and_replay(10, &[(99, 3)], &[(101, 4)], &buffered),
            Ok(2)
        );
        assert_eq!(book.last_update_id(), Some(31));
    }

    #[test]
    fn snapshot_replay_reports_missing_bridge_at_next_sequence() {
        let mut book = LocalOrderBook::default();
        let error = book
            .load_snapshot_and_replay(10, &[(99, 3)], &[(101, 4)], &[update(12, 12, None)])
            .unwrap_err();
        assert_eq!(
            error,
            OrderBookError::SequenceGap {
                expected: 11,
                first: 12,
                previous: None
            }
        );
    }

    #[test]
    fn snapshot_and_contiguous_diff_update_the_book() {
        let mut book = LocalOrderBook::default();
        book.load_snapshot(10, &[(99, 3)], &[(101, 4)]).unwrap();
        assert_eq!(
            book.apply_diff(&update(11, 12, Some(10))),
            Ok(DepthApplyResult::Applied)
        );
        assert_eq!(book.last_update_id(), Some(12));
        assert_eq!(book.best_bid(), Some((99, 4)));
        assert_eq!(book.best_ask(), Some((101, 5)));
    }

    #[test]
    fn later_diff_uses_pu_continuity_even_when_u_jumps() {
        let mut book = LocalOrderBook::default();
        let buffered = [update(11, 12, None), update(20, 21, Some(12))];
        assert_eq!(
            book.load_snapshot_and_replay(10, &[(99, 3)], &[(101, 4)], &buffered),
            Ok(2)
        );
        assert_eq!(book.last_update_id(), Some(21));
    }

    #[test]
    fn duplicate_is_idempotent_but_gap_invalidates_until_resync() {
        let mut book = LocalOrderBook::default();
        book.load_snapshot(10, &[(99, 3)], &[(101, 4)]).unwrap();
        assert_eq!(
            book.apply_diff(&update(10, 10, Some(9))),
            Ok(DepthApplyResult::Duplicate)
        );
        assert!(matches!(
            book.apply_diff(&update(13, 13, Some(12))),
            Err(OrderBookError::SequenceGap { .. })
        ));
        assert!(!book.is_valid());
        assert_eq!(
            book.apply_diff(&update(14, 14, Some(13))),
            Err(OrderBookError::SnapshotRequired)
        );
    }

    #[test]
    fn crossed_or_invalid_snapshot_is_rejected() {
        let mut book = LocalOrderBook::default();
        assert_eq!(
            book.load_snapshot(1, &[(101, 1)], &[(100, 1)]),
            Err(OrderBookError::CrossedBook)
        );
        assert_eq!(
            book.load_snapshot(1, &[(99, -1)], &[(101, 1)]),
            Err(OrderBookError::InvalidLevel)
        );
    }
}
