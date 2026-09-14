//! Pure, passive maker-exit decisions shared by paper and live adapters.

use crate::execution::{OrderIntent, Side};

use super::{DualFlattenPlan, FlattenPhase, FundingScheduleStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitBook {
    pub bid: i64,
    pub ask: i64,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitConstraints {
    pub min_price: i64,
    pub max_price: i64,
    pub price_tick: i64,
    pub min_quantity: i64,
    pub max_quantity: i64,
    pub quantity_step: i64,
    pub min_notional: i64,
    pub quantity_scale: u32,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitWorkingOrder {
    None,
    Pending,
    Confirmed {
        side: Side,
        price: i64,
        remaining: i64,
        reduce_only: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MakerExitInput {
    pub symbol: u32,
    pub position: i64,
    pub position_confirmed: bool,
    pub now_ms: u64,
    pub max_book_age_ms: u64,
    pub plan: DualFlattenPlan,
    /// Permit passive inventory reduction when the live fair-value signal
    /// has reversed, even outside the scheduled flatten windows.
    pub allow_reversion_exit: bool,
    pub book: Option<ExitBook>,
    pub constraints: Option<ExitConstraints>,
    pub working: ExitWorkingOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitBlockReason {
    InvalidInput,
    InvalidBook,
    StaleBook,
    MissingConstraints,
    InvalidConstraints,
    StaleConstraints,
    Dust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MakerExitDecision {
    Trading,
    Flat,
    WaitForReconciliation,
    KeepWorking,
    CancelWorking,
    Submit(OrderIntent),
    ResidualExposure,
    Blocked(ExitBlockReason),
}

pub fn decide_maker_exit(input: MakerExitInput) -> MakerExitDecision {
    // Use a position-aware phase even when flat so schedule classification is
    // independent of a zero balance.
    let phase = input.plan.phase_at(input.now_ms, true);

    if !input.position_confirmed || matches!(input.working, ExitWorkingOrder::Pending) {
        return MakerExitDecision::WaitForReconciliation;
    }

    if input.symbol == 0 {
        return MakerExitDecision::Blocked(ExitBlockReason::InvalidInput);
    }
    let position_quantity = match input.position.checked_abs() {
        Some(quantity) => quantity,
        None => return MakerExitDecision::Blocked(ExitBlockReason::InvalidInput),
    };

    if phase == FlattenPhase::Trading && !input.allow_reversion_exit {
        return if input.plan.funding.status == FundingScheduleStatus::Unknown {
            MakerExitDecision::Blocked(ExitBlockReason::InvalidInput)
        } else {
            MakerExitDecision::Trading
        };
    }

    if position_quantity == 0 {
        return match input.working {
            ExitWorkingOrder::None => MakerExitDecision::Flat,
            ExitWorkingOrder::Pending => MakerExitDecision::WaitForReconciliation,
            ExitWorkingOrder::Confirmed { .. } => MakerExitDecision::CancelWorking,
        };
    }

    if phase == FlattenPhase::ResidualExposure {
        return MakerExitDecision::ResidualExposure;
    }

    let quote = match desired_quote(&input) {
        Ok(quote) => quote,
        Err(reason) => {
            return match input.working {
                ExitWorkingOrder::Confirmed { .. } => MakerExitDecision::CancelWorking,
                ExitWorkingOrder::None | ExitWorkingOrder::Pending => {
                    MakerExitDecision::Blocked(reason)
                }
            };
        }
    };

    match input.working {
        ExitWorkingOrder::Confirmed {
            side,
            price,
            remaining,
            reduce_only,
        } if reduce_only
            && side == quote.side
            && price == quote.price
            && remaining > 0
            && remaining <= position_quantity
            && remaining <= quote.constraints.max_quantity =>
        {
            MakerExitDecision::KeepWorking
        }
        ExitWorkingOrder::Confirmed { .. } => MakerExitDecision::CancelWorking,
        ExitWorkingOrder::Pending => MakerExitDecision::WaitForReconciliation,
        ExitWorkingOrder::None => match fresh_quantity(position_quantity, quote) {
            Ok(quantity) => MakerExitDecision::Submit(match quote.side {
                Side::Buy => {
                    OrderIntent::reduce_only_maker_buy(input.symbol, quote.price, quantity)
                }
                Side::Sell => {
                    OrderIntent::reduce_only_maker_sell(input.symbol, quote.price, quantity)
                }
            }),
            Err(reason) => MakerExitDecision::Blocked(reason),
        },
    }
}

#[derive(Debug, Clone, Copy)]
struct ExitQuote {
    side: Side,
    price: i64,
    constraints: ExitConstraints,
}

fn desired_quote(input: &MakerExitInput) -> Result<ExitQuote, ExitBlockReason> {
    let constraints = validated_constraints(input)?;
    let book = validated_book(input)?;
    let (side, price) = if input.position > 0 {
        (Side::Sell, book.ask)
    } else {
        (Side::Buy, book.bid)
    };

    if price < constraints.min_price
        || price > constraints.max_price
        || price % constraints.price_tick != 0
    {
        return Err(ExitBlockReason::InvalidConstraints);
    }

    Ok(ExitQuote {
        side,
        price,
        constraints,
    })
}

fn validated_constraints(input: &MakerExitInput) -> Result<ExitConstraints, ExitBlockReason> {
    let constraints = input
        .constraints
        .ok_or(ExitBlockReason::MissingConstraints)?;
    if constraints.min_price <= 0
        || constraints.max_price < constraints.min_price
        || constraints.price_tick <= 0
        || constraints.min_quantity <= 0
        || constraints.max_quantity < constraints.min_quantity
        || constraints.quantity_step <= 0
        || constraints.min_notional <= 0
        || constraints.observed_at_ms > input.now_ms
    {
        return Err(ExitBlockReason::InvalidConstraints);
    }
    if input.now_ms - constraints.observed_at_ms > constraints.max_age_ms {
        return Err(ExitBlockReason::StaleConstraints);
    }
    if 10_i128.checked_pow(constraints.quantity_scale).is_none() {
        return Err(ExitBlockReason::InvalidConstraints);
    }
    Ok(constraints)
}

fn validated_book(input: &MakerExitInput) -> Result<ExitBook, ExitBlockReason> {
    let book = input.book.ok_or(ExitBlockReason::InvalidBook)?;
    if book.bid <= 0 || book.ask <= book.bid || book.observed_at_ms > input.now_ms {
        return Err(ExitBlockReason::InvalidBook);
    }
    if input.now_ms - book.observed_at_ms > input.max_book_age_ms {
        return Err(ExitBlockReason::StaleBook);
    }
    Ok(book)
}

fn fresh_quantity(position_quantity: i64, quote: ExitQuote) -> Result<i64, ExitBlockReason> {
    let capped = position_quantity.min(quote.constraints.max_quantity);
    let rounded = capped - capped % quote.constraints.quantity_step;
    if rounded < quote.constraints.min_quantity {
        return Err(ExitBlockReason::Dust);
    }

    let notional = i128::from(quote.price)
        .checked_mul(i128::from(rounded))
        .ok_or(ExitBlockReason::InvalidConstraints)?;
    let floor = i128::from(quote.constraints.min_notional)
        .checked_mul(
            10_i128
                .checked_pow(quote.constraints.quantity_scale)
                .ok_or(ExitBlockReason::InvalidConstraints)?,
        )
        .ok_or(ExitBlockReason::InvalidConstraints)?;
    if notional < floor {
        return Err(ExitBlockReason::Dust);
    }
    Ok(rounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::{FundingRateKind, FundingSchedule};

    fn input() -> MakerExitInput {
        MakerExitInput {
            symbol: 7,
            position: 10,
            position_confirmed: true,
            now_ms: 1_000,
            max_book_age_ms: 100,
            plan: DualFlattenPlan::new(
                1_000,
                None,
                FundingSchedule::new(
                    Some(10_000),
                    Some(8),
                    Some(100),
                    FundingRateKind::Regular,
                    1_000,
                )
                .unwrap(),
                1_800_000,
                9_000,
            )
            .unwrap(),
            allow_reversion_exit: false,
            book: Some(ExitBook {
                bid: 9_880,
                ask: 9_890,
                observed_at_ms: 1_000,
            }),
            constraints: Some(ExitConstraints {
                min_price: 1,
                max_price: 20_000,
                price_tick: 1,
                min_quantity: 1,
                max_quantity: 50,
                quantity_step: 1,
                min_notional: 1,
                quantity_scale: 0,
                observed_at_ms: 1_000,
                max_age_ms: 100,
            }),
            working: ExitWorkingOrder::None,
        }
    }

    #[test]
    fn submits_passive_quote_for_long_and_short_positions() {
        let long_input = input();
        assert_eq!(
            decide_maker_exit(long_input),
            MakerExitDecision::Submit(OrderIntent::reduce_only_maker_sell(7, 9_890, 10))
        );

        let mut short_input = input();
        short_input.position = -10;
        assert_eq!(
            decide_maker_exit(short_input),
            MakerExitDecision::Submit(OrderIntent::reduce_only_maker_buy(7, 9_880, 10))
        );
    }

    #[test]
    fn reversion_exit_is_allowed_outside_scheduled_window() {
        let mut value = input();
        value.allow_reversion_exit = true;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Submit(OrderIntent::reduce_only_maker_sell(7, 9_890, 10))
        );
    }

    #[test]
    fn rejects_unknown_state_and_invalid_inputs_before_submitting() {
        let mut value = input();
        value.position_confirmed = false;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::WaitForReconciliation
        );

        value = input();
        value.working = ExitWorkingOrder::Pending;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::WaitForReconciliation
        );

        value = input();
        value.symbol = 0;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidInput)
        );

        value = input();
        value.position = i64::MIN;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidInput)
        );
    }

    #[test]
    fn rejects_invalid_or_stale_books_and_constraints() {
        let mut value = input();
        value.book = Some(ExitBook {
            bid: 9_890,
            ask: 9_890,
            observed_at_ms: 1_000,
        });
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidBook)
        );

        value = input();
        value.book = Some(ExitBook {
            bid: 9_880,
            ask: 9_890,
            observed_at_ms: 1_101,
        });
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidBook)
        );

        value = input();
        value.book = Some(ExitBook {
            bid: 9_880,
            ask: 9_890,
            observed_at_ms: 899,
        });
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::StaleBook)
        );

        value = input();
        value.constraints = None;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::MissingConstraints)
        );

        value = input();
        value.constraints.as_mut().unwrap().observed_at_ms = 1_101;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidConstraints)
        );

        value = input();
        value.constraints.as_mut().unwrap().observed_at_ms = 899;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::StaleConstraints)
        );
    }

    #[test]
    fn caps_rounds_down_and_reports_dust_without_overreducing() {
        let mut value = input();
        value.position = 150;
        value.constraints = Some(ExitConstraints {
            min_price: 1,
            max_price: 20_000,
            price_tick: 1,
            min_quantity: 10,
            max_quantity: 125,
            quantity_step: 10,
            min_notional: 1,
            quantity_scale: 2,
            observed_at_ms: 1_000,
            max_age_ms: 100,
        });
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Submit(OrderIntent::reduce_only_maker_sell(7, 9_890, 120))
        );

        value.position = 9;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::Dust)
        );
    }

    #[test]
    fn keeps_only_safe_known_orders_and_cancels_unsafe_ones() {
        let mut value = input();
        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Sell,
            price: 9_890,
            remaining: 5,
            reduce_only: true,
        };
        assert_eq!(decide_maker_exit(value), MakerExitDecision::KeepWorking);

        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Buy,
            price: 9_890,
            remaining: 5,
            reduce_only: true,
        };
        assert_eq!(decide_maker_exit(value), MakerExitDecision::CancelWorking);

        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Sell,
            price: 9_889,
            remaining: 5,
            reduce_only: true,
        };
        assert_eq!(decide_maker_exit(value), MakerExitDecision::CancelWorking);

        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Sell,
            price: 9_890,
            remaining: 5,
            reduce_only: false,
        };
        assert_eq!(decide_maker_exit(value), MakerExitDecision::CancelWorking);
    }

    #[test]
    fn keeps_accepted_partial_remainder_without_reapplying_new_order_filters() {
        let mut value = input();
        value.position = 5;
        value.constraints.as_mut().unwrap().min_quantity = 10;
        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Sell,
            price: 9_890,
            remaining: 5,
            reduce_only: true,
        };

        assert_eq!(decide_maker_exit(value), MakerExitDecision::KeepWorking);
    }

    #[test]
    fn flat_and_expired_positions_do_not_synthesize_fills() {
        let mut value = input();
        value.position = 0;
        assert_eq!(decide_maker_exit(value), MakerExitDecision::Flat);

        value = input();
        value.now_ms = 10_000;
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::ResidualExposure
        );
    }

    #[test]
    fn flat_working_orders_are_reconciled_and_unknown_schedules_do_not_allow_trading() {
        let mut value = input();
        value.position = 0;
        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Sell,
            price: 9_890,
            remaining: 1,
            reduce_only: true,
        };
        assert_eq!(decide_maker_exit(value), MakerExitDecision::CancelWorking);

        value = input();
        value.now_ms = 999;
        value.plan = DualFlattenPlan::new(
            999,
            None,
            FundingSchedule::new(None, None, None, FundingRateKind::Unknown, 999).unwrap(),
            1_800_000,
            9_000,
        )
        .unwrap();
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidInput)
        );
    }

    #[test]
    fn pre_window_entries_follow_known_or_unknown_funding_before_flat_reconciliation() {
        let mut value = input();
        value.position = 0;
        value.plan = DualFlattenPlan::new(
            1_000,
            None,
            FundingSchedule::new(
                Some(20_000),
                Some(8),
                Some(100),
                FundingRateKind::Regular,
                1_000,
            )
            .unwrap(),
            1_800_000,
            9_000,
        )
        .unwrap();
        assert_eq!(decide_maker_exit(value), MakerExitDecision::Trading);

        value.working = ExitWorkingOrder::Confirmed {
            side: Side::Buy,
            price: 9_880,
            remaining: 1,
            reduce_only: false,
        };
        assert_eq!(decide_maker_exit(value), MakerExitDecision::Trading);

        value.plan = DualFlattenPlan::new(
            1_000,
            None,
            FundingSchedule::new(None, None, None, FundingRateKind::Unknown, 1_000).unwrap(),
            1_800_000,
            9_000,
        )
        .unwrap();
        assert_eq!(
            decide_maker_exit(value),
            MakerExitDecision::Blocked(ExitBlockReason::InvalidInput)
        );
    }
}
