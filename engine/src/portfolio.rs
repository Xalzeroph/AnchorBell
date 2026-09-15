use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationState {
    Active,
    Released,
    Settled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskReservation {
    pub order_id: String,
    pub margin_quote_ticks: i128,
    pub stress_quote_ticks: i128,
    pub state: ReservationState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationBook {
    pub reservations: Vec<RiskReservation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub quote_asset: String,
    pub equity_quote_ticks: i128,
    pub available_margin_quote_ticks: i128,
    pub maintenance_margin_quote_ticks: i128,
    pub reserved_margin_quote_ticks: i128,
    pub stress_budget_quote_ticks: i128,
    pub used_stress_quote_ticks: i128,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
    pub source_digest: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortfolioError {
    InvalidSnapshot,
    StaleSnapshot,
    DuplicateReservation,
    ReservationNotFound,
    InvalidReservationState,
    InsufficientMargin,
    MaintenanceLimit,
    StressLimit,
}

impl ReservationBook {
    pub fn validate(&self) -> Result<(), PortfolioError> {
        let mut ids = BTreeMap::new();
        for reservation in &self.reservations {
            if reservation.order_id.trim().is_empty()
                || reservation.margin_quote_ticks < 0
                || reservation.stress_quote_ticks < 0
                || ids.insert(reservation.order_id.as_str(), ()).is_some()
            {
                return Err(PortfolioError::InvalidReservationState);
            }
        }
        Ok(())
    }

    pub fn active_totals(&self) -> Result<(i128, i128), PortfolioError> {
        self.validate()?;
        self.reservations
            .iter()
            .filter(|item| item.state == ReservationState::Active)
            .try_fold((0_i128, 0_i128), |(margin, stress), item| {
                Ok((
                    margin
                        .checked_add(item.margin_quote_ticks)
                        .ok_or(PortfolioError::MaintenanceLimit)?,
                    stress
                        .checked_add(item.stress_quote_ticks)
                        .ok_or(PortfolioError::StressLimit)?,
                ))
            })
    }

    pub fn reserve(
        &mut self,
        order_id: impl Into<String>,
        margin_quote_ticks: i128,
        stress_quote_ticks: i128,
    ) -> Result<(), PortfolioError> {
        self.validate()?;
        let order_id = order_id.into();
        if order_id.trim().is_empty()
            || margin_quote_ticks < 0
            || stress_quote_ticks < 0
            || self
                .reservations
                .iter()
                .any(|item| item.order_id == order_id)
        {
            return Err(
                if order_id.trim().is_empty() || margin_quote_ticks < 0 || stress_quote_ticks < 0 {
                    PortfolioError::InvalidReservationState
                } else {
                    PortfolioError::DuplicateReservation
                },
            );
        }
        self.reservations.push(RiskReservation {
            order_id,
            margin_quote_ticks,
            stress_quote_ticks,
            state: ReservationState::Active,
        });
        Ok(())
    }

    pub fn release(&mut self, order_id: &str) -> Result<(), PortfolioError> {
        let item = self
            .reservations
            .iter_mut()
            .find(|item| item.order_id == order_id)
            .ok_or(PortfolioError::ReservationNotFound)?;
        if item.state != ReservationState::Active {
            return Err(PortfolioError::InvalidReservationState);
        }
        item.state = ReservationState::Released;
        Ok(())
    }

    pub fn settle(&mut self, order_id: &str) -> Result<(), PortfolioError> {
        let item = self
            .reservations
            .iter_mut()
            .find(|item| item.order_id == order_id)
            .ok_or(PortfolioError::ReservationNotFound)?;
        if item.state != ReservationState::Active {
            return Err(PortfolioError::InvalidReservationState);
        }
        item.state = ReservationState::Settled;
        Ok(())
    }
}

impl PortfolioSnapshot {
    pub fn validate_at(&self, now_ms: u64) -> Result<(), PortfolioError> {
        if self.quote_asset.trim().is_empty()
            || self.equity_quote_ticks < 0
            || self.available_margin_quote_ticks < 0
            || self.maintenance_margin_quote_ticks < 0
            || self.reserved_margin_quote_ticks < 0
            || self.stress_budget_quote_ticks < 0
            || self.used_stress_quote_ticks < 0
            || self.maintenance_margin_quote_ticks > self.equity_quote_ticks
            || self.reserved_margin_quote_ticks > self.equity_quote_ticks
            || self.used_stress_quote_ticks > self.stress_budget_quote_ticks
            || self.source_digest.trim().is_empty()
        {
            return Err(PortfolioError::InvalidSnapshot);
        }
        if now_ms < self.observed_at_ms
            || now_ms.saturating_sub(self.observed_at_ms) > self.max_age_ms
        {
            return Err(PortfolioError::StaleSnapshot);
        }
        Ok(())
    }

    pub fn validate_with_reservations(
        &self,
        reservations: &ReservationBook,
        now_ms: u64,
    ) -> Result<(), PortfolioError> {
        self.validate_at(now_ms)?;
        let (margin, stress) = reservations.active_totals()?;
        if margin > self.reserved_margin_quote_ticks || stress > self.used_stress_quote_ticks {
            return Err(PortfolioError::InvalidSnapshot);
        }
        Ok(())
    }

    pub fn permits_additional_risk(
        &self,
        now_ms: u64,
        required_margin_quote_ticks: i128,
        required_stress_quote_ticks: i128,
    ) -> Result<(), PortfolioError> {
        self.validate_at(now_ms)?;
        if required_margin_quote_ticks < 0 || required_stress_quote_ticks < 0 {
            return Err(PortfolioError::InvalidSnapshot);
        }
        if self.available_margin_quote_ticks < required_margin_quote_ticks {
            return Err(PortfolioError::InsufficientMargin);
        }
        let post_maintenance = self
            .maintenance_margin_quote_ticks
            .checked_add(required_margin_quote_ticks)
            .ok_or(PortfolioError::MaintenanceLimit)?;
        if post_maintenance > self.equity_quote_ticks {
            return Err(PortfolioError::MaintenanceLimit);
        }
        let post_stress = self
            .used_stress_quote_ticks
            .checked_add(required_stress_quote_ticks)
            .ok_or(PortfolioError::StressLimit)?;
        if post_stress > self.stress_budget_quote_ticks {
            return Err(PortfolioError::StressLimit);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> PortfolioSnapshot {
        PortfolioSnapshot {
            quote_asset: "USDT".into(),
            equity_quote_ticks: 1_000,
            available_margin_quote_ticks: 500,
            maintenance_margin_quote_ticks: 100,
            reserved_margin_quote_ticks: 100,
            stress_budget_quote_ticks: 800,
            used_stress_quote_ticks: 200,
            observed_at_ms: 1_000,
            max_age_ms: 100,
            source_digest: "digest".into(),
        }
    }

    #[test]
    fn reservation_book_is_unique_and_releases_only_once() {
        let mut book = ReservationBook::default();
        book.reserve("o1", 40, 60).unwrap();
        assert_eq!(
            book.reserve("o1", 1, 1),
            Err(PortfolioError::DuplicateReservation)
        );
        assert_eq!(book.active_totals(), Ok((40, 60)));
        book.release("o1").unwrap();
        assert_eq!(book.active_totals(), Ok((0, 0)));
        assert_eq!(
            book.release("o1"),
            Err(PortfolioError::InvalidReservationState)
        );
    }

    #[test]
    fn snapshot_must_cover_active_reservations() {
        let mut book = ReservationBook::default();
        book.reserve("o1", 101, 1).unwrap();
        assert_eq!(
            snapshot().validate_with_reservations(&book, 1_050),
            Err(PortfolioError::InvalidSnapshot)
        );
    }

    #[test]
    fn permits_only_within_margin_maintenance_and_stress_budgets() {
        let p = snapshot();
        assert_eq!(p.permits_additional_risk(1_050, 400, 600), Ok(()));
        assert_eq!(
            p.permits_additional_risk(1_050, 501, 1),
            Err(PortfolioError::InsufficientMargin)
        );
        assert_eq!(
            p.permits_additional_risk(1_050, 1, 601),
            Err(PortfolioError::StressLimit)
        );
    }

    #[test]
    fn rejects_stale_or_inconsistent_observations() {
        let mut p = snapshot();
        assert_eq!(
            p.permits_additional_risk(1_101, 1, 1),
            Err(PortfolioError::StaleSnapshot)
        );
        p.maintenance_margin_quote_ticks = 1_001;
        assert_eq!(
            p.permits_additional_risk(1_050, 1, 1),
            Err(PortfolioError::InvalidSnapshot)
        );
    }
}
