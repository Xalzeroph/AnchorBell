//! Exchange-local session calendars for the two supported equity regions.

use super::universe::EquityRegion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindow {
    pub open_minute: u16,
    pub close_minute: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueSessionState {
    Weekend,
    Holiday,
    Closed,
    PreOpenFlatten,
    PreOpenAuction,
    Open,
    MiddayBreak,
    ClosingAuction,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EquitySessionCalendar {
    pub region: EquityRegion,
    pub windows: &'static [SessionWindow],
}

const A_SHARE_WINDOWS: &[SessionWindow] = &[
    SessionWindow {
        open_minute: 570,
        close_minute: 690,
    },
    SessionWindow {
        open_minute: 780,
        close_minute: 900,
    },
];

const HONG_KONG_WINDOWS: &[SessionWindow] = &[
    SessionWindow {
        open_minute: 570,
        close_minute: 720,
    },
    SessionWindow {
        open_minute: 780,
        close_minute: 960,
    },
];

pub const A_SHARE_CALENDAR: EquitySessionCalendar = EquitySessionCalendar {
    region: EquityRegion::AShare,
    windows: A_SHARE_WINDOWS,
};

pub const HONG_KONG_CALENDAR: EquitySessionCalendar = EquitySessionCalendar {
    region: EquityRegion::HongKong,
    windows: HONG_KONG_WINDOWS,
};

pub fn calendar_for(region: EquityRegion) -> EquitySessionCalendar {
    match region {
        EquityRegion::AShare => A_SHARE_CALENDAR,
        EquityRegion::HongKong => HONG_KONG_CALENDAR,
    }
}
impl EquitySessionCalendar {
    /// Returns the active or next conservative exit boundary in exchange-local
    /// time. Unsupported calendar dates deliberately fail closed.
    pub fn exit_deadline_at(&self, timestamp_ms: u64) -> Option<u64> {
        let (day, local_minute) = local_day_and_minute(timestamp_ms)?;
        let (date_key, weekday) = date_and_weekday(day)?;
        if !Self::calendar_snapshot_supported(date_key) {
            return None;
        }
        if !self.is_trading_date(date_key, weekday) {
            return self.next_exit_deadline_after(day);
        }

        let morning_boundary = self.morning_auction_start();
        let morning_close = self.windows.first()?.close_minute;
        let afternoon_boundary = self
            .has_afternoon_session(date_key)
            .then(|| self.windows.get(1).map(|window| window.open_minute))
            .flatten();

        if local_minute < morning_boundary {
            return timestamp_at_local_minute(day, morning_boundary);
        }
        if local_minute < morning_close {
            return timestamp_at_local_minute(day, morning_boundary);
        }
        if let Some(afternoon_boundary) = afternoon_boundary {
            if local_minute < afternoon_boundary {
                return timestamp_at_local_minute(day, afternoon_boundary);
            }
            if local_minute < self.active_final_close_minute(date_key) {
                return timestamp_at_local_minute(day, afternoon_boundary);
            }
        }
        self.next_exit_deadline_after(day)
    }

    fn morning_auction_start(&self) -> u16 {
        match self.region {
            EquityRegion::AShare => 555,
            EquityRegion::HongKong => 540,
        }
    }

    fn has_afternoon_session(&self, date_key: u32) -> bool {
        self.windows
            .get(1)
            .is_some_and(|window| self.effective_final_close_minute(date_key) > window.open_minute)
    }

    fn active_final_close_minute(&self, date_key: u32) -> u16 {
        match self.region {
            EquityRegion::HongKong if self.effective_final_close_minute(date_key) == 960 => 970,
            _ => self.effective_final_close_minute(date_key),
        }
    }

    fn is_trading_date(&self, date_key: u32, weekday: u8) -> bool {
        Self::calendar_snapshot_supported(date_key) && weekday <= 5 && !self.is_holiday(date_key)
    }

    fn next_exit_deadline_after(&self, day: i64) -> Option<u64> {
        for offset in 1..=366 {
            let candidate_day = day.checked_add(offset)?;
            let (date_key, weekday) = date_and_weekday(candidate_day)?;
            if !Self::calendar_snapshot_supported(date_key) {
                return None;
            }
            if self.is_trading_date(date_key, weekday) {
                return timestamp_at_local_minute(candidate_day, self.morning_auction_start());
            }
        }
        None
    }

    /// Converts a UTC timestamp to the exchange-local Gregorian date key YYYYMMDD.
    pub fn date_key_from_timestamp(timestamp_ms: u64) -> u32 {
        let days = ((timestamp_ms / 1_000) + 8 * 3_600) / 86_400;
        let (year, month, day) = civil_from_days(days as i64);
        (year as u32) * 10_000 + month * 100 + day
    }

    /// Returns whether the date is covered by the versioned official snapshot.
    /// Unknown years deliberately remain fail-closed for production decisions.
    pub fn calendar_snapshot_supported(date_key: u32) -> bool {
        matches!(date_key / 10_000, 2026)
    }

    pub fn is_holiday(&self, date_key: u32) -> bool {
        match self.region {
            EquityRegion::AShare => matches!(
                date_key,
                20260101
                    | 20260216..=20260220
                    | 20260223
                    | 20260406
                    | 20260501..=20260505
                    | 20260619
                    | 20260925
                    | 20261001..=20261007
            ),
            EquityRegion::HongKong => matches!(
                date_key,
                20260101
                    | 20260217..=20260219
                    | 20260403
                    | 20260406..=20260407
                    | 20260501
                    | 20260525
                    | 20260619
                    | 20260701
                    | 20261001
                    | 20261019
                    | 20261225
            ),
        }
    }

    pub fn effective_final_close_minute(&self, date_key: u32) -> u16 {
        if self.region == EquityRegion::HongKong
            && matches!(date_key, 20260216 | 20261224 | 20261231)
        {
            720
        } else {
            self.windows
                .last()
                .map(|window| window.close_minute)
                .unwrap_or(0)
        }
    }

    pub fn after_final_close(&self, date_key: u32, weekday: u8, local_minute: u16) -> bool {
        Self::calendar_snapshot_supported(date_key)
            && weekday <= 5
            && !self.is_holiday(date_key)
            && local_minute >= self.effective_final_close_minute(date_key)
    }

    /// Returns the most recent completed final close before a UTC timestamp.
    ///
    /// A close is considered complete only after one additional minute so a
    /// one-minute Binance index-price candle can be retrieved for that close.
    /// This keeps the exchange calendar as the sole authority for the anchor
    /// timestamp and fails closed for unsupported calendar snapshots.
    pub fn latest_completed_close_before(&self, timestamp_ms: u64) -> Option<u64> {
        let (current_day, _local_minute) = local_day_and_minute(timestamp_ms)?;
        for offset in 0..=366 {
            let day = current_day.checked_sub(offset)?;
            let (date_key, weekday) = date_and_weekday(day)?;
            if !Self::calendar_snapshot_supported(date_key)
                || self.is_holiday(date_key)
                || weekday > 5
            {
                continue;
            }
            let close_minute = self.effective_final_close_minute(date_key);
            let close_at_ms = timestamp_at_local_minute(day, close_minute)?;
            if close_at_ms.saturating_add(60_000) <= timestamp_ms {
                return Some(close_at_ms);
            }
        }
        None
    }

    pub fn entry_allowed_on_date(
        &self,
        date_key: u32,
        weekday: u8,
        local_minute: u16,
        flatten_lead_minutes: u16,
        allow_midday_break: bool,
        closing_auction: bool,
    ) -> bool {
        if !Self::calendar_snapshot_supported(date_key) {
            return false;
        }
        if self.is_holiday(date_key) {
            return false;
        }
        if self.region == EquityRegion::HongKong
            && matches!(date_key, 20260216 | 20261224 | 20261231)
            && local_minute >= 720
        {
            return false;
        }
        self.entry_allowed_detailed(
            weekday,
            local_minute,
            false,
            flatten_lead_minutes,
            allow_midday_break,
            closing_auction,
        )
    }

    /// Classifies a minute in China/Hong Kong local time.
    ///
    /// weekday is ISO weekday 1..=7 and local_minute is 0..=1439.
    /// Holidays are intentionally supplied by a separate calendar provider.
    pub fn state_at(
        &self,
        weekday: u8,
        local_minute: u16,
        flatten_lead_minutes: u16,
    ) -> VenueSessionState {
        if weekday == 0 || weekday > 7 || local_minute >= 1_440 {
            return VenueSessionState::Unknown;
        }
        if weekday > 5 {
            return VenueSessionState::Weekend;
        }

        for window in self.windows {
            if local_minute >= window.open_minute && local_minute < window.close_minute {
                return VenueSessionState::Open;
            }
            let flatten_start = window.open_minute.saturating_sub(flatten_lead_minutes);
            if local_minute >= flatten_start && local_minute < window.open_minute {
                return VenueSessionState::PreOpenFlatten;
            }
        }
        VenueSessionState::Closed
    }

    /// Classifies exchange events that are hidden by the compact exchange API.
    /// holiday is supplied by an authoritative exchange calendar provider.
    pub fn detailed_state_at(
        &self,
        weekday: u8,
        local_minute: u16,
        holiday: bool,
        flatten_lead_minutes: u16,
        closing_auction: bool,
    ) -> VenueSessionState {
        if weekday == 0 || weekday > 7 || local_minute >= 1_440 {
            return VenueSessionState::Unknown;
        }
        if weekday > 5 {
            return VenueSessionState::Weekend;
        }
        if holiday {
            return VenueSessionState::Holiday;
        }

        let (pre_open, auction_start, auction_end, midday_start, midday_end): (
            u16,
            u16,
            u16,
            u16,
            u16,
        ) = match self.region {
            EquityRegion::AShare => (555, 897, 900, 690, 780),
            EquityRegion::HongKong => (540, 960, 970, 720, 780),
        };
        if local_minute >= pre_open.saturating_sub(flatten_lead_minutes) && local_minute < pre_open
        {
            return VenueSessionState::PreOpenFlatten;
        }
        if local_minute >= pre_open && local_minute < self.windows[0].open_minute {
            return VenueSessionState::PreOpenAuction;
        }
        if local_minute >= midday_start && local_minute < midday_end {
            return VenueSessionState::MiddayBreak;
        }
        if closing_auction && local_minute >= auction_start && local_minute < auction_end {
            return VenueSessionState::ClosingAuction;
        }
        if self
            .windows
            .iter()
            .any(|window| local_minute >= window.open_minute && local_minute < window.close_minute)
        {
            return VenueSessionState::Open;
        }
        VenueSessionState::Closed
    }

    pub fn entry_allowed_detailed(
        &self,
        weekday: u8,
        local_minute: u16,
        holiday: bool,
        flatten_lead_minutes: u16,
        allow_midday_break: bool,
        closing_auction: bool,
    ) -> bool {
        match self.detailed_state_at(
            weekday,
            local_minute,
            holiday,
            flatten_lead_minutes,
            closing_auction,
        ) {
            VenueSessionState::Closed => true,
            VenueSessionState::MiddayBreak => allow_midday_break,
            _ => false,
        }
    }

    pub fn entry_allowed(&self, weekday: u8, local_minute: u16, flatten_lead_minutes: u16) -> bool {
        self.state_at(weekday, local_minute, flatten_lead_minutes) == VenueSessionState::Closed
    }
}

/// Howard Hinnant's integer Gregorian conversion, kept allocation-free for
/// session checks on the market-data hot path.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

fn local_day_and_minute(timestamp_ms: u64) -> Option<(i64, u16)> {
    let local_seconds = (timestamp_ms / 1_000).checked_add(8 * 3_600)?;
    let days = local_seconds / 86_400;
    let day = i64::try_from(days).ok()?;
    let minute = u16::try_from((local_seconds % 86_400) / 60).ok()?;
    Some((day, minute))
}

fn date_and_weekday(days_since_epoch: i64) -> Option<(u32, u8)> {
    let (year, month, day) = civil_from_days(days_since_epoch);
    let year = u32::try_from(year).ok()?;
    let date_key = year
        .checked_mul(10_000)?
        .checked_add(month.checked_mul(100)?)?
        .checked_add(day)?;
    let weekday = u8::try_from((days_since_epoch + 3).rem_euclid(7) + 1).ok()?;
    Some((date_key, weekday))
}

fn timestamp_at_local_minute(days_since_epoch: i64, local_minute: u16) -> Option<u64> {
    let local_seconds = days_since_epoch
        .checked_mul(86_400)?
        .checked_add(i64::from(local_minute).checked_mul(60)?)?;
    let utc_seconds = local_seconds.checked_sub(8 * 3_600)?;
    u64::try_from(utc_seconds.checked_mul(1_000)?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp_for_local(year: i32, month: u32, day: u32, minute: u16) -> u64 {
        let days = days_from_civil(year, month, day);
        ((days * 86_400 + i64::from(minute) * 60 - 8 * 3_600) * 1_000) as u64
    }

    fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
        let year = i64::from(year) - i64::from(month <= 2);
        let era = if year >= 0 { year } else { year - 399 } / 400;
        let yoe = year - era * 400;
        let month = i64::from(month);
        let day = i64::from(day);
        let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }

    #[test]
    fn a_share_calendar_has_distinct_morning_and_afternoon_windows() {
        assert_eq!(
            A_SHARE_CALENDAR.state_at(1, 600, 30),
            VenueSessionState::Open
        );
        assert_eq!(
            A_SHARE_CALENDAR.state_at(1, 750, 30),
            VenueSessionState::PreOpenFlatten
        );
        assert_eq!(
            A_SHARE_CALENDAR.state_at(1, 720, 30),
            VenueSessionState::Closed
        );
    }

    #[test]
    fn hong_kong_calendar_has_a_longer_morning_session() {
        assert_eq!(
            HONG_KONG_CALENDAR.state_at(1, 700, 30),
            VenueSessionState::Open
        );
        assert_eq!(
            HONG_KONG_CALENDAR.state_at(1, 750, 30),
            VenueSessionState::PreOpenFlatten
        );
        assert_eq!(
            HONG_KONG_CALENDAR.state_at(1, 735, 30),
            VenueSessionState::Closed
        );
    }

    #[test]
    fn weekend_and_invalid_clock_values_fail_closed() {
        assert_eq!(
            A_SHARE_CALENDAR.state_at(6, 600, 30),
            VenueSessionState::Weekend
        );
        assert_eq!(
            A_SHARE_CALENDAR.state_at(0, 600, 30),
            VenueSessionState::Unknown
        );
        assert_eq!(
            A_SHARE_CALENDAR.state_at(1, 1_440, 30),
            VenueSessionState::Unknown
        );
        assert!(!A_SHARE_CALENDAR.entry_allowed(1, 570, 30));
    }

    #[test]
    fn detailed_rules_expose_auction_and_midday_breaks() {
        assert_eq!(
            A_SHARE_CALENDAR.detailed_state_at(1, 555, false, 30, true),
            VenueSessionState::PreOpenAuction
        );
        assert_eq!(
            A_SHARE_CALENDAR.detailed_state_at(1, 700, false, 30, true),
            VenueSessionState::MiddayBreak
        );
        assert_eq!(
            HONG_KONG_CALENDAR.detailed_state_at(1, 965, false, 30, true),
            VenueSessionState::ClosingAuction
        );
        assert!(!A_SHARE_CALENDAR.entry_allowed_detailed(1, 700, false, 30, false, true));
        assert!(A_SHARE_CALENDAR.entry_allowed_detailed(1, 700, false, 30, true, true));
        assert!(!A_SHARE_CALENDAR.entry_allowed_detailed(1, 600, true, 30, true, true));
    }

    #[test]
    fn official_2026_holidays_and_half_days_are_fail_closed() {
        assert!(A_SHARE_CALENDAR.is_holiday(20260925));
        assert!(HONG_KONG_CALENDAR.is_holiday(20261019));
        assert!(!HONG_KONG_CALENDAR.is_holiday(20261224));
        assert_eq!(
            HONG_KONG_CALENDAR.effective_final_close_minute(20261224),
            720
        );
        assert!(HONG_KONG_CALENDAR.after_final_close(20261224, 4, 720));
        assert!(!HONG_KONG_CALENDAR.entry_allowed_on_date(20261224, 4, 720, 30, true, true));
    }

    #[test]
    fn unsupported_calendar_year_fails_closed() {
        assert!(!EquitySessionCalendar::calendar_snapshot_supported(
            20270101
        ));
        assert!(!A_SHARE_CALENDAR.entry_allowed_on_date(20270101, 5, 600, 30, true, true));
    }

    #[test]
    fn latest_completed_close_uses_previous_trading_close_during_session() {
        let during_session = timestamp_for_local(2026, 1, 6, 600);
        assert_eq!(
            A_SHARE_CALENDAR.latest_completed_close_before(during_session),
            Some(timestamp_for_local(2026, 1, 5, 900))
        );
    }

    #[test]
    fn latest_completed_close_uses_same_day_after_final_close() {
        let after_close = timestamp_for_local(2026, 1, 5, 901);
        assert_eq!(
            A_SHARE_CALENDAR.latest_completed_close_before(after_close),
            Some(timestamp_for_local(2026, 1, 5, 900))
        );
    }

    #[test]
    fn date_key_uses_china_local_time() {
        assert_eq!(EquitySessionCalendar::date_key_from_timestamp(0), 19700101);
    }

    #[test]
    fn exit_deadline_keeps_active_morning_boundary_and_uses_afternoon_reopen() {
        let morning = timestamp_for_local(2026, 1, 5, 600);
        assert_eq!(
            A_SHARE_CALENDAR.exit_deadline_at(morning),
            Some(timestamp_for_local(2026, 1, 5, 555))
        );

        let lunch = timestamp_for_local(2026, 1, 5, 700);
        assert_eq!(
            A_SHARE_CALENDAR.exit_deadline_at(lunch),
            Some(timestamp_for_local(2026, 1, 5, 780))
        );
    }

    #[test]
    fn exit_deadline_advances_across_weekends_holidays_and_half_days() {
        let friday_night = timestamp_for_local(2026, 1, 9, 1_000);
        assert_eq!(
            A_SHARE_CALENDAR.exit_deadline_at(friday_night),
            Some(timestamp_for_local(2026, 1, 12, 555))
        );

        let holiday = timestamp_for_local(2026, 1, 1, 700);
        assert_eq!(
            A_SHARE_CALENDAR.exit_deadline_at(holiday),
            Some(timestamp_for_local(2026, 1, 2, 555))
        );

        let hong_kong_half_day_after_close = timestamp_for_local(2026, 2, 16, 730);
        assert_eq!(
            HONG_KONG_CALENDAR.exit_deadline_at(hong_kong_half_day_after_close),
            Some(timestamp_for_local(2026, 2, 20, 540))
        );
    }

    #[test]
    fn exit_deadline_does_not_invent_unsupported_sessions() {
        assert_eq!(
            A_SHARE_CALENDAR.exit_deadline_at(timestamp_for_local(2027, 1, 4, 600)),
            None
        );
        assert_eq!(
            A_SHARE_CALENDAR.exit_deadline_at(timestamp_for_local(2025, 12, 31, 700)),
            None
        );
    }

    #[test]
    fn exit_deadline_keeps_hong_kong_closing_auction_on_afternoon_boundary() {
        let closing_auction = timestamp_for_local(2026, 1, 5, 965);
        assert_eq!(
            HONG_KONG_CALENDAR.exit_deadline_at(closing_auction),
            Some(timestamp_for_local(2026, 1, 5, 780))
        );
    }
}
