use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};

use crate::{
    analytics_validation::EvidenceState,
    market::binance::{BinanceMarketEvent, BookTicker, MarkPrice},
    simulation::AnchorSnapshot,
};

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceConfig {
    pub horizons_ms: Vec<u64>,
    pub min_abs_residual_bps: i64,
    pub max_pending_points_per_symbol: usize,
}

impl Default for EvidenceConfig {
    fn default() -> Self {
        Self {
            horizons_ms: vec![1_000, 5_000, 30_000, 300_000],
            min_abs_residual_bps: 2,
            max_pending_points_per_symbol: 10_000,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceHorizonSummary {
    pub horizon_ms: u64,
    pub samples: u64,
    pub improved: u64,
    pub mean_signed_improvement_bps: i64,
    pub mean_price_index_change_bps: i64,
    pub mean_index_anchor_change_bps: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceSummary {
    pub evidence_id: String,
    pub methodology_id: String,
    pub evidence_state: EvidenceState,
    pub anchor_authority_passed: bool,
    pub status: String,
    pub anchor_semantics: String,
    pub price_semantics: String,
    pub index_semantics: String,
    pub mark_semantics: String,
    pub observations: u64,
    pub eligible_observations: u64,
    pub anchor_integrity_violations: u64,
    pub horizons: Vec<EvidenceHorizonSummary>,
    pub price_discovery_status: String,
    pub economic_edge_status: String,
    pub survival_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceRecord {
    pub symbol: String,
    pub episode_id: String,
    pub start_event_time_ms: u64,
    pub end_event_time_ms: u64,
    pub horizon_ms: u64,
    pub residual_start_bps: i64,
    pub residual_end_bps: i64,
    pub signed_improvement_bps: i64,
    pub price_index_change_bps: i64,
    pub index_anchor_change_bps: i64,
}

#[derive(Debug, Clone)]
struct Point {
    event_time_ms: u64,
    episode_id: String,
    anchor_ticks: i64,
    price_ticks: i64,
    index_ticks: i64,
    residual_bps: i64,
    done: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
struct QuoteState {
    bid: Option<i64>,
    ask: Option<i64>,
    mark: Option<MarkPrice>,
    event_time_ms: u64,
}

pub struct EvidenceAccumulator {
    anchor_authority_passed: bool,
    config: EvidenceConfig,
    quotes: BTreeMap<String, QuoteState>,
    pending: BTreeMap<String, VecDeque<Point>>,
    last_anchors: BTreeMap<String, AnchorSnapshot>,
    observations: u64,
    eligible_observations: u64,
    anchor_integrity_violations: u64,
    stats: BTreeMap<u64, (u64, u64, i128, i128, i128)>,
}

impl EvidenceAccumulator {
    pub fn new(config: EvidenceConfig) -> Self {
        let mut normalized = config;
        normalized.horizons_ms.sort_unstable();
        normalized.horizons_ms.dedup();
        if normalized.horizons_ms.is_empty() {
            normalized.horizons_ms.push(1_000);
        }
        Self {
            anchor_authority_passed: false,
            config: normalized,
            quotes: BTreeMap::new(),
            pending: BTreeMap::new(),
            last_anchors: BTreeMap::new(),
            observations: 0,
            eligible_observations: 0,
            anchor_integrity_violations: 0,
            stats: BTreeMap::new(),
        }
    }
    pub fn set_anchor_authority(&mut self, passed: bool) {
        self.anchor_authority_passed = passed;
    }
    pub fn evidence_id(&self) -> String {
        "anchorbell-evidence-v1".to_owned()
    }

    pub fn observe(
        &mut self,
        event: &BinanceMarketEvent,
        received_at_ms: u64,
        anchors: &BTreeMap<String, AnchorSnapshot>,
    ) -> Vec<EvidenceRecord> {
        let (symbol, event_time_ms) = match event {
            BinanceMarketEvent::BookTicker(book) => {
                self.update_book(book);
                (book.symbol.to_ascii_uppercase(), book.event_time_ms)
            }
            BinanceMarketEvent::MarkPrice(mark) => {
                self.update_mark(mark);
                (mark.symbol.to_ascii_uppercase(), mark.event_time_ms)
            }
            _ => return Vec::new(),
        };
        let now = if event_time_ms > 0 {
            event_time_ms
        } else {
            received_at_ms
        };
        self.observations = self.observations.saturating_add(1);
        let Some(anchor) = anchors.get(&symbol).copied() else {
            return Vec::new();
        };
        if let Some(previous) = self.last_anchors.get(&symbol).copied() {
            if previous.observed_at_ms == anchor.observed_at_ms
                && previous.close_price_ticks != anchor.close_price_ticks
            {
                self.anchor_integrity_violations =
                    self.anchor_integrity_violations.saturating_add(1);
            }
        }
        self.last_anchors.insert(symbol.clone(), anchor);
        let Some(quote) = self.quotes.get(&symbol) else {
            return Vec::new();
        };
        let (Some(bid), Some(ask), Some(index_ticks)) = (
            quote.bid,
            quote.ask,
            quote.mark.as_ref().map(|mark| mark.index_price.0),
        ) else {
            return Vec::new();
        };
        let price_ticks = bid.saturating_add(ask) / 2;
        if price_ticks <= 0 || anchor.close_price_ticks <= 0 || index_ticks <= 0 {
            return Vec::new();
        }
        let residual_bps = basis_bps(price_ticks, anchor.close_price_ticks);
        if residual_bps.abs() < self.config.min_abs_residual_bps {
            return self.flush_due(&symbol, now, price_ticks, index_ticks);
        }
        self.eligible_observations = self.eligible_observations.saturating_add(1);
        let episode_id = format!("{}:{}", symbol, anchor.observed_at_ms);
        let queue = self.pending.entry(symbol.clone()).or_default();
        queue.push_back(Point {
            event_time_ms: now,
            episode_id,
            anchor_ticks: anchor.close_price_ticks,
            price_ticks,
            index_ticks,
            residual_bps,
            done: Vec::new(),
        });
        while queue.len() > self.config.max_pending_points_per_symbol.max(1) {
            queue.pop_front();
        }
        self.flush_due(&symbol, now, price_ticks, index_ticks)
    }

    fn update_book(&mut self, book: &BookTicker) {
        let quote = self
            .quotes
            .entry(book.symbol.to_ascii_uppercase())
            .or_default();
        quote.bid = Some(book.bid_price.0);
        quote.ask = Some(book.ask_price.0);
        quote.event_time_ms = quote.event_time_ms.max(book.event_time_ms);
    }

    fn update_mark(&mut self, mark: &MarkPrice) {
        let quote = self
            .quotes
            .entry(mark.symbol.to_ascii_uppercase())
            .or_default();
        quote.mark = Some(mark.clone());
        quote.event_time_ms = quote.event_time_ms.max(mark.event_time_ms);
    }

    fn flush_due(
        &mut self,
        symbol: &str,
        now: u64,
        price_ticks: i64,
        index_ticks: i64,
    ) -> Vec<EvidenceRecord> {
        let horizons = self.config.horizons_ms.clone();
        let mut output = Vec::new();
        let mut updates: BTreeMap<u64, (u64, u64, i128, i128, i128)> = BTreeMap::new();
        if let Some(queue) = self.pending.get_mut(symbol) {
            for point in queue.iter_mut() {
                for horizon in &horizons {
                    if point.done.contains(horizon)
                        || now < point.event_time_ms.saturating_add(*horizon)
                    {
                        continue;
                    }
                    let end_residual = basis_bps(price_ticks, point.anchor_ticks);
                    let signed = -point.residual_bps.signum() * (end_residual - point.residual_bps);
                    let pi_change = basis_bps(price_ticks, index_ticks)
                        .saturating_sub(basis_bps(point.price_ticks, point.index_ticks));
                    let ia_change = basis_bps(index_ticks, point.anchor_ticks)
                        .saturating_sub(basis_bps(point.index_ticks, point.anchor_ticks));
                    let entry = updates.entry(*horizon).or_default();
                    entry.0 = entry.0.saturating_add(1);
                    if signed > 0 {
                        entry.1 = entry.1.saturating_add(1);
                    }
                    entry.2 += i128::from(signed);
                    entry.3 += i128::from(pi_change);
                    entry.4 += i128::from(ia_change);
                    output.push(EvidenceRecord {
                        symbol: symbol.to_owned(),
                        episode_id: point.episode_id.clone(),
                        start_event_time_ms: point.event_time_ms,
                        end_event_time_ms: now,
                        horizon_ms: *horizon,
                        residual_start_bps: point.residual_bps,
                        residual_end_bps: end_residual,
                        signed_improvement_bps: signed,
                        price_index_change_bps: pi_change,
                        index_anchor_change_bps: ia_change,
                    });
                    point.done.push(*horizon);
                }
            }
            while queue
                .front()
                .is_some_and(|point| point.done.len() == horizons.len())
            {
                queue.pop_front();
            }
        }
        for (horizon, (samples, improved, signed, pi, ia)) in updates {
            let entry = self.stats.entry(horizon).or_default();
            entry.0 = entry.0.saturating_add(samples);
            entry.1 = entry.1.saturating_add(improved);
            entry.2 += signed;
            entry.3 += pi;
            entry.4 += ia;
        }
        output
    }

    pub fn summary(&self) -> EvidenceSummary {
        let horizons = self
            .config
            .horizons_ms
            .iter()
            .map(|horizon| {
                let (samples, improved, signed, pi, ia) =
                    self.stats.get(horizon).copied().unwrap_or_default();
                EvidenceHorizonSummary {
                    horizon_ms: *horizon,
                    samples,
                    improved,
                    mean_signed_improvement_bps: mean_i128(signed, samples),
                    mean_price_index_change_bps: mean_i128(pi, samples),
                    mean_index_anchor_change_bps: mean_i128(ia, samples),
                }
            })
            .collect();
        let evidence_state = if self.observations == 0 {
            EvidenceState::X0NoData
        } else if self.eligible_observations == 0 {
            EvidenceState::X1Observed
        } else {
            EvidenceState::X2Eligible
        };
        EvidenceSummary {
            evidence_id: self.evidence_id(),
            anchor_authority_passed: self.anchor_authority_passed,
            methodology_id: "anchorbell-analytics-methods-v1".to_owned(),
            evidence_state,
            anchor_semantics:
                "configured AnchorSnapshot; external-close transformation is caller-owned"
                    .to_owned(),
            price_semantics: "best_bid_ask_mid (not last trade)".to_owned(),
            index_semantics: "Binance MarkPrice.index_price".to_owned(),
            mark_semantics:
                "Binance MarkPrice.mark_price (used for feed state, not substituted for index)"
                    .to_owned(),
            status: if !self.anchor_authority_passed {
                "invalid_reference".to_owned()
            } else if self.eligible_observations == 0 {
                "insufficient_data".to_owned()
            } else {
                "observational".to_owned()
            },
            observations: self.observations,
            eligible_observations: self.eligible_observations,
            anchor_integrity_violations: self.anchor_integrity_violations,
            horizons,
            price_discovery_status: "not_available_in_live_simulation".to_owned(),
            economic_edge_status: "evaluated_per_ledger".to_owned(),
            survival_status: "evaluated_per_ledger".to_owned(),
        }
    }
}
fn mean_i128(sum: i128, count: u64) -> i64 {
    if count == 0 {
        0
    } else {
        (sum / i128::from(count)).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}

fn basis_bps(value: i64, reference: i64) -> i64 {
    if reference <= 0 {
        return 0;
    }
    ((i128::from(value) - i128::from(reference)) * 10_000 / i128::from(reference))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_improvement_is_positive_when_residual_converges() {
        assert!(basis_bps(101, 100) > 0);
        assert_eq!(
            -basis_bps(101, 100).signum() * (0 - basis_bps(101, 100)),
            100
        );
    }
    #[test]
    fn evidence_requires_authoritative_anchor_before_claiming_observational_status() {
        let mut accumulator = EvidenceAccumulator::new(EvidenceConfig::default());
        let invalid = accumulator.summary();
        assert!(!invalid.anchor_authority_passed);
        assert_eq!(invalid.status, "invalid_reference");
        accumulator.set_anchor_authority(true);
        let ready = accumulator.summary();
        assert!(ready.anchor_authority_passed);
        assert_eq!(ready.status, "insufficient_data");
    }
}
