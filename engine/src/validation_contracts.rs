use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const VALIDATION_CONTRACT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreregistrationManifest {
    pub schema_version: u16,
    pub manifest_id: String,
    pub strategy_version: String,
    pub data_digest: String,
    pub horizons_ms: Vec<u64>,
    pub min_effect_bps: i64,
    pub alpha_ppm: u32,
    pub seed: u64,
    pub sealed: bool,
}

impl PreregistrationManifest {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != VALIDATION_CONTRACT_SCHEMA_VERSION
            || self.manifest_id.trim().is_empty()
            || self.strategy_version.trim().is_empty()
            || self.data_digest.trim().is_empty()
            || self.horizons_ms.is_empty()
            || self.min_effect_bps < 0
            || self.alpha_ppm == 0
            || self.alpha_ppm >= 1_000_000
        {
            return Err("invalid preregistration manifest");
        }
        if self.horizons_ms.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("horizons must be strictly increasing");
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, &'static str> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| "manifest serialization failed")?;
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClosureEpisodeKey {
    pub symbol: String,
    pub closure_type: String,
    pub close_time_ms: u64,
    pub open_time_ms: u64,
    pub rule_regime_id: String,
    pub anchor_version: String,
}

impl ClosureEpisodeKey {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.symbol.trim().is_empty()
            || self.closure_type.trim().is_empty()
            || self.rule_regime_id.trim().is_empty()
            || self.anchor_version.trim().is_empty()
            || self.close_time_ms >= self.open_time_ms
        {
            return Err("invalid closure episode key");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureEpisode {
    pub episode_id: String,
    pub key: ClosureEpisodeKey,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
}

impl ClosureEpisode {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.key.validate()?;
        if self.episode_id.trim().is_empty()
            || self.start_time_ms < self.key.close_time_ms
            || self.end_time_ms < self.start_time_ms
        {
            return Err("invalid closure episode");
        }
        Ok(())
    }
}
#[derive(Debug, Default)]
pub struct EpisodeRegistry {
    sealed: bool,
    ids: BTreeSet<String>,
    episodes: Vec<ClosureEpisode>,
}

impl EpisodeRegistry {
    pub fn insert(&mut self, episode: ClosureEpisode) -> Result<(), &'static str> {
        if self.sealed {
            return Err("episode registry is sealed");
        }
        episode.validate()?;
        if !self.ids.insert(episode.episode_id.clone()) {
            return Err("duplicate episode id");
        }
        self.episodes.push(episode);
        Ok(())
    }

    pub fn seal(&mut self) {
        self.sealed = true;
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn len(&self) -> usize {
        self.episodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.episodes.is_empty()
    }

    pub fn episodes(&self) -> &[ClosureEpisode] {
        &self.episodes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OnlineObservation {
    pub symbol: String,
    pub valid_time_ms: u64,
    pub received_at_ms: u64,
    pub price_ticks: i64,
    pub index_ticks: i64,
    pub anchor_ticks: i64,
}

impl OnlineObservation {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.symbol.trim().is_empty()
            || self.valid_time_ms == 0
            || self.received_at_ms < self.valid_time_ms
            || self.price_ticks <= 0
            || self.index_ticks <= 0
            || self.anchor_ticks <= 0
        {
            return Err("invalid online observation");
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpeningLabel {
    pub valid_time_ms: u64,
    pub known_at_ms: u64,
    pub opening_reference_ticks: i64,
    pub first_trade_ticks: i64,
}

impl OpeningLabel {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.valid_time_ms == 0
            || self.known_at_ms < self.valid_time_ms
            || self.opening_reference_ticks <= 0
            || self.first_trade_ticks <= 0
        {
            return Err("invalid future opening label");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OfflineLabeledObservation {
    pub observation: OnlineObservation,
    pub label: OpeningLabel,
}

impl OfflineLabeledObservation {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.observation.validate()?;
        self.label.validate()?;
        if self.label.known_at_ms < self.observation.valid_time_ms {
            return Err("label known time precedes observation time");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BasisDecomposition {
    pub price_index_bps: i64,
    pub index_anchor_bps: i64,
    pub price_anchor_bps: i64,
}

impl BasisDecomposition {
    pub fn from_ticks(price_ticks: i64, index_ticks: i64, anchor_ticks: i64) -> Option<Self> {
        if price_ticks <= 0 || index_ticks <= 0 || anchor_ticks <= 0 {
            return None;
        }
        let basis = |value: i64, reference: i64| -> i64 {
            ((i128::from(value) - i128::from(reference)) * 10_000 / i128::from(reference)) as i64
        };
        Some(Self {
            price_index_bps: basis(price_ticks, index_ticks),
            index_anchor_bps: basis(index_ticks, anchor_ticks),
            price_anchor_bps: basis(price_ticks, anchor_ticks),
        })
    }

    pub fn identity_error_bps(self) -> i64 {
        self.price_anchor_bps
            .saturating_sub(self.price_index_bps)
            .saturating_sub(self.index_anchor_bps)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FalsificationStatus {
    NotRun,
    Passed,
    Failed,
    Inconclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationVerdict {
    Rejected,
    Inconclusive,
    DescriptiveSupported,
    MechanismSupported,
    ConditionalAlpha,
    EconomicallyTradable,
    Deployable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HypothesisGate {
    pub labels_complete: bool,
    pub h_r_supported: bool,
    pub h_m_supported: bool,
    pub h_d_information_state: bool,
    pub h_e_positive: bool,
    pub h_s_survives: bool,
    pub falsification: FalsificationStatus,
}

pub fn adjudicate_hypotheses(gate: HypothesisGate) -> ValidationVerdict {
    if !gate.labels_complete || matches!(gate.falsification, FalsificationStatus::Failed) {
        return ValidationVerdict::Inconclusive;
    }
    if !gate.h_r_supported {
        return ValidationVerdict::Rejected;
    }
    if !gate.h_m_supported {
        return ValidationVerdict::DescriptiveSupported;
    }
    if gate.h_d_information_state {
        return ValidationVerdict::ConditionalAlpha;
    }
    if !gate.h_e_positive {
        return ValidationVerdict::MechanismSupported;
    }
    if !gate.h_s_survives {
        return ValidationVerdict::EconomicallyTradable;
    }
    ValidationVerdict::Deployable
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CostBound {
    pub fee_bps: i64,
    pub funding_bps: i64,
    pub queue_bps: i64,
    pub adverse_selection_bps: i64,
    pub unwind_bps: i64,
    pub tail_bps: i64,
    pub model_bps: i64,
}

impl CostBound {
    pub fn total_bps(&self) -> Option<i64> {
        let values = [
            self.fee_bps,
            self.funding_bps,
            self.queue_bps,
            self.adverse_selection_bps,
            self.unwind_bps,
            self.tail_bps,
            self.model_bps,
        ];
        if values.iter().any(|value| *value < 0) {
            return None;
        }
        Some(
            values
                .into_iter()
                .fold(0_i64, |total, value| total.saturating_add(value)),
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct EconomicEdgeBound {
    pub edge_lcb_bps: i64,
    pub cost_ucb_bps: i64,
}

impl EconomicEdgeBound {
    pub fn is_positive(self) -> bool {
        self.edge_lcb_bps > self.cost_ucb_bps
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn episode(id: &str) -> ClosureEpisode {
        ClosureEpisode {
            episode_id: id.to_owned(),
            key: ClosureEpisodeKey {
                symbol: "CXMTUSDT".to_owned(),
                closure_type: "overnight".to_owned(),
                close_time_ms: 1_000,
                open_time_ms: 10_000,
                rule_regime_id: "orderbook-ewma-v1".to_owned(),
                anchor_version: "anchor-1".to_owned(),
            },
            start_time_ms: 1_000,
            end_time_ms: 2_000,
        }
    }

    #[test]
    fn preregistration_digest_is_deterministic() {
        let manifest = PreregistrationManifest {
            schema_version: VALIDATION_CONTRACT_SCHEMA_VERSION,
            manifest_id: "m-1".to_owned(),
            strategy_version: "strategy-1".to_owned(),
            data_digest: "sha256:data".to_owned(),
            horizons_ms: vec![1_000, 5_000],
            min_effect_bps: 5,
            alpha_ppm: 50_000,
            seed: 7,
            sealed: true,
        };
        assert_eq!(manifest.digest(), manifest.digest());
    }

    #[test]
    fn episode_registry_rejects_duplicates_and_post_seal_writes() {
        let mut registry = EpisodeRegistry::default();
        registry.insert(episode("e-1")).unwrap();
        assert!(registry.insert(episode("e-1")).is_err());
        registry.seal();
        assert!(registry.insert(episode("e-2")).is_err());
    }

    #[test]
    fn future_labels_are_separate_from_online_observations() {
        let observation = OnlineObservation {
            symbol: "CXMTUSDT".to_owned(),
            valid_time_ms: 2_000,
            received_at_ms: 2_010,
            price_ticks: 98_000,
            index_ticks: 98_100,
            anchor_ticks: 100_000,
        };
        let label = OpeningLabel {
            valid_time_ms: 10_000,
            known_at_ms: 10_001,
            opening_reference_ticks: 101_000,
            first_trade_ticks: 101_100,
        };
        assert!(OfflineLabeledObservation { observation, label }
            .validate()
            .is_ok());
    }

    #[test]
    fn basis_decomposition_is_explicit() {
        let basis = BasisDecomposition::from_ticks(101, 100, 100).unwrap();
        assert_eq!(basis.price_index_bps, basis.price_anchor_bps);
        assert_eq!(basis.index_anchor_bps, 0);
    }

    #[test]
    fn verdict_cannot_claim_deployable_without_all_gates() {
        let gate = HypothesisGate {
            labels_complete: true,
            h_r_supported: true,
            h_m_supported: true,
            h_d_information_state: false,
            h_e_positive: true,
            h_s_survives: false,
            falsification: FalsificationStatus::Passed,
        };
        assert_eq!(
            adjudicate_hypotheses(gate),
            ValidationVerdict::EconomicallyTradable
        );
    }
}
