use crate::model::{
    AccountSnapshot, AnchorEpisode, CandidateOrder, EpisodePhase, EvidenceFrame,
    OutcomeDistribution, Side, ValidatedOrder,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyNode {
    BindAnchor,
    RequireClosedWindow,
    MeasureResidual,
    EstimateJointOutcome,
    AdmitIfRobustValueBeatsWait,
    QuotePassive,
    ReduceAtEarliestDeadline,
    ReconcileBeforeResume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyPlan {
    pub version: String,
    pub nodes: Vec<StrategyNode>,
    pub invariants: Vec<String>,
}

impl StrategyPlan {
    pub fn anchor_closed_maker(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            nodes: vec![
                StrategyNode::BindAnchor,
                StrategyNode::RequireClosedWindow,
                StrategyNode::MeasureResidual,
                StrategyNode::EstimateJointOutcome,
                StrategyNode::AdmitIfRobustValueBeatsWait,
                StrategyNode::QuotePassive,
                StrategyNode::ReduceAtEarliestDeadline,
                StrategyNode::ReconcileBeforeResume,
            ],
            invariants: vec![
                "anchor".into(),
                "external_closed".into(),
                "binance_contract".into(),
                "causal".into(),
                "maker_entry".into(),
                "reconciled_account".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Wait {
        reason: DecisionReason,
    },
    PlaceMaker {
        order: ValidatedOrder,
        robust_value_pico_bps: i64,
    },
    ReduceOnly {
        order: ValidatedOrder,
        robust_value_pico_bps: i64,
    },
    ResidualExposure {
        position: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionReason {
    IncompleteEvidence,
    OutsideEntryWindow,
    StaleMarket,
    NoRobustEdge,
    ContractRejected,
    PositionLimit,
    HardDeadline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyLimits {
    pub requested_quantity: i64,
    pub minimum_robust_value_pico_bps: i64,
}

pub struct DecisionEngine {
    pub plan: StrategyPlan,
    pub limits: PolicyLimits,
}

impl DecisionEngine {
    pub fn decide(&self, frame: &EvidenceFrame) -> Decision {
        if !frame.complete_for_entry() || !frame.account.reconciled {
            return Decision::Wait {
                reason: DecisionReason::IncompleteEvidence,
            };
        }
        if frame.market.validate(frame.now_ms).is_err() {
            return Decision::Wait {
                reason: DecisionReason::StaleMarket,
            };
        }
        let phase = frame.episode.phase_at(frame.now_ms, frame.account.position);
        if phase == EpisodePhase::Closed {
            return if frame.account.position == 0 {
                Decision::Wait {
                    reason: DecisionReason::HardDeadline,
                }
            } else {
                Decision::ResidualExposure {
                    position: frame.account.position,
                }
            };
        }
        if frame.account.position != 0 && phase == EpisodePhase::Reducing {
            return self.reduce(frame);
        }
        if !frame.episode.entry_allowed(frame.now_ms) {
            return Decision::Wait {
                reason: DecisionReason::OutsideEntryWindow,
            };
        }
        let buy = self.entry(frame, Side::Buy, &frame.buy_outcome);
        let sell = self.entry(frame, Side::Sell, &frame.sell_outcome);
        match (buy, sell) {
            (Some((b, bv)), Some((_s, sv)))
                if bv >= sv && bv > self.limits.minimum_robust_value_pico_bps =>
            {
                Decision::PlaceMaker {
                    order: b,
                    robust_value_pico_bps: bv,
                }
            }
            (Some((b, bv)), Some((_s, sv)))
                if bv > self.limits.minimum_robust_value_pico_bps && bv > sv =>
            {
                Decision::PlaceMaker {
                    order: b,
                    robust_value_pico_bps: bv,
                }
            }
            (Some((b, bv)), None) if bv > self.limits.minimum_robust_value_pico_bps => {
                Decision::PlaceMaker {
                    order: b,
                    robust_value_pico_bps: bv,
                }
            }
            (None, Some((s, sv))) if sv > self.limits.minimum_robust_value_pico_bps => {
                Decision::PlaceMaker {
                    order: s,
                    robust_value_pico_bps: sv,
                }
            }
            _ => Decision::Wait {
                reason: DecisionReason::NoRobustEdge,
            },
        }
    }

    fn entry(
        &self,
        frame: &EvidenceFrame,
        side: Side,
        outcome: &OutcomeDistribution,
    ) -> Option<(ValidatedOrder, i64)> {
        let value = outcome.lower_value()?;
        let remaining = match side {
            Side::Buy => frame
                .account
                .max_position
                .saturating_sub(frame.account.position),
            Side::Sell => frame
                .account
                .max_position
                .saturating_add(frame.account.position),
        };
        let quantity = self.limits.requested_quantity.min(remaining);
        if quantity <= 0 {
            return None;
        }
        let candidate = CandidateOrder {
            symbol: frame.contract.symbol.clone(),
            side,
            price: frame.market.book.passive_price(side),
            quantity: crate::model::Quantity(quantity),
            reduce_only: false,
        };
        frame
            .contract
            .validate(candidate, frame.market.book, frame.account, frame.now_ms)
            .ok()
            .map(|order| (order, value))
    }

    fn reduce(&self, frame: &EvidenceFrame) -> Decision {
        let side = if frame.account.position > 0 {
            Side::Sell
        } else {
            Side::Buy
        };
        let quantity = frame.account.position.unsigned_abs().min(i64::MAX as u64) as i64;
        if quantity <= 0 {
            return Decision::Wait {
                reason: DecisionReason::NoRobustEdge,
            };
        }
        let candidate = CandidateOrder {
            symbol: frame.contract.symbol.clone(),
            side,
            price: frame.market.book.passive_price(side),
            quantity: crate::model::Quantity(quantity),
            reduce_only: true,
        };
        match frame
            .contract
            .validate(candidate, frame.market.book, frame.account, frame.now_ms)
        {
            Ok(order) => Decision::ReduceOnly {
                order,
                robust_value_pico_bps: 0,
            },
            Err(_) => Decision::Wait {
                reason: DecisionReason::ContractRejected,
            },
        }
    }
}

pub fn wait_account() -> AccountSnapshot {
    AccountSnapshot {
        position: 0,
        max_position: 0,
        reconciled: false,
        margin_available_ppm: 0,
    }
}

pub fn episode_state(episode: &AnchorEpisode, now_ms: u64, position: i64) -> EpisodePhase {
    episode.phase_at(now_ms, position)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn frame(now: u64) -> EvidenceFrame {
        let anchor = Anchor::new("a", "equity", PriceTicks(100_000), 0, 10_000, "digest").unwrap();
        let window = ClosedWindow::new(100, 9_000, 9_500, 10_000).unwrap();
        let episode = AnchorEpisode::new("ep", anchor, window, Some(9_400)).unwrap();
        let book = Book {
            bid: PriceTicks(98_000),
            ask: PriceTicks(98_100),
            bid_quantity: Quantity(100),
            ask_quantity: Quantity(100),
            observed_at_ms: now,
            sequence: 1,
        };
        let contract = BinanceContract {
            symbol: "BTCUSDT".into(),
            status: ContractStatus::Trading,
            contract_type: "PERPETUAL".into(),
            price_filter: PriceFilter {
                min: PriceTicks(1),
                max: PriceTicks(i64::MAX),
                tick: 1,
            },
            quantity_filter: QuantityFilter {
                min: Quantity(1),
                max: Quantity(1_000),
                step: 1,
            },
            min_notional_ticks: 1,
            position_mode: PositionMode::OneWay,
            post_only_supported: true,
            observed_at_ms: now,
            max_age_ms: 100,
            digest: "rules".into(),
        };
        let outcome = OutcomeDistribution {
            scenarios: vec![
                OutcomeScenario {
                    weight_bps: 5_000,
                    pnl_pico_bps: 20_000_000_000,
                    terminal: true,
                },
                OutcomeScenario {
                    weight_bps: 5_000,
                    pnl_pico_bps: 10_000_000_000,
                    terminal: true,
                },
            ],
            uncertainty_pico_bps: 1_000_000_000,
        };
        EvidenceFrame {
            now_ms: now,
            episode,
            contract,
            market: MarketSnapshot {
                book,
                index: PriceTicks(98_050),
                mark: PriceTicks(98_050),
                server_time_ms: now,
                max_age_ms: 100,
            },
            account: AccountSnapshot {
                position: 0,
                max_position: 500,
                reconciled: true,
                margin_available_ppm: 1_000_000,
            },
            external_closed: true,
            calendar_known: true,
            funding_known: true,
            model_version: "model-v1".into(),
            buy_outcome: outcome.clone(),
            sell_outcome: outcome,
        }
    }

    #[test]
    fn admits_only_a_validated_passive_order() {
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
                minimum_robust_value_pico_bps: 1,
            },
        };
        assert!(matches!(
            engine.decide(&frame(200)),
            Decision::PlaceMaker { .. }
        ));
    }

    #[test]
    fn closed_window_is_a_hard_gate() {
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
                minimum_robust_value_pico_bps: 1,
            },
        };
        assert_eq!(
            engine.decide(&frame(9_600)),
            Decision::Wait {
                reason: DecisionReason::OutsideEntryWindow
            }
        );
    }
}
