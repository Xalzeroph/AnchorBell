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
        let wait_value = match frame.wait_outcome.lower_value() {
            Some(value) => value,
            None => {
                return Decision::Wait {
                    reason: DecisionReason::IncompleteEvidence,
                };
            }
        };
        match (buy, sell) {
            (Some((b, bv)), Some((_s, sv))) if bv >= sv && bv > wait_value => {
                Decision::PlaceMaker {
                    order: b,
                    robust_value_pico_bps: bv,
                }
            }
            (Some((_b, bv)), Some((s, sv))) if sv > bv && sv > wait_value => Decision::PlaceMaker {
                order: s,
                robust_value_pico_bps: sv,
            },
            (Some((b, bv)), None) if bv > wait_value => Decision::PlaceMaker {
                order: b,
                robust_value_pico_bps: bv,
            },
            (None, Some((s, sv))) if sv > wait_value => Decision::PlaceMaker {
                order: s,
                robust_value_pico_bps: sv,
            },
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
        let value = frame
            .calibration
            .robust_executable_value(side, outcome.lower_value()?);
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
            position_side: crate::model::PositionSide::Both,
            time_in_force: crate::model::TimeInForce::Gtx,
            working_type: crate::model::WorkingType::ContractPrice,
            price_protect: false,
            trigger_price: None,
            close_position: false,
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
            position_side: crate::model::PositionSide::Both,
            time_in_force: crate::model::TimeInForce::Gtx,
            working_type: crate::model::WorkingType::ContractPrice,
            price_protect: false,
            trigger_price: None,
            close_position: false,
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

    fn cycle(side: Side, entry_price: i64, exit_price: i64) -> OutcomeScenario {
        OutcomeScenario::cycle(
            5_000,
            ExecutionCycle {
                side,
                anchor_price: PriceTicks(100_000),
                entry_price: PriceTicks(entry_price),
                exit_price: PriceTicks(exit_price),
                requested_quantity: Quantity(10),
                entry_filled_quantity: 10,
                exit_filled_quantity: 10,
                entry_fee_pico_bps: 0,
                exit_fee_pico_bps: 0,
                exit_cost_pico_bps: 0,
                funding_cost_pico_bps: 0,
                deadline_risk_pico_bps: 0,
                entry_at_ms: 1,
                exit_at_ms: 2,
                deadline_ms: 3,
            },
        )
    }

    pub(crate) fn frame(now: u64) -> EvidenceFrame {
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
            reduce_only_supported: true,
            close_position_supported: true,
            conditional_orders_supported: false,
            trigger_protect_bps: 0,
            rate_limit_remaining: 100,
            observed_at_ms: now,
            max_age_ms: 100,
            digest: "rules".into(),
        };
        let uncertainty = UncertaintyBudget {
            anchor_pico_bps: 0,
            execution_pico_bps: 500_000_000,
            timing_pico_bps: 0,
            model_pico_bps: 500_000_000,
        };
        let buy_outcome = OutcomeDistribution {
            scenarios: vec![
                cycle(Side::Buy, 98_000, 100_000),
                cycle(Side::Buy, 98_000, 99_000),
            ],
            uncertainty,
        };
        let sell_outcome = OutcomeDistribution {
            scenarios: vec![
                cycle(Side::Sell, 102_000, 100_000),
                cycle(Side::Sell, 102_000, 101_000),
            ],
            uncertainty,
        };
        let mut calibration_state = crate::calibration::CalibrationState::new(
            String::from_utf8(vec![66, 84, 67, 85, 83, 68, 84]).unwrap(),
        )
        .unwrap();
        for i in 0..crate::calibration::MIN_EFFECTIVE_SAMPLES {
            calibration_state
                .observe(crate::calibration::CalibrationObservation {
                    event_at_ms: i as u64 + 1,
                    side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    attempted_quantity: 10,
                    filled_quantity: 5,
                    markout_pico_bps: Some(20_000_000_000),
                })
                .unwrap();
        }
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
            calibration: calibration_state.snapshot().unwrap(),
            buy_outcome,
            sell_outcome,
            wait_outcome: OutcomeDistribution {
                scenarios: vec![OutcomeScenario::wait(10_000, 0)],
                uncertainty: UncertaintyBudget {
                    anchor_pico_bps: 0,
                    execution_pico_bps: 0,
                    timing_pico_bps: 0,
                    model_pico_bps: 0,
                },
            },
        }
    }

    #[test]
    fn admits_only_a_validated_passive_order() {
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
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

#[cfg(test)]
mod wait_competition_tests {
    use super::*;
    use crate::calibration::CalibrationState;
    use crate::model::{OutcomeScenario, UncertaintyBudget};

    #[test]
    fn wait_is_an_explicit_competing_action() {
        let mut evidence = super::tests::frame(200);
        evidence.calibration = CalibrationState::new("BTCUSDT")
            .unwrap()
            .decision_snapshot()
            .unwrap();
        evidence.buy_outcome = OutcomeDistribution {
            scenarios: vec![OutcomeScenario::wait(10_000, -1)],
            uncertainty: UncertaintyBudget {
                anchor_pico_bps: 0,
                execution_pico_bps: 0,
                timing_pico_bps: 0,
                model_pico_bps: 0,
            },
        };
        evidence.sell_outcome = evidence.buy_outcome.clone();
        evidence.wait_outcome = OutcomeDistribution {
            scenarios: vec![OutcomeScenario::wait(10_000, 0)],
            uncertainty: UncertaintyBudget {
                anchor_pico_bps: 0,
                execution_pico_bps: 0,
                timing_pico_bps: 0,
                model_pico_bps: 0,
            },
        };
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
            },
        };
        assert_eq!(
            engine.decide(&evidence),
            Decision::Wait {
                reason: DecisionReason::NoRobustEdge
            }
        );
    }
}
