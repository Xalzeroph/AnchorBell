use serde::Serialize;

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

    pub fn is_legal(&self) -> bool {
        if self.version.trim().is_empty() {
            return false;
        }
        let required = [
            StrategyNode::BindAnchor,
            StrategyNode::RequireClosedWindow,
            StrategyNode::MeasureResidual,
            StrategyNode::EstimateJointOutcome,
            StrategyNode::AdmitIfRobustValueBeatsWait,
            StrategyNode::QuotePassive,
            StrategyNode::ReduceAtEarliestDeadline,
            StrategyNode::ReconcileBeforeResume,
        ];
        let positions: Vec<usize> = required
            .iter()
            .filter_map(|required_node| self.nodes.iter().position(|node| node == required_node))
            .collect();
        positions.len() == required.len()
            && positions.windows(2).all(|pair| pair[0] < pair[1])
            && [
                "anchor",
                "external_closed",
                "binance_contract",
                "causal",
                "maker_entry",
                "reconciled_account",
            ]
            .iter()
            .all(|invariant| self.invariants.iter().any(|item| item == invariant))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
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
    EmergencyReduceOnly {
        order: ValidatedOrder,
    },
    ResidualExposure {
        position: i64,
        long_position: i64,
        short_position: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    IncompleteEvidence,
    CalibrationUnavailable,
    InsufficientHistory,
    OutsideEntryWindow,
    StaleMarket,
    NoRobustEdge,
    AmbiguousDirection,
    ContractRejected,
    PortfolioRejected,
    PositionLimit,
    HardDeadline,
}

impl DecisionReason {
    pub const fn code(self) -> &'static str {
        match self {
            Self::IncompleteEvidence => "incomplete_evidence",
            Self::CalibrationUnavailable => "calibration_unavailable",
            Self::InsufficientHistory => "insufficient_history",
            Self::OutsideEntryWindow => "outside_entry_window",
            Self::StaleMarket => "stale_market",
            Self::NoRobustEdge => "no_robust_edge",
            Self::AmbiguousDirection => "ambiguous_direction",
            Self::ContractRejected => "contract_rejected",
            Self::PortfolioRejected => "portfolio_rejected",
            Self::PositionLimit => "position_limit",
            Self::HardDeadline => "hard_deadline",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyLimits {
    pub requested_quantity: i64,
}

pub struct DecisionEngine {
    pub plan: StrategyPlan,
    pub limits: PolicyLimits,
}

struct EntryEvaluation {
    opportunity: Option<(ValidatedOrder, i64)>,
    rejection: Option<DecisionReason>,
}

impl DecisionEngine {
    pub fn decide(&self, frame: &EvidenceFrame) -> Decision {
        if !frame.structurally_valid()
            || frame
                .account
                .validate_for(frame.contract.position_mode, frame.now_ms)
                .is_err()
            || !frame.account.reconciled
        {
            return Decision::Wait {
                reason: DecisionReason::IncompleteEvidence,
            };
        }
        if frame.market.validate(frame.now_ms).is_err() {
            return Decision::Wait {
                reason: DecisionReason::StaleMarket,
            };
        }
        let has_position = frame.account.has_position(frame.contract.position_mode);
        let phase = frame.episode.phase_at(frame.now_ms, has_position);
        if phase == EpisodePhase::Closed {
            return if !has_position {
                Decision::Wait {
                    reason: DecisionReason::HardDeadline,
                }
            } else {
                self.emergency_reduce(frame)
            };
        }
        if has_position && phase == EpisodePhase::Reducing {
            return self.reduce(frame);
        }
        if frame.calibration.phase != crate::calibration::CalibrationPhase::Admitted {
            return Decision::Wait {
                reason: match frame.calibration.phase {
                    crate::calibration::CalibrationPhase::ColdStart => {
                        DecisionReason::CalibrationUnavailable
                    }
                    crate::calibration::CalibrationPhase::Validating => {
                        DecisionReason::InsufficientHistory
                    }
                    crate::calibration::CalibrationPhase::Admitted => {
                        DecisionReason::IncompleteEvidence
                    }
                },
            };
        }
        if !frame.complete_for_entry() {
            return Decision::Wait {
                reason: DecisionReason::IncompleteEvidence,
            };
        }
        if !frame.episode.entry_allowed(frame.now_ms) {
            return Decision::Wait {
                reason: DecisionReason::OutsideEntryWindow,
            };
        }
        let buy = self.entry(frame, Side::Buy, &frame.buy_outcome);
        let sell = self.entry(frame, Side::Sell, &frame.sell_outcome);
        let buy_rejection = buy.rejection;
        let sell_rejection = sell.rejection;
        let wait_value = match frame.wait_outcome.lower_value() {
            Some(value) => value,
            None => {
                return Decision::Wait {
                    reason: DecisionReason::IncompleteEvidence,
                };
            }
        };
        match (buy.opportunity, sell.opportunity) {
            (Some((b, bv)), Some((_s, sv))) if bv > sv && bv > wait_value => Decision::PlaceMaker {
                order: b,
                robust_value_pico_bps: bv,
            },
            (Some((_b, bv)), Some((s, sv))) if sv > bv && sv > wait_value => Decision::PlaceMaker {
                order: s,
                robust_value_pico_bps: sv,
            },
            (Some((b, bv)), Some((s, sv))) if bv == sv && bv > wait_value => {
                if b.queue_ahead_quantity < s.queue_ahead_quantity {
                    Decision::PlaceMaker {
                        order: b,
                        robust_value_pico_bps: bv,
                    }
                } else if s.queue_ahead_quantity < b.queue_ahead_quantity {
                    Decision::PlaceMaker {
                        order: s,
                        robust_value_pico_bps: sv,
                    }
                } else {
                    Decision::Wait {
                        reason: DecisionReason::AmbiguousDirection,
                    }
                }
            }
            (Some((b, bv)), None) if bv > wait_value => Decision::PlaceMaker {
                order: b,
                robust_value_pico_bps: bv,
            },
            (None, Some((s, sv))) if sv > wait_value => Decision::PlaceMaker {
                order: s,
                robust_value_pico_bps: sv,
            },
            _ => Decision::Wait {
                reason: buy_rejection
                    .or(sell_rejection)
                    .unwrap_or(DecisionReason::NoRobustEdge),
            },
        }
    }
    fn entry(
        &self,
        frame: &EvidenceFrame,
        side: Side,
        outcome: &OutcomeDistribution,
    ) -> EntryEvaluation {
        let Some(path_value) = outcome.lower_value() else {
            return EntryEvaluation {
                opportunity: None,
                rejection: Some(DecisionReason::IncompleteEvidence),
            };
        };
        let Some(value) = frame.calibration.robust_executable_value(side, path_value) else {
            return EntryEvaluation {
                opportunity: None,
                rejection: Some(DecisionReason::IncompleteEvidence),
            };
        };
        if frame
            .portfolio
            .permits_additional_risk(
                frame.now_ms,
                frame.entry_margin_quote_ticks,
                frame.entry_stress_quote_ticks,
            )
            .is_err()
        {
            return EntryEvaluation {
                opportunity: None,
                rejection: Some(DecisionReason::PortfolioRejected),
            };
        }
        let position_side = match frame.contract.position_mode {
            crate::model::PositionMode::OneWay => crate::model::PositionSide::Both,
            crate::model::PositionMode::Hedge if side == Side::Buy => {
                crate::model::PositionSide::Long
            }
            crate::model::PositionMode::Hedge => crate::model::PositionSide::Short,
        };
        let remaining = frame
            .account
            .leg_quantity(frame.contract.position_mode, side, position_side)
            .unwrap_or(0);
        let quantity = self.limits.requested_quantity.min(remaining);
        if quantity <= 0 {
            return EntryEvaluation {
                opportunity: None,
                rejection: Some(DecisionReason::PositionLimit),
            };
        }
        let candidate = CandidateOrder {
            symbol: frame.contract.symbol.clone(),
            side,
            price: frame.market.book.passive_price(side),
            quantity: crate::model::Quantity(quantity),
            reduce_only: false,
            position_side,
            time_in_force: crate::model::TimeInForce::Gtx,
            working_type: crate::model::WorkingType::ContractPrice,
            price_protect: false,
            trigger_price: None,
            close_position: false,
        };
        match frame
            .contract
            .validate(candidate, frame.market.book, &frame.account, frame.now_ms)
        {
            Ok(order) => EntryEvaluation {
                opportunity: Some((order, value)),
                rejection: None,
            },
            Err(_) => EntryEvaluation {
                opportunity: None,
                rejection: Some(DecisionReason::ContractRejected),
            },
        }
    }

    fn emergency_reduce(&self, frame: &EvidenceFrame) -> Decision {
        let Some((side, position_side, quantity)) =
            frame.account.reduction(frame.contract.position_mode)
        else {
            return Decision::ResidualExposure {
                position: frame.account.position,
                long_position: frame.account.long_position,
                short_position: frame.account.short_position,
            };
        };
        let candidate = CandidateOrder {
            symbol: frame.contract.symbol.clone(),
            side,
            price: match side {
                Side::Buy => frame.market.book.ask,
                Side::Sell => frame.market.book.bid,
            },
            quantity: crate::model::Quantity(quantity),
            reduce_only: frame.contract.position_mode == crate::model::PositionMode::OneWay,
            position_side,
            time_in_force: crate::model::TimeInForce::Ioc,
            working_type: crate::model::WorkingType::ContractPrice,
            price_protect: false,
            trigger_price: None,
            close_position: false,
        };
        match frame.contract.validate_emergency_reduce_only(
            candidate,
            frame.market.book,
            &frame.account,
            frame.now_ms,
        ) {
            Ok(order) => Decision::EmergencyReduceOnly { order },
            Err(_) => Decision::ResidualExposure {
                position: frame.account.position,
                long_position: frame.account.long_position,
                short_position: frame.account.short_position,
            },
        }
    }

    fn reduce(&self, frame: &EvidenceFrame) -> Decision {
        let Some((side, position_side, quantity)) =
            frame.account.reduction(frame.contract.position_mode)
        else {
            return Decision::ResidualExposure {
                position: frame.account.position,
                long_position: frame.account.long_position,
                short_position: frame.account.short_position,
            };
        };
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
            reduce_only: frame.contract.position_mode == crate::model::PositionMode::OneWay,
            position_side,
            time_in_force: crate::model::TimeInForce::Gtx,
            working_type: crate::model::WorkingType::ContractPrice,
            price_protect: false,
            trigger_price: None,
            close_position: false,
        };
        match frame
            .contract
            .validate(candidate, frame.market.book, &frame.account, frame.now_ms)
        {
            Ok(mut order) => {
                order.route = crate::model::OrderRoute::PassiveReduceOnly;
                Decision::ReduceOnly {
                    order,
                    robust_value_pico_bps: 0,
                }
            }
            Err(_) => Decision::Wait {
                reason: DecisionReason::ContractRejected,
            },
        }
    }
}

pub fn wait_account() -> AccountSnapshot {
    AccountSnapshot {
        position: 0,
        long_position: 0,
        short_position: 0,
        max_position: 0,
        reconciled: false,
        margin_available_ppm: 0,
        observed_at_ms: 0,
        max_age_ms: 0,
        source_digest: String::new(),
    }
}

pub fn episode_state(episode: &AnchorEpisode, now_ms: u64, has_position: bool) -> EpisodePhase {
    episode.phase_at(now_ms, has_position)
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
                entry_queue_ahead_quantity: 100,
                exit_queue_ahead_quantity: 100,
                entry_traded_through_quantity: 110,
                exit_traded_through_quantity: 110,
                entry_latency_ms: 50,
                exit_latency_ms: 50,
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
        let anchor = Anchor::new("a", "BTCUSDT", PriceTicks(100_000), 0, 10_000, "digest").unwrap();
        let window = ClosedWindow::new(100, 9_000, 9_500, 10_000).unwrap();
        let episode = AnchorEpisode::new(
            "ep",
            anchor,
            window,
            Some(FundingSchedule::new(9_400, 28_800_000, 1, 20_000, "funding").unwrap()),
        )
        .unwrap();
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
            max_notional_ticks: None,
            position_mode: PositionMode::OneWay,
            post_only_supported: true,
            reduce_only_supported: true,
            close_position_supported: true,
            conditional_orders_supported: false,
            trigger_protect_bps: 0,
            rate_limit_remaining: 100,
            max_open_orders: 100,
            open_orders: 0,
            self_trade_prevention_enabled: true,
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
                cycle(Side::Sell, 102_000, 99_000),
                cycle(Side::Sell, 102_000, 100_000),
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
                index_observed_at_ms: now,
                index_max_age_ms: 100,
                mark: PriceTicks(98_050),
                mark_observed_at_ms: now,
                mark_max_age_ms: 100,
                server_time_ms: now,
                max_age_ms: 100,
                source_digest: "market-digest".into(),
            },
            account: AccountSnapshot {
                position: 0,
                long_position: 0,
                short_position: 0,
                max_position: 500,
                reconciled: true,
                margin_available_ppm: 1_000_000,
                observed_at_ms: now,
                max_age_ms: 100,
                source_digest: "account-digest".into(),
            },
            portfolio: crate::portfolio::PortfolioSnapshot {
                quote_asset: "USDT".into(),
                equity_quote_ticks: 1_000_000,
                available_margin_quote_ticks: 1_000_000,
                maintenance_margin_quote_ticks: 0,
                reserved_margin_quote_ticks: 0,
                stress_budget_quote_ticks: 1_000_000,
                used_stress_quote_ticks: 0,
                observed_at_ms: now,
                max_age_ms: 100,
                source_digest: "portfolio-digest".into(),
            },
            entry_margin_quote_ticks: 100,
            entry_stress_quote_ticks: 100,
            closure: ClosureEvidence {
                event_id: "session-close-1".into(),
                closed_at_ms: 100,
                observed_at_ms: 100,
                calendar_known: true,
                source_digest: "calendar-digest".into(),
            },
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
    fn portfolio_budget_rejects_new_risk_without_rewriting_the_market_edge() {
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
            },
        };
        let mut evidence = frame(200);
        evidence.portfolio.available_margin_quote_ticks = 99;
        assert_eq!(
            engine.decide(&evidence),
            Decision::Wait {
                reason: DecisionReason::PortfolioRejected
            }
        );
    }

    #[test]
    fn calibration_gate_uses_stable_machine_codes() {
        assert_eq!(
            DecisionReason::CalibrationUnavailable.code(),
            "calibration_unavailable"
        );
        assert_eq!(
            serde_json::to_string(&Decision::Wait {
                reason: DecisionReason::InsufficientHistory,
            })
            .unwrap(),
            r#"{"kind":"wait","data":{"reason":"insufficient_history"}}"#
        );
    }

    #[test]
    fn a_plan_is_illegal_when_any_required_semantic_node_is_missing() {
        let mut plan = StrategyPlan::anchor_closed_maker("plan-v1");
        plan.nodes
            .retain(|node| *node != StrategyNode::MeasureResidual);
        assert!(!plan.is_legal());
    }

    #[test]
    fn anchor_contract_and_calibration_must_share_instrument_identity() {
        let mut evidence = frame(200);
        evidence.contract.symbol = "ETHUSDT".into();
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
            },
        };
        assert_eq!(
            engine.decide(&evidence),
            Decision::Wait {
                reason: DecisionReason::IncompleteEvidence
            }
        );
    }

    #[test]
    fn exact_value_and_queue_ties_do_not_create_directional_bias() {
        let mut evidence = frame(200);
        evidence.sell_outcome = evidence.buy_outcome.clone();
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
            },
        };
        assert_eq!(
            engine.decide(&evidence),
            Decision::Wait {
                reason: DecisionReason::AmbiguousDirection
            }
        );
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
                reason: DecisionReason::CalibrationUnavailable
            }
        );
    }
}

#[cfg(test)]
mod emergency_tests {
    use super::*;
    use crate::model::{OrderRoute, TimeInForce};

    #[test]
    fn hard_deadline_uses_a_separately_audited_reduce_route() {
        let mut evidence = super::tests::frame(10_000);
        evidence.account.position = 10;
        evidence.market.book.observed_at_ms = 10_000;
        evidence.market.server_time_ms = 10_000;
        evidence.contract.observed_at_ms = 10_000;
        let engine = DecisionEngine {
            plan: StrategyPlan::anchor_closed_maker("plan-v1"),
            limits: PolicyLimits {
                requested_quantity: 10,
            },
        };
        match engine.decide(&evidence) {
            Decision::EmergencyReduceOnly { order } => {
                assert_eq!(order.route, OrderRoute::EmergencyReduceOnly);
                assert_eq!(order.order.time_in_force, TimeInForce::Ioc);
                assert!(order.order.reduce_only);
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }
}
