use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKind {
    Wait,
    Place,
    Requote,
    Cancel,
    Reduce,
    EmergencyReduce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionEconomics {
    pub executable_value_pico_bps: i128,
    pub queue_priority_loss_pico_bps: i128,
    pub cancel_race_loss_pico_bps: i128,
    pub deadline_loss_pico_bps: i128,
    pub uncertainty_pico_bps: i128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionValueError {
    NegativeCost,
    Overflow,
}

impl ActionEconomics {
    pub fn conservative_value(self) -> Result<i128, ActionValueError> {
        if self.queue_priority_loss_pico_bps < 0
            || self.cancel_race_loss_pico_bps < 0
            || self.deadline_loss_pico_bps < 0
            || self.uncertainty_pico_bps < 0
        {
            return Err(ActionValueError::NegativeCost);
        }
        let costs = self
            .queue_priority_loss_pico_bps
            .checked_add(self.cancel_race_loss_pico_bps)
            .and_then(|value| value.checked_add(self.deadline_loss_pico_bps))
            .and_then(|value| value.checked_add(self.uncertainty_pico_bps))
            .ok_or(ActionValueError::Overflow)?;
        self.executable_value_pico_bps
            .checked_sub(costs)
            .ok_or(ActionValueError::Overflow)
    }

    pub fn improves_on(self, alternative: Self) -> Result<bool, ActionValueError> {
        Ok(self.conservative_value()? > alternative.conservative_value()?)
    }

    pub fn requote_is_worthwhile(
        current: Self,
        replacement: Self,
    ) -> Result<bool, ActionValueError> {
        replacement.improves_on(current)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActionChoice {
    pub kind: ActionKind,
    pub economics: ActionEconomics,
}

impl ActionChoice {
    pub fn beats(self, alternative: Self) -> Result<bool, ActionValueError> {
        self.economics.improves_on(alternative.economics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn economics(
        value: i128,
        queue: i128,
        cancel: i128,
        deadline: i128,
        uncertainty: i128,
    ) -> ActionEconomics {
        ActionEconomics {
            executable_value_pico_bps: value,
            queue_priority_loss_pico_bps: queue,
            cancel_race_loss_pico_bps: cancel,
            deadline_loss_pico_bps: deadline,
            uncertainty_pico_bps: uncertainty,
        }
    }

    #[test]
    fn requote_requires_net_improvement_after_all_transition_costs() {
        let current = economics(1_000, 100, 20, 30, 50);
        let replacement = economics(1_200, 180, 40, 30, 50);
        assert!(ActionEconomics::requote_is_worthwhile(current, replacement).unwrap());
        let expensive = economics(1_200, 250, 80, 30, 50);
        assert!(!ActionEconomics::requote_is_worthwhile(current, expensive).unwrap());
    }

    #[test]
    fn unknown_or_negative_costs_never_become_free_value() {
        let invalid = economics(100, -1, 0, 0, 0);
        assert_eq!(
            invalid.conservative_value(),
            Err(ActionValueError::NegativeCost)
        );
    }
}
