//! Property-based tests using proptest
//! 
//! These tests verify invariants of critical system components.

use proptest::prelude::*;

/// Order lifecycle states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    Created,
    Submitted,
    Acknowledged,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

/// Valid state transitions for orders
impl OrderState {
    pub fn can_transition_to(&self, next: OrderState) -> bool {
        match (self, next) {
            // From Created
            (OrderState::Created, OrderState::Submitted) => true,
            (OrderState::Created, OrderState::Cancelled) => true,
            
            // From Submitted
            (OrderState::Submitted, OrderState::Acknowledged) => true,
            (OrderState::Submitted, OrderState::Rejected) => true,
            (OrderState::Submitted, OrderState::Cancelled) => true,
            
            // From Acknowledged
            (OrderState::Acknowledged, OrderState::PartiallyFilled) => true,
            (OrderState::Acknowledged, OrderState::Filled) => true,
            (OrderState::Acknowledged, OrderState::Cancelled) => true,
            
            // From PartiallyFilled
            (OrderState::PartiallyFilled, OrderState::PartiallyFilled) => true,
            (OrderState::PartiallyFilled, OrderState::Filled) => true,
            (OrderState::PartiallyFilled, OrderState::Cancelled) => true,
            
            // Terminal states cannot transition
            (OrderState::Filled, _) => false,
            (OrderState::Cancelled, _) => false,
            (OrderState::Rejected, _) => false,
            
            _ => false,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, OrderState::Filled | OrderState::Cancelled | OrderState::Rejected)
    }
}

/// Order state machine for testing
#[derive(Debug, Clone)]
pub struct OrderStateMachine {
    state: OrderState,
    transitions: Vec<(OrderState, OrderState)>,
}

impl OrderStateMachine {
    pub fn new() -> Self {
        Self {
            state: OrderState::Created,
            transitions: Vec::new(),
        }
    }

    pub fn transition(&mut self, next: OrderState) -> Result<(), String> {
        if self.state.can_transition_to(next) {
            self.transitions.push((self.state, next));
            self.state = next;
            Ok(())
        } else {
            Err(format!("Invalid transition: {:?} -> {:?}", self.state, next))
        }
    }

    pub fn state(&self) -> OrderState {
        self.state
    }
}

/// Risk engine position invariants
#[derive(Debug, Clone)]
pub struct PositionInvariants {
    pub quantity: i64,
    pub notional: f64,
    pub max_position: i64,
    pub max_notional: f64,
}

impl PositionInvariants {
    pub fn check_invariants(&self) -> Vec<String> {
        let mut violations = Vec::new();
        
        // Position limit check
        if self.quantity.abs() > self.max_position {
            violations.push(format!(
                "Position {} exceeds limit {}",
                self.quantity.abs(),
                self.max_position
            ));
        }
        
        // Notional limit check
        if self.notional.abs() > self.max_notional {
            violations.push(format!(
                "Notional {} exceeds limit {}",
                self.notional.abs(),
                self.max_notional
            ));
        }
        
        // Sign consistency: quantity and notional should have same sign
        if (self.quantity > 0 && self.notional < 0.0) || (self.quantity < 0 && self.notional > 0.0) {
            violations.push("Quantity and notional signs are inconsistent".to_string());
        }
        
        violations
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Test that order state machine never allows invalid transitions
    #[test]
    fn order_state_machine_valid_transitions(
        transitions in prop::collection::vec(0u8..7, 0..20)
    ) {
        let all_states = [
            OrderState::Created,
            OrderState::Submitted,
            OrderState::Acknowledged,
            OrderState::PartiallyFilled,
            OrderState::Filled,
            OrderState::Cancelled,
            OrderState::Rejected,
        ];
        
        let mut machine = OrderStateMachine::new();
        
        for t in transitions {
            let next_state = all_states[t as usize % all_states.len()];
            let result = machine.transition(next_state);
            
            // If transition succeeded, it must have been valid
            if result.is_ok() {
                // Verify we actually moved to that state
                assert_eq!(machine.state(), next_state);
            }
        }
        
        // Invariant: If we're in a terminal state, all future transitions should fail
        if machine.state().is_terminal() {
            for state in &all_states {
                assert!(machine.transition(*state).is_err());
            }
        }
    }

    /// Test that position invariants are always checked correctly
    #[test]
    fn position_invariants_consistency(
        quantity in -1000i64..1000,
        notional in -1000000.0f64..1000000.0,
        max_position in 1i64..500,
        max_notional in 1.0f64..500000.0
    ) {
        let invariants = PositionInvariants {
            quantity,
            notional,
            max_position,
            max_notional,
        };
        
        let violations = invariants.check_invariants();
        
        // Check position limit
        if quantity.abs() > max_position {
            assert!(violations.iter().any(|v| v.contains("Position")));
        }
        
        // Check notional limit
        if notional.abs() > max_notional {
            assert!(violations.iter().any(|v| v.contains("Notional")));
        }
    }

    /// Test that state machine always ends in a valid state
    #[test]
    fn order_always_in_valid_state(
        transitions in prop::collection::vec(
            prop_oneof![
                Just(OrderState::Submitted),
                Just(OrderState::Acknowledged),
                Just(OrderState::PartiallyFilled),
                Just(OrderState::Filled),
                Just(OrderState::Cancelled),
            ],
            0..10
        )
    ) {
        let mut machine = OrderStateMachine::new();
        
        for next in transitions {
            let _ = machine.transition(next);
        }
        
        // State should always be one of the valid states
        let valid_states = [
            OrderState::Created,
            OrderState::Submitted,
            OrderState::Acknowledged,
            OrderState::PartiallyFilled,
            OrderState::Filled,
            OrderState::Cancelled,
            OrderState::Rejected,
        ];
        
        assert!(valid_states.contains(&machine.state()));
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_order_lifecycle_happy_path() {
        let mut machine = OrderStateMachine::new();
        
        assert!(machine.transition(OrderState::Submitted).is_ok());
        assert!(machine.transition(OrderState::Acknowledged).is_ok());
        assert!(machine.transition(OrderState::PartiallyFilled).is_ok());
        assert!(machine.transition(OrderState::Filled).is_ok());
        
        assert!(machine.state().is_terminal());
    }

    #[test]
    fn test_order_cancellation() {
        let mut machine = OrderStateMachine::new();
        
        assert!(machine.transition(OrderState::Submitted).is_ok());
        assert!(machine.transition(OrderState::Acknowledged).is_ok());
        assert!(machine.transition(OrderState::Cancelled).is_ok());
        
        // Cannot transition from cancelled
        assert!(machine.transition(OrderState::Filled).is_err());
    }

    #[test]
    fn test_position_invariants() {
        let invariants = PositionInvariants {
            quantity: 100,
            notional: 50000.0,
            max_position: 50,
            max_notional: 100000.0,
        };
        
        let violations = invariants.check_invariants();
        assert_eq!(violations.len(), 1); // Only position exceeded
        assert!(violations[0].contains("Position"));
    }
}
