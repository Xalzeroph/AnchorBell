use std::collections::BTreeMap;

use serde::Serialize;

use crate::platform::{
    HealthSnapshot, ReadinessReport, RegistryError, RuntimeProfile, SystemRegistry, SystemRole,
    SystemState,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthTransition {
    pub system_id: String,
    pub from: SystemState,
    pub to: SystemState,
    pub stale: bool,
    pub observed_at_ms: u64,
    pub diagnostics: Vec<String>,
}

/// The live control plane is the only admission surface between runtime
/// observations and new exchange risk. It is deliberately independent of
/// strategy and exchange clients so every live entrypoint can reuse it.
#[derive(Debug)]
pub struct RuntimeControlPlane {
    registry: SystemRegistry,
    published_health: BTreeMap<String, (SystemState, bool)>,
    health_events: Vec<HealthTransition>,
}

impl Default for RuntimeControlPlane {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeControlPlane {
    pub fn new() -> Self {
        let mut registry = SystemRegistry::default();
        // Use a deterministic epoch so replay/tests can establish their own
        // clock without violating health timestamp monotonicity.
        registry.bootstrap_health(0);
        let mut control_plane = Self {
            registry,
            published_health: BTreeMap::new(),
            health_events: Vec::new(),
        };
        control_plane.capture_health_baseline();
        control_plane
    }

    pub fn registry(&self) -> &SystemRegistry {
        &self.registry
    }

    pub fn drain_health_events(&mut self) -> Vec<HealthTransition> {
        std::mem::take(&mut self.health_events)
    }

    pub fn tick(&mut self, now_ms: u64) -> Vec<String> {
        let changed = self.registry.mark_stale_at(now_ms);
        self.capture_health_transitions();
        changed
    }

    pub fn readiness(&mut self, now_ms: u64) -> ReadinessReport {
        self.registry.mark_stale_at(now_ms);
        self.capture_health_transitions();
        self.registry
            .readiness_for_role(SystemRole::ExecutionGateway, now_ms)
    }

    pub fn execution_ready(&mut self, now_ms: u64) -> bool {
        self.readiness(now_ms).ready
    }

    /// Activate a composition profile from registry-owned dependency closure.
    pub fn activate_profile(
        &mut self,
        profile: RuntimeProfile,
        observed_at_ms: u64,
    ) -> Result<(), RegistryError> {
        self.registry.profile_system_ids(profile)?;
        // Profile activation only validates and discovers the registry-owned
        // closure. A system becomes Ready only after its real producer reports
        // a heartbeat; topology alone must never create liveness.
        let _ = observed_at_ms;
        Ok(())
    }

    /// Report the minimum live bootstrap contract after credentials, market
    /// metadata and the initial exchange reconciliation have succeeded.
    pub fn bootstrap_ready(&mut self, observed_at_ms: u64) -> Result<(), RegistryError> {
        self.signal_ready("runtime_bootstrap", observed_at_ms)
    }

    pub fn observe_market(&mut self, observed_at_ms: u64) -> Result<(), RegistryError> {
        // Registered market participants declare this signal beside their
        // implementation; the control plane never owns a system ID list.
        self.signal_ready("market_events", observed_at_ms)
    }

    pub fn observe_reference(&mut self, observed_at_ms: u64) -> Result<(), RegistryError> {
        self.signal_ready("reference_data", observed_at_ms)
    }

    pub fn observe_user_data(&mut self, observed_at_ms: u64) -> Result<(), RegistryError> {
        self.signal_ready("user_data", observed_at_ms)
    }

    pub fn degrade(
        &mut self,
        id: &str,
        observed_at_ms: u64,
        reason: &str,
    ) -> Result<(), RegistryError> {
        let mut snapshot = self
            .registry
            .health(id)
            .cloned()
            .unwrap_or_else(|| HealthSnapshot::discovered(id, observed_at_ms));
        snapshot.observed_at_ms = observed_at_ms;
        snapshot.stale = false;
        snapshot.state = crate::platform::SystemState::Degraded;
        snapshot.diagnostics.push(reason.to_owned());
        let result = self.registry.report_health(snapshot);
        if result.is_ok() {
            self.capture_health_transitions();
        }
        result
    }

    pub fn mark_ready(&mut self, id: &str, observed_at_ms: u64) -> Result<(), RegistryError> {
        self.ready(id, observed_at_ms)
    }

    pub fn mark_halted(
        &mut self,
        id: &str,
        observed_at_ms: u64,
        reason: &str,
    ) -> Result<(), RegistryError> {
        let mut snapshot = self
            .registry
            .health(id)
            .cloned()
            .unwrap_or_else(|| HealthSnapshot::discovered(id, observed_at_ms));
        snapshot.observed_at_ms = observed_at_ms;
        snapshot.stale = false;
        snapshot.state = SystemState::Halted;
        snapshot.diagnostics.push(reason.to_owned());
        let result = self.registry.report_health(snapshot);
        if result.is_ok() {
            self.capture_health_transitions();
        }
        result
    }

    fn signal_ready(&mut self, signal: &str, observed_at_ms: u64) -> Result<(), RegistryError> {
        let result = self.registry.heartbeat_signal(signal, observed_at_ms);
        if result.is_ok() {
            self.capture_health_transitions();
        }
        result.map(|_| ())
    }

    fn ready(&mut self, id: &str, observed_at_ms: u64) -> Result<(), RegistryError> {
        let result = self.registry.heartbeat(id, observed_at_ms);
        if result.is_ok() {
            self.capture_health_transitions();
        }
        result
    }

    fn capture_health_baseline(&mut self) {
        self.published_health = self
            .registry
            .health_snapshots()
            .map(|snapshot| (snapshot.system_id.clone(), (snapshot.state, snapshot.stale)))
            .collect();
    }

    fn capture_health_transitions(&mut self) {
        for snapshot in self.registry.health_snapshots() {
            let current = (snapshot.state, snapshot.stale);
            let previous = self
                .published_health
                .insert(snapshot.system_id.clone(), current);
            if previous == Some(current) {
                continue;
            }
            let from = previous.map_or(SystemState::Discovered, |(state, _)| state);
            self.health_events.push(HealthTransition {
                system_id: snapshot.system_id.clone(),
                from,
                to: snapshot.state,
                stale: snapshot.stale,
                observed_at_ms: snapshot.observed_at_ms,
                diagnostics: snapshot.diagnostics.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_admission_is_closed_until_observations_arrive() {
        let mut plane = RuntimeControlPlane::new();
        assert!(!plane.execution_ready(1_000));
        plane.bootstrap_ready(1_000).unwrap();
        assert!(!plane.execution_ready(1_000));
        plane.observe_reference(1_000).unwrap();
        plane.observe_market(1_000).unwrap();
        assert!(plane.execution_ready(1_000));
    }

    #[test]
    fn silence_expires_market_admission_automatically() {
        let mut plane = RuntimeControlPlane::new();
        plane.bootstrap_ready(1_000).unwrap();
        plane.observe_reference(1_000).unwrap();
        plane.observe_market(1_000).unwrap();
        assert!(!plane.execution_ready(3_001));
    }

    #[test]
    fn health_transitions_are_emitted_once_and_recovery_is_observable() {
        let mut plane = RuntimeControlPlane::new();
        assert!(plane.drain_health_events().is_empty());

        plane.bootstrap_ready(1_000).unwrap();
        let bootstrap_events = plane.drain_health_events();
        assert!(bootstrap_events.iter().any(|event| {
            event.system_id == "control.kernel"
                && event.from == SystemState::Discovered
                && event.to == SystemState::Ready
                && !event.stale
        }));
        plane.bootstrap_ready(1_000).unwrap();
        assert!(plane.drain_health_events().is_empty());

        plane.observe_market(1_000).unwrap();
        let ready_events = plane.drain_health_events();
        assert!(ready_events
            .iter()
            .any(|event| event.system_id == "market.binance" && !event.stale));

        plane.execution_ready(3_001);
        let stale_events = plane.drain_health_events();
        assert!(stale_events
            .iter()
            .any(|event| event.system_id == "market.binance" && event.stale));

        plane.observe_market(3_001).unwrap();
        let recovery_events = plane.drain_health_events();
        assert!(recovery_events
            .iter()
            .any(|event| event.system_id == "market.binance" && !event.stale));
    }
}
