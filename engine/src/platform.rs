use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Architectural plane owned by a subsystem.
///
/// Planes are dependency-ordered. A lower plane may not depend on a higher
/// plane; communication is always through typed contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PlatformLayer {
    Control,
    MarketData,
    Decision,
    Execution,
    Observability,
    Operations,
    Simulation,
    Analytics,
}

/// Runtime authority for a system's decisions or data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Authority {
    Binance,
    ExternalReference,
    Derived,
    Operator,
    Internal,
}

/// Whether a component can be replaced without changing the safety kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutability {
    ImmutableCore,
    GovernedPolicy,
    RuntimeState,
}

/// Canonical system role. This is intentionally operational vocabulary; analysis
/// and historical validation are consumers of the platform, not the platform
/// authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemRole {
    Registry,
    ExchangeAdapter,
    ReferenceData,
    Anchor,
    Strategy,
    Portfolio,
    Risk,
    Funding,
    ExecutionGateway,
    Lifecycle,
    Simulation,
    Replay,
    Backtest,
    Observability,
    Audit,
    ControlConsole,
    Recovery,
    Analytics,
}

/// Lifecycle/health state reported by every registered system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemState {
    Discovered,
    Ready,
    Degraded,
    Halted,
    Draining,
}

/// Compile-time identity and dependency contract for one subsystem.
#[derive(Debug, Clone, Serialize)]
pub struct SystemDescriptor {
    pub id: &'static str,
    pub layer: PlatformLayer,
    pub role: SystemRole,
    pub authority: Authority,
    pub mutability: Mutability,
    pub dependencies: &'static [&'static str],
    pub health_interval_ms: u64,
    pub restartable: bool,
}

/// Recovery behavior is part of the system contract, not an operator checklist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryPolicy {
    Halt,
    RestartThenReconcile,
    ReconcileThenResume,
    OperatorOnly,
}

/// The executable contract exposed by a registered system node.
#[derive(Debug, Clone, Serialize)]
pub struct SystemContract {
    pub system_id: &'static str,
    pub requires: &'static [&'static str],
    pub provides: &'static [&'static str],
    pub consumes: &'static [&'static str],
    pub recovery: RecoveryPolicy,
}

#[derive(Debug, Clone)]
pub struct SystemRegistration {
    pub descriptor: SystemDescriptor,
    pub contract: SystemContract,
    pub implementation: &'static str,
    pub profile_roots: &'static [RuntimeProfile],
    pub health_signals: &'static [&'static str],
}
inventory::collect!(SystemRegistration);

/// Runtime composition profile. Profiles describe system roots; dependencies
/// are expanded from the registry so entrypoints never maintain ID lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuntimeProfile {
    Live,
    Dashboard,
    Simulation,
    Batch,
    Backtest,
}

impl RuntimeProfile {
    pub fn roots(self) -> Vec<&'static str> {
        let mut roots = inventory::iter::<SystemRegistration>()
            .filter(|registration| registration.profile_roots.contains(&self))
            .map(|registration| registration.descriptor.id)
            .collect::<Vec<_>>();
        roots.sort_unstable();
        roots
    }
}

pub const PLATFORM_MANIFEST_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize)]
pub struct PlatformManifestEntry {
    pub descriptor: SystemDescriptor,
    pub contract: SystemContract,
    pub implementation: Option<&'static str>,
    pub health: HealthSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlatformManifest {
    pub schema_version: u16,
    pub systems: Vec<PlatformManifestEntry>,
}

impl SystemDescriptor {
    pub fn contract(&self) -> SystemContract {
        SystemContract {
            system_id: self.id,
            requires: self.dependencies,
            provides: &[],
            consumes: &[],
            recovery: recovery_for(self.role, self.restartable),
        }
    }
}

const fn recovery_for(role: SystemRole, restartable: bool) -> RecoveryPolicy {
    if matches!(
        role,
        SystemRole::Registry | SystemRole::Risk | SystemRole::Recovery
    ) {
        RecoveryPolicy::Halt
    } else if matches!(role, SystemRole::ControlConsole) {
        RecoveryPolicy::OperatorOnly
    } else if restartable {
        RecoveryPolicy::RestartThenReconcile
    } else {
        RecoveryPolicy::ReconcileThenResume
    }
}

/// Runtime health signal. Producers update this asynchronously; decision and
/// execution paths only consume the last validated snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub system_id: String,
    pub state: SystemState,
    pub observed_at_ms: u64,
    pub stale: bool,
    pub invariant_failures: u32,
    pub queue_depth: u64,
    pub error_rate_ppm: u64,
    pub diagnostics: Vec<String>,
}

impl HealthSnapshot {
    pub fn discovered(system_id: impl Into<String>, observed_at_ms: u64) -> Self {
        Self {
            system_id: system_id.into(),
            state: SystemState::Discovered,
            observed_at_ms,
            stale: true,
            invariant_failures: 0,
            queue_depth: 0,
            error_rate_ppm: 0,
            diagnostics: vec!["awaiting_first_health_report".to_owned()],
        }
    }

    pub fn ready(system_id: impl Into<String>, observed_at_ms: u64) -> Self {
        Self {
            system_id: system_id.into(),
            state: SystemState::Ready,
            observed_at_ms,
            stale: false,
            invariant_failures: 0,
            queue_depth: 0,
            error_rate_ppm: 0,
            diagnostics: Vec::new(),
        }
    }

    pub fn is_tradable(&self) -> bool {
        matches!(self.state, SystemState::Ready) && !self.stale && self.invariant_failures == 0
    }

    pub fn is_fresh_at(&self, now_ms: u64, interval_ms: u64) -> bool {
        !self.stale
            && self.observed_at_ms <= now_ms
            && interval_ms > 0
            && now_ms.saturating_sub(self.observed_at_ms) <= interval_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    DuplicateSystem(String),
    InvalidSystemId(String),
    InvalidHealthInterval(String),
    LayerViolation {
        system: String,
        dependency: String,
        system_layer: PlatformLayer,
        dependency_layer: PlatformLayer,
    },
    DuplicateDependency {
        system: String,
        dependency: String,
    },
    SelfDependency(String),
    MissingDependency {
        system: String,
        dependency: String,
    },
    DependencyCycle,
    HealthTimestampRegression {
        system: String,
        previous_ms: u64,
        observed_ms: u64,
    },
    ImmutableReplacement(String),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateSystem(id) => write!(f, "duplicate system id: {id}"),
            Self::InvalidSystemId(id) => write!(f, "invalid system id: {id}"),
            Self::InvalidHealthInterval(id) => {
                write!(f, "system {id} must declare a positive health interval")
            }
            Self::LayerViolation {
                system,
                dependency,
                system_layer,
                dependency_layer,
            } => write!(
                f,
                "layer violation: {system} ({system_layer:?}) depends on {dependency} ({dependency_layer:?})"
            ),
            Self::DuplicateDependency { system, dependency } => {
                write!(
                    f,
                    "system {system} declares duplicate dependency {dependency}"
                )
            }
            Self::SelfDependency(id) => write!(f, "system cannot depend on itself: {id}"),
            Self::MissingDependency { system, dependency } => {
                write!(f, "system {system} depends on missing system {dependency}")
            }
            Self::DependencyCycle => write!(f, "system dependency cycle detected"),
            Self::HealthTimestampRegression {
                system,
                previous_ms,
                observed_ms,
            } => write!(
                f,
                "health timestamp regressed for {system}: {observed_ms} < {previous_ms}"
            ),
            Self::ImmutableReplacement(id) => {
                write!(f, "immutable core system cannot be replaced: {id}")
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// Why a registered system cannot currently participate in live execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessReport {
    pub system_id: String,
    pub checked_at_ms: u64,
    pub ready: bool,
    pub blockers: Vec<String>,
    pub candidate_providers: Vec<String>,
    pub selected_provider: Option<String>,
}

impl ReadinessReport {
    pub fn blocked(
        system_id: impl Into<String>,
        checked_at_ms: u64,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            system_id: system_id.into(),
            checked_at_ms,
            ready: false,
            blockers: vec![reason.into()],
            candidate_providers: Vec::new(),
            selected_provider: None,
        }
    }
}

/// Runtime system topology and health registry.
///
/// The registry is the single topology source for supervisors, diagnostics,
/// dashboards and automated recovery. It does not execute strategy logic and
/// cannot mutate immutable exchange/order contracts.
#[derive(Debug, Clone)]
pub struct SystemRegistry {
    descriptors: BTreeMap<&'static str, SystemDescriptor>,
    health: BTreeMap<String, HealthSnapshot>,
    contracts: BTreeMap<&'static str, SystemContract>,
    implementations: BTreeMap<&'static str, &'static str>,
    profile_roots: BTreeMap<RuntimeProfile, Vec<&'static str>>,
    health_signal_systems: BTreeMap<&'static str, Vec<&'static str>>,
}

impl Default for SystemRegistry {
    fn default() -> Self {
        Self::from_registered().expect("registered system catalog must be valid")
    }
}

impl SystemRegistry {
    pub fn registrations() -> Vec<&'static SystemRegistration> {
        let mut registrations = inventory::iter::<SystemRegistration>().collect::<Vec<_>>();
        registrations.sort_by_key(|registration| registration.descriptor.id);
        registrations
    }

    pub fn from_registered() -> Result<Self, RegistryError> {
        let registrations = Self::registrations();
        let descriptors = registrations
            .iter()
            .map(|registration| registration.descriptor.clone())
            .collect::<Vec<_>>();
        let mut registry = Self::from_catalog(descriptors)?;
        for registration in registrations {
            if registration.implementation.trim().is_empty() {
                return Err(RegistryError::InvalidSystemId(format!(
                    "{} has no implementation binding",
                    registration.descriptor.id
                )));
            }
            registry
                .contracts
                .insert(registration.descriptor.id, registration.contract.clone());
            registry
                .implementations
                .insert(registration.descriptor.id, registration.implementation);
            for profile in registration.profile_roots {
                registry
                    .profile_roots
                    .entry(*profile)
                    .or_default()
                    .push(registration.descriptor.id);
            }
            for signal in registration.health_signals {
                registry
                    .health_signal_systems
                    .entry(*signal)
                    .or_default()
                    .push(registration.descriptor.id);
            }
        }
        for roots in registry.profile_roots.values_mut() {
            roots.sort_unstable();
            roots.dedup();
        }
        for systems in registry.health_signal_systems.values_mut() {
            systems.sort_unstable();
            systems.dedup();
        }
        registry.validate_registration_contracts()?;
        Ok(registry)
    }

    fn validate_registration_contracts(&self) -> Result<(), RegistryError> {
        let providers = self
            .contracts
            .values()
            .flat_map(|contract| contract.provides.iter().copied())
            .collect::<BTreeSet<_>>();
        for (id, contract) in &self.contracts {
            let Some(descriptor) = self.descriptor(id) else {
                return Err(RegistryError::InvalidSystemId(format!(
                    "contract has no descriptor: {id}"
                )));
            };
            if *id != contract.system_id || contract.requires != descriptor.dependencies {
                return Err(RegistryError::InvalidSystemId(format!(
                    "contract metadata mismatch for {id}"
                )));
            }
            for capability in contract.provides.iter().chain(contract.consumes.iter()) {
                if capability.trim().is_empty() || capability.chars().any(char::is_whitespace) {
                    return Err(RegistryError::InvalidSystemId(format!(
                        "invalid capability {capability}"
                    )));
                }
            }
            for required in contract.consumes {
                if !providers.contains(required) {
                    return Err(RegistryError::InvalidSystemId(format!(
                        "{id} consumes unprovided capability {required}"
                    )));
                }
            }
        }
        for id in self.descriptors.keys() {
            if !self.implementations.contains_key(id) {
                return Err(RegistryError::InvalidSystemId(format!(
                    "{id} has no implementation binding"
                )));
            }
        }
        Ok(())
    }

    pub fn catalog() -> Vec<SystemDescriptor> {
        Self::registrations()
            .into_iter()
            .map(|registration| registration.descriptor.clone())
            .collect()
    }

    /// Expand a profile from its registered roots and dependency closure.
    /// The registry owns topology; entrypoints only choose a profile.
    pub fn profile_system_ids(
        &self,
        profile: RuntimeProfile,
    ) -> Result<Vec<&'static str>, RegistryError> {
        let mut ids = BTreeSet::new();
        for root in profile.roots() {
            self.collect_profile_dependencies(root, &mut ids)?;
        }
        Ok(ids.into_iter().collect())
    }

    fn collect_profile_dependencies(
        &self,
        id: &str,
        ids: &mut BTreeSet<&'static str>,
    ) -> Result<(), RegistryError> {
        let Some(descriptor) = self.descriptors.get(id) else {
            return Err(RegistryError::MissingDependency {
                system: id.to_owned(),
                dependency: "profile root".to_owned(),
            });
        };
        if !ids.insert(descriptor.id) {
            return Ok(());
        }
        for dependency in descriptor.dependencies {
            self.collect_profile_dependencies(dependency, ids)?;
        }
        Ok(())
    }

    pub fn from_catalog(descriptors: Vec<SystemDescriptor>) -> Result<Self, RegistryError> {
        let mut registry = Self {
            descriptors: BTreeMap::new(),
            health: BTreeMap::new(),
            contracts: BTreeMap::new(),
            implementations: BTreeMap::new(),
            profile_roots: BTreeMap::new(),
            health_signal_systems: BTreeMap::new(),
        };
        for descriptor in descriptors {
            if descriptor.id.trim().is_empty() || descriptor.id.chars().any(char::is_whitespace) {
                return Err(RegistryError::InvalidSystemId(descriptor.id.to_owned()));
            }
            if descriptor.health_interval_ms == 0 {
                return Err(RegistryError::InvalidHealthInterval(
                    descriptor.id.to_owned(),
                ));
            }
            let mut dependencies = BTreeSet::new();
            for dependency in descriptor.dependencies {
                if dependency == &descriptor.id {
                    return Err(RegistryError::SelfDependency(descriptor.id.to_owned()));
                }
                if !dependencies.insert(*dependency) {
                    return Err(RegistryError::DuplicateDependency {
                        system: descriptor.id.to_owned(),
                        dependency: (*dependency).to_owned(),
                    });
                }
            }
            if registry.descriptors.contains_key(descriptor.id) {
                return Err(RegistryError::DuplicateSystem(descriptor.id.to_owned()));
            }
            registry.descriptors.insert(descriptor.id, descriptor);
        }
        registry.validate_topology()?;
        Ok(registry)
    }

    pub fn descriptors(&self) -> impl Iterator<Item = &SystemDescriptor> {
        self.descriptors.values()
    }

    pub fn descriptor(&self, id: &str) -> Option<&SystemDescriptor> {
        self.descriptors.get(id)
    }

    pub fn contract(&self, id: &str) -> Option<SystemContract> {
        self.contracts.get(id).cloned().or_else(|| {
            self.descriptor(id).map(|descriptor| {
                let provides = Self::registrations()
                    .into_iter()
                    .find(|registration| registration.descriptor.role == descriptor.role)
                    .map(|registration| registration.contract.provides)
                    .unwrap_or(&[]);
                SystemContract {
                    system_id: descriptor.id,
                    requires: descriptor.dependencies,
                    provides,
                    consumes: &[],
                    recovery: recovery_for(descriptor.role, descriptor.restartable),
                }
            })
        })
    }

    pub fn contracts(&self) -> impl Iterator<Item = SystemContract> + '_ {
        self.descriptors.keys().filter_map(|id| self.contract(id))
    }

    pub fn health(&self, id: &str) -> Option<&HealthSnapshot> {
        self.health.get(id)
    }

    pub fn health_snapshots(&self) -> impl Iterator<Item = &HealthSnapshot> {
        self.health.values()
    }

    pub fn manifest(&self) -> PlatformManifest {
        PlatformManifest {
            schema_version: PLATFORM_MANIFEST_SCHEMA_VERSION,
            systems: self
                .descriptors
                .values()
                .filter_map(|descriptor| {
                    self.health(descriptor.id)
                        .cloned()
                        .map(|health| PlatformManifestEntry {
                            descriptor: descriptor.clone(),
                            contract: self
                                .contract(descriptor.id)
                                .unwrap_or_else(|| descriptor.contract()),
                            implementation: self.implementations.get(descriptor.id).copied(),
                            health,
                        })
                })
                .collect(),
        }
    }

    /// Register every discovered system before runtime tasks start. Missing or
    /// stale health is intentionally non-tradable until a producer reports ready.
    pub fn bootstrap_health(&mut self, observed_at_ms: u64) {
        for id in self.descriptors.keys() {
            self.health
                .entry((*id).to_owned())
                .or_insert_with(|| HealthSnapshot::discovered(*id, observed_at_ms));
        }
    }

    /// Convert overdue health reports into an explicit stale state.
    pub fn mark_stale_at(&mut self, now_ms: u64) -> Vec<String> {
        let mut changed = Vec::new();
        for (id, descriptor) in &self.descriptors {
            if let Some(snapshot) = self.health.get_mut(*id) {
                let stale = !snapshot.is_fresh_at(now_ms, descriptor.health_interval_ms);
                if stale != snapshot.stale {
                    snapshot.stale = stale;
                    if stale {
                        snapshot
                            .diagnostics
                            .push("health_report_expired".to_owned());
                    }
                    changed.push((*id).to_owned());
                }
            }
        }
        changed
    }

    /// Evaluate a system together with its complete dependency closure.
    pub fn readiness_at(&self, id: &str, now_ms: u64) -> ReadinessReport {
        let mut visiting = BTreeSet::new();
        self.readiness_visit(id, now_ms, &mut visiting)
    }

    pub fn require_ready(&self, id: &str, now_ms: u64) -> Result<(), ReadinessReport> {
        let report = self.readiness_at(id, now_ms);
        if report.ready {
            Ok(())
        } else {
            Err(report)
        }
    }

    /// Resolve a capability to registered providers and select one ready
    /// provider deterministically. Providers are failover alternatives, so one
    /// healthy provider is sufficient; callers depend on capabilities, not venues.
    pub fn readiness_for_capability(&self, capability: &str, now_ms: u64) -> ReadinessReport {
        let providers = self
            .descriptors
            .values()
            .filter(|descriptor| {
                self.contract(descriptor.id)
                    .is_some_and(|contract| contract.provides.contains(&capability))
            })
            .collect::<Vec<_>>();
        if providers.is_empty() {
            return ReadinessReport::blocked(
                format!("capability:{capability}"),
                now_ms,
                "capability_not_registered",
            );
        }
        let candidate_providers = providers
            .iter()
            .map(|provider| provider.id.to_owned())
            .collect::<Vec<_>>();
        let mut blockers = Vec::new();
        let mut selected_provider = None;
        for provider in providers {
            let report = self.readiness_at(provider.id, now_ms);
            if report.ready && selected_provider.is_none() {
                selected_provider = Some(provider.id.to_owned());
            } else if !report.ready {
                blockers.extend(
                    report
                        .blockers
                        .into_iter()
                        .map(|reason| format!("{}:{reason}", provider.id)),
                );
            }
        }
        if selected_provider.is_some() {
            blockers.clear();
        }
        ReadinessReport {
            system_id: format!("capability:{capability}"),
            checked_at_ms: now_ms,
            ready: selected_provider.is_some(),
            blockers,
            candidate_providers,
            selected_provider,
        }
    }

    pub fn require_capability(&self, capability: &str, now_ms: u64) -> Result<(), ReadinessReport> {
        let report = self.readiness_for_capability(capability, now_ms);
        if report.ready {
            Ok(())
        } else {
            Err(report)
        }
    }

    pub fn readiness_for_role(&self, role: SystemRole, now_ms: u64) -> ReadinessReport {
        let providers = self
            .descriptors
            .values()
            .filter(|descriptor| descriptor.role == role)
            .collect::<Vec<_>>();
        if providers.is_empty() {
            return ReadinessReport::blocked(
                format!("role:{role:?}"),
                now_ms,
                "role_not_registered",
            );
        }
        let candidate_providers = providers
            .iter()
            .map(|provider| provider.id.to_owned())
            .collect::<Vec<_>>();
        let mut blockers = Vec::new();
        let mut selected_provider = None;
        for provider in providers {
            let report = self.readiness_at(provider.id, now_ms);
            if report.ready && selected_provider.is_none() {
                selected_provider = Some(provider.id.to_owned());
            } else if !report.ready {
                blockers.extend(
                    report
                        .blockers
                        .into_iter()
                        .map(|reason| format!("{}:{reason}", provider.id)),
                );
            }
        }
        if selected_provider.is_some() {
            blockers.clear();
        }
        ReadinessReport {
            system_id: format!("role:{role:?}"),
            checked_at_ms: now_ms,
            ready: selected_provider.is_some(),
            blockers,
            candidate_providers,
            selected_provider,
        }
    }

    pub fn require_role(&self, role: SystemRole, now_ms: u64) -> Result<(), ReadinessReport> {
        let report = self.readiness_for_role(role, now_ms);
        if report.ready {
            Ok(())
        } else {
            Err(report)
        }
    }

    fn readiness_visit(
        &self,
        id: &str,
        now_ms: u64,
        visiting: &mut BTreeSet<String>,
    ) -> ReadinessReport {
        if !visiting.insert(id.to_owned()) {
            return ReadinessReport::blocked(id, now_ms, "dependency_cycle");
        }
        let Some(descriptor) = self.descriptors.get(id) else {
            visiting.remove(id);
            return ReadinessReport::blocked(id, now_ms, "system_not_registered");
        };
        let mut blockers = Vec::new();
        match self.health.get(id) {
            None => blockers.push("health_report_missing".to_owned()),
            Some(snapshot) if !snapshot.is_tradable() => {
                blockers.push("health_not_tradable".to_owned())
            }
            Some(snapshot) if !snapshot.is_fresh_at(now_ms, descriptor.health_interval_ms) => {
                blockers.push("health_report_stale".to_owned())
            }
            Some(_) => {}
        }
        for dependency in descriptor.dependencies {
            let report = self.readiness_visit(dependency, now_ms, visiting);
            if !report.ready {
                blockers.extend(
                    report
                        .blockers
                        .into_iter()
                        .map(|reason| format!("{dependency}:{reason}")),
                );
            }
        }
        visiting.remove(id);
        ReadinessReport {
            system_id: id.to_owned(),
            checked_at_ms: now_ms,
            ready: blockers.is_empty(),
            blockers,
            candidate_providers: Vec::new(),
            selected_provider: None,
        }
    }

    pub fn report_health(&mut self, snapshot: HealthSnapshot) -> Result<(), RegistryError> {
        if !self.descriptors.contains_key(snapshot.system_id.as_str()) {
            return Err(RegistryError::MissingDependency {
                system: snapshot.system_id,
                dependency: "registered descriptor".to_owned(),
            });
        }
        if let Some(previous) = self.health.get(snapshot.system_id.as_str()) {
            if snapshot.observed_at_ms < previous.observed_at_ms {
                return Err(RegistryError::HealthTimestampRegression {
                    system: snapshot.system_id,
                    previous_ms: previous.observed_at_ms,
                    observed_ms: snapshot.observed_at_ms,
                });
            }
        }
        self.health.insert(snapshot.system_id.clone(), snapshot);
        Ok(())
    }

    /// Record a liveness heartbeat without discarding accumulated queue,
    /// error, or invariant telemetry supplied by the subsystem.
    pub fn heartbeat(&mut self, id: &str, observed_at_ms: u64) -> Result<(), RegistryError> {
        let Some(snapshot) = self.health.get(id).cloned() else {
            return self.report_health(HealthSnapshot::ready(id, observed_at_ms));
        };
        let next_state = match snapshot.state {
            SystemState::Halted | SystemState::Draining => snapshot.state,
            SystemState::Degraded if snapshot.invariant_failures > 0 => SystemState::Degraded,
            _ => SystemState::Ready,
        };
        self.report_health(HealthSnapshot {
            observed_at_ms,
            stale: false,
            state: next_state,
            diagnostics: snapshot
                .diagnostics
                .into_iter()
                .filter(|reason| reason != "health_report_expired")
                .collect(),
            ..snapshot
        })
    }

    pub fn heartbeat_signal(
        &mut self,
        signal: &str,
        observed_at_ms: u64,
    ) -> Result<Vec<String>, RegistryError> {
        let systems = self
            .health_signal_systems
            .get(signal)
            .cloned()
            .unwrap_or_default();
        for id in &systems {
            self.heartbeat(id, observed_at_ms)?;
        }
        Ok(systems.into_iter().map(str::to_owned).collect())
    }

    pub fn unhealthy(&self) -> impl Iterator<Item = &HealthSnapshot> {
        self.health
            .values()
            .filter(|snapshot| !snapshot.is_tradable())
    }

    pub fn replace_policy(
        &mut self,
        id: &str,
        replacement: SystemDescriptor,
    ) -> Result<(), RegistryError> {
        let current = self
            .descriptors
            .get(id)
            .ok_or_else(|| RegistryError::MissingDependency {
                system: id.to_owned(),
                dependency: "existing descriptor".to_owned(),
            })?;
        let current_id = current.id;
        if current.mutability == Mutability::ImmutableCore {
            return Err(RegistryError::ImmutableReplacement(id.to_owned()));
        }
        if replacement.id != id {
            return Err(RegistryError::DuplicateSystem(replacement.id.to_owned()));
        }
        if replacement.layer != current.layer
            || replacement.role != current.role
            || replacement.authority != current.authority
            || replacement.mutability != current.mutability
            || replacement.restartable != current.restartable
            || replacement.health_interval_ms != current.health_interval_ms
        {
            return Err(RegistryError::ImmutableReplacement(id.to_owned()));
        }
        let previous = self.descriptors.insert(replacement.id, replacement);
        if let Err(error) = self.validate_topology() {
            if let Some(previous) = previous {
                self.descriptors.insert(current_id, previous);
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn validate_topology(&self) -> Result<(), RegistryError> {
        for descriptor in self.descriptors.values() {
            for dependency in descriptor.dependencies {
                let Some(dependency_descriptor) = self.descriptors.get(dependency) else {
                    return Err(RegistryError::MissingDependency {
                        system: descriptor.id.to_owned(),
                        dependency: (*dependency).to_owned(),
                    });
                };
                if dependency_descriptor.layer > descriptor.layer {
                    return Err(RegistryError::LayerViolation {
                        system: descriptor.id.to_owned(),
                        dependency: (*dependency).to_owned(),
                        system_layer: descriptor.layer,
                        dependency_layer: dependency_descriptor.layer,
                    });
                }
            }
        }
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for id in self.descriptors.keys() {
            self.visit(id, &mut visiting, &mut visited)?;
        }
        Ok(())
    }

    fn visit(
        &self,
        id: &str,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> Result<(), RegistryError> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.to_owned()) {
            return Err(RegistryError::DependencyCycle);
        }
        let descriptor = self
            .descriptors
            .get(id)
            .expect("dependencies checked before cycle walk");
        for dependency in descriptor.dependencies {
            self.visit(dependency, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id.to_owned());
        Ok(())
    }
}

macro_rules! register_system {
    ($id:literal, $layer:expr, $role:expr, $authority:expr, $mutability:expr,
     $dependencies:expr, $health_interval_ms:expr, $restartable:expr,
     $provides:expr, $consumes:expr, $profile_roots:expr, $health_signals:expr,
     $implementation:literal, $source:literal) => {
        const _: &[u8] = include_bytes!($source);
        inventory::submit! {
            SystemRegistration {
                descriptor: SystemDescriptor {
                    id: $id, layer: $layer, role: $role, authority: $authority,
                    mutability: $mutability, dependencies: $dependencies,
                    health_interval_ms: $health_interval_ms, restartable: $restartable,
                },
                contract: SystemContract {
                    system_id: $id, requires: $dependencies, provides: $provides,
                    consumes: $consumes, recovery: recovery_for($role, $restartable),
                },
                implementation: $implementation,
                profile_roots: $profile_roots,
                health_signals: $health_signals,
            }
        }
    };
}

register_system!(
    "control.kernel",
    PlatformLayer::Control,
    SystemRole::Registry,
    Authority::Internal,
    Mutability::ImmutableCore,
    &[],
    5_000,
    false,
    &["system.discovery", "system.topology"],
    &[],
    &[],
    &["runtime_bootstrap"],
    "engine::platform",
    "platform.rs"
);
register_system!(
    "market.binance",
    PlatformLayer::MarketData,
    SystemRole::ExchangeAdapter,
    Authority::Binance,
    Mutability::ImmutableCore,
    &["control.kernel"],
    2_000,
    true,
    &["market.events", "market.connection"],
    &[],
    &[],
    &["market_events"],
    "engine::market::binance",
    "market/binance.rs"
);
register_system!(
    "market.reference",
    PlatformLayer::MarketData,
    SystemRole::ReferenceData,
    Authority::ExternalReference,
    Mutability::ImmutableCore,
    &["control.kernel"],
    30_000,
    true,
    &["reference.fx", "reference.metadata", "reference.close"],
    &[],
    &[],
    &["reference_data"],
    "engine::market::fx",
    "market/fx.rs"
);
register_system!(
    "market.anchor",
    PlatformLayer::MarketData,
    SystemRole::Anchor,
    Authority::Derived,
    Mutability::ImmutableCore,
    &["market.binance", "market.reference"],
    2_000,
    true,
    &["anchor.snapshot"],
    &["market.events", "reference.close", "reference.fx"],
    &[],
    &["market_events"],
    "engine::runtime::reference_authority",
    "runtime/reference_authority.rs"
);
register_system!(
    "decision.strategy",
    PlatformLayer::Decision,
    SystemRole::Strategy,
    Authority::Internal,
    Mutability::GovernedPolicy,
    &["market.anchor"],
    1_000,
    true,
    &["decision.intent"],
    &["anchor.snapshot"],
    &[RuntimeProfile::Live],
    &[],
    "engine::strategy",
    "strategy/mod.rs"
);
register_system!(
    "decision.portfolio",
    PlatformLayer::Decision,
    SystemRole::Portfolio,
    Authority::Internal,
    Mutability::GovernedPolicy,
    &["decision.strategy", "decision.risk"],
    1_000,
    true,
    &["decision.allocation"],
    &["decision.intent", "risk.admission"],
    &[RuntimeProfile::Live],
    &[],
    "engine::strategy::capital",
    "strategy/capital.rs"
);
register_system!(
    "decision.risk",
    PlatformLayer::Decision,
    SystemRole::Risk,
    Authority::Internal,
    Mutability::ImmutableCore,
    &["market.binance", "market.anchor"],
    500,
    false,
    &["risk.admission", "risk.flatten"],
    &["market.events", "anchor.snapshot"],
    &[RuntimeProfile::Live],
    &["market_events"],
    "engine::risk",
    "risk.rs"
);
register_system!(
    "decision.funding",
    PlatformLayer::Decision,
    SystemRole::Funding,
    Authority::Binance,
    Mutability::GovernedPolicy,
    &["market.binance", "decision.risk"],
    2_000,
    true,
    &["funding.schedule", "funding.deadline"],
    &["market.events", "risk.admission"],
    &[RuntimeProfile::Live],
    &[],
    "engine::execution::funding_risk",
    "execution/funding_risk.rs"
);
register_system!(
    "execution.gateway",
    PlatformLayer::Execution,
    SystemRole::ExecutionGateway,
    Authority::Binance,
    Mutability::ImmutableCore,
    &["market.binance", "decision.risk"],
    1_000,
    true,
    &["execution.submit", "execution.cancel"],
    &["risk.admission", "decision.intent"],
    &[RuntimeProfile::Live],
    &["market_events"],
    "engine::execution::gateway",
    "execution/gateway.rs"
);
register_system!(
    "execution.lifecycle",
    PlatformLayer::Execution,
    SystemRole::Lifecycle,
    Authority::Binance,
    Mutability::ImmutableCore,
    &["execution.gateway", "decision.risk"],
    1_000,
    true,
    &["execution.lifecycle", "execution.reconcile"],
    &["execution.submit", "execution.cancel"],
    &[RuntimeProfile::Live],
    &["user_data"],
    "engine::execution::lifecycle",
    "execution/lifecycle.rs"
);
register_system!(
    "simulation.runtime",
    PlatformLayer::Simulation,
    SystemRole::Simulation,
    Authority::Derived,
    Mutability::GovernedPolicy,
    &["market.anchor", "decision.strategy", "decision.risk"],
    5_000,
    true,
    &["simulation.run"],
    &[
        "market.events",
        "anchor.snapshot",
        "decision.intent",
        "risk.admission"
    ],
    &[RuntimeProfile::Simulation, RuntimeProfile::Batch],
    &[],
    "engine::simulation",
    "simulation.rs"
);
register_system!(
    "simulation.replay",
    PlatformLayer::Simulation,
    SystemRole::Replay,
    Authority::Derived,
    Mutability::GovernedPolicy,
    &["market.binance"],
    5_000,
    true,
    &["simulation.replay"],
    &["market.events"],
    &[],
    &[],
    "engine::simulation::replay",
    "simulation/replay.rs"
);
register_system!(
    "simulation.backtest",
    PlatformLayer::Simulation,
    SystemRole::Backtest,
    Authority::Derived,
    Mutability::GovernedPolicy,
    &["simulation.replay", "decision.strategy"],
    5_000,
    true,
    &["simulation.validation"],
    &["simulation.replay", "decision.intent"],
    &[RuntimeProfile::Backtest],
    &[],
    "engine::backtest",
    "backtest.rs"
);
register_system!(
    "analytics.validation",
    PlatformLayer::Analytics,
    SystemRole::Analytics,
    Authority::Derived,
    Mutability::GovernedPolicy,
    &["simulation.backtest"],
    10_000,
    true,
    &["analytics.evidence"],
    &["simulation.validation"],
    &[RuntimeProfile::Backtest],
    &[],
    "engine::analytics_validation",
    "analytics_validation.rs"
);
register_system!(
    "observability.telemetry",
    PlatformLayer::Observability,
    SystemRole::Observability,
    Authority::Internal,
    Mutability::ImmutableCore,
    &["control.kernel"],
    5_000,
    true,
    &["telemetry.health"],
    &[],
    &[],
    &["runtime_bootstrap"],
    "engine::observability",
    "observability.rs"
);
register_system!(
    "observability.audit",
    PlatformLayer::Observability,
    SystemRole::Audit,
    Authority::Internal,
    Mutability::ImmutableCore,
    &["observability.telemetry"],
    5_000,
    true,
    &["audit.events"],
    &["telemetry.health"],
    &[
        RuntimeProfile::Live,
        RuntimeProfile::Simulation,
        RuntimeProfile::Batch,
        RuntimeProfile::Backtest
    ],
    &[],
    "engine::runtime::audit",
    "runtime/audit.rs"
);
register_system!(
    "operations.supervisor",
    PlatformLayer::Operations,
    SystemRole::Recovery,
    Authority::Internal,
    Mutability::ImmutableCore,
    &["control.kernel", "execution.lifecycle"],
    1_000,
    false,
    &["recovery.orchestration"],
    &["execution.lifecycle", "risk.admission"],
    &[RuntimeProfile::Live],
    &[],
    "engine::runtime::supervisor",
    "runtime/supervisor.rs"
);
register_system!(
    "operations.console",
    PlatformLayer::Operations,
    SystemRole::ControlConsole,
    Authority::Operator,
    Mutability::GovernedPolicy,
    &["control.kernel", "observability.telemetry"],
    10_000,
    true,
    &["control.operations"],
    &["telemetry.health"],
    &[RuntimeProfile::Dashboard],
    &[],
    "engine::bin::anchorbell_dashboard",
    "bin/anchorbell_dashboard.rs"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_catalog_is_complete_and_acyclic() {
        let registry = SystemRegistry::default();
        assert!(registry.descriptor("execution.gateway").is_some());
        assert!(registry.descriptor("simulation.runtime").is_some());
        assert!(registry.validate_topology().is_ok());
    }

    #[test]
    fn profiles_expand_only_registered_dependency_closures() {
        let registry = SystemRegistry::default();
        let dashboard = registry
            .profile_system_ids(RuntimeProfile::Dashboard)
            .unwrap();
        assert!(dashboard.contains(&"operations.console"));
        assert!(dashboard.contains(&"control.kernel"));
        assert!(dashboard.contains(&"observability.telemetry"));
        assert!(!dashboard.contains(&"execution.gateway"));
        assert!(registry.validate_topology().is_ok());
    }

    #[test]
    fn unknown_health_cannot_be_admitted() {
        let mut registry = SystemRegistry::default();
        let result = registry.report_health(HealthSnapshot::ready("unknown", 1));
        assert!(result.is_err());
    }

    #[test]
    fn immutable_core_cannot_be_replaced() {
        let mut registry = SystemRegistry::default();
        let replacement = SystemDescriptor {
            id: "decision.risk",
            layer: PlatformLayer::Decision,
            role: SystemRole::Risk,
            authority: Authority::Internal,
            mutability: Mutability::GovernedPolicy,
            dependencies: &["market.binance", "market.anchor"],
            health_interval_ms: 500,
            restartable: false,
        };
        assert!(matches!(
            registry.replace_policy("decision.risk", replacement),
            Err(RegistryError::ImmutableReplacement(_))
        ));
    }

    #[test]
    fn health_is_fail_closed() {
        let snapshot = HealthSnapshot::ready("decision.risk", 1);
        assert!(snapshot.is_tradable());
        let mut stale = snapshot.clone();
        stale.stale = true;
        assert!(!stale.is_tradable());
    }

    #[test]
    fn readiness_requires_health_for_the_complete_dependency_closure() {
        let mut registry = SystemRegistry::default();
        let blocked = registry.readiness_at("execution.gateway", 1_000);
        assert!(!blocked.ready);
        assert!(blocked
            .blockers
            .iter()
            .any(|reason| reason.contains("health_report_missing")));

        for id in [
            "control.kernel",
            "market.binance",
            "market.reference",
            "market.anchor",
            "decision.risk",
            "execution.gateway",
        ] {
            registry
                .report_health(HealthSnapshot::ready(id, 1_000))
                .unwrap();
        }
        assert!(registry.readiness_at("execution.gateway", 1_000).ready);

        let changed = registry.mark_stale_at(2_001);
        assert!(changed.iter().any(|id| id == "decision.risk"));
        assert!(!registry.readiness_at("execution.gateway", 2_001).ready);
    }

    #[test]
    fn operational_systems_are_registered_as_first_class_nodes() {
        let registry = SystemRegistry::default();
        assert!(registry.descriptor("operations.supervisor").is_some());
        assert!(registry.descriptor("analytics.validation").is_some());
        assert!(registry.validate_topology().is_ok());
    }

    #[test]
    fn contracts_expose_capabilities_and_recovery() {
        let registry = SystemRegistry::default();
        let contract = registry.contract("execution.gateway").unwrap();
        assert_eq!(contract.requires, &["market.binance", "decision.risk"]);
        assert!(contract.provides.contains(&"execution.submit"));
        assert_eq!(contract.recovery, RecoveryPolicy::RestartThenReconcile);
        assert_eq!(registry.contracts().count(), registry.descriptors().count());
    }

    #[test]
    fn capability_readiness_resolves_provider_closure() {
        let mut registry = SystemRegistry::default();
        registry.bootstrap_health(1_000);
        let blocked = registry.readiness_for_capability("execution.submit", 1_000);
        assert!(!blocked.ready);
        for id in [
            "control.kernel",
            "market.binance",
            "market.reference",
            "market.anchor",
            "decision.risk",
            "execution.gateway",
        ] {
            registry
                .report_health(HealthSnapshot::ready(id, 1_000))
                .unwrap();
        }
        assert!(registry
            .require_capability("execution.submit", 1_000)
            .is_ok());
        assert!(registry
            .readiness_for_capability("missing.capability", 1_000)
            .blockers
            .contains(&"capability_not_registered".to_owned()));
    }

    #[test]
    fn invalid_policy_replacement_is_atomic() {
        let mut registry = SystemRegistry::default();
        let replacement = SystemDescriptor {
            id: "decision.strategy",
            layer: PlatformLayer::Decision,
            role: SystemRole::Strategy,
            authority: Authority::Internal,
            mutability: Mutability::GovernedPolicy,
            dependencies: &["missing.parent"],
            health_interval_ms: 1_000,
            restartable: true,
        };
        assert!(registry
            .replace_policy("decision.strategy", replacement)
            .is_err());
        assert_eq!(
            registry
                .descriptor("decision.strategy")
                .expect("original descriptor remains")
                .dependencies,
            ["market.anchor"]
        );
    }

    #[test]
    fn capability_readiness_selects_a_ready_failover_provider() {
        let mut registry = SystemRegistry::from_catalog(vec![
            SystemDescriptor {
                id: "control.kernel",
                layer: PlatformLayer::Control,
                role: SystemRole::Registry,
                authority: Authority::Internal,
                mutability: Mutability::ImmutableCore,
                dependencies: &[],
                health_interval_ms: 5_000,
                restartable: false,
            },
            SystemDescriptor {
                id: "execution.primary",
                layer: PlatformLayer::Execution,
                role: SystemRole::ExecutionGateway,
                authority: Authority::Binance,
                mutability: Mutability::ImmutableCore,
                dependencies: &["control.kernel"],
                health_interval_ms: 1_000,
                restartable: true,
            },
            SystemDescriptor {
                id: "execution.standby",
                layer: PlatformLayer::Execution,
                role: SystemRole::ExecutionGateway,
                authority: Authority::Binance,
                mutability: Mutability::ImmutableCore,
                dependencies: &["control.kernel"],
                health_interval_ms: 1_000,
                restartable: true,
            },
        ])
        .unwrap();
        registry.bootstrap_health(1_000);
        registry
            .report_health(HealthSnapshot::ready("control.kernel", 1_000))
            .unwrap();
        registry
            .report_health(HealthSnapshot::ready("execution.standby", 1_000))
            .unwrap();

        let report = registry.readiness_for_capability("execution.submit", 1_000);
        assert!(report.ready);
        assert_eq!(
            report.candidate_providers,
            vec!["execution.primary", "execution.standby"]
        );
        assert_eq!(
            report.selected_provider.as_deref(),
            Some("execution.standby")
        );
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn health_timestamp_regression_is_rejected() {
        let mut registry = SystemRegistry::default();
        registry
            .report_health(HealthSnapshot::ready("control.kernel", 2_000))
            .unwrap();
        assert!(matches!(
            registry.report_health(HealthSnapshot::ready("control.kernel", 1_999)),
            Err(RegistryError::HealthTimestampRegression { .. })
        ));
    }

    #[test]
    fn heartbeat_preserves_subsystem_telemetry() {
        let mut registry = SystemRegistry::default();
        let mut snapshot = HealthSnapshot::ready("control.kernel", 1_000);
        snapshot.queue_depth = 12;
        snapshot.error_rate_ppm = 7;
        snapshot.diagnostics.push("queue_observed".to_owned());
        registry.report_health(snapshot).unwrap();
        registry.heartbeat("control.kernel", 2_000).unwrap();

        let current = registry.health("control.kernel").unwrap();
        assert_eq!(current.queue_depth, 12);
        assert_eq!(current.error_rate_ppm, 7);
        assert!(current.diagnostics.contains(&"queue_observed".to_owned()));
        assert_eq!(current.state, SystemState::Ready);
        assert_eq!(current.observed_at_ms, 2_000);
    }

    #[test]
    fn topology_rejects_dependencies_that_cross_upward() {
        let result = SystemRegistry::from_catalog(vec![
            SystemDescriptor {
                id: "control.kernel",
                layer: PlatformLayer::Control,
                role: SystemRole::Registry,
                authority: Authority::Internal,
                mutability: Mutability::ImmutableCore,
                dependencies: &[],
                health_interval_ms: 5_000,
                restartable: false,
            },
            SystemDescriptor {
                id: "decision.strategy",
                layer: PlatformLayer::Decision,
                role: SystemRole::Strategy,
                authority: Authority::Internal,
                mutability: Mutability::GovernedPolicy,
                dependencies: &["analytics.validation"],
                health_interval_ms: 1_000,
                restartable: true,
            },
            SystemDescriptor {
                id: "analytics.validation",
                layer: PlatformLayer::Analytics,
                role: SystemRole::Analytics,
                authority: Authority::Derived,
                mutability: Mutability::RuntimeState,
                dependencies: &["control.kernel"],
                health_interval_ms: 1_000,
                restartable: true,
            },
        ]);
        assert!(matches!(result, Err(RegistryError::LayerViolation { .. })));
    }

    #[test]
    fn heartbeat_cannot_reactivate_halted_or_invariant_broken_systems() {
        let mut registry = SystemRegistry::default();
        let mut halted = HealthSnapshot::ready("control.kernel", 1_000);
        halted.state = SystemState::Halted;
        registry.report_health(halted).unwrap();
        registry.heartbeat("control.kernel", 2_000).unwrap();
        assert_eq!(
            registry.health("control.kernel").unwrap().state,
            SystemState::Halted
        );

        let mut degraded = HealthSnapshot::ready("control.kernel", 3_000);
        degraded.state = SystemState::Degraded;
        degraded.invariant_failures = 1;
        registry.report_health(degraded).unwrap();
        registry.heartbeat("control.kernel", 4_000).unwrap();
        let current = registry.health("control.kernel").unwrap();
        assert_eq!(current.state, SystemState::Degraded);
        assert_eq!(current.invariant_failures, 1);
    }
}
