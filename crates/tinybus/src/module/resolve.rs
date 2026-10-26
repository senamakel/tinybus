//! Pure manifest dependency resolution and deterministic load ordering, and the
//! [`Resolver`] that turns a pinned release into a serving module.
//!
//! # The ladder
//!
//! [`Resolver::ensure_loaded`] resolves a module once per process and remembers
//! the outcome, including failure. A library is never unloaded, so a module that
//! was refused, faulted or failed to initialise keeps what it mapped and loading
//! it again cannot reach a different outcome. Retrying would pay a download and
//! a `dlopen` on every call to arrive at the same error, so a failure is cached
//! and returned directly; fixing a module means restarting the process.
//!
//! Resolution order is cheapest first: already loaded, an override, the module
//! search path, then the release cache (verified against the host's pinned
//! digest, downloaded on a miss when allowed), then pruning of stale versions.
//!
//! Each module has one slot in [`super::resolution`]. The first caller runs the
//! resolution as a process-lifetime task and every other caller waits on its
//! outcome; two modules never queue behind each other.
//! [`Resolver::ensure_loaded_within`] bounds the wait, never the resolution.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::module::failure::{FailureReporter, Reason};
use crate::module::host::{ModuleInfo, ModuleState};
use crate::module::manifest::ModuleManifest;
use crate::module::platform::host_candidates;
use crate::module::resolution::{self, Claim, Resolution, ResolutionState, Waited};
use crate::module::runtime::ModuleRuntime;
use crate::module::{
    Availability, HostCallbackBinding, LinkedModule, ReleaseAsset, ReleasePlan, ReleaseRecord,
    SearchPathConfig, load_first_admitted, prune_stale_versions,
};

type HostCallbackFactory = Arc<
    dyn Fn(usize) -> Pin<
            Box<
                dyn Future<Output = Result<(Arc<HostCallbackBinding>, serde_json::Value), String>>
                    + Send,
            >,
        > + Send
        + Sync,
>;

#[derive(Clone)]
enum CallbackLoad {
    Single(Arc<HostCallbackBinding>, serde_json::Value),
    PerAttempt {
        factory: HostCallbackFactory,
        selected: Arc<Mutex<Option<Arc<HostCallbackBinding>>>>,
    },
}

fn recover_admission(
    result: Result<(), String>,
    modules: &[ModuleInfo],
    record: &ReleaseRecord,
) -> Result<(), String> {
    if result.is_err() && modules.iter().any(|info| is_live_match(info, record)) {
        Ok(())
    } else {
        result
    }
}

fn is_live_match(info: &ModuleInfo, record: &ReleaseRecord) -> bool {
    info.enabled
        && info.manifest.bus_name.as_str() == record.bus_name
        && info.manifest.object_path.as_str() == record.object_path
        && matches!(
            info.state,
            ModuleState::Resolved
                | ModuleState::Initializing
                | ModuleState::Ready
                | ModuleState::Serving
        )
}

/// Result indexes refer to the input manifest slice.
pub(crate) struct LoadOrder {
    pub(crate) order: Vec<usize>,
    pub(crate) unresolved: Vec<(usize, String)>,
}

/// Reject collisions, ignore absent optional dependencies, and topologically
/// order every remaining manifest.
pub(crate) fn resolve(
    manifests: &[ModuleManifest],
    initially_available: &HashSet<String>,
) -> LoadOrder {
    let duplicate_modules = duplicates(manifests.iter().map(|manifest| &manifest.module.name));
    let duplicate_bus_names = duplicates(manifests.iter().map(|manifest| &manifest.bus_name));
    let mut unresolved = Vec::new();
    let mut pending = Vec::new();
    for (index, manifest) in manifests.iter().enumerate() {
        if duplicate_modules.contains(&manifest.module.name) {
            unresolved.push((
                index,
                "two artifacts declare the same module name".to_string(),
            ));
        } else if duplicate_bus_names.contains(&manifest.bus_name) {
            unresolved.push((index, "two modules claim the same bus name".to_string()));
        } else {
            pending.push(index);
        }
    }

    let mut available = initially_available.clone();
    let mut order = Vec::new();
    while !pending.is_empty() {
        let ready = pending.iter().position(|index| {
            manifests[*index]
                .requires
                .iter()
                .filter(|dependency| !dependency.optional)
                .all(|dependency| available.contains(dependency.interface.interface.as_str()))
        });
        if let Some(position) = ready {
            let index = pending.remove(position);
            available.extend(
                manifests[index]
                    .provides
                    .iter()
                    .map(|provided| provided.version.interface.to_string()),
            );
            order.push(index);
            continue;
        }

        // Remove genuinely blocked providers first. Their interfaces then stop
        // masking missing dependencies in consumers behind them; only a fixed
        // point with no absent provider is a real cycle.
        let declared = pending
            .iter()
            .flat_map(|index| manifests[*index].provides.iter())
            .map(|provided| provided.version.interface.to_string())
            .collect::<HashSet<_>>();
        let blocked = pending.iter().position(|index| {
            manifests[*index]
                .requires
                .iter()
                .filter(|dependency| !dependency.optional)
                .any(|dependency| {
                    !available.contains(dependency.interface.interface.as_str())
                        && !declared.contains(dependency.interface.interface.as_str())
                })
        });
        if let Some(position) = blocked {
            let index = pending.remove(position);
            let missing = manifests[index]
                .requires
                .iter()
                .filter(|dependency| !dependency.optional)
                .find(|dependency| {
                    !available.contains(dependency.interface.interface.as_str())
                        && !declared.contains(dependency.interface.interface.as_str())
                })
                .expect("blocked module has a missing dependency");
            unresolved.push((
                index,
                format!(
                    "required interface {} has no provider",
                    missing.interface.interface
                ),
            ));
            continue;
        }
        for index in pending.drain(..) {
            unresolved.push((index, "module dependency cycle detected".to_string()));
        }
    }
    LoadOrder { order, unresolved }
}

fn duplicates<T: Eq + std::hash::Hash>(values: impl Iterator<Item = T>) -> HashSet<T> {
    let mut counts = HashMap::new();
    for value in values {
        *counts.entry(value).or_insert(0usize) += 1;
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

/// Where a module's artifact should come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModuleSource {
    /// The registry-pinned GitHub release for this build.
    Pinned,
    /// A local artifact, for developing a module against a live host.
    LocalFile {
        /// Absolute path to the platform library.
        path: String,
    },
    /// A module compiled into the host executable, from a factory registered in
    /// [`ResolverInputs::linked`]. Admitted through the same descriptor,
    /// manifest, dependency and init checks as a loaded library; the host
    /// executable's digest is the attestation. A missing factory is a terminal
    /// failure, never a silent fall back to the release.
    Linked,
}

/// Why a bounded [`Resolver::ensure_loaded_within`] did not end with the module
/// serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadError {
    /// Terminal for the process; a message fit for a user or a model.
    Failed(String),
    /// Switched off by the host's [`ModuleGate`]. Nothing was downloaded,
    /// mapped or registered. Carries the label the gate returned.
    Disabled(&'static str),
    /// Still being downloaded, verified or initialised when the bound passed.
    /// Asking again later can succeed.
    StillLoading,
}

impl LoadError {
    /// The message for a caller that cannot distinguish the two cases.
    #[must_use]
    pub fn into_message(self) -> String {
        match self {
            Self::Failed(message) => message,
            Self::StillLoading => "the module is still loading; try again shortly".to_string(),
            Self::Disabled(label) => format!("the module is switched off by host policy ({label})"),
        }
    }
}

/// Future returned by a [`RuntimeProvider`].
pub type RuntimeFuture =
    Pin<Box<dyn Future<Output = crate::Result<&'static ModuleRuntime>> + Send>>;

/// Starts (or returns) the host's process-wide [`ModuleRuntime`], lazily, so a
/// refusal or a cached outcome never starts a broker. The SDK polls this future
/// on a dedicated startup executor, independent of any caller runtime. Tasks
/// that must outlive the provider future must be owned by the returned
/// [`ModuleRuntime`] or another retained host executor; detached tasks on this
/// temporary executor stop when startup finishes.
pub type RuntimeProvider = Arc<dyn Fn() -> RuntimeFuture + Send + Sync>;

/// Builds the configuration blob handed to a module when it is initialised.
pub type ModuleConfigProvider = Arc<dyn Fn(&str) -> serde_json::Value + Send + Sync>;

/// Decides, per module id, whether host policy switches the module off. Returns
/// a short label (e.g. the setting that did it) when it is off, which the
/// resolver returns as [`LoadError::Disabled`] before it resolves, reports or
/// starts anything.
pub type ModuleGate = Arc<dyn Fn(&str) -> Option<&'static str> + Send + Sync>;

/// Called with a module id after its resolution finished, successfully or not,
/// and before any caller waiting on it is released.
pub type ResolvedHook = Arc<dyn Fn(&str) + Send + Sync>;

/// Builds the [`LinkedModule`] compiled into the host for one module id. A plain
/// `fn` so a registration carries no state.
pub type LinkedFactory = fn() -> crate::Result<LinkedModule>;

/// Process-wide set of module factories compiled into a host executable.
/// Registration replaces a prior factory for the same id, allowing repeated
/// host installation to remain idempotent.
pub struct LinkedRegistry {
    factories: Mutex<BTreeMap<&'static str, LinkedFactory>>,
}

impl LinkedRegistry {
    /// Create an empty registry, usually for isolated host or test wiring.
    #[must_use]
    pub fn new() -> Self {
        Self {
            factories: Mutex::new(BTreeMap::new()),
        }
    }

    /// The process-global registry used by a host binary.
    #[must_use]
    pub fn global() -> Arc<Self> {
        static REGISTRY: OnceLock<Arc<LinkedRegistry>> = OnceLock::new();
        Arc::clone(REGISTRY.get_or_init(|| Arc::new(Self::new())))
    }

    fn factories(&self) -> std::sync::MutexGuard<'_, BTreeMap<&'static str, LinkedFactory>> {
        self.factories
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Register a factory for a module id. A later registration replaces it.
    pub fn register(&self, id: &'static str, factory: LinkedFactory) {
        let replaced = self.factories().insert(id, factory).is_some();
        tracing::debug!(module = id, replaced, "linked module factory registered");
    }

    /// Whether this registry has a factory for `id`.
    #[must_use]
    pub fn is_registered(&self, id: &str) -> bool {
        self.factories().contains_key(id)
    }

    /// Return the registered factory for `id`, if one exists.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<LinkedFactory> {
        self.factories().get(id).copied()
    }
}

impl Default for LinkedRegistry {
    fn default() -> Self {
        Self::new()
    }
}

static BUNDLED_RELEASES: OnceLock<PathBuf> = OnceLock::new();

/// Register the installer-owned directory containing release archives.
/// Its files still pass the ordinary digest and module admission checks.
pub fn set_bundled_releases_dir(path: PathBuf) -> Result<(), PathBuf> {
    BUNDLED_RELEASES.set(path)
}

/// Resolve the installer-owned archive directory from the registered path,
/// the explicitly named environment variable, or `bundled-modules/` beside
/// the current executable, in that order. Only existing directories qualify.
#[must_use]
pub fn resolve_bundled_dir(env_var_name: &str) -> Option<PathBuf> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    resolve_bundled_dir_from(
        BUNDLED_RELEASES.get().cloned(),
        std::env::var_os(env_var_name).map(PathBuf::from),
        exe_dir,
    )
}

fn resolve_bundled_dir_from(
    registered: Option<PathBuf>,
    from_env: Option<PathBuf>,
    exe_dir: Option<PathBuf>,
) -> Option<PathBuf> {
    let found = [
        registered,
        from_env,
        exe_dir.map(|dir| dir.join("bundled-modules")),
    ]
    .into_iter()
    .flatten()
    .find(|dir| dir.is_dir());
    if let Some(dir) = &found {
        tracing::debug!(path = %dir.display(), "bundled module directory resolved");
    }
    found
}

#[path = "resolve_access.rs"]
pub(super) mod access;

/// Plain inputs a host builds a [`Resolver`] from.
pub struct ResolverInputs {
    /// Whether modules are enabled at all.
    pub enabled: bool,
    /// Where the release cache lives. `None` means nothing can be installed.
    pub install_dir: Option<PathBuf>,
    /// Whether a release-cache miss may reach the network.
    pub allow_download: bool,
    /// A read-only installer-shipped cache root, consulted first.
    pub bundled_dir: Option<PathBuf>,
    /// Per-module artifact sources. A module absent here, or `Pinned`, loads the
    /// pinned release.
    pub sources: HashMap<String, ModuleSource>,
    /// Registry of factories for `Linked` sources. The host binary uses
    /// [`LinkedRegistry::global`]; tests and embedders may inject their own.
    pub linked: Arc<LinkedRegistry>,
    /// Search path policy for the module host. `None` means no implicit paths.
    pub search_paths: Option<SearchPathConfig>,
    /// Library file stems the search-path rung must not open, e.g. the
    /// libraries of modules the host has switched off.
    pub search_skip: Vec<String>,
    /// Host policy that switches modules off by id. `None` switches nothing off.
    pub gate: Option<ModuleGate>,
    /// Called when a resolution finishes, before waiting callers are released.
    pub on_resolved: Option<ResolvedHook>,
    /// The compiled registry.
    pub records: Vec<&'static ReleaseRecord>,
    /// Sentence appended to every terminal loader error, e.g. "This is terminal
    /// for the running process". Empty appends nothing the second time.
    pub terminal_marker: String,
    /// Starts the module runtime.
    pub runtime: RuntimeProvider,
    /// Per-module initialisation configuration.
    pub module_config: ModuleConfigProvider,
    /// Terminal-outcome reporting. Share one across the process.
    pub reporter: Arc<FailureReporter>,
}

#[cfg(test)]
type InitialAdmissionHook = Arc<dyn Fn(&'static ModuleRuntime, &ReleaseRecord) + Send + Sync>;

/// Resolves registry records into serving modules. Cheap to clone.
#[derive(Clone)]
pub struct Resolver {
    inner: Arc<ResolverInputs>,
    #[cfg(test)]
    after_initial_host_check: Option<InitialAdmissionHook>,
}

impl Resolver {
    /// A resolver over `inputs`.
    #[must_use]
    pub fn new(inputs: ResolverInputs) -> Self {
        Self {
            inner: Arc::new(inputs),
            #[cfg(test)]
            after_initial_host_check: None,
        }
    }

    /// The record for `id`, if the registry has one.
    #[must_use]
    pub fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        self.inner.records.iter().copied().find(|r| r.id == id)
    }

    /// Whether modules are enabled.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    /// The reporter terminal outcomes go through.
    #[must_use]
    pub fn reporter(&self) -> &FailureReporter {
        &self.inner.reporter
    }

    /// The module runtime, started on first use.
    ///
    /// # Errors
    ///
    /// Whatever the host's provider returns when the runtime cannot start.
    pub async fn runtime(&self) -> crate::Result<&'static ModuleRuntime> {
        let runtime = (self.inner.runtime)().await?;
        runtime
            .host()
            .configure_search_path_config(self.inner.search_paths.clone());
        Ok(runtime)
    }

    /// Report a terminal outcome for `record`, once per process.
    pub fn report(&self, record: &ReleaseRecord, reason: Reason) {
        self.inner
            .reporter
            .report(record.id, record.version, reason);
    }

    /// Report a failure of one invocation of `record`.
    pub fn report_invocation(&self, record: &ReleaseRecord, reason: Reason) {
        self.inner
            .reporter
            .report_invocation(record.id, record.version, reason);
    }

    /// Ensure `id` is loaded and serving, loading it if this is the first ask.
    ///
    /// Waits without bound; prefer [`Self::ensure_loaded_within`] from any path
    /// that has a deadline of its own.
    ///
    /// # Errors
    ///
    /// A message fit for a user or a model when the module cannot be loaded.
    pub async fn ensure_loaded(&self, id: &str) -> Result<(), String> {
        self.ensure_loaded_inner(id, None, None)
            .await
            .map_err(LoadError::into_message)
    }

    /// Resolve a local artifact while binding one prepared host callback set to
    /// the exact registry identity before initialization. The capability is
    /// retained by the admitted module for its lifetime. Linked, search-path,
    /// and release-cache sources fail closed until their admission paths can
    /// preserve the same identity binding.
    ///
    /// A callback load cannot join a generic in-flight or already-loaded
    /// resolution. Its binding must belong to this resolver's host and all
    /// declared endpoints must already be served.
    ///
    /// # Errors
    ///
    /// Returns a safe load error if the module is unavailable, the binding is
    /// invalid, or the configured source cannot preserve callback identity.
    pub async fn ensure_loaded_with_host_callbacks(
        &self,
        id: &str,
        binding: Arc<HostCallbackBinding>,
        module_config: serde_json::Value,
    ) -> Result<(), String> {
        self.ensure_loaded_inner(id, None, Some(CallbackLoad::Single(binding, module_config)))
            .await
            .map_err(LoadError::into_message)
    }

    /// Resolve a pinned release while preparing a fresh callback binding for
    /// each candidate artifact. Failed candidates consume and revoke only their
    /// own binding; the returned capability is retained by the exact admitted
    /// artifact. Linked, search-path and generic in-flight loads fail closed.
    pub async fn ensure_loaded_with_host_callback_factory<F, Fut>(
        &self,
        id: &str,
        factory: F,
    ) -> Result<Arc<HostCallbackBinding>, String>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(Arc<HostCallbackBinding>, serde_json::Value), String>>
            + Send
            + 'static,
    {
        let selected = Arc::new(Mutex::new(None));
        let factory: HostCallbackFactory = Arc::new(move |attempt| Box::pin(factory(attempt)));
        self.ensure_loaded_inner(
            id,
            None,
            Some(CallbackLoad::PerAttempt {
                factory,
                selected: Arc::clone(&selected),
            }),
        )
        .await
        .map_err(LoadError::into_message)?;

        if let Some(binding) = selected.lock().expect("callback selection lock").clone() {
            return Ok(binding);
        }
        let record = self
            .find(id)
            .ok_or_else(|| format!("unknown module '{id}'"))?;
        let runtime = self
            .runtime()
            .await
            .map_err(|_| format!("module '{id}' is unavailable: the module bus could not start"))?;
        runtime
            .host()
            .retained_host_callback_binding(record.bus_name)
            .ok_or_else(|| format!("module '{id}' has no retained callback binding"))
    }

    /// Resolve, initialize and wait for a module without sending a business
    /// message. Strict attested calls use this host-owned barrier before
    /// querying the broker for a recipient proof.
    pub async fn prepare_attested(&self, id: &str) -> Result<(), String> {
        self.ensure_loaded(id).await?;
        let record = self
            .find(id)
            .ok_or_else(|| "module is unavailable".to_string())?;
        let runtime = self
            .runtime()
            .await
            .map_err(|_| "module bus is unavailable".to_string())?;
        // Keep native initialization on the process-lifetime module runtime.
        // A caller's deadline may cancel its wait, but must not cancel the
        // initializer or strand the module between `INITIALIZING` and `READY`.
        let host = runtime.host();
        runtime
            .spawn(async move { host.ensure_ready(record.bus_name).await })
            .await
            .map_err(|_| "module readiness task did not finish".to_string())?
            .map_err(|_| "module did not reach READY".to_string())
    }

    /// [`Self::ensure_loaded`] with a bound on how long this caller waits.
    ///
    /// The bound is on the *wait*, never on the resolution: a caller that gives
    /// up leaves the download running and the slot intact, and a later call
    /// finds the outcome.
    ///
    /// # Errors
    ///
    /// [`LoadError::Failed`] when the module cannot be loaded in this process;
    /// [`LoadError::StillLoading`] when `within` passed first.
    pub async fn ensure_loaded_within(
        &self,
        id: &str,
        within: Option<Duration>,
    ) -> Result<(), LoadError> {
        self.ensure_loaded_inner(id, within, None).await
    }

    async fn ensure_loaded_inner(
        &self,
        id: &str,
        within: Option<Duration>,
        callbacks: Option<CallbackLoad>,
    ) -> Result<(), LoadError> {
        if !self.inner.enabled {
            if let Some(record) = self.find(id) {
                self.report(record, Reason::Disabled);
            }
            return Err(LoadError::Failed(format!(
                "module '{id}' is unavailable: modules are disabled in configuration"
            )));
        }
        let record = self
            .find(id)
            .ok_or_else(|| LoadError::Failed(format!("unknown module '{id}'")))?;
        if let Some(label) = self.inner.gate.as_ref().and_then(|gate| gate(id)) {
            tracing::debug!(
                module = id,
                label,
                "[tinybus::module] refused: switched off by host policy"
            );
            return Err(LoadError::Disabled(label));
        }

        if let Some((binding, _)) = callbacks.as_ref() {
            if !matches!(
                self.inner.sources.get(id),
                Some(ModuleSource::LocalFile { .. })
            ) {
                return Err(LoadError::Failed(format!(
                    "module '{id}' host callbacks require a verified local-file source"
                )));
            }
            let runtime = self.runtime().await.map_err(|_| {
                LoadError::Failed(format!(
                    "module '{id}' is unavailable: the module bus could not start"
                ))
            })?;
            if runtime
                .host()
                .list()
                .iter()
                .any(|info| info.manifest.bus_name.as_str() == record.bus_name)
            {
                if runtime
                    .host()
                    .retains_host_callback_binding(record.bus_name, binding)
                {
                    return Ok(());
                }
                return Err(LoadError::Failed(format!(
                    "module '{id}' is already loaded without this callback binding"
                )));
            }
            runtime
                .host()
                .validate_host_callback_binding(binding)
                .map_err(|error| {
                    LoadError::Failed(format!(
                        "module '{id}' callback binding is invalid: {error}"
                    ))
                })?;
        }

        let receiver = match resolution::global().claim(id) {
            Claim::Done(Resolution::Ready) => {
                if let Some((binding, _)) = callbacks.as_ref() {
                    let runtime = self.runtime().await.map_err(|_| {
                        LoadError::Failed(format!(
                            "module '{id}' is unavailable: the module bus could not start"
                        ))
                    })?;
                    if runtime
                        .host()
                        .retains_host_callback_binding(record.bus_name, binding)
                    {
                        return Ok(());
                    }
                    return Err(LoadError::Failed(format!(
                        "module '{id}' was loaded without the requested callback binding"
                    )));
                }
                return Ok(());
            }
            Claim::Done(Resolution::Failed(reason)) => {
                // The original resolution may have completed before a reporting
                // client was bound. Revisit reporting at the cached caller; the
                // reporter's key suppresses duplicates once delivered.
                self.report(record, Reason::ResolutionFailed);
                return Err(LoadError::Failed(reason));
            }
            Claim::Wait(receiver) => {
                if callbacks.is_some() {
                    return Err(LoadError::Failed(format!(
                        "module '{id}' is already resolving without this callback binding"
                    )));
                }
                receiver
            }
            Claim::Run { sender, receiver } => {
                // Runtime startup can itself be slow, and a task spawned on the
                // caller's executor would be cancelled if that executor shuts
                // down. Give this one startup attempt its own executor thread;
                // once the module runtime exists, start_resolution transfers
                // the actual load work to that process-owned runtime.
                let resolver = self.clone();
                let task_sender = sender.clone();
                let thread = std::thread::Builder::new()
                    .name("tinybus-resolution-startup".to_string())
                    .spawn(move || {
                        let startup_runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build();
                        match startup_runtime {
                            Ok(runtime) => {
                        runtime.block_on(resolver.start_resolution(
                            record,
                            task_sender,
                            callbacks,
                        ));
                            }
                            Err(error) => {
                                let reason = format!(
                                    "module '{}' is unavailable: the module startup executor could not start ({error})",
                                    record.id
                                );
                                resolver.report(record, Reason::ResolutionFailed);
                                resolution::global().complete(
                                    record.id,
                                    Resolution::Failed(reason),
                                    task_sender,
                                );
                            }
                        }
                    });
                if let Err(error) = thread {
                    let reason = format!(
                        "module '{}' is unavailable: the module startup executor could not start ({error})",
                        record.id
                    );
                    self.report(record, Reason::ResolutionFailed);
                    resolution::global().complete(record.id, Resolution::Failed(reason), sender);
                }
                receiver
            }
        };
        match resolution::global().wait(id, receiver, within).await {
            Waited::Ready => Ok(()),
            Waited::Failed(reason) => {
                self.report(record, Reason::ResolutionFailed);
                Err(LoadError::Failed(reason))
            }
            Waited::StillLoading => Err(LoadError::StillLoading),
        }
    }

    /// The state of `id` as the resolution table reports it, without touching it.
    #[must_use]
    pub fn state_of(&self, id: &str) -> Availability {
        match resolution::global().peek(id) {
            ResolutionState::Unresolved => Availability::Available,
            ResolutionState::Loading => Availability::Loading,
            ResolutionState::Ready => Availability::Ready,
            ResolutionState::Failed(_) => Availability::Failed,
        }
    }

    /// The reported state of `record` and, for a failed or unsupported module,
    /// why. Configuration is authoritative: a module that stays loaded after a
    /// request is still unusable to a caller whose configuration disables
    /// modules.
    #[must_use]
    pub fn status_of(&self, record: &ReleaseRecord) -> (Availability, Option<String>) {
        if !self.inner.enabled {
            return (
                Availability::Unsupported,
                Some("modules are disabled in configuration".to_string()),
            );
        }
        if let Some(label) = self.inner.gate.as_ref().and_then(|gate| gate(record.id)) {
            return (
                Availability::Disabled,
                Some(format!("switched off by host policy ({label})")),
            );
        }
        match resolution::global().peek(record.id) {
            ResolutionState::Ready => (Availability::Ready, None),
            ResolutionState::Loading => (Availability::Loading, None),
            ResolutionState::Failed(reason) => (Availability::Failed, Some(reason)),
            ResolutionState::Unresolved
                if record.supports_this_host() || self.has_local_source(record.id) =>
            {
                (Availability::Available, None)
            }
            ResolutionState::Unresolved => (
                Availability::Unsupported,
                Some("no artifact is published for this platform".to_string()),
            ),
        }
    }

    /// Whether `id` has a source other than the pinned release, which can serve
    /// it on a host the release publishes no artifact for.
    fn has_local_source(&self, id: &str) -> bool {
        matches!(
            self.inner.sources.get(id),
            Some(ModuleSource::LocalFile { .. } | ModuleSource::Linked)
        )
    }

    /// Run the resolution for `record` on its startup executor and record its
    /// outcome. Once the module runtime is available, the resolution work moves
    /// there so caller runtime teardown cannot cancel work other callers await.
    /// Without a module runtime there is nothing to load into, so the outcome is
    /// recorded inline instead.
    async fn start_resolution(
        &self,
        record: &'static ReleaseRecord,
        sender: tokio::sync::watch::Sender<Option<Resolution>>,
        callbacks: Option<(Arc<HostCallbackBinding>, serde_json::Value)>,
    ) {
        let this = self.clone();
        let id = record.id;
        let work = async move {
            tracing::info!(
                module = id,
                version = record.version,
                "[tinybus::module] resolving"
            );
            let resolution = match this.ladder(record, callbacks).await {
                Ok(()) => {
                    tracing::info!(module = id, "[tinybus::module] serving");
                    Resolution::Ready
                }
                Err(reason) => {
                    // Every failure is terminal for the process (the outcome is
                    // cached), whichever rung produced it.
                    let reason = this.mark_terminal(reason);
                    this.report(record, Reason::ResolutionFailed);
                    Resolution::Failed(reason)
                }
            };
            if let Some(hook) = &this.inner.on_resolved {
                // A hook that panics must not strand the callers waiting on
                // this resolution.
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(id))).is_err() {
                    tracing::warn!(module = id, "[tinybus::module] resolved hook panicked");
                }
            }
            resolution::global().complete(id, resolution, sender);
        };
        match self.runtime().await {
            Ok(runtime) => {
                runtime.spawn(work);
            }
            Err(error) => {
                tracing::warn!(error = %error, "[tinybus::module] the module bus could not start");
                work.await;
            }
        }
    }

    /// Get `record` serving: loaded, override, search path, release cache, prune.
    async fn ladder(
        &self,
        record: &'static ReleaseRecord,
        callbacks: Option<(Arc<HostCallbackBinding>, serde_json::Value)>,
    ) -> Result<(), String> {
        let runtime = self.runtime().await.map_err(|_| {
            format!(
                "module '{}' is unavailable: the module bus could not start",
                record.id
            )
        })?;

        // Already admitted under this exact registry identity: loaded from the
        // search path at boot, or by an earlier explicit local load.
        if runtime
            .host()
            .list()
            .iter()
            .any(|info| is_live_match(info, record))
        {
            if callbacks.is_some() {
                return Err(format!(
                    "module '{}' is already loaded without the requested callback binding",
                    record.id
                ));
            }
            return Ok(());
        }

        #[cfg(test)]
        if let Some(hook) = &self.after_initial_host_check {
            hook(runtime, record);
        }

        // An override points at a developer's own build, or at a module linked
        // into the host. It is checked before the pinned release so a module can
        // be iterated on against a live host.
        match self.inner.sources.get(record.id) {
            Some(ModuleSource::LocalFile { path }) => {
                let path = PathBuf::from(path);
                let result = if let Some((binding, module_config)) = callbacks.clone() {
                    let host = runtime.host();
                    let handle = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        handle.block_on(async move {
                            host.load_file_with_host_callbacks_for(
                                path,
                                module_config,
                                &binding,
                                record.bus_name,
                                record.object_path,
                            )
                            .await
                            .map_err(|error| {
                                format!("module '{}' callback load failed: {error}", record.id)
                            })?;
                            host.retain_host_callback_binding(record.bus_name, binding)
                                .map_err(|error| {
                                    format!(
                                        "module '{}' callback retention failed: {error}",
                                        record.id
                                    )
                                })
                        })
                    })
                    .await
                    .map_err(|error| {
                        format!(
                            "module '{}' callback loader did not finish: {error}",
                            record.id
                        )
                    })?
                } else {
                    let module_config = (self.inner.module_config)(record.id);
                    self.blocking(move || {
                        Self::load_local(runtime, &path, record.id, module_config)
                    })
                    .await
                };
                if callbacks.is_some() {
                    return result;
                }
                return recover_admission(result, &runtime.host().list(), record);
            }
            Some(ModuleSource::Linked) => {
                if callbacks.is_some() {
                    return Err(format!(
                        "module '{}' cannot use host callbacks from a linked source",
                        record.id
                    ));
                }
                let factory = self.inner.linked.get(record.id);
                let module_config = (self.inner.module_config)(record.id);
                let result = self
                    .blocking(move || Self::load_linked(runtime, record, factory, module_config))
                    .await;
                return recover_admission(result, &runtime.host().list(), record);
            }
            Some(ModuleSource::Pinned) | None => {}
        }

        if callbacks.is_some() {
            return Err(format!(
                "module '{}' host callbacks require a verified local-file source",
                record.id
            ));
        }

        // The search path tinybus itself honours. A refused search-path artifact
        // is ordinary (most directories hold nothing), so errors are dropped and
        // only an enabled, nonterminal match on the registry identity counts.
        let requested_record = record;
        let skip = self.inner.search_skip.clone();
        let found_on_search_path = tokio::task::spawn_blocking(move || {
            runtime
                .host()
                .load_search_paths_except(&skip)
                .into_iter()
                .flatten()
                .any(|info| is_live_match(&info, requested_record))
        })
        .await
        .unwrap_or(false);
        if found_on_search_path {
            return Ok(());
        }
        // A direct host load can win admission while the search-path rung is
        // running. Observe it before beginning a download or cache load.
        if runtime
            .host()
            .list()
            .iter()
            .any(|info| is_live_match(info, record))
        {
            return Ok(());
        }

        // The release cache: a verified artifact from an earlier launch, or a
        // download into the same place. Off the runtime worker, because the cold
        // path fetches, hashes, extracts and `dlopen`s synchronously.
        let Some(root) = self.inner.install_dir.clone() else {
            let error = Err(format!(
                "module '{}' is unavailable: no directory is available to install modules into",
                record.id
            ));
            return recover_admission(error, &runtime.host().list(), record);
        };
        let allow_download = self.inner.allow_download;
        let module_config = (self.inner.module_config)(record.id);
        let cache_root = root.clone();
        let bundled = self.inner.bundled_dir.clone();
        let result = self
            .blocking(move || {
                Self::load_cached(
                    runtime,
                    record,
                    &cache_root,
                    &module_config,
                    allow_download,
                    bundled.as_deref(),
                )
            })
            .await;
        if let Err(error) = result {
            return recover_admission(Err(error), &runtime.host().list(), record);
        }
        prune_stale_versions(&root, record.id, record.version);
        Ok(())
    }

    /// Run a blocking module operation on the blocking pool. A panic in the
    /// loader is reported rather than propagated, and every error is marked
    /// terminal.
    async fn blocking<F>(&self, work: F) -> Result<(), String>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        self.runtime()
            .await
            .map_err(|error| {
                self.mark_terminal(format!("the module bus could not start: {error}"))
            })?
            .blocking(work)
            .await
            .map_err(|error| self.mark_terminal(error))
    }

    /// Append the terminal-fault sentence to `error` unless it already has it.
    #[must_use]
    pub fn mark_terminal(&self, error: String) -> String {
        let marker = self.inner.terminal_marker.as_str();
        if marker.is_empty() || error.contains(marker) {
            return error;
        }
        format!("{error}. {marker}; restart the app to try again")
    }

    /// Load the pinned release for this host through the release cache.
    ///
    /// Tries the preferred artifact first and falls through on admission failure:
    /// a host newer than the newest published build runs that build, and one
    /// whose toolchain the newest artifact does not match falls back.
    ///
    /// # Errors
    ///
    /// The sanitised message from [`load_first_admitted`].
    pub fn load_cached(
        runtime: &ModuleRuntime,
        record: &ReleaseRecord,
        install_root: &Path,
        module_config: &serde_json::Value,
        allow_download: bool,
        bundled_root: Option<&Path>,
    ) -> Result<(), String> {
        let assets: Vec<ReleaseAsset<'static>> = host_candidates()
            .iter()
            .filter_map(|key| record.asset_for(key))
            .copied()
            .collect();
        let plan = ReleasePlan {
            id: record.id,
            version: record.version,
            release_url: record.release_url,
            assets: &assets,
            install_root,
            bundled_root,
            allow_download,
        };
        load_first_admitted(runtime.host(), &plan, module_config).map(|_| ())
    }

    /// Attach the module linked into this executable for `id`.
    ///
    /// Runs the same descriptor, manifest, dependency and init checks as a
    /// released module, with the same private configuration. Errors carry static
    /// text only.
    ///
    /// # Errors
    ///
    /// A terminal message when no factory is registered, the factory fails, or
    /// admission refuses the module.
    pub fn load_linked(
        runtime: &ModuleRuntime,
        record: &ReleaseRecord,
        factory: Option<LinkedFactory>,
        module_config: serde_json::Value,
    ) -> Result<(), String> {
        let id = record.id;
        let Some(factory) = factory else {
            return Err(format!(
                "module '{id}' is unavailable: no linked factory is registered in this binary"
            ));
        };
        let module = factory().map_err(|_| {
            // The factory's error can carry a path or a credential; only the
            // module id and a fixed reason are logged.
            tracing::warn!(module = id, "[tinybus::module] linked factory failed");
            format!("module '{id}' is unavailable: its linked factory could not be started")
        })?;
        if module.manifest.bus_name.as_str() != record.bus_name
            || module.manifest.object_path.as_str() != record.object_path
        {
            return Err(format!(
                "module '{id}' is unavailable: its linked manifest does not match the registry record"
            ));
        }
        match runtime
            .host()
            .attach_linked_with_config(module, module_config)
        {
            Ok(_) => {
                tracing::info!(
                    module = id,
                    "[tinybus::module] loaded from the linked executable"
                );
                Ok(())
            }
            Err(error) => Err(format!(
                "module '{id}' could not be attached from the linked executable: {error}"
            )),
        }
    }

    /// Load a platform library from `path`.
    ///
    /// # Errors
    ///
    /// A terminal message; the library stays mapped whatever the outcome.
    pub fn load_local(
        runtime: &ModuleRuntime,
        path: &Path,
        id: &str,
        module_config: serde_json::Value,
    ) -> Result<(), String> {
        match runtime.host().load_file_with_config(path, module_config) {
            Ok(_) => {
                tracing::info!(
                    module = id,
                    "[tinybus::module] loaded from a local artifact"
                );
                Ok(())
            }
            Err(err) => Err(format!(
                "module '{id}' could not be loaded from its local artifact: {err}. This is \
                 terminal for the running process; restart the app to try again"
            )),
        }
    }
}

#[cfg(test)]
#[path = "resolve_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "resolve_test_support.rs"]
mod test_support;

#[cfg(test)]
#[path = "resolve_attested_tests.rs"]
mod attested_tests;
