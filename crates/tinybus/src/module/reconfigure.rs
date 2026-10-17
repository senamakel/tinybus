//! Serialize configuration changes and proxy admission for a loaded module.
//!
//! A [`ReconfigurableModule`] serializes configuration transitions. Callers
//! that need the selected configuration to remain active for a whole bus call
//! hold [`ReconfigureGuard`] and obtain the proxy through the guard. A plain
//! [`ReconfigurableModule::proxy`] only serializes selection and reconfiguration;
//! it cannot protect later work performed through the returned proxy.

use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, MutexGuard};

use super::client::{ModuleAccess, ModuleCallError, ProxyFailure, ReconfigureFailure};
use crate::Proxy;

/// A hash of serialized module configuration, with no configuration retained.
#[derive(Clone, Eq, PartialEq)]
pub struct ConfigFingerprint([u8; 32]);

impl ConfigFingerprint {
    /// Serialize and hash `config`; the serialized bytes are discarded before
    /// this function returns.
    ///
    /// # Errors
    /// Returns an error if `config` cannot be serialized as JSON.
    pub fn of(config: &impl Serialize) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(config)?;
        Ok(Self(Sha256::digest(bytes).into()))
    }
}

/// How a configuration change is delivered to the module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reconfigure {
    /// Use TinyBus' module lifecycle control operation.
    Lifecycle,
    /// Call the named module member with the configuration as its sole argument.
    Member(&'static str),
}

/// Coordinates configuration-sensitive access to one registered module.
pub struct ReconfigurableModule {
    access: Box<dyn ModuleAccess>,
    module_id: &'static str,
    strategy: Reconfigure,
    retry_attempts: usize,
    retry_delay: Duration,
    mark_faulted: bool,
    transition: Mutex<()>,
    loaded: std::sync::Mutex<Option<ConfigFingerprint>>,
}

impl ReconfigurableModule {
    /// Create a coordinator for the registered module identified by `module_id`.
    #[must_use]
    pub fn new(access: impl ModuleAccess + 'static, module_id: &'static str) -> Self {
        Self {
            access: Box::new(access),
            module_id,
            strategy: Reconfigure::Lifecycle,
            retry_attempts: 1,
            retry_delay: Duration::ZERO,
            mark_faulted: false,
            transition: Mutex::new(()),
            loaded: std::sync::Mutex::new(None),
        }
    }

    /// Select how a changed configuration is applied.
    #[must_use]
    pub fn with_strategy(mut self, strategy: Reconfigure) -> Self {
        self.strategy = strategy;
        self
    }

    /// Retry only typed `Initializing` responses, with `attempts` total tries.
    /// A value of zero is treated as one attempt.
    #[must_use]
    pub fn retry_while_initializing(mut self, attempts: usize, delay: Duration) -> Self {
        self.retry_attempts = attempts.max(1);
        self.retry_delay = delay;
        self
    }

    /// Mark the resolver's module slot terminal when reconfiguration reports
    /// the module unavailable.
    #[must_use]
    pub fn mark_faulted_on_unavailable(mut self, enabled: bool) -> Self {
        self.mark_faulted = enabled;
        self
    }

    /// Record the configuration supplied to the module's successful load.
    ///
    /// # Errors
    /// Returns an error if the configuration cannot be serialized.
    pub fn record_loaded_with(&self, config: &impl Serialize) -> Result<(), serde_json::Error> {
        let fingerprint = ConfigFingerprint::of(config)?;
        *self.loaded_state() = Some(fingerprint);
        Ok(())
    }

    /// Forget which configuration the module currently holds.
    pub fn invalidate(&self) {
        *self.loaded_state() = None;
    }

    /// Obtain a proxy after reconciling the module's configuration.
    ///
    /// Concurrent calls serialize their transitions. The returned proxy is no
    /// longer protected after this method returns; use [`Self::exclusive`] and
    /// [`ReconfigureGuard::proxy`] when a subsequent call must share the same
    /// configuration without another caller changing it in between.
    ///
    /// # Errors
    /// Returns a sanitized module outcome or a JSON serialization error.
    pub async fn proxy(&self, config: &impl Serialize) -> Result<Proxy, ModuleCallError> {
        let _guard = self.transition.lock().await;
        self.proxy_locked(config).await
    }

    /// Hold configuration admission exclusively through a caller's bus call.
    ///
    /// All callers using this module's guard must use `guard.proxy(config)`;
    /// calling [`Self::proxy`] while holding this guard would wait on the same
    /// lock.
    pub async fn exclusive(&self) -> ReconfigureGuard<'_> {
        ReconfigureGuard {
            module: self,
            _guard: self.transition.lock().await,
        }
    }

    async fn proxy_locked(&self, config: &impl Serialize) -> Result<Proxy, ModuleCallError> {
        let Some(record) = self.access.find(self.module_id) else {
            return Err(ModuleCallError::Unavailable);
        };
        if !self.access.enabled() {
            return Err(ModuleCallError::Unavailable);
        }
        self.access
            .ensure_loaded(record.id)
            .await
            .map_err(|_| ModuleCallError::Unavailable)?;

        let fingerprint =
            ConfigFingerprint::of(config).map_err(|_| ModuleCallError::ModuleFault)?;
        let current = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if current.as_ref() != Some(&fingerprint) {
            // Clear before the operation: any failure leaves the held config
            // unknown, so reverting to the prior config still triggers a retry.
            *self.loaded_state() = None;
            let value = serde_json::to_value(config).map_err(|_| ModuleCallError::ModuleFault)?;
            self.reconfigure(value).await?;
            *self.loaded_state() = Some(fingerprint);
        }

        self.access
            .proxy(record.bus_name, record.object_path)
            .await
            .map_err(|failure| match failure {
                ProxyFailure::Transport => ModuleCallError::TransportFailed,
                ProxyFailure::Contract => ModuleCallError::IncompatibleContract,
            })
    }

    async fn reconfigure(&self, config: serde_json::Value) -> Result<(), ModuleCallError> {
        let mut outcome = Err(ReconfigureFailure::Failed);
        for attempt in 0..self.retry_attempts {
            outcome = match self.strategy {
                Reconfigure::Lifecycle => {
                    self.access
                        .reinitialize(self.module_id, config.clone())
                        .await
                }
                Reconfigure::Member(member) => {
                    self.member_reconfigure(member, config.clone()).await
                }
            };
            match outcome {
                Ok(()) => return Ok(()),
                Err(ReconfigureFailure::Initializing) if attempt + 1 < self.retry_attempts => {
                    if !self.retry_delay.is_zero() {
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
                Err(_) => break,
            }
        }
        if matches!(
            outcome,
            Err(ReconfigureFailure::Unavailable | ReconfigureFailure::Initializing)
        ) && self.mark_faulted
        {
            self.access.mark_faulted(self.module_id);
        }
        Err(match outcome {
            Err(ReconfigureFailure::Unavailable | ReconfigureFailure::Initializing) => {
                ModuleCallError::Unavailable
            }
            Err(ReconfigureFailure::Unsupported | ReconfigureFailure::Failed) | Ok(()) => {
                ModuleCallError::ModuleFault
            }
        })
    }

    async fn member_reconfigure(
        &self,
        member: &'static str,
        config: serde_json::Value,
    ) -> Result<(), ReconfigureFailure> {
        let record = self
            .access
            .find(self.module_id)
            .ok_or(ReconfigureFailure::Unavailable)?;
        if !self.access.enabled() {
            return Err(ReconfigureFailure::Unavailable);
        }
        self.access
            .ensure_loaded(record.id)
            .await
            .map_err(|_| ReconfigureFailure::Unavailable)?;
        let proxy = self
            .access
            .proxy(record.bus_name, record.object_path)
            .await
            .map_err(|failure| match failure {
                ProxyFailure::Transport | ProxyFailure::Contract => ReconfigureFailure::Failed,
            })?;
        proxy
            .call_confidential::<serde_json::Value>(member, (config,))
            .await
            .map(|_| ())
            .map_err(|error| super::resolve::access::reconfigure_failure(&error))
    }

    fn loaded_state(&self) -> std::sync::MutexGuard<'_, Option<ConfigFingerprint>> {
        self.loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A configuration admission lock held across proxy use.
pub struct ReconfigureGuard<'a> {
    module: &'a ReconfigurableModule,
    _guard: MutexGuard<'a, ()>,
}

impl ReconfigureGuard<'_> {
    /// Reconcile the configuration while retaining exclusive access until this
    /// guard is dropped.
    pub async fn proxy(&mut self, config: &impl Serialize) -> Result<Proxy, ModuleCallError> {
        self.module.proxy_locked(config).await
    }
}

#[cfg(test)]
#[path = "reconfigure_tests.rs"]
mod tests;
