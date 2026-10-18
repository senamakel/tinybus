//! A module reservation and host callback whose cleanup waits for STOP acknowledgement.

use std::{
    error::Error as StdError,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    runtime::Handle,
    sync::{oneshot, watch},
};

use super::runtime::ModuleRuntime;

/// The lifecycle operation that failed. Values deliberately contain no resource identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePhase {
    /// The module did not provide a reservation.
    Reserve,
    /// The reservation failed host-side validation.
    Validate,
    /// The module object could not be constructed.
    Object,
    /// The host callback factory returned an error or panicked.
    Callback,
    /// The driver could not produce a callback address.
    Address,
    /// The callback could not be registered on the module runtime.
    Export,
    /// Starting the reserved module failed.
    Start,
    /// A stop attempt failed or was not acknowledged.
    Stop,
    /// The dedicated runtime could no longer accept cleanup work.
    Runtime,
}

/// Content-free outcome metadata sent to the caller-provided reporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseReportOutcome {
    /// A STOP attempt acknowledged shutdown.
    Acknowledged,
    /// A STOP attempt returned an error.
    Failed,
    /// A STOP attempt exceeded its configured timeout.
    TimedOut,
    /// The driver reported that the reservation had faulted.
    Faulted,
    /// The finite retry sequence ended without acknowledgement.
    Exhausted,
    /// The driver could not prove whether the operation settled.
    Unacknowledged,
}

/// Result of a driver STOP operation, with retry safety stated explicitly.
pub enum StopOutcome<E> {
    /// Native STOP acknowledged completion.
    Acknowledged,
    /// The operation is known to have settled without acknowledgement and can safely be retried.
    RetryableFailure(E),
    /// The operation may still be active or its completion is unknown; it must not be retried.
    Unacknowledged(E),
}

/// Content-free lifecycle metadata; it never includes a reservation, object, or driver error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseReport {
    /// The lifecycle phase being reported.
    pub phase: LeasePhase,
    /// One-based STOP attempt number, when applicable.
    pub attempt: Option<usize>,
    /// The outcome of this event.
    pub outcome: LeaseReportOutcome,
}

/// Bounded STOP retry timing.
///
/// The first attempt begins immediately. Each delay starts one additional attempt, so the total
/// number of attempts is exactly `retry_delays.len() + 1`.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    per_attempt_timeout: Duration,
    retry_delays: Vec<Duration>,
}

impl RetryPolicy {
    /// Create a policy with a timeout for every STOP call and a finite backoff sequence.
    ///
    /// # Errors
    /// Returns an error when a STOP attempt has no positive timeout.
    pub fn new(
        per_attempt_timeout: Duration,
        retry_delays: Vec<Duration>,
    ) -> Result<Self, RetryPolicyError> {
        if per_attempt_timeout.is_zero() {
            return Err(RetryPolicyError);
        }
        Ok(Self {
            per_attempt_timeout,
            retry_delays,
        })
    }

    /// Maximum duration of one STOP call.
    #[must_use]
    pub fn per_attempt_timeout(&self) -> Duration {
        self.per_attempt_timeout
    }

    /// The finite delays between STOP attempts.
    #[must_use]
    pub fn retry_delays(&self) -> &[Duration] {
        &self.retry_delays
    }
}

/// A retry policy was missing a positive per-attempt timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicyError;

impl std::fmt::Display for RetryPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("STOP attempt timeout must be positive")
    }
}

impl StdError for RetryPolicyError {}

/// A redacted lifecycle error retaining the original typed driver error for explicit inspection.
pub struct LeaseError<E> {
    phase: LeasePhase,
    attempt: Option<usize>,
    acknowledged: bool,
    driver_error: Option<Arc<E>>,
}

impl<E> LeaseError<E> {
    fn new(
        phase: LeasePhase,
        attempt: Option<usize>,
        acknowledged: bool,
        driver_error: Option<E>,
    ) -> Self {
        Self {
            phase,
            attempt,
            acknowledged,
            driver_error: driver_error.map(Arc::new),
        }
    }

    /// Lifecycle stage that failed.
    #[must_use]
    pub fn phase(&self) -> LeasePhase {
        self.phase
    }

    /// STOP attempt number associated with the error, if one occurred.
    #[must_use]
    pub fn attempt(&self) -> Option<usize> {
        self.attempt
    }

    /// Whether the native STOP operation was acknowledged.
    #[must_use]
    pub fn acknowledged(&self) -> bool {
        self.acknowledged
    }

    /// The original typed driver error, when a driver operation returned one.
    #[must_use]
    pub fn driver_error(&self) -> Option<&E> {
        self.driver_error.as_deref()
    }
}

impl<E> std::fmt::Debug for LeaseError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseError")
            .field("phase", &self.phase)
            .field("attempt", &self.attempt)
            .field("acknowledged", &self.acknowledged)
            .field(
                "driver_error",
                &self.driver_error.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl<E> std::fmt::Display for LeaseError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.attempt {
            Some(attempt) => write!(
                formatter,
                "module lease {:?} phase failed (attempt {attempt}, acknowledged={})",
                self.phase, self.acknowledged
            ),
            None => write!(
                formatter,
                "module lease {:?} phase failed (acknowledged={})",
                self.phase, self.acknowledged
            ),
        }
    }
}

impl<E: Send + Sync + 'static> StdError for LeaseError<E> {}

/// Host-side driver for one module's reservation protocol.
///
/// The implementation must bind every lifecycle operation to the peer that created the
/// reservation; a well-known-name replacement must never receive an old reservation handle.
#[async_trait::async_trait]
pub trait RuntimeLeaseDriver: Send + Sync + 'static {
    /// Opaque module reservation token.
    type Reservation: Send + Sync + 'static;
    /// Object or handle retained by the public lease.
    type Object: Send + Sync + 'static;
    /// Typed driver error, available through [`LeaseError::driver_error`].
    type Error: StdError + Send + Sync + 'static;

    /// Reserve module-side resources.
    async fn reserve(&self) -> Result<Self::Reservation, Self::Error>;
    /// Validate the reservation before any object construction or callback export.
    fn validate(&self, reservation: &Self::Reservation) -> Result<(), Self::Error>;
    /// Return the exact callback name and path for this reservation and object.
    fn callback_address(
        &self,
        reservation: &Self::Reservation,
        object: &Self::Object,
    ) -> Result<(crate::BusName, crate::ObjectPath), Self::Error>;
    /// Construct the host-side object or handle after validation.
    async fn object(&self, reservation: &Self::Reservation) -> Result<Self::Object, Self::Error>;
    /// Start work associated with this reservation.
    async fn start(&self, reservation: &Self::Reservation) -> Result<(), Self::Error>;
    /// Stop work associated with this reservation.
    ///
    /// `RetryableFailure` is valid only when the driver has authoritative evidence that this
    /// operation settled and a later STOP cannot overlap it. Timeouts, lost replies, malformed
    /// responses, and faulted peers must be `Unacknowledged` unless separate evidence proves
    /// settlement. `Acknowledged` is the only outcome that permits callback withdrawal.
    async fn stop(&self, reservation: &Self::Reservation) -> StopOutcome<Self::Error>;
    /// Whether this reservation has reached a terminal fault that forbids another STOP attempt.
    fn failed(&self, reservation: &Self::Reservation) -> bool;
}

type WorkerFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type StartWorker<E> =
    Box<dyn FnOnce(watch::Sender<CloseState<E>>) -> WorkerFuture + Send + 'static>;

enum CloseState<E> {
    Open,
    Running,
    Acknowledged,
    Unacknowledged(Arc<LeaseError<E>>),
}

impl<E> Clone for CloseState<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Open => Self::Open,
            Self::Running => Self::Running,
            Self::Acknowledged => Self::Acknowledged,
            Self::Unacknowledged(error) => Self::Unacknowledged(error.clone()),
        }
    }
}

struct LeaseControl<E> {
    runtime: Handle,
    users: AtomicUsize,
    state: watch::Sender<CloseState<E>>,
    starter: Mutex<Option<StartWorker<E>>>,
}

impl<E: Send + Sync + 'static> LeaseControl<E> {
    fn new(runtime: Handle, starter: StartWorker<E>) -> Self {
        let (state, _) = watch::channel(CloseState::Open);
        Self {
            runtime,
            users: AtomicUsize::new(1),
            state,
            starter: Mutex::new(Some(starter)),
        }
    }

    fn start(&self) {
        let starter = self
            .starter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(starter) = starter {
            self.state.send_replace(CloseState::Running);
            let sender = self.state.clone();
            let worker = starter(sender);
            self.runtime.spawn(worker);
        }
    }

    async fn wait(&self) -> Result<(), Arc<LeaseError<E>>> {
        let mut state = self.state.subscribe();
        self.start();
        loop {
            match state.borrow().clone() {
                CloseState::Acknowledged => return Ok(()),
                CloseState::Unacknowledged(error) => return Err(error),
                CloseState::Open | CloseState::Running => {}
            }
            if state.changed().await.is_err() {
                return Err(Arc::new(LeaseError::new(
                    LeasePhase::Runtime,
                    None,
                    false,
                    None,
                )));
            }
        }
    }
}

/// A cloneable owner of a started module reservation and its callback object.
pub struct RuntimeLease<T, E: Send + Sync + 'static> {
    object: Arc<T>,
    control: Arc<LeaseControl<E>>,
}

impl<T, E: Send + Sync + 'static> RuntimeLease<T, E> {
    /// The object or host handle created for this reservation.
    #[must_use]
    pub fn object(&self) -> &T {
        &self.object
    }

    /// Wait for the one shared cleanup worker. Cancelling this future never cancels that worker.
    pub async fn close(&self) -> Result<(), Arc<LeaseError<E>>>
    where
        E: Send + Sync + 'static,
    {
        self.control.wait().await
    }
}

impl<T, E: Send + Sync + 'static> Clone for RuntimeLease<T, E> {
    fn clone(&self) -> Self {
        self.control.users.fetch_add(1, Ordering::Relaxed);
        Self {
            object: self.object.clone(),
            control: self.control.clone(),
        }
    }
}

impl<T, E: Send + Sync + 'static> Drop for RuntimeLease<T, E> {
    fn drop(&mut self) {
        if self.control.users.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.control.start();
        }
    }
}

impl ModuleRuntime {
    /// Reserve, validate, export a callback, and start a module on this runtime's owned task.
    ///
    /// This is a normal function: it submits the owned activation transaction before returning
    /// the future. Cancelling the returned future only abandons its result; activation and any
    /// required rollback continue on the dedicated runtime.
    pub fn lease<D, F, I, R>(
        &self,
        driver: D,
        make_callback: F,
        policy: RetryPolicy,
        reporter: R,
    ) -> impl Future<Output = Result<RuntimeLease<D::Object, D::Error>, LeaseError<D::Error>>>
    + Send
    + 'static
    where
        D: RuntimeLeaseDriver,
        F: FnOnce(&D::Reservation, &D::Object) -> Result<I, D::Error> + Send + 'static,
        I: crate::Interface,
        R: Fn(LeaseReport) + Send + Sync + 'static,
    {
        let runtime = self.dedicated_handle();
        let host_objects = self.lease_host_object_registry();
        let (response, result) = oneshot::channel();
        self.spawn(async move {
            let activated = activate(
                runtime,
                host_objects,
                driver,
                make_callback,
                policy,
                reporter,
            )
            .await;
            let _ = response.send(activated);
        });
        async move {
            result
                .await
                .unwrap_or_else(|_| Err(LeaseError::new(LeasePhase::Runtime, None, false, None)))
        }
    }
}

async fn activate<D, F, I, R>(
    runtime: Handle,
    host_objects: Arc<tokio::sync::Mutex<super::runtime::HostObjectRegistry>>,
    driver: D,
    make_callback: F,
    policy: RetryPolicy,
    reporter: R,
) -> Result<RuntimeLease<D::Object, D::Error>, LeaseError<D::Error>>
where
    D: RuntimeLeaseDriver,
    F: FnOnce(&D::Reservation, &D::Object) -> Result<I, D::Error> + Send + 'static,
    I: crate::Interface,
    R: Fn(LeaseReport) + Send + Sync + 'static,
{
    let driver = Arc::new(driver);
    let reservation = match driver.reserve().await {
        Ok(reservation) => Arc::new(reservation),
        Err(error) => {
            return Err(LeaseError::new(
                LeasePhase::Reserve,
                None,
                false,
                Some(error),
            ));
        }
    };

    // This cleanup owner exists before validation and before any later await. The returned lease
    // and every rollback path share this one worker starter and exact callback guard slot.
    let callback_guard = Arc::new(Mutex::new(None));
    let cleanup_driver = driver.clone();
    let cleanup_reservation = reservation.clone();
    let cleanup_policy = policy.clone();
    let cleanup_reporter = Arc::new(reporter);
    let cleanup_guard = callback_guard.clone();
    let starter: StartWorker<D::Error> = Box::new(move |state| {
        Box::pin(cleanup_worker(
            state,
            cleanup_driver,
            cleanup_reservation,
            cleanup_guard,
            cleanup_policy,
            cleanup_reporter,
        ))
    });
    let control = Arc::new(LeaseControl::new(runtime.clone(), starter));

    if let Err(error) = driver.validate(&reservation) {
        let _ = control.wait().await;
        return Err(LeaseError::new(
            LeasePhase::Validate,
            None,
            false,
            Some(error),
        ));
    }

    let object = match driver.object(&reservation).await {
        Ok(object) => Arc::new(object),
        Err(error) => {
            let _ = control.wait().await;
            return Err(LeaseError::new(
                LeasePhase::Object,
                None,
                false,
                Some(error),
            ));
        }
    };

    let callback = match catch_unwind(AssertUnwindSafe(|| make_callback(&reservation, &object))) {
        Ok(Ok(callback)) => callback,
        Ok(Err(error)) => {
            let _ = control.wait().await;
            return Err(LeaseError::new(
                LeasePhase::Callback,
                None,
                false,
                Some(error),
            ));
        }
        Err(_) => {
            let _ = control.wait().await;
            return Err(LeaseError::new(LeasePhase::Callback, None, false, None));
        }
    };

    let (name, path) = match driver.callback_address(&reservation, &object) {
        Ok(address) => address,
        Err(error) => {
            let _ = control.wait().await;
            return Err(LeaseError::new(
                LeasePhase::Address,
                None,
                false,
                Some(error),
            ));
        }
    };
    let host_object = super::runtime::serve_host_object_transaction(
        host_objects,
        runtime.clone(),
        name,
        path,
        callback,
    )
    .await;
    match host_object {
        Ok(guard) => {
            *callback_guard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(guard)
        }
        Err(_) => {
            let _ = control.wait().await;
            return Err(LeaseError::new(LeasePhase::Export, None, false, None));
        }
    }

    if let Err(error) = driver.start(&reservation).await {
        let _ = control.wait().await;
        return Err(LeaseError::new(LeasePhase::Start, None, false, Some(error)));
    }

    Ok(RuntimeLease { object, control })
}

async fn cleanup_worker<D, E>(
    state: watch::Sender<CloseState<E>>,
    driver: Arc<D>,
    reservation: Arc<D::Reservation>,
    callback_guard: Arc<Mutex<Option<super::runtime::HostObjectGuard>>>,
    policy: RetryPolicy,
    reporter: Arc<dyn Fn(LeaseReport) + Send + Sync>,
) where
    D: RuntimeLeaseDriver<Error = E>,
    E: StdError + Send + Sync + 'static,
{
    for attempt in 1..=policy.retry_delays.len() + 1 {
        if driver.failed(&reservation) {
            report(
                &reporter,
                LeaseReport {
                    phase: LeasePhase::Stop,
                    attempt: Some(attempt),
                    outcome: LeaseReportOutcome::Faulted,
                },
            );
            let error = Arc::new(LeaseError::new(
                LeasePhase::Stop,
                Some(attempt),
                false,
                None,
            ));
            state.send_replace(CloseState::Unacknowledged(error));
            std::future::pending::<()>().await;
        }
        let mut stop = Box::pin(driver.stop(&reservation));
        let result = match tokio::time::timeout(policy.per_attempt_timeout, &mut stop).await {
            Ok(result) => result,
            Err(_) => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::TimedOut,
                    },
                );
                state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
                    LeasePhase::Stop,
                    Some(attempt),
                    false,
                    None,
                ))));
                // A local deadline cannot cancel remote work. Keep the original operation alive;
                // only its eventual result can acknowledge cleanup or permit a later retry.
                match stop.await {
                    StopOutcome::Acknowledged => {
                        report(
                            &reporter,
                            LeaseReport {
                                phase: LeasePhase::Stop,
                                attempt: Some(attempt),
                                outcome: LeaseReportOutcome::Acknowledged,
                            },
                        );
                        let guard = callback_guard
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some(guard) = guard {
                            guard.remove().await;
                        }
                        state.send_replace(CloseState::Acknowledged);
                        return;
                    }
                    StopOutcome::RetryableFailure(error) => {
                        state.send_replace(CloseState::Running);
                        StopOutcome::RetryableFailure(error)
                    }
                    StopOutcome::Unacknowledged(error) => {
                        report(
                            &reporter,
                            LeaseReport {
                                phase: LeasePhase::Stop,
                                attempt: Some(attempt),
                                outcome: LeaseReportOutcome::Unacknowledged,
                            },
                        );
                        state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
                            LeasePhase::Stop,
                            Some(attempt),
                            false,
                            Some(error),
                        ))));
                        std::future::pending::<()>().await;
                        unreachable!("unacknowledged cleanup remains parked")
                    }
                }
            }
        };
        match result {
            StopOutcome::Acknowledged => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::Acknowledged,
                    },
                );
                let guard = callback_guard
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(guard) = guard {
                    guard.remove().await;
                }
                state.send_replace(CloseState::Acknowledged);
                return;
            }
            StopOutcome::RetryableFailure(error) => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::Failed,
                    },
                );
                if attempt == policy.retry_delays.len() + 1 {
                    state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
                        LeasePhase::Stop,
                        Some(attempt),
                        false,
                        Some(error),
                    ))));
                    std::future::pending::<()>().await;
                }
            }
            StopOutcome::Unacknowledged(error) => {
                report(
                    &reporter,
                    LeaseReport {
                        phase: LeasePhase::Stop,
                        attempt: Some(attempt),
                        outcome: LeaseReportOutcome::Unacknowledged,
                    },
                );
                state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
                    LeasePhase::Stop,
                    Some(attempt),
                    false,
                    Some(error),
                ))));
                std::future::pending::<()>().await;
            }
        }
        if driver.failed(&reservation) {
            report(
                &reporter,
                LeaseReport {
                    phase: LeasePhase::Stop,
                    attempt: Some(attempt),
                    outcome: LeaseReportOutcome::Faulted,
                },
            );
            state.send_replace(CloseState::Unacknowledged(Arc::new(LeaseError::new(
                LeasePhase::Stop,
                Some(attempt),
                false,
                None,
            ))));
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(policy.retry_delays[attempt - 1]).await;
    }
    unreachable!("finite retry sequence always terminates in the loop")
}

fn report(reporter: &Arc<dyn Fn(LeaseReport) + Send + Sync>, event: LeaseReport) {
    // Host reporting is observational and cannot interrupt the cleanup owner.
    let _ = catch_unwind(AssertUnwindSafe(|| reporter(event)));
}

#[cfg(test)]
#[path = "lease_tests.rs"]
mod tests;
