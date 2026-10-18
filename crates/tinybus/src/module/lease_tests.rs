use super::*;

// These tests are a source draft against the approved RuntimeLease API shape.
// They are intentionally not compiled while U5 is unpublished and the API
// implementation has not been authorized.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::{mpsc, oneshot};

const CALLBACK_NAME: &str = "ai.tinyhumans.tinybus.tests.RuntimeLease";

#[derive(Clone, Debug)]
struct Identity(Arc<()>, usize);

#[derive(Debug)]
struct Reservation(Identity);
#[derive(Debug)]
struct Object(Identity);

#[derive(Debug)]
struct DriverError(&'static str);

impl std::fmt::Display for DriverError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for DriverError {}

enum DriverCall {
    Reserve(oneshot::Sender<Result<Reservation, DriverError>>),
    Object(Identity, oneshot::Sender<Result<Object, DriverError>>),
    Start(oneshot::Sender<Result<(), DriverError>>),
    Stop(
        std::time::Instant,
        oneshot::Sender<StopOutcome<DriverError>>,
    ),
    Validated,
}

#[derive(Clone)]
struct ControlledDriver {
    calls: mpsc::UnboundedSender<DriverCall>,
    failed: Arc<AtomicBool>,
    validation_error: Arc<AtomicBool>,
    start_error: Arc<AtomicBool>,
    validation_panic: Arc<AtomicBool>,
    address_panic: Arc<AtomicBool>,
    start_panic: Arc<AtomicBool>,
    stop_panic: Arc<AtomicBool>,
    failed_panic: Arc<AtomicBool>,
    identity: Identity,
}

#[async_trait::async_trait]
impl RuntimeLeaseDriver for ControlledDriver {
    type Reservation = Reservation;
    type Object = Object;
    type Error = DriverError;

    async fn reserve(&self) -> Result<Self::Reservation, Self::Error> {
        let (reply, result) = oneshot::channel();
        self.calls.send(DriverCall::Reserve(reply)).unwrap();
        result.await.unwrap()
    }

    fn validate(&self, _: &Self::Reservation) -> Result<(), Self::Error> {
        self.calls.send(DriverCall::Validated).unwrap();
        assert!(
            !self.validation_panic.load(Ordering::Acquire),
            "private validation panic payload"
        );
        if self.validation_error.load(Ordering::Acquire) {
            Err(DriverError("private validation detail"))
        } else {
            Ok(())
        }
    }

    fn callback_address(
        &self,
        reservation: &Self::Reservation,
        _: &Self::Object,
    ) -> Result<(crate::BusName, crate::ObjectPath), Self::Error> {
        assert!(
            !self.address_panic.load(Ordering::Acquire),
            "private address panic payload"
        );
        assert!(Arc::ptr_eq(&self.identity.0, &reservation.0.0));
        Ok((
            CALLBACK_NAME.parse().unwrap(),
            format!(
                "/ai/tinyhumans/tinybus/tests/RuntimeLease{}",
                reservation.0.1
            )
            .parse()
            .unwrap(),
        ))
    }

    async fn object(&self, reservation: &Self::Reservation) -> Result<Self::Object, Self::Error> {
        let (reply, result) = oneshot::channel();
        self.calls
            .send(DriverCall::Object(reservation.0.clone(), reply))
            .unwrap();
        result.await.unwrap()
    }

    async fn start(&self, _: &Self::Reservation) -> Result<(), Self::Error> {
        let (reply, result) = oneshot::channel();
        self.calls.send(DriverCall::Start(reply)).unwrap();
        result.await.unwrap()?;
        assert!(
            !self.start_panic.load(Ordering::Acquire),
            "private start panic payload"
        );
        if self.start_error.load(Ordering::Acquire) {
            Err(DriverError("private start detail"))
        } else {
            Ok(())
        }
    }

    async fn stop(&self, _: &Self::Reservation) -> StopOutcome<Self::Error> {
        let (reply, result) = oneshot::channel();
        self.calls
            .send(DriverCall::Stop(std::time::Instant::now(), reply))
            .unwrap();
        assert!(
            !self.stop_panic.load(Ordering::Acquire),
            "private stop panic payload"
        );
        result.await.unwrap()
    }

    fn failed(&self, _: &Self::Reservation) -> bool {
        assert!(
            !self.failed_panic.load(Ordering::Acquire),
            "private fault predicate panic payload"
        );
        self.failed.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct Callback(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl crate::Interface for Callback {
    fn name(&self) -> crate::InterfaceName {
        "ai.tinyhumans.tinybus.tests.RuntimeLease".parse().unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Touch".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!(self.0.fetch_add(1, Ordering::AcqRel) + 1))
    }
}

struct Harness {
    runtime: crate::module::ModuleRuntime,
    driver: ControlledDriver,
    calls: mpsc::UnboundedReceiver<DriverCall>,
    callback_calls: Arc<AtomicUsize>,
    reports: mpsc::UnboundedReceiver<LeaseReport>,
    report_sender: mpsc::UnboundedSender<LeaseReport>,
}

impl Harness {
    fn new() -> Self {
        let runtime =
            crate::module::ModuleRuntime::start(&crate::module::RuntimeNames::default(), None)
                .unwrap();
        let (calls, receiver) = mpsc::unbounded_channel();
        let (report_sender, reports) = mpsc::unbounded_channel();
        Self {
            runtime,
            driver: ControlledDriver {
                calls,
                failed: Arc::new(AtomicBool::new(false)),
                validation_error: Arc::new(AtomicBool::new(false)),
                start_error: Arc::new(AtomicBool::new(false)),
                validation_panic: Arc::new(AtomicBool::new(false)),
                address_panic: Arc::new(AtomicBool::new(false)),
                start_panic: Arc::new(AtomicBool::new(false)),
                stop_panic: Arc::new(AtomicBool::new(false)),
                failed_panic: Arc::new(AtomicBool::new(false)),
                identity: Identity(Arc::new(()), 1),
            },
            calls: receiver,
            callback_calls: Arc::new(AtomicUsize::new(0)),
            reports,
            report_sender,
        }
    }

    fn open(
        &self,
    ) -> impl std::future::Future<
        Output = Result<RuntimeLease<Object, DriverError>, LeaseError<DriverError>>,
    > + Send
    + 'static {
        let calls = self.callback_calls.clone();
        self.open_with(move |reservation, object| {
            if !Arc::ptr_eq(&reservation.0.0, &object.0.0) {
                return Err(DriverError("reservation/object mismatch"));
            }
            Ok(Callback(calls))
        })
    }

    fn open_with<F>(
        &self,
        make_callback: F,
    ) -> impl std::future::Future<
        Output = Result<RuntimeLease<Object, DriverError>, LeaseError<DriverError>>,
    > + Send
    + 'static
    where
        F: FnOnce(&Reservation, &Object) -> Result<Callback, DriverError> + Send + 'static,
    {
        let policy = RetryPolicy::new(
            std::time::Duration::from_millis(20),
            vec![std::time::Duration::ZERO, std::time::Duration::ZERO],
        )
        .unwrap();
        self.open_with_policy(make_callback, policy)
    }

    fn open_with_policy<F>(
        &self,
        make_callback: F,
        policy: RetryPolicy,
    ) -> impl std::future::Future<
        Output = Result<RuntimeLease<Object, DriverError>, LeaseError<DriverError>>,
    > + Send
    + 'static
    where
        F: FnOnce(&Reservation, &Object) -> Result<Callback, DriverError> + Send + 'static,
    {
        let reports = self.report_sender.clone();
        self.open_with_policy_and_reporter(make_callback, policy, move |report| {
            let _ = reports.send(report);
        })
    }

    fn open_with_policy_and_reporter<F, R>(
        &self,
        make_callback: F,
        policy: RetryPolicy,
        reporter: R,
    ) -> impl std::future::Future<
        Output = Result<RuntimeLease<Object, DriverError>, LeaseError<DriverError>>,
    > + Send
    + 'static
    where
        F: FnOnce(&Reservation, &Object) -> Result<Callback, DriverError> + Send + 'static,
        R: Fn(LeaseReport) + Send + Sync + 'static,
    {
        let driver = self.driver.clone();
        self.runtime.lease(driver, make_callback, policy, reporter)
    }

    async fn opened(&mut self) -> RuntimeLease<Object, DriverError> {
        self.opened_with_policy(
            RetryPolicy::new(
                std::time::Duration::from_millis(20),
                vec![std::time::Duration::ZERO, std::time::Duration::ZERO],
            )
            .unwrap(),
        )
        .await
    }

    async fn opened_with_policy(
        &mut self,
        policy: RetryPolicy,
    ) -> RuntimeLease<Object, DriverError> {
        let calls = self.callback_calls.clone();
        let opening = tokio::spawn(self.open_with_policy(
            move |reservation, object| {
                if !Arc::ptr_eq(&reservation.0.0, &object.0.0) {
                    return Err(DriverError("reservation/object mismatch"));
                }
                Ok(Callback(calls))
            },
            policy,
        ));
        match self.calls.recv().await.unwrap() {
            DriverCall::Reserve(reply) => {
                reply
                    .send(Ok(Reservation(self.driver.identity.clone())))
                    .unwrap();
            }
            _ => panic!("reserve must be the first driver operation"),
        }
        assert!(matches!(
            self.calls.recv().await,
            Some(DriverCall::Validated)
        ));
        match self.calls.recv().await.unwrap() {
            DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
            _ => panic!("object creation follows validation"),
        }
        match self.calls.recv().await.unwrap() {
            DriverCall::Start(reply) => reply.send(Ok(())).unwrap(),
            _ => panic!("start follows callback registration"),
        }
        opening.await.unwrap().unwrap()
    }

    async fn callback(&self) -> crate::Result<serde_json::Value> {
        self.callback_at(self.driver.identity.1).await
    }

    async fn callback_at(&self, id: usize) -> crate::Result<serde_json::Value> {
        self.runtime
            .proxy(
                CALLBACK_NAME,
                &format!("/ai/tinyhumans/tinybus/tests/RuntimeLease{id}"),
            )
            .unwrap()
            .call("Touch", ())
            .await
    }
}

#[tokio::test]
async fn acknowledged_close_removes_only_its_exact_callback_registration() {
    let mut harness = Harness::new();
    let first = harness.opened().await;
    let second_driver = ControlledDriver {
        identity: Identity(Arc::new(()), 2),
        ..harness.driver.clone()
    };
    let callback_calls = harness.callback_calls.clone();
    let reports = harness.report_sender.clone();
    let policy = RetryPolicy::new(
        std::time::Duration::from_millis(20),
        vec![std::time::Duration::ZERO],
    )
    .unwrap();
    let opening = tokio::spawn(harness.runtime.lease(
        second_driver.clone(),
        move |reservation: &Reservation, object: &Object| {
            if !Arc::ptr_eq(&reservation.0.0, &object.0.0) {
                return Err(DriverError("reservation/object mismatch"));
            }
            Ok(Callback(callback_calls))
        },
        policy,
        move |report| {
            let _ = reports.send(report);
        },
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(second_driver.identity.clone())))
            .unwrap(),
        _ => panic!("second reservation begins independently"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("second object follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Start(reply) => reply.send(Ok(())).unwrap(),
        _ => panic!("second callback is registered before start"),
    }
    let second = opening.await.unwrap().unwrap();
    assert_eq!(harness.callback_at(1).await.unwrap(), serde_json::json!(1));
    assert_eq!(harness.callback_at(2).await.unwrap(), serde_json::json!(2));

    let first_close = tokio::spawn(async move { first.close().await });
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("first close stops only its reservation"),
    }
    assert!(first_close.await.unwrap().is_ok());
    assert!(harness.callback_at(1).await.is_err());
    assert_eq!(harness.callback_at(2).await.unwrap(), serde_json::json!(3));

    let second_close = tokio::spawn(async move { second.close().await });
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("second close stops its own reservation"),
    }
    assert!(second_close.await.unwrap().is_ok());
    assert!(harness.callback_at(2).await.is_err());
}

#[tokio::test]
async fn acknowledged_close_preserves_a_newer_registration_at_its_callback_path() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let replacement_calls = Arc::new(AtomicUsize::new(0));
    let replacement = harness
        .runtime
        .serve_host_object(
            CALLBACK_NAME.parse().unwrap(),
            "/ai/tinyhumans/tinybus/tests/RuntimeLease1"
                .parse()
                .unwrap(),
            Callback(replacement_calls.clone()),
        )
        .await
        .unwrap();
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(1));

    let closing = tokio::spawn(async move { lease.close().await });
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("close starts STOP"),
    }
    assert!(closing.await.unwrap().is_ok());
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(2));
    assert_eq!(replacement_calls.load(Ordering::Acquire), 2);
    drop(replacement);
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn cancelling_the_open_waiter_does_not_lose_a_late_reservation() {
    let mut harness = Harness::new();
    let opening = tokio::spawn(harness.open());
    let reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply,
        _ => panic!("reserve must begin the activation transaction"),
    };
    opening.abort();
    reply
        .send(Ok(Reservation(harness.driver.identity.clone())))
        .unwrap();

    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object creation follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Start(reply) => reply.send(Ok(())).unwrap(),
        _ => panic!("start follows callback registration"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("a lease whose result was abandoned is rolled back"),
    }
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn validation_panic_after_reserve_still_stops_the_owned_reservation() {
    let mut harness = Harness::new();
    harness
        .driver
        .validation_panic
        .store(true, Ordering::Release);
    let opening = tokio::spawn(harness.open());
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    let stop = tokio::time::timeout(std::time::Duration::from_secs(2), harness.calls.recv())
        .await
        .expect("validation panic must not discard the reservation")
        .unwrap();
    match stop {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("panic rollback uses STOP"),
    }
    assert!(opening.await.unwrap().is_err());
}

#[tokio::test]
async fn callback_address_panic_after_object_creation_still_stops_the_reservation() {
    let mut harness = Harness::new();
    harness.driver.address_panic.store(true, Ordering::Release);
    let opening = tokio::spawn(harness.open());
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object follows validation"),
    }
    let stop = tokio::time::timeout(std::time::Duration::from_secs(2), harness.calls.recv())
        .await
        .expect("address panic must not discard the reservation")
        .unwrap();
    match stop {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("panic rollback uses STOP"),
    }
    assert!(opening.await.unwrap().is_err());
}

#[tokio::test]
async fn start_panic_keeps_the_exported_callback_until_rollback_ack() {
    let mut harness = Harness::new();
    harness.driver.start_panic.store(true, Ordering::Release);
    let opening = tokio::spawn(harness.open());
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Start(reply) => reply.send(Ok(())).unwrap(),
        _ => panic!("start follows callback registration"),
    }
    let stop = tokio::time::timeout(std::time::Duration::from_secs(2), harness.calls.recv())
        .await
        .expect("start panic must retain rollback ownership")
        .unwrap();
    let DriverCall::Stop(_, reply) = stop else {
        panic!("panic rollback uses STOP")
    };
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(1));
    reply.send(StopOutcome::Acknowledged).unwrap();
    assert!(opening.await.unwrap().is_err());
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn validation_failure_after_reserve_still_stops_and_never_starts() {
    let mut harness = Harness::new();
    harness
        .driver
        .validation_error
        .store(true, Ordering::Release);
    let opening = tokio::spawn(harness.open());
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve must be first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("validation error is rolled back before object/export/start"),
    }
    let Err(error) = opening.await.unwrap() else {
        panic!("invalid reservation must fail activation");
    };
    assert!(!format!("{error:?}").contains("private validation detail"));
    assert!(!error.to_string().contains("private validation detail"));
    assert!(
        harness.calls.try_recv().is_err(),
        "failed validation has no later stage"
    );
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn object_failure_after_reserve_keeps_the_cleanup_owner_until_stop_ack() {
    let mut harness = Harness::new();
    let opening = tokio::spawn(harness.open());
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve must be first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(_, reply) => reply
            .send(Err(DriverError("private object detail")))
            .unwrap(),
        _ => panic!("object failure follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("object failure rolls back the already-owned reservation"),
    }
    let Err(error) = opening.await.unwrap() else {
        panic!("object creation failure must fail activation");
    };
    assert!(!format!("{error:?}").contains("private object detail"));
    assert!(!error.to_string().contains("private object detail"));
    assert!(
        harness.calls.try_recv().is_err(),
        "failed object creation never serves or starts"
    );
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn start_failure_keeps_the_exported_callback_until_stop_ack() {
    let mut harness = Harness::new();
    harness.driver.start_error.store(true, Ordering::Release);
    let opening = tokio::spawn(harness.open());
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Start(reply) => reply.send(Ok(())).unwrap(),
        _ => panic!("start follows callback export"),
    }
    let stop_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("start error rolls back the reservation"),
    };
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(1));
    stop_reply.send(StopOutcome::Acknowledged).unwrap();
    let Err(error) = opening.await.unwrap() else {
        panic!("failed start cannot produce a lease");
    };
    assert_eq!(error.phase(), LeasePhase::Start);
    assert_eq!(error.driver_error().unwrap().0, "private start detail");
    assert!(harness.callback().await.is_err());
    assert!(harness.calls.try_recv().is_err());
}

#[tokio::test]
async fn callback_factory_error_rolls_back_before_start_or_export() {
    let mut harness = Harness::new();
    let opening =
        tokio::spawn(harness.open_with(|_, _| Err(DriverError("private callback factory detail"))));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("factory error rolls back before start"),
    }
    let Err(error) = opening.await.unwrap() else {
        panic!("callback factory error cannot produce a lease");
    };
    assert_eq!(error.phase(), LeasePhase::Callback);
    assert_eq!(
        error.driver_error().unwrap().0,
        "private callback factory detail"
    );
    assert!(
        !error
            .to_string()
            .contains("private callback factory detail")
    );
    assert!(harness.calls.try_recv().is_err());
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn callback_factory_panic_still_rolls_back_the_reserved_resource() {
    let mut harness = Harness::new();
    let opening = tokio::spawn(harness.open_with(|_, _| -> Result<Callback, DriverError> {
        std::panic::panic_any(());
    }));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("factory panic still starts rollback"),
    }
    let Err(error) = opening.await.unwrap() else {
        panic!("panicking callback factory cannot create a lease");
    };
    assert_eq!(error.phase(), LeasePhase::Callback);
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn reporter_panic_cannot_interrupt_acknowledged_callback_withdrawal() {
    let mut harness = Harness::new();
    let calls = harness.callback_calls.clone();
    let opening = tokio::spawn(
        harness.open_with_policy_and_reporter(
            move |reservation, object| {
                assert!(Arc::ptr_eq(&reservation.0.0, &object.0.0));
                Ok(Callback(calls))
            },
            RetryPolicy::new(
                std::time::Duration::from_millis(20),
                vec![std::time::Duration::ZERO],
            )
            .unwrap(),
            |_| std::panic::panic_any(()),
        ),
    );
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Validated)
    ));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Object(identity, reply) => reply.send(Ok(Object(identity))).unwrap(),
        _ => panic!("object follows validation"),
    }
    match harness.calls.recv().await.unwrap() {
        DriverCall::Start(reply) => reply.send(Ok(())).unwrap(),
        _ => panic!("callback registers before start"),
    }
    let lease = opening.await.unwrap().unwrap();
    let closing = tokio::spawn(async move { lease.close().await });
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply.send(StopOutcome::Acknowledged).unwrap(),
        _ => panic!("close sends STOP"),
    }
    assert!(closing.await.unwrap().is_ok());
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn acknowledged_stop_is_the_only_event_that_withdraws_the_callback() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(1));

    let closing = tokio::spawn(async move { lease.close().await });
    let stop_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("close starts one STOP"),
    };
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(2));
    stop_reply.send(StopOutcome::Acknowledged).unwrap();
    assert!(closing.await.unwrap().is_ok());
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn exhausted_stop_parks_the_callback_until_an_acknowledgement() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let closing = tokio::spawn(async move { lease.close().await });

    for _ in 0..3 {
        match harness.calls.recv().await.unwrap() {
            DriverCall::Stop(_, reply) => reply
                .send(StopOutcome::RetryableFailure(DriverError(
                    "private stop detail",
                )))
                .unwrap(),
            _ => panic!("bounded retry calls STOP only"),
        }
    }
    let Err(error) = closing.await.unwrap() else {
        panic!("exhausted settled failures are not an acknowledgement");
    };
    assert_eq!(error.driver_error().unwrap().0, "private stop detail");
    assert!(!format!("{error:?}").contains("private stop detail"));
    assert!(!error.to_string().contains("private stop detail"));
    assert!(std::error::Error::source(&*error).is_none());
    assert_eq!(harness.callback().await.unwrap(), serde_json::json!(1));
    assert!(
        harness.calls.try_recv().is_err(),
        "exhaustion does not busy-loop"
    );
}

#[tokio::test]
async fn retryable_stop_uses_the_configured_nonzero_backoff_sequence_in_order() {
    let mut harness = Harness::new();
    let first_delay = std::time::Duration::from_millis(40);
    let second_delay = std::time::Duration::from_millis(5);
    let lease = harness
        .opened_with_policy(
            RetryPolicy::new(
                std::time::Duration::from_secs(1),
                vec![first_delay, second_delay],
            )
            .unwrap(),
        )
        .await;
    let closing = tokio::spawn(async move { lease.close().await });
    let mut starts = Vec::new();
    for _ in 1..=3 {
        match harness.calls.recv().await.unwrap() {
            DriverCall::Stop(started, reply) => {
                starts.push(started);
                reply
                    .send(StopOutcome::RetryableFailure(DriverError("settled")))
                    .unwrap();
            }
            _ => panic!("retry sequence contains only STOP"),
        }
    }
    assert_eq!(starts.len(), 3);
    assert!(starts[1].duration_since(starts[0]) >= first_delay);
    assert!(starts[2].duration_since(starts[1]) >= second_delay);
    assert!(closing.await.unwrap().is_err());
    assert!(
        harness.calls.try_recv().is_err(),
        "the retry vector is finite"
    );
    assert!(harness.callback().await.is_ok());
}

#[tokio::test]
async fn ambiguous_stop_failure_is_reported_without_retry_or_callback_withdrawal() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let closing = tokio::spawn(async move { lease.close().await });
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply
            .send(StopOutcome::Unacknowledged(DriverError(
                "private transport timeout",
            )))
            .unwrap(),
        _ => panic!("close starts STOP"),
    }
    let Err(error) = closing.await.unwrap() else {
        panic!("an uncertain result is not an acknowledgement");
    };
    assert_eq!(error.driver_error().unwrap().0, "private transport timeout");
    assert!(!format!("{error:?}").contains("private transport timeout"));
    assert!(!error.to_string().contains("private transport timeout"));
    assert!(std::error::Error::source(&*error).is_none());
    let report = harness.reports.recv().await.unwrap();
    assert_eq!(report.outcome, LeaseReportOutcome::Unacknowledged);
    assert!(!format!("{report:?}").contains("private transport timeout"));
    assert!(
        harness.callback().await.is_ok(),
        "uncertain STOP retains its callback"
    );
    assert!(
        harness.calls.try_recv().is_err(),
        "uncertain STOP is never retried"
    );
}

#[tokio::test]
async fn a_stop_deadline_keeps_the_remote_operation_owned_until_its_late_ack() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let closing = tokio::spawn(async move { lease.close().await });
    let first_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("close starts one STOP worker"),
    };

    loop {
        if matches!(
            harness.reports.recv().await.unwrap().outcome,
            LeaseReportOutcome::TimedOut
        ) {
            break;
        }
    }
    assert!(
        harness.callback().await.is_ok(),
        "deadline is not STOP acknowledgement"
    );
    assert!(
        harness.calls.try_recv().is_err(),
        "unresolved STOP remains the only remote operation"
    );
    first_reply.send(StopOutcome::Acknowledged).unwrap();
    assert!(
        closing.await.unwrap().is_err(),
        "close observes the deadline as unacknowledged"
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness.callback().await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late STOP acknowledgement withdraws the callback");
    assert!(
        harness.calls.try_recv().is_err(),
        "late ACK causes no duplicate STOP"
    );
}

#[tokio::test]
async fn cloned_close_waiters_and_last_user_drop_share_one_stop_worker() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let first = lease.clone();
    let second = lease.clone();
    let first_waiter = tokio::spawn(async move { first.close().await });
    let stop_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("first close starts the worker"),
    };
    let second_waiter = tokio::spawn(async move { second.close().await });
    drop(lease);
    first_waiter.abort();
    assert!(
        harness.calls.try_recv().is_err(),
        "joining close and Drop do not duplicate STOP"
    );
    stop_reply.send(StopOutcome::Acknowledged).unwrap();
    assert!(second_waiter.await.unwrap().is_ok());
    assert!(harness.calls.try_recv().is_err());
    assert!(harness.callback().await.is_err());
}

#[tokio::test]
async fn timing_out_a_close_waiter_does_not_cancel_the_dedicated_stop_worker() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let waiter = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_millis(1), lease.close()).await
    });
    let stop_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("close starts a dedicated stop worker"),
    };
    assert!(
        waiter.await.unwrap().is_err(),
        "the external close wait can expire"
    );
    assert!(
        harness.callback().await.is_ok(),
        "wait timeout is not STOP acknowledgement"
    );
    stop_reply.send(StopOutcome::Acknowledged).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness.callback().await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original worker removes the callback after its acknowledgement");
    assert!(
        harness.calls.try_recv().is_err(),
        "caller timeout creates no second STOP"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destroying_the_callers_runtime_does_not_cancel_the_dedicated_stop_worker() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (destroy_tx, destroy_rx) = std::sync::mpsc::channel();
    let caller = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.spawn(async move {
            let _ = lease.close().await;
        });
        ready_tx.send(()).unwrap();
        destroy_rx.recv().unwrap();
        drop(runtime);
    });
    tokio::task::spawn_blocking(move || ready_rx.recv().unwrap())
        .await
        .unwrap();
    let stop_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("caller close starts the dedicated worker"),
    };
    destroy_tx.send(()).unwrap();
    tokio::task::spawn_blocking(move || caller.join().unwrap())
        .await
        .unwrap();
    assert!(
        harness.callback().await.is_ok(),
        "caller runtime drop is not STOP ACK"
    );
    stop_reply.send(StopOutcome::Acknowledged).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness.callback().await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the runtime-owned worker completes after its caller runtime is gone");
    assert!(harness.calls.try_recv().is_err());
}

#[tokio::test]
async fn dropping_the_last_lease_outside_tokio_starts_cleanup_on_the_owned_runtime() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    tokio::task::spawn_blocking(move || std::thread::spawn(move || drop(lease)).join().unwrap())
        .await
        .unwrap();
    let stop_reply = match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply,
        _ => panic!("last-user Drop starts STOP"),
    };
    assert!(harness.callback().await.is_ok());
    stop_reply.send(StopOutcome::Acknowledged).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if harness.callback().await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the dedicated worker withdraws after ACK");
}

#[tokio::test]
async fn logical_failure_stops_retrying_without_claiming_acknowledgement() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    harness.driver.failed.store(true, Ordering::Release);
    let closing = tokio::spawn(async move { lease.close().await });
    assert!(closing.await.unwrap().is_err());
    assert!(
        harness.calls.try_recv().is_err(),
        "fault before first attempt sends no STOP"
    );
    assert!(
        harness.calls.try_recv().is_err(),
        "failed module has no subsequent retry"
    );
    assert!(
        harness.callback().await.is_ok(),
        "logical failure is not STOP ACK"
    );
}

#[tokio::test]
async fn stop_panic_after_export_parks_the_callback_without_acknowledgement() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    harness.driver.stop_panic.store(true, Ordering::Release);
    let close = tokio::spawn(async move { lease.close().await });
    assert!(matches!(
        harness.calls.recv().await,
        Some(DriverCall::Stop(_, _))
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), close)
            .await
            .expect("STOP panic must publish an unacknowledged result")
            .unwrap()
            .is_err()
    );
    assert!(
        harness.callback().await.is_ok(),
        "a panicking STOP has no acknowledgement"
    );
    assert!(
        harness.calls.try_recv().is_err(),
        "panic does not start a duplicate STOP"
    );
}

#[tokio::test]
async fn fault_predicate_panic_parks_the_callback_without_sending_stop() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    harness.driver.failed_panic.store(true, Ordering::Release);
    let close = tokio::spawn(async move { lease.close().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), close)
            .await
            .expect("fault predicate panic must publish an unacknowledged result")
            .unwrap()
            .is_err()
    );
    assert!(
        harness.callback().await.is_ok(),
        "a panicking fault check is not an ACK"
    );
    assert!(
        harness.calls.try_recv().is_err(),
        "fault check panic prevents unsafe retry"
    );
}
