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
const CALLBACK_PATH: &str = "/ai/tinyhumans/tinybus/tests/RuntimeLease";

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
        assert!(Arc::ptr_eq(&self.identity.0, &reservation.0.0));
        Ok((
            CALLBACK_NAME.parse().unwrap(),
            format!("/ai/tinyhumans/tinybus/tests/RuntimeLease{}", reservation.0.1)
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
        result.await.unwrap()
    }

    fn failed(&self, _: &Self::Reservation) -> bool {
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
        let driver = self.driver.clone();
        let reports = self.report_sender.clone();
        let policy = RetryPolicy::new(
            std::time::Duration::from_millis(20),
            vec![std::time::Duration::ZERO, std::time::Duration::ZERO],
        )
        .unwrap();
        self.runtime.lease(
            driver,
            make_callback,
            policy,
            move |report| {
                let _ = reports.send(report);
            },
        )
    }

    async fn opened(&mut self) -> RuntimeLease<Object, DriverError> {
        let opening = tokio::spawn(self.open());
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
        self.runtime
            .proxy(
                CALLBACK_NAME,
                &format!("/ai/tinyhumans/tinybus/tests/RuntimeLease{}", self.driver.identity.1),
            )
            .unwrap()
            .call("Touch", ())
            .await
    }
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
    assert!(matches!(harness.calls.recv().await, Some(DriverCall::Validated)));
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
    let opening = tokio::spawn(harness.open_with(|_, _| {
        Err(DriverError("private callback factory detail"))
    }));
    match harness.calls.recv().await.unwrap() {
        DriverCall::Reserve(reply) => reply
            .send(Ok(Reservation(harness.driver.identity.clone())))
            .unwrap(),
        _ => panic!("reserve is first"),
    }
    assert!(matches!(harness.calls.recv().await, Some(DriverCall::Validated)));
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
    assert_eq!(error.driver_error().unwrap().0, "private callback factory detail");
    assert!(!error.to_string().contains("private callback factory detail"));
    assert!(harness.calls.try_recv().is_err());
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
async fn ambiguous_stop_failure_is_reported_without_retry_or_callback_withdrawal() {
    let mut harness = Harness::new();
    let lease = harness.opened().await;
    let closing = tokio::spawn(async move { lease.close().await });
    match harness.calls.recv().await.unwrap() {
        DriverCall::Stop(_, reply) => reply
            .send(StopOutcome::Unacknowledged(DriverError("private transport timeout")))
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
    assert!(harness.callback().await.is_ok(), "uncertain STOP retains its callback");
    assert!(harness.calls.try_recv().is_err(), "uncertain STOP is never retried");
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
