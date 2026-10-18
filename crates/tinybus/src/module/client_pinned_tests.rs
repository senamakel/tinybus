//! Ordinary module bindings retain their resolved peer without native proof.

use super::*;
use crate::broker::Broker;
use crate::module::failure::{FailureReport, FailureSink};
use crate::transport::memory::MemoryBus;
use crate::{Connection, Interface, InterfaceName, MemberName, ObjectPath, StreamRef};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "ordinary-pinned-client-test",
    bus_name: "ai.tinyhumans.test.OrdinaryPinnedClient",
    object_path: "/ai/tinyhumans/test/OrdinaryPinnedClient",
    version: "0.3.2",
    release_url: "https://example.invalid/tinybus",
    // Ordinary fixtures and linked host factories intentionally have no
    // release archive/library pair to attest.
    assets: &[],
};

struct DiscardReports;

impl FailureSink for DiscardReports {
    fn capture(&self, _: &FailureReport<'_>) {}
}

fn reporter() -> FailureReporter {
    FailureReporter::new(Arc::new(DiscardReports))
}

#[derive(Default)]
struct RecordedReports(Mutex<Vec<Reason>>);

impl FailureSink for RecordedReports {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0.lock().unwrap().push(report.reason);
    }
}

struct Access {
    connection: Connection,
    reporter: FailureReporter,
    enabled: bool,
}

impl ModuleAccess for Access {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async move {
            self.connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)
        })
    }
}

#[derive(Clone)]
struct Peer {
    label: &'static str,
    connection: Connection,
    received: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Interface for Peer {
    fn name(&self) -> InterfaceName {
        InterfaceName::new(RECORD.bus_name).unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        ["Who", "Upload", "Read", "Release"]
            .map(|member| MemberName::new(member).unwrap())
            .to_vec()
    }

    async fn call(&self, member: &MemberName, args: Value) -> crate::Result<Value> {
        self.events.lock().unwrap().push(member.as_str().to_owned());
        match member.as_str() {
            "Who" | "Read" | "Release" => Ok(serde_json::json!(self.label)),
            "Upload" => {
                let (stream,): (StreamRef,) = serde_json::from_value(args)?;
                let bytes = self.connection.read_stream(&stream).await?;
                self.received.send(bytes.clone()).unwrap();
                Ok(serde_json::json!(self.label))
            }
            _ => Err(crate::Error::failed("unexpected test method")),
        }
    }
}

#[derive(Clone)]
struct HandoverArguments {
    stream: StreamRef,
    started: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    barrier: Arc<std::sync::Barrier>,
}

impl serde::Serialize for HandoverArguments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        // Keep the upload unopened until the test has reassigned the public
        // alias; this proves the stream producer uses the pinned destination.
        self.barrier.wait();
        serde::Serialize::serialize(&self.stream, serializer)
    }
}

async fn peer(
    bus: &MemoryBus,
    label: &'static str,
    received: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    events: Arc<Mutex<Vec<String>>>,
) -> Connection {
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    for path in [RECORD.object_path, "/child"] {
        connection
            .serve_at(
                ObjectPath::new(path).unwrap(),
                Peer {
                    label,
                    connection: connection.clone(),
                    received: received.clone(),
                    events: events.clone(),
                },
            )
            .await
            .unwrap();
    }
    connection
}

#[tokio::test]
async fn an_ordinary_fixture_with_no_release_assets_can_bind() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(&bus, "fixture", received, Arc::new(Mutex::new(Vec::new()))).await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy);

    let pinned = client.bind(RECORD.id).await.unwrap();
    let response: String = pinned.call("Who", ()).await.unwrap();
    assert_eq!(response, "fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn ordinary_binding_keeps_root_object_and_stream_with_the_original_peer() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let (received_a_tx, mut received_a_rx) = tokio::sync::mpsc::unbounded_channel();
    let (received_b_tx, mut received_b_rx) = tokio::sync::mpsc::unbounded_channel();
    let events_a = Arc::new(Mutex::new(Vec::new()));
    let events_b = Arc::new(Mutex::new(Vec::new()));
    let service_a = peer(&bus, "peer-a", received_a_tx, events_a.clone()).await;
    let service_b = peer(&bus, "peer-b", received_b_tx, events_b.clone()).await;
    service_a.request_name(RECORD.bus_name).await.unwrap();

    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy);
    let pinned = client.bind(RECORD.id).await.unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started_tx)));
    let call_client = pinned.clone();
    let payload = b"ordinary pinned payload".to_vec();
    let call_payload = payload.clone();
    let call_started = started.clone();
    let call_barrier = barrier.clone();
    let upload = tokio::spawn(async move {
        call_client
            .call_with_stream::<String, _, _>(
                "Upload",
                move |stream| HandoverArguments {
                    stream: stream.clone(),
                    started: call_started,
                    barrier: call_barrier,
                },
                &call_payload,
            )
            .await
    });

    started_rx.await.unwrap();
    service_a.release_name(RECORD.bus_name).await.unwrap();
    service_b.request_name(RECORD.bus_name).await.unwrap();
    barrier.wait();
    assert_eq!(upload.await.unwrap().unwrap(), "peer-a");
    assert_eq!(received_a_rx.recv().await.unwrap(), payload);
    assert!(received_b_rx.try_recv().is_err());

    let object = pinned.object_client("/child").unwrap();
    let response: String = object.call("Read", ()).await.unwrap();
    assert_eq!(response, "peer-a");
    let response: String = object.call("Release", ()).await.unwrap();
    assert_eq!(response, "peer-a");
    assert_eq!(events_a.lock().unwrap().as_slice(), &["Upload", "Read", "Release"]);
    assert!(events_b.lock().unwrap().is_empty());

    service_a.close().await.unwrap();
    let root_after_close: Result<String, _> = pinned.call("Who", ()).await;
    let object_after_close: Result<String, _> = object.call("Read", ()).await;
    assert!(root_after_close.is_err());
    assert!(object_after_close.is_err());
    assert!(events_b.lock().unwrap().is_empty());
    assert!(received_b_rx.try_recv().is_err());
    broker_task.abort();
}

#[tokio::test]
async fn ordinary_binding_refuses_disabled_unknown_and_unowned_modules() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let disabled_reports = Arc::new(RecordedReports::default());
    let disabled = ModuleClient::new(Access {
        connection: caller.clone(),
        reporter: FailureReporter::new(disabled_reports.clone()),
        enabled: false,
    });
    let disabled_result = disabled.bind(RECORD.id).await;
    assert!(matches!(disabled_result, Err(failure) if failure.kind == ModuleCallError::Unavailable));
    assert_eq!(disabled_reports.0.lock().unwrap().as_slice(), &[Reason::Disabled]);

    let unknown_reports = Arc::new(RecordedReports::default());
    let unknown = ModuleClient::new(Access {
        connection: caller.clone(),
        reporter: FailureReporter::new(unknown_reports.clone()),
        enabled: true,
    });
    let unknown_result = unknown.bind("not-registered").await;
    assert!(matches!(unknown_result, Err(failure) if failure.kind == ModuleCallError::Unavailable));
    assert_eq!(unknown_reports.0.lock().unwrap().len(), 1);

    // The broker has no peer advertising the registered well-known name.
    let unowned_reports = Arc::new(RecordedReports::default());
    let unowned_proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let unowned = ModuleClient::new(Access {
        connection: caller,
        reporter: FailureReporter::new(unowned_reports.clone()),
        enabled: true,
    })
    .with_fixture(unowned_proxy);
    let unowned_result = unowned.bind(RECORD.id).await;
    assert!(matches!(unowned_result, Err(failure) if failure.kind == ModuleCallError::Unavailable));
    assert_eq!(unowned_reports.0.lock().unwrap().as_slice(), &[Reason::ModuleUnavailable]);
}
