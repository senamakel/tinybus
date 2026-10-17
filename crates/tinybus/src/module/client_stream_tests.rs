//! Streamed module calls preserve the same checks and reporting as typed calls.

use super::*;
use crate::broker::Broker;
use crate::module::failure::{FailureReport, FailureSink};
use crate::transport::memory::MemoryBus;
use crate::{Connection, Error, Interface, InterfaceName, MemberName, ObjectPath, StreamRef};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "stream-client-test",
    bus_name: "ai.tinyhumans.test.StreamClient",
    object_path: "/ai/tinyhumans/test/StreamClient",
    version: "0.2.1",
    release_url: "https://example.invalid/tinybus",
    assets: &[],
};

#[derive(Default)]
struct Reports(Mutex<Vec<Reason>>);

impl FailureSink for Reports {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0.lock().unwrap().push(report.reason);
    }
}

struct StreamAccess {
    connection: Connection,
    reporter: FailureReporter,
    enabled: bool,
    ready: bool,
}

impl ModuleAccess for StreamAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, _id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.ready {
                Ok(())
            } else {
                Err("not ready".to_string())
            }
        })
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

struct ObservedTransport {
    inner: Box<dyn crate::ports::Transport>,
    opens: Arc<AtomicUsize>,
    aborts: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait]
impl crate::ports::Transport for ObservedTransport {
    async fn send(&self, message: crate::Message) -> crate::Result<()> {
        let interface = message.header.interface.as_ref().map(InterfaceName::as_str);
        let member = message.header.member.as_ref().map(MemberName::as_str);
        if interface == Some(crate::stream::STREAM_INTERFACE) {
            match member {
                Some("Open") => {
                    self.opens.fetch_add(1, Ordering::SeqCst);
                }
                Some("Abort") => {
                    self.aborts.send(()).unwrap();
                }
                _ => {}
            }
        }
        self.inner.send(message).await
    }

    async fn recv(&self) -> crate::Result<Option<crate::Message>> {
        self.inner.recv().await
    }

    async fn close(&self) -> crate::Result<()> {
        self.inner.close().await
    }
}

#[derive(Clone)]
enum Behavior {
    Consume(tokio::sync::mpsc::UnboundedSender<Vec<u8>>),
    Invalid,
    Unavailable,
    Stall {
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        resume: Arc<tokio::sync::Notify>,
    },
}

struct StreamService {
    connection: Connection,
    behavior: Behavior,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Interface for StreamService {
    fn name(&self) -> InterfaceName {
        InterfaceName::new(RECORD.bus_name).unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        ["Upload", "InvalidInput", "Unavailable", "Stall"]
            .map(|name| MemberName::new(name).unwrap())
            .to_vec()
    }

    async fn call(&self, member: &MemberName, args: Value) -> crate::Result<Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match (&self.behavior, member.as_str()) {
            (Behavior::Consume(received), "Upload") => {
                let (stream,): (StreamRef,) = serde_json::from_value(args)?;
                let bytes = self.connection.read_stream(&stream).await?;
                received.send(bytes.clone()).unwrap();
                Ok(serde_json::json!(bytes.len()))
            }
            (Behavior::Invalid, "InvalidInput") => Err(Error::MethodFailed {
                name: "ai.tinyhumans.tinydocs.Error.InvalidInput".to_string(),
                message: "secret document detail".to_string(),
            }),
            (Behavior::Unavailable, "Unavailable") => Err(Error::MethodFailed {
                name: Error::MODULE_UNAVAILABLE.to_string(),
                message: "secret unavailable detail".to_string(),
            }),
            (Behavior::Stall { entered, resume }, "Stall") => {
                entered.send(()).unwrap();
                resume.notified().await;
                Ok(Value::Null)
            }
            _ => Err(Error::failed("unexpected test method")),
        }
    }
}

struct Fixture {
    client: ModuleClient,
    connection: Connection,
    opens: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    aborts: tokio::sync::mpsc::UnboundedReceiver<()>,
    reports: Arc<Reports>,
}

async fn fixture(behavior: Behavior) -> Fixture {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());

    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    service
        .serve_at(
            ObjectPath::new(RECORD.object_path).unwrap(),
            StreamService {
                connection: service.clone(),
                behavior: behavior.clone(),
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();
    service
        .serve_at(
            ObjectPath::new("/child").unwrap(),
            StreamService {
                connection: service.clone(),
                behavior,
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();

    let opens = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (aborts_tx, aborts) = tokio::sync::mpsc::unbounded_channel();
    let observed = ObservedTransport {
        inner: bus.connect().await.unwrap(),
        opens: opens.clone(),
        aborts: aborts_tx,
    };
    let connection = Connection::connect(Box::new(observed)).await.unwrap();
    let reports = Arc::new(Reports::default());
    let access = StreamAccess {
        connection: connection.clone(),
        reporter: FailureReporter::new(reports.clone()),
        enabled: true,
        ready: true,
    };
    let proxy = connection
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    Fixture {
        client: ModuleClient::new(access).with_fixture(proxy),
        connection,
        opens,
        calls,
        aborts,
        reports,
    }
}

#[tokio::test]
async fn module_calls_stream_empty_and_past_window_payloads() {
    let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
    let fixture = fixture(Behavior::Consume(received_tx)).await;
    let bytes = vec![0x5a; crate::stream::MAX_CHUNK_LEN * 9 + 7];

    let length: usize = fixture
        .client
        .call_with_stream_detailed(
            RECORD.id,
            "Upload",
            |stream| serde_json::json!([stream]),
            &bytes,
        )
        .await
        .unwrap();
    assert_eq!(length, bytes.len());
    assert_eq!(received_rx.recv().await.unwrap(), bytes);

    let empty_length: usize = fixture
        .client
        .call_with_stream_detailed(
            RECORD.id,
            "Upload",
            |stream| serde_json::json!([stream]),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(empty_length, 0);
    assert!(received_rx.recv().await.unwrap().is_empty());
    assert_eq!(fixture.opens.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn invalid_or_unavailable_module_stream_calls_do_not_run_the_builder_or_open_a_stream() {
    let (received_tx, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let fixture = fixture(Behavior::Consume(received_tx)).await;
    let builder_calls = Arc::new(AtomicUsize::new(0));
    assert!(
        fixture
            .client
            .call_with_stream_detailed::<Value, _, _>(
                "unknown",
                "Upload",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );

    let proxy = fixture.client.fixture_proxy().unwrap();
    let disabled = ModuleClient::new(StreamAccess {
        connection: fixture.connection.clone(),
        reporter: FailureReporter::new(Arc::new(Reports::default())),
        enabled: false,
        ready: true,
    })
    .with_fixture(proxy.clone());
    assert!(
        disabled
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "Upload",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );

    let unready = ModuleClient::new(StreamAccess {
        connection: fixture.connection.clone(),
        reporter: FailureReporter::new(Arc::new(Reports::default())),
        enabled: true,
        ready: false,
    });
    assert!(
        unready
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "Upload",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );

    assert!(
        fixture
            .client
            .call_with_stream_detailed::<Value, _, _>(
                RECORD.id,
                "invalid/member",
                {
                    let builder_calls = builder_calls.clone();
                    move |stream: &StreamRef| {
                        builder_calls.fetch_add(1, Ordering::SeqCst);
                        serde_json::json!([stream])
                    }
                },
                b"x",
            )
            .await
            .is_err()
    );
    assert_eq!(builder_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn detailed_stream_failures_preserve_wire_input_errors_and_sanitize_renderings() {
    let invalid_fixture = fixture(Behavior::Invalid).await;
    let failure = invalid_fixture
        .client
        .call_with_stream_detailed::<Value, _, _>(
            RECORD.id,
            "InvalidInput",
            |stream| serde_json::json!([stream]),
            b"untrusted input",
        )
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(
        failure.wire_name,
        "ai.tinyhumans.tinydocs.Error.InvalidInput"
    );
    assert_eq!(failure.message, "secret document detail");
    for rendered in [format!("{failure:?}"), failure.to_string()] {
        assert!(!rendered.contains("InvalidInput"));
        assert!(!rendered.contains("secret document detail"));
    }

    let object = invalid_fixture
        .client
        .object_client(RECORD.id, "/child")
        .await
        .unwrap();
    let object_failure = object
        .call_detailed::<Value>("InvalidInput", ())
        .await
        .unwrap_err();
    assert_eq!(object_failure.wire_name, failure.wire_name);
    assert_eq!(object_failure.message, failure.message);
    let unavailable_fixture = fixture(Behavior::Unavailable).await;
    let unavailable_object = unavailable_fixture
        .client
        .object_client(RECORD.id, "/child")
        .await
        .unwrap();
    let unavailable = unavailable_object
        .call_detailed::<Value>("Unavailable", ())
        .await
        .unwrap_err();
    assert_eq!(unavailable.kind, ModuleCallError::Unavailable);
    assert_eq!(unavailable.wire_name, Error::MODULE_UNAVAILABLE);
    assert_eq!(unavailable.message, "secret unavailable detail");

    let sanitized = object.call::<Value>("InvalidInput", ()).await.unwrap_err();
    assert_eq!(sanitized, ModuleCallError::ModuleFault);
    assert!(!format!("{sanitized:?}").contains("secret document detail"));
    assert!(!sanitized.to_string().contains("secret document detail"));
    let reports = invalid_fixture.reports.0.lock().unwrap();
    assert_eq!(
        reports.as_slice(),
        &[
            Reason::ModuleFault,
            Reason::ModuleFault,
            Reason::ModuleFault,
        ]
    );
}

#[tokio::test]
async fn method_failure_and_cancellation_abort_a_streamed_upload() {
    let failed_fixture = fixture(Behavior::Invalid).await;
    let bytes = vec![0x33; crate::stream::MAX_CHUNK_LEN * 12];
    let failure = failed_fixture
        .client
        .call_with_stream_detailed::<Value, _, _>(
            RECORD.id,
            "InvalidInput",
            |stream| serde_json::json!([stream]),
            &bytes,
        )
        .await
        .unwrap_err();
    assert_eq!(
        failure.wire_name,
        "ai.tinyhumans.tinydocs.Error.InvalidInput"
    );
    let mut failed_fixture = failed_fixture;
    tokio::time::timeout(Duration::from_secs(10), failed_fixture.aborts.recv())
        .await
        .expect("method failure dropped the unfinished stream writer")
        .expect("the abort observer remained connected");

    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut stalled = fixture(Behavior::Stall {
        entered: entered_tx,
        resume: resume.clone(),
    })
    .await;
    let bytes = vec![0x44; crate::stream::MAX_CHUNK_LEN * 12];
    let task = {
        let client = stalled.client.clone();
        tokio::spawn(async move {
            client
                .call_with_stream_detailed::<Value, _, _>(
                    RECORD.id,
                    "Stall",
                    |stream| serde_json::json!([stream]),
                    &bytes,
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
        .await
        .expect("stalled method was dispatched")
        .expect("the method-entry observer remained connected");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), stalled.aborts.recv())
        .await
        .expect("cancellation dropped the unfinished stream writer")
        .expect("the abort observer remained connected");
    resume.notify_one();
}
