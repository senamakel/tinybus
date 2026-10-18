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
    version: "0.3.1",
    release_url: "https://example.invalid/tinybus",
    // Ordinary fixtures and linked host factories intentionally have no
    // release archive/library pair to attest.
    assets: &[],
};

const REMOTE_DETAIL: &str = "private ordinary-call detail: fixture-token-7319";

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
        ["Who", "Upload", "Read", "Release", "Fail", "Hang"]
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
            "Fail" => Err(crate::Error::MethodFailed {
                name: "ai.tinyhumans.docs.Error.Rejected".to_owned(),
                message: REMOTE_DETAIL.to_owned(),
            }),
            "Hang" => std::future::pending().await,
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

    let pinned = client.bind(RECORD.id).await.unwrap_or_else(|failure| {
        panic!(
            "ordinary linked factory binding failed ({failure:?}); resolver state: {:?}",
            client.access.find(RECORD.id).map(|record| record.id)
        )
    });
    let response: String = pinned.call("Who", ()).await.unwrap();
    assert_eq!(response, "fixture");
}

#[cfg(feature = "modules")]
struct LinkedOrdinaryContext {
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_deliver(
    context: *mut std::ffi::c_void,
    bytes: *const u8,
    len: usize,
) -> i32 {
    let context = unsafe { &*(context.cast::<LinkedOrdinaryContext>()) };
    let bytes = unsafe { std::slice::from_raw_parts(bytes, len) }.to_vec();
    context
        .sender
        .send(bytes)
        .map_or(-1, |_| crate::module::abi::TB_OK)
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_shutdown(context: *mut std::ffi::c_void, _: u64) -> i32 {
    drop(unsafe { Box::from_raw(context.cast::<LinkedOrdinaryContext>()) });
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn linked_ordinary_init(
    host: *const crate::module::abi::TbHostVtable,
    out: *mut crate::module::abi::TbModuleVtable,
) -> i32 {
    let host = unsafe { *host };
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let context = Box::into_raw(Box::new(LinkedOrdinaryContext { sender }));
    unsafe {
        *out = crate::module::abi::TbModuleVtable {
            size: std::mem::size_of::<crate::module::abi::TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: context.cast(),
            deliver: linked_ordinary_deliver,
            shutdown: linked_ordinary_shutdown,
            reinitialize: None,
        };
    }
    let host_context = host.host_ctx as usize;
    let send = host.send;
    std::thread::spawn(move || {
        while let Some(bytes) = receiver.blocking_recv() {
            let Ok(message) = serde_json::from_slice::<crate::Message>(&bytes) else {
                continue;
            };
            if message.header.kind != crate::message::MessageKind::MethodCall {
                continue;
            }
            let value = message
                .body
                .as_array()
                .and_then(|values| values.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let reply = crate::Message::method_return(&message.header, value);
            let bytes = serde_json::to_vec(&reply).unwrap();
            let _ = unsafe {
                send(
                    host_context as *mut std::ffi::c_void,
                    bytes.as_ptr(),
                    bytes.len(),
                )
            };
        }
    });
    unsafe { (host.ready)(host.host_ctx) };
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
fn linked_ordinary_factory() -> crate::Result<crate::module::LinkedModule> {
    use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, PanicPolicy};
    use crate::{BusName, ObjectPath, Version};

    Ok(crate::module::LinkedModule {
        descriptor: crate::module::abi::TbAbiDescriptor::current(RECORD.id, RECORD.version),
        manifest: crate::module::manifest::ModuleManifest {
            schema: MANIFEST_SCHEMA,
            module: ModuleIdentity {
                name: RECORD.id.to_owned(),
                version: Version::new(0, 3, 1),
                description: String::new(),
                homepage: None,
                license: String::new(),
            },
            bus_name: BusName::new(RECORD.bus_name).unwrap(),
            object_path: ObjectPath::new(RECORD.object_path).unwrap(),
            provides: Vec::new(),
            requires: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            lazy_init: false,
            worker_threads: 1,
            on_panic: PanicPolicy::Detach,
        },
        init: linked_ordinary_init,
        init_with_setup: None,
    })
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn ordinary_binding_accepts_a_resolver_owned_linked_factory_without_library_proof() {
    use crate::module::runtime::{ModuleRuntime, RuntimeNames};
    use crate::module::{LinkedRegistry, ModuleSource, Resolver, ResolverInputs};
    use std::collections::HashMap;
    use std::sync::OnceLock;

    static RUNTIME: OnceLock<ModuleRuntime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        ModuleRuntime::start(&RuntimeNames::default(), None).expect("test runtime starts")
    });
    let linked = Arc::new(LinkedRegistry::new());
    linked.register(RECORD.id, linked_ordinary_factory);
    let resolver = Resolver::new(ResolverInputs {
        enabled: true,
        install_dir: None,
        allow_download: false,
        bundled_dir: None,
        sources: HashMap::from([(RECORD.id.to_owned(), ModuleSource::Linked)]),
        linked,
        search_paths: None,
        search_skip: Vec::new(),
        gate: None,
        on_resolved: None,
        records: vec![&RECORD],
        terminal_marker: String::new(),
        runtime: Arc::new(move || Box::pin(async move { Ok(runtime) })),
        module_config: Arc::new(|_| serde_json::Value::Null),
        reporter: Arc::new(FailureReporter::new(Arc::new(DiscardReports))),
    });

    assert!(RECORD.assets.is_empty());
    let client = ModuleClient::new(resolver);
    let pinned = client.bind(RECORD.id).await.unwrap();
    let response: String = pinned.call("Echo", "linked-host-factory").await.unwrap();
    assert_eq!(response, "linked-host-factory");
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
    assert_eq!(
        events_a.lock().unwrap().as_slice(),
        &["Upload", "Read", "Release"]
    );
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
    assert!(
        matches!(disabled_result, Err(failure) if failure.kind == ModuleCallError::Unavailable)
    );
    assert_eq!(
        disabled_reports.0.lock().unwrap().as_slice(),
        &[Reason::Disabled]
    );

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
    assert_eq!(
        unowned_reports.0.lock().unwrap().as_slice(),
        &[Reason::ModuleUnavailable]
    );
}

#[tokio::test]
async fn ordinary_bound_detailed_failures_keep_fields_and_sanitize_renderings() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(
        &bus,
        "failure-peer",
        received,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let pinned = ModuleClient::new(Access {
        connection: caller,
        reporter: reporter(),
        enabled: true,
    })
    .with_fixture(proxy)
    .bind(RECORD.id)
    .await
    .unwrap();

    let failure = pinned
        .call_detailed::<String>("Fail", ())
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(failure.wire_name, "ai.tinyhumans.docs.Error.Rejected");
    assert_eq!(failure.message, REMOTE_DETAIL);
    assert!(!format!("{failure:?}").contains(REMOTE_DETAIL));
    assert!(!failure.to_string().contains(REMOTE_DETAIL));
}

#[tokio::test]
async fn ordinary_bound_calls_keep_the_configured_deadline_and_report_timeout() {
    let bus = MemoryBus::new();
    let _broker = Broker::new().spawn(bus.clone());
    let (received, _received_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = peer(
        &bus,
        "slow-peer",
        received,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    service.request_name(RECORD.bus_name).await.unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let reports = Arc::new(RecordedReports::default());
    let pinned = ModuleClient::new(Access {
        connection: caller,
        reporter: FailureReporter::new(reports.clone()),
        enabled: true,
    })
    .with_fixture(proxy)
    .with_timeout(std::time::Duration::from_millis(20))
    .bind(RECORD.id)
    .await
    .unwrap();

    let result: Result<String, _> = pinned.call("Hang", ()).await;
    assert!(matches!(result, Err(ModuleCallError::TransportFailed)));
    assert_eq!(
        reports.0.lock().unwrap().as_slice(),
        &[Reason::TransportFailed]
    );
}

#[test]
fn unsupported_module_availability_keeps_the_unavailable_classification() {
    let error = crate::Error::ModuleUnavailable {
        module: RECORD.id.to_owned(),
        state: "unsupported".to_owned(),
        detail: "the host does not support this module source".to_owned(),
    };

    assert_eq!(
        classify(&error),
        (ModuleCallError::Unavailable, Reason::ModuleUnavailable)
    );
}
