use super::*;
use crate::module::failure::{FailureReport, FailureReporter, FailureSink};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Recording(Mutex<Vec<(String, Reason)>>);

impl FailureSink for Recording {
    fn capture(&self, report: &FailureReport<'_>) {
        self.0
            .lock()
            .unwrap()
            .push((report.module.to_string(), report.reason));
    }
}

static RECORD: ReleaseRecord = ReleaseRecord {
    id: "client-test-module",
    bus_name: "ai.tinyhumans.test.ClientModule",
    object_path: "/ai/tinyhumans/test/ClientModule",
    version: "0.0.0",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
    assets: &[],
};

/// A host without the loader: it knows its registry and refuses every load.
struct Loaderless {
    enabled: bool,
    reporter: FailureReporter,
}

impl ModuleAccess for Loaderless {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        let _ = id;
        Box::pin(async {
            self.reporter()
                .report(RECORD.id, RECORD.version, Reason::LoaderDisabled);
            Err("this build has no module loader".to_string())
        })
    }

    fn proxy<'a>(
        &'a self,
        _bus_name: &'a str,
        _object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        Box::pin(async { Err(ProxyFailure::Transport) })
    }
}

struct ReadyAccess {
    connection: crate::Connection,
    reporter: FailureReporter,
    proxy_failure: Option<ProxyFailure>,
}

impl ModuleAccess for ReadyAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == RECORD.id).then_some(&RECORD)
    }

    fn enabled(&self) -> bool {
        true
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
            if let Some(failure) = self.proxy_failure {
                return Err(failure);
            }
            self.connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)
        })
    }
}

fn resolver(enabled: bool) -> (Arc<Recording>, Loaderless) {
    let sink = Arc::new(Recording::default());
    let access = Loaderless {
        enabled,
        reporter: FailureReporter::new(sink.clone()),
    };
    (sink, access)
}

#[tokio::test]
async fn lifecycle_defaults_keep_loaderless_access_source_compatible() {
    let (_, access) = resolver(true);
    assert!(access.prepare_attested(RECORD.id).await.is_err());
    assert_eq!(
        access
            .reinitialize(RECORD.id, serde_json::Value::Null)
            .await,
        Err(ReconfigureFailure::Unsupported)
    );
    access.mark_faulted(RECORD.id);
}

fn method_failed(name: &str) -> Error {
    Error::MethodFailed {
        name: name.to_string(),
        message: String::new(),
    }
}

#[test]
fn bus_errors_classify_to_closed_outcomes() {
    let cases = [
        (
            Error::ModuleUnavailable {
                module: "m".to_string(),
                state: String::new(),
                detail: String::new(),
            },
            ModuleCallError::Unavailable,
            Reason::ModuleUnavailable,
        ),
        (
            method_failed("ai.tinyhumans.tinybus.Error.ModuleUnavailable"),
            ModuleCallError::Unavailable,
            Reason::ModuleUnavailable,
        ),
        (
            method_failed(Error::UNKNOWN_METHOD),
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
        (
            Error::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
        (
            method_failed(Error::NOT_ATTESTED),
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
        (
            method_failed("com.example.Whatever"),
            ModuleCallError::ModuleFault,
            Reason::ModuleFault,
        ),
        (
            Error::Transport("down".to_string()),
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
    ];
    for (error, outcome, reason) in cases {
        assert_eq!(classify(&error), (outcome, reason), "{error:?}");
    }
}

#[test]
fn wire_kind_is_the_closed_classification_not_the_wire_error_name() {
    let error = method_failed("com.example.PrivateFailure");
    assert_eq!(wire_kind(&error), ModuleCallError::ModuleFault);
}

#[test]
fn unavailable_proxy_failures_are_deduplicated_as_terminal_module_state() {
    let sink = Arc::new(Recording::default());
    let reporter = FailureReporter::new(sink.clone());
    let error = Error::ModuleUnavailable {
        module: RECORD.id.to_owned(),
        state: "faulted".to_owned(),
        detail: "module is faulted".to_owned(),
    };
    assert_eq!(
        report_proxy_failure(&reporter, &RECORD, &error),
        ModuleCallError::Unavailable
    );
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleUnavailable)]
    );
}

#[test]
fn call_errors_render_the_already_reported_prefix() {
    for error in [
        ModuleCallError::Unavailable,
        ModuleCallError::IncompatibleContract,
        ModuleCallError::TransportFailed,
        ModuleCallError::ModuleFault,
    ] {
        assert!(
            error
                .to_string()
                .starts_with("MODULE_CALL_REPORTED: module ")
        );
    }
}

#[tokio::test]
async fn an_unknown_module_is_unavailable_and_reported_anonymously() {
    let (sink, resolver) = resolver(true);
    let client = ModuleClient::new(resolver);
    let result: Result<String, _> = client.call("not-registered", "Member", ()).await;
    assert_eq!(result, Err(ModuleCallError::Unavailable));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[("unregistered".to_string(), Reason::UnknownModule)]
    );
}

#[tokio::test]
async fn a_disabled_registry_refuses_without_starting_anything() {
    let (sink, resolver) = resolver(false);
    let client = ModuleClient::new(resolver);
    let result: Result<String, _> = client.call(RECORD.id, "Member", ()).await;
    assert_eq!(result, Err(ModuleCallError::Unavailable));
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::Disabled)]
    );
}

#[test]
fn malformed_json_is_a_module_fault_reported_by_identity_only() {
    let (sink, resolver) = resolver(true);
    let client = ModuleClient::new(resolver);
    let decoded: Result<serde_json::Value, _> = client.decode_json(RECORD.id, "not json {");
    assert_eq!(decoded, Err(ModuleCallError::ModuleFault));
    let decoded: Result<serde_json::Value, _> = client.decode_json(RECORD.id, "{\"a\":1}");
    assert!(decoded.is_ok());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleFault)]
    );
}

#[tokio::test]
async fn a_fixture_proxy_is_classified_and_reported_when_the_call_fails() {
    use crate::broker::Broker;
    use crate::transport::memory::MemoryBus;

    let (sink, resolver) = resolver(true);
    let transport = MemoryBus::new();
    Broker::new().spawn(transport.clone());
    let connection = crate::Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = connection
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(resolver)
        .with_fixture(proxy)
        .with_timeout(Duration::from_millis(500));
    assert!(client.fixture_proxy().is_some());
    let result: Result<String, _> = client.call(RECORD.id, "Member", ()).await;
    assert!(result.is_err());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::ModuleFault)]
    );
}

struct DetailedFailure;

#[async_trait::async_trait]
impl crate::Interface for DetailedFailure {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(RECORD.bus_name).unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Fail").unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(Error::MethodFailed {
            name: "com.example.PrivateFailure".to_string(),
            message: "private remote detail".to_string(),
        })
    }
}

#[tokio::test]
async fn detailed_call_preserves_remote_failure_without_rendering_it() {
    let sink = Arc::new(Recording::default());
    let transport = crate::transport::memory::MemoryBus::new();
    let _broker = crate::broker::Broker::new().spawn(transport.clone());
    let service = crate::Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    service
        .serve_at(
            crate::ObjectPath::new(RECORD.object_path).unwrap(),
            DetailedFailure,
        )
        .await
        .unwrap();
    let caller = crate::Connection::connect(transport.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(ReadyAccess {
        connection: caller,
        reporter: FailureReporter::new(sink.clone()),
        proxy_failure: None,
    })
    .with_fixture(proxy);

    let failure = client
        .call_detailed::<String>(RECORD.id, "Fail", ())
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(failure.wire_name, "com.example.PrivateFailure");
    assert_eq!(failure.message, "private remote detail");
    for rendered in [format!("{failure:?}"), failure.to_string()] {
        assert!(!rendered.contains("com.example.PrivateFailure"));
        assert!(!rendered.contains("private remote detail"));
    }
    assert_eq!(sink.0.lock().unwrap().len(), 1);
}

fn strict_record() -> &'static ReleaseRecord {
    let host_key = Box::leak(
        crate::module::platform::host_candidates()
            .into_iter()
            .next()
            .expect("the test host has a published module key")
            .into_boxed_str(),
    );
    let archive = Box::leak("attested-test.tar.gz".to_string().into_boxed_str());
    let archive_sha = Box::leak("a".repeat(64).into_boxed_str());
    let library_sha = Box::leak("b".repeat(64).into_boxed_str());
    let assets = Box::leak(
        vec![crate::module::ReleaseAsset {
            host_key,
            archive,
            sha256: archive_sha,
            library_sha256: Some(library_sha),
        }]
        .into_boxed_slice(),
    );
    Box::leak(Box::new(ReleaseRecord {
        id: "attested-client-test",
        bus_name: "ai.tinyhumans.test.AttestedClient",
        object_path: "/ai/tinyhumans/test/AttestedClient",
        version: "1.0.0",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v1.0.0",
        assets,
    }))
}

struct PreparedAccess {
    record: &'static ReleaseRecord,
    connection: crate::Connection,
    reporter: FailureReporter,
    preparations: Arc<std::sync::atomic::AtomicUsize>,
}

impl ModuleAccess for PreparedAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == self.record.id).then_some(self.record)
    }

    fn enabled(&self) -> bool {
        true
    }

    fn reporter(&self) -> &FailureReporter {
        &self.reporter
    }

    fn ensure_loaded<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_attested<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        self.preparations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

struct UnpreparedAccess(PreparedAccess);

impl ModuleAccess for UnpreparedAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        self.0.find(id)
    }

    fn enabled(&self) -> bool {
        self.0.enabled()
    }

    fn reporter(&self) -> &FailureReporter {
        self.0.reporter()
    }

    fn ensure_loaded<'a>(&'a self, id: &'a str) -> AccessFuture<'a, Result<(), String>> {
        self.0.ensure_loaded(id)
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        self.0.proxy(bus_name, object_path)
    }
}

struct SerializeCounter(Arc<std::sync::atomic::AtomicUsize>);

impl serde::Serialize for SerializeCounter {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        serializer.serialize_unit()
    }
}

#[tokio::test]
async fn an_unprepared_custom_access_refuses_before_serializing_confidential_arguments() {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let connection = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let record = strict_record();
    let sink = Arc::new(Recording::default());
    let preparations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let access = UnpreparedAccess(PreparedAccess {
        record,
        connection,
        reporter: FailureReporter::new(sink.clone()),
        preparations,
    });
    let client = ModuleClient::new(access);
    let serializations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let result: Result<String, _> = client
        .call_attested(
            record.id,
            "Consume",
            SerializeCounter(serializations.clone()),
        )
        .await;
    assert_eq!(result, Err(ModuleCallError::Unavailable));
    assert_eq!(serializations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::ModuleUnavailable)]
    );
    broker_task.abort();
}

struct AttestedReceiver {
    label: &'static str,
    calls: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[async_trait::async_trait]
impl crate::Interface for AttestedReceiver {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new("ai.tinyhumans.test.AttestedClient").unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Consume").unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        args: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        self.calls.lock().unwrap().push(args);
        Ok(serde_json::json!(self.label))
    }
}

struct HandoverArgs {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    barrier: Arc<std::sync::Barrier>,
}

impl serde::Serialize for HandoverArgs {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        self.barrier.wait();
        serializer.serialize_unit()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_confidential_call_stays_on_the_attested_unique_peer_after_name_handover() {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let service_a = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_b = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let record = strict_record();
    let calls_a = Arc::new(Mutex::new(Vec::new()));
    let calls_b = Arc::new(Mutex::new(Vec::new()));
    for (service, label, calls) in [
        (&service_a, "attested", calls_a.clone()),
        (&service_b, "successor", calls_b.clone()),
    ] {
        service
            .serve_at(
                crate::ObjectPath::new(record.object_path).unwrap(),
                AttestedReceiver { label, calls },
            )
            .await
            .unwrap();
    }
    service_a.request_name(record.bus_name).await.unwrap();
    broker.attest_module(
        &service_a.unique_name().unwrap(),
        crate::attest::Attestation {
            name: crate::BusName::new(record.bus_name).unwrap(),
            sha256: record.assets[0].sha256.to_string(),
            library_sha256: record.assets[0].library_sha256.map(str::to_string),
        },
    );
    let sink = Arc::new(Recording::default());
    let preparations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let access = PreparedAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(sink),
        preparations: preparations.clone(),
    };
    let client = ModuleClient::new(access);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let args = HandoverArgs {
        started: Mutex::new(Some(started_tx)),
        barrier: barrier.clone(),
    };
    let call = tokio::spawn(async move {
        client
            .call_attested::<String>(record.id, "Consume", args)
            .await
    });
    started_rx.await.unwrap();
    service_a.release_name(record.bus_name).await.unwrap();
    service_b.request_name(record.bus_name).await.unwrap();
    barrier.wait();

    assert_eq!(call.await.unwrap(), Ok("attested".to_string()));
    assert_eq!(preparations.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(calls_a.lock().unwrap().len(), 1);
    assert!(calls_b.lock().unwrap().is_empty());
    broker_task.abort();
}

struct JsonFixture;
#[async_trait::async_trait]
impl crate::Interface for JsonFixture {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new(RECORD.bus_name).unwrap()
    }
    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Json").unwrap()]
    }
    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!("{\"answer\":42}"))
    }
}

#[tokio::test]
async fn fixture_json_and_object_calls_preserve_deadlines_and_confidential_refusals() {
    let (sink, resolver) = resolver(true);
    let bus = crate::transport::memory::MemoryBus::new();
    let _broker = crate::broker::Broker::new();
    let _broker_task = _broker.clone().spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    service.request_name(RECORD.bus_name).await.unwrap();
    for path in [RECORD.object_path, "/child"] {
        service
            .serve_at(crate::ObjectPath::new(path).unwrap(), JsonFixture)
            .await
            .unwrap();
    }
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = caller
        .proxy(RECORD.bus_name, RECORD.object_path, RECORD.bus_name)
        .unwrap();
    let client = ModuleClient::new(resolver)
        .with_fixture(proxy)
        .with_timeout(Duration::from_secs(1));
    let reply: serde_json::Value = client.call_json(RECORD.id, "Json", ()).await.unwrap();
    assert_eq!(reply, serde_json::json!({"answer":42}));
    let object = client.object_client(RECORD.id, "/child").await.unwrap();
    let reply: String = object.call("Json", ()).await.unwrap();
    assert_eq!(reply, "{\"answer\":42}");

    let direct_sink = Arc::new(Recording::default());
    let direct = ModuleClient::new(ReadyAccess {
        connection: caller.clone(),
        reporter: FailureReporter::new(direct_sink.clone()),
        proxy_failure: None,
    });
    let direct_object = direct.object_client(RECORD.id, "/child").await.unwrap();
    let reply: String = direct_object.call("Json", ()).await.unwrap();
    assert_eq!(reply, "{\"answer\":42}");

    for (failure, expected, reason) in [
        (
            ProxyFailure::Transport,
            ModuleCallError::TransportFailed,
            Reason::TransportFailed,
        ),
        (
            ProxyFailure::Contract,
            ModuleCallError::IncompatibleContract,
            Reason::IncompatibleContract,
        ),
    ] {
        let reporter = Arc::new(Recording::default());
        let failing = ModuleClient::new(ReadyAccess {
            connection: caller.clone(),
            reporter: FailureReporter::new(reporter.clone()),
            proxy_failure: Some(failure),
        });
        let result: Result<String, _> = failing.call(RECORD.id, "Json", ()).await;
        assert_eq!(result, Err(expected));
        assert_eq!(
            reporter.0.lock().unwrap().as_slice(),
            &[(RECORD.id.to_string(), reason)]
        );
    }
    assert!(
        direct
            .object_client(RECORD.id, "invalid path")
            .await
            .is_err()
    );
    assert_eq!(
        direct_sink.0.lock().unwrap().as_slice(),
        &[(RECORD.id.to_string(), Reason::IncompatibleContract)]
    );
    let reply: Result<String, _> = object.call_confidential("Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::TransportFailed));
    let reply: Result<String, _> = client.call_confidential(RECORD.id, "Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::TransportFailed));
    let reply: Result<serde_json::Value, _> =
        client.call_confidential_json(RECORD.id, "Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::TransportFailed));
    assert!(
        sink.0
            .lock()
            .unwrap()
            .iter()
            .all(|(_, reason)| *reason == Reason::TransportFailed)
    );

    #[cfg(feature = "modules")]
    {
        _broker.attest_module(
            &service.unique_name().unwrap(),
            crate::attest::Attestation {
                name: crate::BusName::new(RECORD.bus_name).unwrap(),
                sha256: "a".repeat(64),
                library_sha256: None,
            },
        );
        let reply: serde_json::Value = client
            .call_confidential_json(RECORD.id, "Json", ())
            .await
            .unwrap();
        assert_eq!(reply, serde_json::json!({ "answer": 42 }));
    }
}

#[tokio::test]
async fn unavailable_loads_and_explicit_reports_use_only_registered_identity() {
    let (sink, resolver) = resolver(true);
    let client = ModuleClient::new(resolver);
    let reply: Result<String, _> = client.call(RECORD.id, "Json", ()).await;
    assert_eq!(reply, Err(ModuleCallError::Unavailable));
    assert!(client.object_client(RECORD.id, "/child").await.is_err());
    client.report_incompatible_contract(RECORD.id);
    client.report_unavailable(RECORD.id);
    client.report_malformed_reply("unknown");
    client.report_incompatible_contract("unknown");
    client.report_unavailable("unknown");
    assert!(
        sink.0
            .lock()
            .unwrap()
            .iter()
            .all(|(module, _)| module == RECORD.id)
    );
}
