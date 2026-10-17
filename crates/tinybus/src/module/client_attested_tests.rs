use super::*;

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
    strict_record_with_assets(vec![crate::module::ReleaseAsset {
        host_key,
        archive,
        sha256: archive_sha,
        library_sha256: Some(library_sha),
    }])
}

fn strict_record_with_assets(
    assets: Vec<crate::module::ReleaseAsset<'static>>,
) -> &'static ReleaseRecord {
    let assets = Box::leak(assets.into_boxed_slice());
    Box::leak(Box::new(ReleaseRecord {
        id: "attested-client-test",
        bus_name: "ai.tinyhumans.test.AttestedClient",
        object_path: "/ai/tinyhumans/test/AttestedClient",
        version: "1.0.0",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v1.0.0",
        assets,
    }))
}

#[cfg(feature = "modules")]
fn attested_asset(
    host_key: &'static str,
    suffix: &str,
    archive_digest: char,
    library_digest: char,
) -> crate::module::ReleaseAsset<'static> {
    crate::module::ReleaseAsset {
        host_key,
        archive: Box::leak(format!("attested-{suffix}.tar.gz").into_boxed_str()),
        sha256: Box::leak(archive_digest.to_string().repeat(64).into_boxed_str()),
        library_sha256: Some(Box::leak(
            library_digest.to_string().repeat(64).into_boxed_str(),
        )),
    }
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

struct PendingAccess(PreparedAccess);

impl ModuleAccess for PendingAccess {
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

    fn prepare_attested<'a>(&'a self, _: &'a str) -> AccessFuture<'a, Result<(), String>> {
        Box::pin(std::future::pending())
    }

    fn proxy<'a>(
        &'a self,
        bus_name: &'a str,
        object_path: &'a str,
    ) -> AccessFuture<'a, Result<Proxy, ProxyFailure>> {
        self.0.proxy(bus_name, object_path)
    }
}

#[cfg(feature = "modules")]
struct ReadinessAccess {
    record: &'static ReleaseRecord,
    connection: crate::Connection,
    reporter: FailureReporter,
    host: Arc<crate::module::ModuleHost>,
}

#[cfg(feature = "modules")]
impl ModuleAccess for ReadinessAccess {
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
        Box::pin(async move {
            self.host
                .ensure_ready_within(self.record.bus_name, Duration::from_millis(20))
                .await
                .map_err(|_| "module did not reach READY".to_string())
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

#[cfg(feature = "modules")]
unsafe extern "C" fn never_ready_deliver(_: *mut std::ffi::c_void, _: *const u8, _: usize) -> i32 {
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn no_op_lifecycle(_: *mut std::ffi::c_void, _: u64) -> i32 {
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn no_op_reinitialize(_: *mut std::ffi::c_void, _: *const u8, _: usize) -> i32 {
    crate::module::abi::TB_OK
}

#[cfg(feature = "modules")]
unsafe extern "C" fn never_ready_init(
    _: *const crate::module::abi::TbHostVtable,
    out: *mut crate::module::abi::TbModuleVtable,
) -> i32 {
    unsafe {
        *out = crate::module::abi::TbModuleVtable {
            size: std::mem::size_of::<crate::module::abi::TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: Box::into_raw(Box::new(0u8)).cast(),
            deliver: never_ready_deliver,
            shutdown: no_op_lifecycle,
            reinitialize: Some(no_op_reinitialize),
        };
    }
    crate::module::abi::TB_OK
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
        .call_attested_detailed(
            record.id,
            "Consume",
            SerializeCounter(serializations.clone()),
        )
        .await;
    assert_eq!(result.unwrap_err().kind, ModuleCallError::Unavailable);
    assert_eq!(serializations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::ModuleUnavailable)]
    );
    broker_task.abort();
}

#[tokio::test]
async fn a_short_client_deadline_bounds_readiness_before_argument_serialization() {
    let bus = crate::transport::memory::MemoryBus::new();
    let _broker = crate::broker::Broker::new().spawn(bus.clone());
    let connection = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let record = strict_record();
    let sink = Arc::new(Recording::default());
    let access = PendingAccess(PreparedAccess {
        record,
        connection,
        reporter: FailureReporter::new(sink.clone()),
        preparations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    });
    let client = ModuleClient::new(access).with_timeout(Duration::from_millis(10));
    let serializations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let result: Result<String, _> = client
        .call_attested_detailed(
            record.id,
            "Consume",
            SerializeCounter(serializations.clone()),
        )
        .await;
    assert_eq!(result.unwrap_err().kind, ModuleCallError::Unavailable);
    assert_eq!(serializations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::ModuleUnavailable)]
    );
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn a_lazy_module_that_never_announces_ready_fails_before_serializing_arguments() {
    use crate::module::manifest::{
        MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy, ProvidedInterface,
    };
    use crate::version::{InterfaceVersion, Version};

    let bus = crate::transport::memory::MemoryBus::new();
    let broker = crate::broker::Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = Arc::new(crate::module::ModuleHost::new(broker));
    let connection = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let record = strict_record();
    let version = Version::new(1, 0, 0);
    let manifest = ModuleManifest {
        schema: MANIFEST_SCHEMA,
        module: ModuleIdentity {
            name: "attested-client-test".to_string(),
            version: version.clone(),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: crate::BusName::new(record.bus_name).unwrap(),
        object_path: crate::ObjectPath::new(record.object_path).unwrap(),
        provides: vec![ProvidedInterface {
            version: InterfaceVersion::provided(
                crate::InterfaceName::new(record.bus_name).unwrap(),
                version,
            ),
            methods: vec![crate::MemberName::new("Consume").unwrap()],
            signals: Vec::new(),
        }],
        requires: Vec::new(),
        environment: Vec::new(),
        capabilities: Vec::new(),
        lazy_init: true,
        worker_threads: 1,
        on_panic: PanicPolicy::Detach,
    };
    unsafe {
        host.attach_raw(
            "attested-client.so",
            crate::module::abi::TbAbiDescriptor::current("attested-client-test", "1.0.0"),
            manifest,
            never_ready_init,
        )
    }
    .unwrap();
    let sink = Arc::new(Recording::default());
    let client = ModuleClient::new(ReadinessAccess {
        record,
        connection,
        reporter: FailureReporter::new(sink.clone()),
        host,
    });
    let serializations = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let result: Result<String, _> = client
        .call_attested_detailed(
            record.id,
            "Consume",
            SerializeCounter(serializations.clone()),
        )
        .await;
    assert_eq!(result.unwrap_err().kind, ModuleCallError::Unavailable);
    assert_eq!(serializations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::ModuleUnavailable)]
    );
}

struct AttestedReceiver {
    label: &'static str,
    calls: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[cfg(feature = "modules")]
struct AttestedFailureReceiver {
    name: &'static str,
    message: &'static str,
}

#[cfg(feature = "modules")]
#[async_trait::async_trait]
impl crate::Interface for AttestedFailureReceiver {
    fn name(&self) -> crate::InterfaceName {
        crate::InterfaceName::new("ai.tinyhumans.test.AttestedClient").unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec![crate::MemberName::new("Consume").unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Err(crate::Error::MethodFailed {
            name: self.name.to_string(),
            message: self.message.to_string(),
        })
    }
}

#[cfg(feature = "modules")]
async fn call_with_attestation_failure(
    name: &'static str,
    message: &'static str,
) -> (Result<String, ModuleCallFailure>, Result<String, ModuleCallError>) {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let record = strict_record();
    service
        .serve_at(
            crate::ObjectPath::new(record.object_path).unwrap(),
            AttestedFailureReceiver { name, message },
        )
        .await
        .unwrap();
    service.request_name(record.bus_name).await.unwrap();
    broker.attest_module(
        &service.unique_name().unwrap(),
        crate::attest::Attestation {
            name: crate::BusName::new(record.bus_name).unwrap(),
            sha256: record.assets[0].sha256.to_string(),
            library_sha256: record.assets[0].library_sha256.map(str::to_string),
        },
    );
    let access = PreparedAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(Arc::new(Recording::default())),
        preparations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let client = ModuleClient::new(access);
    let detailed = client
        .call_attested_detailed(record.id, "Consume", ())
        .await;
    let sanitized = client.call_attested(record.id, "Consume", ()).await;
    broker_task.abort();
    (detailed, sanitized)
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn strict_attested_detailed_calls_preserve_remote_input_refusals() {
    let (failure, sanitized) = call_with_attestation_failure(
        "ai.tinyhumans.tinywallet.Error.InvalidInput",
        "wallet address secret is invalid",
    )
    .await;
    let failure = failure.unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(failure.wire_name, "ai.tinyhumans.tinywallet.Error.InvalidInput");
    assert_eq!(failure.message, "wallet address secret is invalid");
    assert_eq!(sanitized, Err(ModuleCallError::ModuleFault));
    assert!(!format!("{failure:?}").contains("wallet address secret"));
    assert!(!failure.to_string().contains("wallet address secret"));
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn strict_attested_detailed_calls_preserve_remote_chain_refusals() {
    let (failure, sanitized) = call_with_attestation_failure(
        "ai.tinyhumans.tinywallet.Error.UnsupportedChain",
        "private configured chain is unsupported",
    )
    .await;
    let failure = failure.unwrap_err();
    assert_eq!(failure.kind, ModuleCallError::ModuleFault);
    assert_eq!(
        failure.wire_name,
        "ai.tinyhumans.tinywallet.Error.UnsupportedChain"
    );
    assert_eq!(failure.message, "private configured chain is unsupported");
    assert_eq!(sanitized, Err(ModuleCallError::ModuleFault));
    assert!(!format!("{failure:?}").contains("private configured chain"));
    assert!(!failure.to_string().contains("private configured chain"));
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

#[cfg(feature = "modules")]
struct AttestedCallObservation {
    result: Result<String, ModuleCallFailure>,
    serialized_arguments: usize,
    receiver_calls: usize,
}

#[cfg(feature = "modules")]
async fn call_with_attestation(
    record: &'static ReleaseRecord,
    archive_sha256: &str,
    library_sha256: &str,
) -> AttestedCallObservation {
    let bus = crate::transport::memory::MemoryBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let service = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = crate::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let receiver_calls = Arc::new(Mutex::new(Vec::new()));
    service
        .serve_at(
            crate::ObjectPath::new(record.object_path).unwrap(),
            AttestedReceiver {
                label: "attested",
                calls: receiver_calls.clone(),
            },
        )
        .await
        .unwrap();
    service.request_name(record.bus_name).await.unwrap();
    broker.attest_module(
        &service.unique_name().unwrap(),
        crate::attest::Attestation {
            name: crate::BusName::new(record.bus_name).unwrap(),
            sha256: archive_sha256.to_string(),
            library_sha256: Some(library_sha256.to_string()),
        },
    );
    let access = PreparedAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(Arc::new(Recording::default())),
        preparations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let serialized_arguments = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = ModuleClient::new(access)
        .call_attested_detailed(
            record.id,
            "Consume",
            SerializeCounter(serialized_arguments.clone()),
        )
        .await;
    broker_task.abort();
    AttestedCallObservation {
        result,
        serialized_arguments: serialized_arguments.load(std::sync::atomic::Ordering::SeqCst),
        receiver_calls: receiver_calls.lock().unwrap().len(),
    }
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn attested_calls_accept_a_verified_compatible_fallback_asset_pair() {
    let candidates = crate::module::platform::host_candidates();
    let first_key = Box::leak(candidates[0].clone().into_boxed_str());
    let fallback_key = Box::leak(candidates.last().unwrap().clone().into_boxed_str());
    // A one-key host models two byte pairs for one compatible target; hosts
    // with ordered fallback candidates exercise the real later candidate.
    let first = attested_asset(first_key, "preferred", 'a', 'b');
    let fallback = attested_asset(fallback_key, "fallback", 'c', 'd');
    let record = strict_record_with_assets(vec![first, fallback]);

    let observed =
        call_with_attestation(record, fallback.sha256, fallback.library_sha256.unwrap()).await;
    assert_eq!(observed.result, Ok("attested".to_string()));
    assert_eq!(observed.serialized_arguments, 1);
    assert_eq!(observed.receiver_calls, 1);
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn attested_calls_reject_archive_and_library_digests_crossed_between_assets() {
    let candidates = crate::module::platform::host_candidates();
    let first_key = Box::leak(candidates[0].clone().into_boxed_str());
    let fallback_key = Box::leak(candidates.last().unwrap().clone().into_boxed_str());
    let first = attested_asset(first_key, "preferred", 'a', 'b');
    let fallback = attested_asset(fallback_key, "fallback", 'c', 'd');
    let record = strict_record_with_assets(vec![first, fallback]);

    let observed =
        call_with_attestation(record, first.sha256, fallback.library_sha256.unwrap()).await;
    assert_eq!(
        observed.result.unwrap_err().kind,
        ModuleCallError::Unavailable
    );
    assert_eq!(observed.serialized_arguments, 0);
    assert_eq!(observed.receiver_calls, 0);
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn attested_calls_reject_a_post_sign_library_digest_not_in_the_release_pin() {
    let host_key = Box::leak(
        crate::module::platform::host_candidates()[0]
            .clone()
            .into_boxed_str(),
    );
    let release_asset = attested_asset(host_key, "notarized", 'a', 'b');
    let record = strict_record_with_assets(vec![release_asset]);
    let post_sign_library = "c".repeat(64);

    let observed = call_with_attestation(record, release_asset.sha256, &post_sign_library).await;
    assert_eq!(
        observed.result.unwrap_err().kind,
        ModuleCallError::Unavailable
    );
    assert_eq!(observed.serialized_arguments, 0);
    assert_eq!(observed.receiver_calls, 0);
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn attested_calls_refuse_post_sign_bytes_before_sending_confidential_arguments() {
    let host_key = Box::leak(
        crate::module::platform::host_candidates()[0]
            .clone()
            .into_boxed_str(),
    );
    let release_asset = attested_asset(host_key, "signed", 'a', 'b');
    let record = strict_record_with_assets(vec![release_asset]);
    let post_sign_library = "c".repeat(64);

    let observed = call_with_attestation(record, release_asset.sha256, &post_sign_library).await;
    assert_eq!(
        observed.result.unwrap_err().kind,
        ModuleCallError::Unavailable
    );
    assert_eq!(observed.serialized_arguments, 0);
    assert_eq!(observed.receiver_calls, 0);
}

#[cfg(feature = "modules")]
#[tokio::test]
async fn attested_calls_reject_a_valid_pair_for_an_unrelated_platform() {
    let compatible_key = Box::leak(
        crate::module::platform::host_candidates()[0]
            .clone()
            .into_boxed_str(),
    );
    let compatible = attested_asset(compatible_key, "compatible", 'a', 'b');
    let unrelated = attested_asset("unrelated-platform-key", "unrelated", 'c', 'd');
    let record = strict_record_with_assets(vec![compatible, unrelated]);

    let observed =
        call_with_attestation(record, unrelated.sha256, unrelated.library_sha256.unwrap()).await;
    assert_eq!(
        observed.result.unwrap_err().kind,
        ModuleCallError::Unavailable
    );
    assert_eq!(observed.serialized_arguments, 0);
    assert_eq!(observed.receiver_calls, 0);
}

#[cfg(feature = "modules")]
struct HandoverArgs {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    barrier: Arc<std::sync::Barrier>,
}

#[cfg(feature = "modules")]
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

#[cfg(feature = "modules")]
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
            .call_attested_detailed::<String>(record.id, "Consume", args)
            .await
    });
    started_rx.await.unwrap();
    service_a.release_name(record.bus_name).await.unwrap();
    service_b.request_name(record.bus_name).await.unwrap();
    barrier.wait();

    assert_eq!(call.await.unwrap().unwrap(), "attested".to_string());
    assert_eq!(preparations.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(calls_a.lock().unwrap().len(), 1);
    assert!(calls_b.lock().unwrap().is_empty());
    broker_task.abort();
}

#[path = "client_attested_timeout_tests.rs"]
mod timeout_tests;
