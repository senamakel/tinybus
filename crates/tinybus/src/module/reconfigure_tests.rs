use super::*;
use crate::broker::Broker;
use crate::module::abi::{TB_BAD_ARGUMENT, TB_OK, TbAbiDescriptor, TbHostVtable, TbModuleVtable};
use crate::module::failure::{FailureReporter, FailureSink};
use crate::module::host::ModuleHost;
use crate::module::manifest::{MANIFEST_SCHEMA, ModuleIdentity, ModuleManifest, PanicPolicy};
use crate::module::{AccessFuture, ReleaseRecord};
use crate::name::{BusName, ObjectPath};
use crate::transport::memory::MemoryBus;
use crate::{Connection, Message, Proxy};
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

const ID: &str = "reconfigure-test-module";
const BUS: &str = "ai.tinyhumans.test.Reconfigure";
const PATH: &str = "/ai/tinyhumans/test/Reconfigure";
static RECORD: ReleaseRecord = ReleaseRecord {
    id: ID,
    bus_name: BUS,
    object_path: PATH,
    version: "0.0.0",
    release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
    assets: &[],
};
static FIXTURE_LOCK: Mutex<()> = Mutex::const_new(());

struct FixtureState {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
    config: Arc<StdMutex<serde_json::Value>>,
}

unsafe extern "C" fn fixture_deliver(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    if ctx.is_null() || ptr.is_null() {
        return crate::module::abi::TB_BAD_ARGUMENT;
    }
    let state = unsafe { &*ctx.cast::<FixtureState>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    match state.tx.try_send(bytes) {
        Ok(()) => TB_OK,
        Err(std::sync::mpsc::TrySendError::Full(_)) => crate::module::abi::TB_BACKPRESSURE,
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => crate::module::abi::TB_CLOSED,
    }
}

unsafe extern "C" fn fixture_reinitialize(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    if ctx.is_null() || (ptr.is_null() && len != 0) {
        return TB_BAD_ARGUMENT;
    }
    let config = if len == 0 {
        serde_json::Value::Null
    } else {
        match serde_json::from_slice(unsafe { std::slice::from_raw_parts(ptr, len) }) {
            Ok(config) => config,
            Err(_) => return TB_BAD_ARGUMENT,
        }
    };
    if config.get("reject").and_then(serde_json::Value::as_bool) == Some(true) {
        return TB_BAD_ARGUMENT;
    }
    let state = unsafe { &*ctx.cast::<FixtureState>() };
    *state.config.lock().expect("fixture config lock") = config;
    TB_OK
}

unsafe extern "C" fn fixture_shutdown(_: *mut c_void, _: u64) -> i32 {
    TB_OK
}

unsafe extern "C" fn fixture_init(host: *const TbHostVtable, out: *mut TbModuleVtable) -> i32 {
    if host.is_null() || out.is_null() {
        return TB_BAD_ARGUMENT;
    }
    let host = unsafe { *host };
    let host_ctx = host.host_ctx as usize;
    let send = host.send;
    let ready = host.ready;
    let config = Arc::new(StdMutex::new(serde_json::Value::Null));
    let service_config = config.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(16);
    std::thread::spawn(move || {
        unsafe { ready(host_ctx as *mut c_void) };
        while let Ok(bytes) = rx.recv() {
            let Ok(message) = serde_json::from_slice::<Message>(&bytes) else {
                continue;
            };
            if message.header.kind != crate::message::MessageKind::MethodCall {
                continue;
            }
            let member = message.header.member.as_ref().map(|name| name.as_str());
            let body = match member {
                Some("ReadConfig") => service_config.lock().unwrap().clone(),
                Some("Configure") => {
                    let config = message
                        .body
                        .as_array()
                        .and_then(|values| values.first())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    *service_config.lock().unwrap() = config.clone();
                    config
                }
                _ => serde_json::Value::Null,
            };
            let reply = Message::method_return(&message.header, body);
            let reply = serde_json::to_vec(&reply).expect("fixture reply serializes");
            let _ = unsafe { send(host_ctx as *mut c_void, reply.as_ptr(), reply.len()) };
        }
    });
    let state = Box::into_raw(Box::new(FixtureState { tx, config }));
    unsafe {
        *out = TbModuleVtable {
            size: std::mem::size_of::<TbModuleVtable>() as u32,
            _reserved: 0,
            module_ctx: state.cast(),
            deliver: fixture_deliver,
            shutdown: fixture_shutdown,
            reinitialize: Some(fixture_reinitialize),
        };
    }
    TB_OK
}

struct Quiet;
impl FailureSink for Quiet {
    fn capture(&self, _: &crate::module::failure::FailureReport<'_>) {}
}

struct FixtureAccess {
    host: Arc<ModuleHost>,
    connection: Connection,
    broker_task: tokio::task::JoinHandle<crate::Result<()>>,
    reporter: FailureReporter,
    reinitialize_calls: Arc<AtomicUsize>,
    initializing_left: AtomicUsize,
}

impl Drop for FixtureAccess {
    fn drop(&mut self) {
        self.broker_task.abort();
    }
}

impl ModuleAccess for FixtureAccess {
    fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
        (id == ID).then_some(&RECORD)
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
            self.connection
                .proxy(bus_name, object_path, bus_name)
                .map_err(|_| ProxyFailure::Contract)
        })
    }
    fn reinitialize<'a>(
        &'a self,
        id: &'a str,
        config: serde_json::Value,
    ) -> AccessFuture<'a, Result<(), ReconfigureFailure>> {
        Box::pin(async move {
            self.reinitialize_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .initializing_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    if left > 0 { Some(left - 1) } else { None }
                })
                .is_ok()
            {
                return Err(ReconfigureFailure::Initializing);
            }
            self.host
                .reinitialize(id, config)
                .await
                .map(|_| ())
                .map_err(|_| ReconfigureFailure::Failed)
        })
    }
}

fn fixture_manifest() -> ModuleManifest {
    ModuleManifest {
        schema: MANIFEST_SCHEMA,
        module: ModuleIdentity {
            name: ID.to_owned(),
            version: crate::version::Version::new(0, 0, 0),
            description: String::new(),
            homepage: None,
            license: String::new(),
        },
        bus_name: BusName::new(BUS).unwrap(),
        object_path: ObjectPath::new(PATH).unwrap(),
        provides: vec![],
        requires: vec![],
        environment: vec![],
        capabilities: vec![],
        lazy_init: false,
        worker_threads: 1,
        on_panic: PanicPolicy::Detach,
    }
}

async fn fixture() -> FixtureAccess {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    host.attach_linked_with_config(
        crate::module::LinkedModule {
            descriptor: TbAbiDescriptor::current(ID, "0.0.0"),
            manifest: fixture_manifest(),
            init: fixture_init,
        },
        serde_json::Value::Null,
    )
    .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    FixtureAccess {
        host,
        connection,
        broker_task,
        reporter: FailureReporter::new(Arc::new(Quiet)),
        reinitialize_calls: Arc::new(AtomicUsize::new(0)),
        initializing_left: AtomicUsize::new(0),
    }
}

async fn read_config(proxy: &Proxy) -> serde_json::Value {
    proxy.call("ReadConfig", ()).await.unwrap()
}

#[test]
fn fingerprints_retain_only_a_digest() {
    let config = serde_json::json!({ "token": "sensitive-value" });
    let fingerprint = ConfigFingerprint::of(&config).unwrap();
    assert_eq!(fingerprint.0.len(), 32);
    assert_eq!(fingerprint, ConfigFingerprint::of(&config).unwrap());
    assert_ne!(
        fingerprint,
        ConfigFingerprint::of(&serde_json::json!({})).unwrap()
    );
}

#[tokio::test]
async fn changed_configuration_reinitializes_and_equal_configuration_does_not() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID);
    let first = serde_json::json!({ "route": "one" });
    let proxy = module.proxy(&first).await.unwrap();
    assert_eq!(read_config(&proxy).await, first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let same = module
        .proxy(&serde_json::json!({ "route": "one" }))
        .await
        .unwrap();
    assert_eq!(
        read_config(&same).await,
        serde_json::json!({ "route": "one" })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let changed = serde_json::json!({ "route": "two" });
    let proxy = module.proxy(&changed).await.unwrap();
    assert_eq!(read_config(&proxy).await, changed);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn recorded_load_configuration_is_reused_until_invalidated() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID);
    module.record_loaded_with(&serde_json::Value::Null).unwrap();
    let proxy = module.proxy(&serde_json::Value::Null).await.unwrap();
    assert_eq!(read_config(&proxy).await, serde_json::Value::Null);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    module.invalidate();
    module.proxy(&serde_json::Value::Null).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failed_change_forgets_the_old_fingerprint_before_a_revert() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID);
    let original = serde_json::json!({ "route": "original" });
    module.proxy(&original).await.unwrap();
    let rejected = serde_json::json!({ "reject": true });
    assert_eq!(
        module.proxy(&rejected).await.unwrap_err(),
        ModuleCallError::ModuleFault
    );
    let reverted = module.proxy(&original).await.unwrap();
    assert_eq!(read_config(&reverted).await, original);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn typed_initializing_results_are_retried_with_the_config() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let mut access = fixture().await;
    access.initializing_left.store(2, Ordering::SeqCst);
    let calls = access.reinitialize_calls.clone();
    let module = ReconfigurableModule::new(access, ID).retry_while_initializing(3, Duration::ZERO);
    let config = serde_json::json!({ "route": "ready" });
    let proxy = module.proxy(&config).await.unwrap();
    assert_eq!(read_config(&proxy).await, config);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn member_strategy_applies_config_through_the_linked_fixture_bus() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let calls = access.reinitialize_calls.clone();
    let module =
        ReconfigurableModule::new(access, ID).with_strategy(Reconfigure::Member("Configure"));
    let config = serde_json::json!({ "route": "member" });
    let proxy = module.proxy(&config).await.unwrap();
    assert_eq!(read_config(&proxy).await, config);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_exclusive_guard_keeps_the_config_stable_until_the_bus_call_finishes() {
    let _test_guard = FIXTURE_LOCK.lock().await;
    let access = fixture().await;
    let module = Arc::new(ReconfigurableModule::new(access, ID));
    let first = serde_json::json!({ "route": "first" });
    let second = serde_json::json!({ "route": "second" });
    let (proxy_ready_tx, proxy_ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let first_module = module.clone();
    let first_config = first.clone();
    let first_task = tokio::spawn(async move {
        let guard = first_module.exclusive().await;
        let proxy = guard.proxy(&first_config).await.unwrap();
        proxy_ready_tx.send(()).unwrap();
        let _ = release_rx.await;
        read_config(&proxy).await
    });
    proxy_ready_rx.await.unwrap();
    let second_module = module.clone();
    let second_config = second.clone();
    let second_task =
        tokio::spawn(async move { second_module.proxy(&second_config).await.unwrap() });
    tokio::task::yield_now().await;
    release_tx.send(()).unwrap();
    assert_eq!(first_task.await.unwrap(), first);
    let second_proxy = second_task.await.unwrap();
    assert_eq!(read_config(&second_proxy).await, second);
}
