//! Native fixture exercising eager contextual dispatch and trusted migration setup.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinybus::{CallContext, Connection, Error, Interface, InterfaceName, MemberName, Result};
use tinybus_module::{ModuleSetupContext, TrustedHost};

struct ContextService {
    connection: Connection,
    signals: Arc<tokio::sync::Mutex<tokio::sync::broadcast::Receiver<tinybus::message::Message>>>,
    trusted_host: Option<TrustedHost>,
    schema_read: Arc<AtomicBool>,
    transformed: Arc<AtomicBool>,
    lease_registered: Arc<AtomicBool>,
}

#[async_trait]
impl Interface for ContextService {
    fn name(&self) -> InterfaceName {
        "org.example.Context".parse().unwrap()
    }
    fn members(&self) -> Vec<MemberName> {
        vec![
            "Inspect".parse().unwrap(),
            "Callback".parse().unwrap(),
            "ConfidentialCallback".parse().unwrap(),
            "WaitForWatchChanged".parse().unwrap(),
            "Schema".parse().unwrap(),
            "Transform".parse().unwrap(),
            "RegisterOrdinaryLease".parse().unwrap(),
        ]
    }
    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("authenticated incoming context required"))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> Result<Value> {
        let sender = context
            .authenticated_sender()
            .ok_or_else(|| Error::failed("unverified caller"))?;
        if args != serde_json::json!([]) && member.as_str() != "Transform" {
            return Err(Error::failed("expected no arguments"));
        }
        match member.as_str() {
            "Inspect" => Ok(serde_json::json!(sender)),
            "Callback" | "ConfidentialCallback" => {
                let proxy = self
                    .connection
                    .proxy(sender.as_str(), "/callback", "org.example.Callback")?;
                let reply: String = if member.as_str() == "ConfidentialCallback" {
                    proxy.call_confidential("Observe", ()).await?
                } else {
                    proxy.call("Observe", ()).await?
                };
                Ok(serde_json::json!([sender, reply]))
            }
            "WaitForWatchChanged" => {
                let message = self
                    .signals
                    .lock()
                    .await
                    .recv()
                    .await
                    .map_err(|_| Error::failed("directed signal receiver closed"))?;
                Ok(message.body)
            }
            "Schema" | "Transform" | "RegisterOrdinaryLease" => {
                let trusted_host = self
                    .trusted_host
                    .as_ref()
                    .ok_or_else(|| Error::failed("trusted migration host unavailable"))?;
                if !trusted_host.authorizes(context) {
                    return Err(Error::failed("trusted migration host required"));
                }
                match member.as_str() {
                    "Schema" => {
                        self.schema_read.store(true, Ordering::Release);
                        Ok(serde_json::json!({
                            "type": "object",
                            "required": ["id"],
                            "properties": {"id": {"type": "string"}}
                        }))
                    }
                    "Transform" => {
                        let Some(id) = args.as_array().and_then(|values| values.first())
                            .and_then(Value::as_str)
                        else {
                            return Err(Error::failed("migration transform input is invalid"));
                        };
                        if !self.schema_read.load(Ordering::Acquire)
                            || self.lease_registered.load(Ordering::Acquire)
                        {
                            return Err(Error::failed("migration ordering is invalid"));
                        }
                        self.transformed.store(true, Ordering::Release);
                        Ok(serde_json::json!({
                            "id": format!("record-{id}"),
                            "ordinary_lease_registered": false
                        }))
                    }
                    "RegisterOrdinaryLease" => {
                        if !self.transformed.load(Ordering::Acquire) {
                            return Err(Error::failed(
                                "migration must finish before lease registration",
                            ));
                        }
                        self.lease_registered.store(true, Ordering::Release);
                        Ok(serde_json::json!(true))
                    }
                    _ => unreachable!(),
                }
            }
            _ => Err(Error::failed("unknown method")),
        }
    }
}

async fn setup(context: ModuleSetupContext, require_trusted_host: bool) -> Result<()> {
    let trusted_host = context.trusted_host().cloned();
    if require_trusted_host && trusted_host.is_none() {
        return Err(Error::failed("trusted migration host binding unavailable"));
    }
    let connection = context.connection().clone();
    let signals = connection
        .proxy("org.example.Context", "/context", "org.example.Context")?
        .receive_directed_signal("WatchChanged")
        .await?;
    connection
        .serve_at(
            "/context".parse()?,
            ContextService {
                connection: connection.clone(),
                signals: Arc::new(tokio::sync::Mutex::new(signals)),
                trusted_host,
                schema_read: Arc::new(AtomicBool::new(false)),
                transformed: Arc::new(AtomicBool::new(false)),
                lease_registered: Arc::new(AtomicBool::new(false)),
            },
        )
        .await?;
    connection.request_name("org.example.Context").await
}

// This eager initializer waits for module-side Hello and forwards its test-only
// configuration into setup. Normal context tests use the default false mode.
unsafe fn eager_start<F, Fut>(
    host: *const tinybus::module::abi::TbHostVtable,
    out: *mut tinybus::module::abi::TbModuleVtable,
    workers: usize,
    detach: bool,
    setup: F,
) -> i32
where
    F: FnOnce(ModuleSetupContext, bool) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let mut table = unsafe { tinybus::module::abi::TbHostVtable::read_compatible(host) }.unwrap();
    // Eager activation has not attached its private transport to the broker.
    assert_eq!(unsafe { table.broker_routing.unwrap()(table.host_ctx) }, 0);
    let config_bytes = unsafe { std::slice::from_raw_parts(table.config.ptr, table.config.len) };
    let require_trusted_host = serde_json::from_slice::<Value>(config_bytes)
        .ok()
        .and_then(|value| value.get("require_trusted_host").and_then(Value::as_bool))
        .unwrap_or(false);
    SEND.set(table.send).unwrap();
    table.send = observe_send;
    let code = unsafe {
        tinybus_module::start_module_with_context(&table, out, workers, detach, move |context| {
            setup(context, require_trusted_host)
        })
    };
    if code == tinybus::module::abi::TB_OK {
        let hello = HELLO.0.lock().unwrap();
        let (hello, timeout) = HELLO
            .1
            .wait_timeout_while(hello, std::time::Duration::from_secs(10), |sent| !*sent)
            .unwrap();
        assert!(
            *hello && !timeout.timed_out(),
            "SDK must send Hello before admission"
        );
    }
    code
}

tinybus_module::module_export! {
    @plain export = {#[unsafe(no_mangle)]},
    start = eager_start,
    setup = setup,
    worker_threads = 1,
    provides = ["org.example.Context"],
    methods = [
        "Inspect", "Callback", "ConfidentialCallback", "WaitForWatchChanged",
        "Schema", "Transform", "RegisterOrdinaryLease"
    ],
    signals = ["WatchChanged"], requires = [], optional = [], lazy = true,
}

// The initializer cannot return (and therefore cannot be admitted) until the
// independent SDK worker has sent Hello. This forces the eager race without
// assuming which thread the scheduler runs first.
static HELLO: (std::sync::Mutex<bool>, std::sync::Condvar) =
    (std::sync::Mutex::new(false), std::sync::Condvar::new());
static SEND: std::sync::OnceLock<
    unsafe extern "C" fn(*mut std::ffi::c_void, *const u8, usize) -> i32,
> = std::sync::OnceLock::new();

unsafe extern "C" fn observe_send(ctx: *mut std::ffi::c_void, ptr: *const u8, len: usize) -> i32 {
    let code = unsafe { SEND.get().unwrap()(ctx, ptr, len) };
    *HELLO.0.lock().unwrap() = true;
    HELLO.1.notify_one();
    code
}
