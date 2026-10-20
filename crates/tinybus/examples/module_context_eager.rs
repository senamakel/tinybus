//! Compiled migration fixture that requires the restricted host binding.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinybus::{CallContext, Error, Interface, InterfaceName, MemberName, Result};
use tinybus_module::{ModuleSetupContext, TrustedHost};

struct MigrationService {
    trusted_host: TrustedHost,
    schema_read: Arc<AtomicBool>,
    transformed: Arc<AtomicBool>,
    lease_registered: Arc<AtomicBool>,
}

#[async_trait]
impl Interface for MigrationService {
    fn name(&self) -> InterfaceName {
        "org.example.TrustedMigration".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![
            "Schema".parse().unwrap(),
            "Transform".parse().unwrap(),
            "RegisterOrdinaryLease".parse().unwrap(),
        ]
    }

    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("authenticated migration host required"))
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> Result<Value> {
        if !self.trusted_host.authorizes(context) {
            return Err(Error::failed("trusted migration host required"));
        }
        match member.as_str() {
            "Schema" if args == serde_json::json!([]) => {
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
            "RegisterOrdinaryLease" if args == serde_json::json!([]) => {
                if !self.transformed.load(Ordering::Acquire) {
                    return Err(Error::failed("migration must finish before lease registration"));
                }
                self.lease_registered.store(true, Ordering::Release);
                Ok(serde_json::json!(true))
            }
            _ => Err(Error::failed("unknown migration method")),
        }
    }
}

async fn setup(context: ModuleSetupContext) -> Result<()> {
    let trusted_host = context
        .trusted_host()
        .cloned()
        .ok_or_else(|| Error::failed("trusted migration host binding unavailable"))?;
    let connection = context.connection().clone();
    connection
        .serve_at(
            "/migration".parse()?,
            MigrationService {
                trusted_host,
                schema_read: Arc::new(AtomicBool::new(false)),
                transformed: Arc::new(AtomicBool::new(false)),
                lease_registered: Arc::new(AtomicBool::new(false)),
            },
        )
        .await?;
    connection.request_name("org.example.TrustedMigration").await
}

// This eager initializer waits for the module-side Hello and then lets setup
// fail closed if the loader omitted its trusted-host binding.
unsafe fn eager_start<F, Fut>(
    host: *const tinybus::module::abi::TbHostVtable,
    out: *mut tinybus::module::abi::TbModuleVtable,
    workers: usize,
    detach: bool,
    setup: F,
) -> i32
where
    F: FnOnce(ModuleSetupContext) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let mut table = unsafe { tinybus::module::abi::TbHostVtable::read_compatible(host) }.unwrap();
    // Eager activation has not attached its private transport to the broker.
    assert_eq!(unsafe { table.broker_routing.unwrap()(table.host_ctx) }, 0);
    SEND.set(table.send).unwrap();
    table.send = observe_send;
    let code = unsafe { tinybus_module::start_module_with_context(&table, out, workers, detach, setup) };
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
    provides = ["org.example.TrustedMigration"],
    methods = ["Schema", "Transform", "RegisterOrdinaryLease"],
    signals = [], requires = [], optional = [], lazy = false,
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
