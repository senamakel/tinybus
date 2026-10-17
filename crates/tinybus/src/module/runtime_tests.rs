use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn setup_runs_before_the_runtime_is_handed_out() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let setup: RuntimeSetup = Box::new(move |_connection, host| {
        Box::pin(async move {
            // Setup runs before the runtime is handed out, but it must already
            // have access to the loader so it can prepare host services before
            // module initialization starts.
            assert!(host.list().is_empty());
            flag.store(true, Ordering::SeqCst);
            Ok(())
        })
    });
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), Some(setup)).unwrap();
    assert!(ran.load(Ordering::SeqCst));
    assert!(runtime.host().list().is_empty());
    assert!(runtime.connection().unique_name().is_some());
}

#[tokio::test]
async fn a_failing_setup_fails_the_start() {
    let captured = Arc::new(std::sync::Mutex::new(None));
    let capture = captured.clone();
    let setup: RuntimeSetup = Box::new(move |connection, _host| {
        *capture.lock().unwrap() = Some(connection);
        Box::pin(async { Err(crate::Error::Transport("setup".to_string())) })
    });
    let result = ModuleRuntime::start(&RuntimeNames::default(), Some(setup));
    assert!(result.is_err());
    let connection = captured.lock().unwrap().take().unwrap();
    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        connection.list_names(),
    )
    .await;
    assert!(closed.is_ok_and(|result| result.is_err()));
}

#[tokio::test]
async fn proxies_validate_names_and_unloaded_modules_do_not_answer() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let proxy = runtime
        .proxy("ai.tinyhumans.test.Nothing", "/ai/tinyhumans/test/Nothing")
        .unwrap();
    let result: crate::Result<serde_json::Value> = proxy.call("Anything", ()).await;
    assert!(result.is_err(), "an unloaded module must not answer");
    assert!(runtime.proxy("not a bus name", "/nope").is_err());
}

#[tokio::test]
async fn work_outlives_the_caller_runtime_and_blocking_reports_errors() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let value = runtime.spawn(async { 7 }).await.unwrap();
    assert_eq!(value, 7);
    assert_eq!(runtime.blocking(|| Ok(())).await, Ok(()));
    assert_eq!(
        runtime.blocking(|| Err("refused".to_string())).await,
        Err("refused".to_string())
    );
    let panicked = runtime.blocking(|| panic!("loader panic")).await;
    assert!(panicked.unwrap_err().contains("did not finish"));
}
