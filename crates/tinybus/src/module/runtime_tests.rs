use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct HostObjectValue(&'static str);

#[async_trait::async_trait]
impl crate::Interface for HostObjectValue {
    fn name(&self) -> crate::InterfaceName {
        "org.example.ModuleRuntimeHostObject".parse().unwrap()
    }

    fn members(&self) -> Vec<crate::MemberName> {
        vec!["Value".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &crate::MemberName,
        _: serde_json::Value,
    ) -> crate::Result<serde_json::Value> {
        Ok(serde_json::json!(self.0))
    }
}

async fn wait_for_owner(connection: &Connection, name: &str, expected: Option<crate::BusName>) {
    for _ in 0..1_000 {
        if connection.name_owner(name).await.unwrap() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("bus name owner did not reach the expected state");
}

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
    let closed =
        tokio::time::timeout(std::time::Duration::from_secs(1), connection.list_names()).await;
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

#[tokio::test]
async fn host_object_guards_share_names_and_only_remove_their_owned_registration() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.RuntimeHostObject".parse().unwrap();
    let first_path: crate::ObjectPath = "/runtime/first".parse().unwrap();
    let second_path: crate::ObjectPath = "/runtime/second".parse().unwrap();
    let first = runtime
        .serve_host_object(name.clone(), first_path.clone(), HostObjectValue("old"))
        .await
        .unwrap();
    let second = runtime
        .serve_host_object(name.clone(), second_path.clone(), HostObjectValue("second"))
        .await
        .unwrap();
    let replacement = runtime
        .serve_host_object(name.clone(), first_path.clone(), HostObjectValue("new"))
        .await
        .unwrap();

    drop(first);
    let old_guard_proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            first_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let replaced: serde_json::Value = old_guard_proxy.call("Value", ()).await.unwrap();
    assert_eq!(replaced, serde_json::json!("new"));
    wait_for_owner(
        runtime.connection(),
        name.as_str(),
        runtime.connection().unique_name(),
    )
    .await;

    drop(second);
    let remaining_proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            second_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let remaining: serde_json::Value = remaining_proxy.call("Value", ()).await.unwrap();
    assert_eq!(remaining, serde_json::json!("second"));

    runtime
        .connection()
        .serve_at(first_path.clone(), HostObjectValue("direct replacement"))
        .await
        .unwrap();
    drop(replacement);
    let direct_replacement = runtime
        .connection()
        .proxy(
            runtime.connection().unique_name().unwrap().as_str(),
            first_path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let value: serde_json::Value = direct_replacement.call("Value", ()).await.unwrap();
    assert_eq!(value, serde_json::json!("direct replacement"));
    wait_for_owner(
        runtime.connection(),
        name.as_str(),
        runtime.connection().unique_name(),
    )
    .await;

    drop(second);
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
    let removed: crate::Result<serde_json::Value> = remaining_proxy.call("Value", ()).await;
    assert!(removed.is_err());
    let direct: serde_json::Value = direct_replacement.call("Value", ()).await.unwrap();
    assert_eq!(direct, serde_json::json!("direct replacement"));
}

#[tokio::test]
async fn dropping_a_host_object_guard_outside_a_tokio_runtime_releases_its_name() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.DropOutsideRuntime".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/drop-outside".parse().unwrap();
    let guard = runtime
        .serve_host_object(name.clone(), path, HostObjectValue("value"))
        .await
        .unwrap();
    std::thread::spawn(move || drop(guard)).join().unwrap();
    wait_for_owner(runtime.connection(), name.as_str(), None).await;
}

#[tokio::test]
async fn dropping_a_host_object_guard_preserves_a_name_owned_before_registration() {
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), None).unwrap();
    let name: crate::BusName = "ai.tinyhumans.test.PreownedHostObject".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/preowned".parse().unwrap();
    runtime
        .connection()
        .request_name(name.as_str())
        .await
        .unwrap();
    let owner = runtime.connection().unique_name();
    let guard = runtime
        .serve_host_object(name.clone(), path, HostObjectValue("value"))
        .await
        .unwrap();
    drop(guard);
    wait_for_owner(runtime.connection(), name.as_str(), owner).await;
}

#[tokio::test]
async fn a_name_collision_leaves_no_host_object_registration_behind() {
    let name: crate::BusName = "ai.tinyhumans.test.HostObjectCollision".parse().unwrap();
    let path: crate::ObjectPath = "/runtime/collision".parse().unwrap();
    let other_peer = Arc::new(std::sync::Mutex::new(None));
    let captured = other_peer.clone();
    let setup_name = name.clone();
    let setup: RuntimeSetup = Box::new(move |connection, _host| {
        Box::pin(async move {
            connection.request_name(setup_name.as_str()).await?;
            *captured.lock().unwrap() = Some(connection);
            Ok(())
        })
    });
    let runtime = ModuleRuntime::start(&RuntimeNames::default(), Some(setup)).unwrap();
    let owner = other_peer.lock().unwrap().as_ref().unwrap().unique_name();
    assert!(
        runtime
            .serve_host_object(name.clone(), path.clone(), HostObjectValue("must not leak"))
            .await
            .is_err()
    );
    wait_for_owner(runtime.connection(), name.as_str(), owner).await;
    let proxy = runtime
        .connection()
        .proxy(
            name.as_str(),
            path.as_str(),
            "org.example.ModuleRuntimeHostObject",
        )
        .unwrap();
    let leaked: crate::Result<serde_json::Value> = proxy.call("Value", ()).await;
    assert!(leaked.is_err());
}
