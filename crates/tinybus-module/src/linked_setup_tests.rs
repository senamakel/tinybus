//! Per-attachment stateful setup for generated linked module exports.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tinybus::broker::Broker;
use tinybus::module::{LinkedModule, ModuleHost, ModuleState};
use tinybus::transport::memory::MemoryBus;
use tinybus::{Connection, Interface};

static DEFAULT_SETUP_CALLS: AtomicUsize = AtomicUsize::new(0);

mod export {
    use super::*;

    async fn default_setup(_: Connection) -> tinybus::Result<()> {
        DEFAULT_SETUP_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(tinybus::Error::failed("default setup must be replaced"))
    }

    crate::module_export_static! {
        setup = default_setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.test.LinkedSetup"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

struct SetupValue(&'static str);

#[async_trait::async_trait]
impl Interface for SetupValue {
    fn name(&self) -> tinybus::InterfaceName {
        "org.example.LinkedSetupValue".parse().unwrap()
    }

    fn members(&self) -> Vec<tinybus::MemberName> {
        vec!["Value".parse().unwrap()]
    }

    async fn call(
        &self,
        _: &tinybus::MemberName,
        _: serde_json::Value,
    ) -> tinybus::Result<serde_json::Value> {
        Ok(serde_json::json!(self.0))
    }
}

fn linked_export(id: &str) -> LinkedModule {
    let mut module = export::linked_module().unwrap();
    module.descriptor.module_name = [0; 64];
    module.descriptor.module_name[..id.len()].copy_from_slice(id.as_bytes());
    module.manifest.module.name = id.to_string();
    module.manifest.bus_name = format!("ai.tinyhumans.test.{id}").parse().unwrap();
    module.manifest.object_path = format!("/ai/tinyhumans/test/{id}").parse().unwrap();
    module
}

async fn wait_for_owner(connection: &Connection, name: &str, expected: Option<tinybus::BusName>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if connection.name_owner(name).await.unwrap() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("module name ownership did not reach the expected state");
}

#[tokio::test]
async fn concurrent_linked_setups_replace_the_default_with_instance_bound_state() {
    DEFAULT_SETUP_CALLS.store(0, Ordering::SeqCst);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let setups = Arc::new(Mutex::new(
        Vec::<(String, &'static str, String, Connection)>::new(),
    ));

    let setup_a = setups.clone();
    let host_a = host.clone();
    let first = host.attach_linked_with_setup(
        linked_export("linked_a"),
        serde_json::json!({}),
        move |connection| async move {
            let peer = connection.unique_name().expect("connected module peer");
            assert_eq!(
                host_a
                    .module_for_sender(&peer)
                    .as_ref()
                    .map(tinybus::module::ModuleId::as_str),
                Some("linked_a")
            );
            connection
                .serve_at("/linked/setup".parse().unwrap(), SetupValue("a"))
                .await?;
            setup_a.lock().unwrap().push((
                "linked_a".to_string(),
                "a",
                peer.to_string(),
                connection,
            ));
            Ok(())
        },
    );

    let setup_b = setups.clone();
    let host_b = host.clone();
    let second = host.attach_linked_with_setup(
        linked_export("linked_b"),
        serde_json::json!({}),
        move |connection| async move {
            let peer = connection.unique_name().expect("connected module peer");
            assert_eq!(
                host_b
                    .module_for_sender(&peer)
                    .as_ref()
                    .map(tinybus::module::ModuleId::as_str),
                Some("linked_b")
            );
            connection
                .serve_at("/linked/setup".parse().unwrap(), SetupValue("b"))
                .await?;
            setup_b.lock().unwrap().push((
                "linked_b".to_string(),
                "b",
                peer.to_string(),
                connection,
            ));
            Ok(())
        },
    );

    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap().state, ModuleState::Ready);
    assert_eq!(second.unwrap().state, ModuleState::Ready);
    assert_eq!(DEFAULT_SETUP_CALLS.load(Ordering::SeqCst), 0);

    let setups = std::mem::take(&mut *setups.lock().unwrap());
    assert_eq!(setups.len(), 2);
    assert_ne!(setups[0].2, setups[1].2);
    for (id, expected, peer, connection) in &setups {
        let proxy = observer
            .proxy(peer, "/linked/setup", "org.example.LinkedSetupValue")
            .unwrap();
        let value: serde_json::Value = proxy.call("Value", ()).await.unwrap();
        assert_eq!(value, serde_json::json!(expected));
        assert_eq!(
            host.module_for_sender(&peer.parse().unwrap())
                .as_ref()
                .map(tinybus::module::ModuleId::as_str),
            Some(id.as_str())
        );

        let attestation = observer
            .attestation(format!("ai.tinyhumans.test.{id}").parse().unwrap())
            .await
            .unwrap()
            .expect("stateful linked setup keeps executable attestation");
        assert_eq!(
            attestation.sha256,
            tinybus::module::sha256_file(std::env::current_exe().unwrap()).unwrap()
        );
        assert!(connection.unique_name().is_some());
    }
    broker_task.abort();
}

#[tokio::test]
async fn a_failing_stateful_setup_faults_and_releases_its_module_peer() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module = linked_export("linked_failure");
    let bus_name = module.manifest.bus_name.clone();
    let result = host
        .attach_linked_with_setup(module, serde_json::json!({}), |connection| async move {
            connection
                .serve_at("/linked/partial".parse().unwrap(), SetupValue("partial"))
                .await?;
            Err(tinybus::Error::failed("stateful setup refused"))
        })
        .await;
    assert!(result.is_err());
    assert!(host.list().iter().any(|module| {
        module.manifest.bus_name == bus_name
            && matches!(
                module.state,
                ModuleState::Failed { .. } | ModuleState::Faulted { .. }
            )
    }));
    wait_for_owner(&observer, bus_name.as_str(), None).await;
    broker_task.abort();
}

#[tokio::test]
async fn linked_setup_admission_rejects_unmet_dependencies_before_running_the_callback() {
    let broker = Broker::new();
    let host = ModuleHost::new(broker);
    let mut module = linked_export("linked_requires_missing");
    module
        .manifest
        .requires
        .push(tinybus::module::manifest::Dependency {
            interface: tinybus::InterfaceVersion::consumed(
                "ai.tinyhumans.test.Missing".parse().unwrap(),
                tinybus::version::Version::new(1, 0, 0),
            ),
            optional: false,
            reason: String::new(),
        });
    let ran = Arc::new(AtomicUsize::new(0));
    let called = ran.clone();
    let result = host
        .attach_linked_with_setup(module, serde_json::json!({}), move |_| async move {
            called.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(host.list().is_empty());

    let mut invalid_descriptor = linked_export("linked_bad_descriptor");
    invalid_descriptor.descriptor.module_version = [0; 32];
    invalid_descriptor.descriptor.module_version[..3].copy_from_slice(b"9.9");
    let callback_ran = ran.clone();
    let result = host
        .attach_linked_with_setup(
            invalid_descriptor,
            serde_json::json!({}),
            move |_| async move {
                callback_ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(host.list().iter().any(|module| {
        module.name == "linked_bad_descriptor"
            && matches!(module.state, ModuleState::Rejected { .. })
    }));

    let mut invalid_identity = linked_export("linked_bad_identity");
    invalid_identity.manifest.module.name.push('\n');
    let callback_ran = Arc::new(AtomicUsize::new(0));
    let callback_count = callback_ran.clone();
    let result = host
        .attach_linked_with_setup(
            invalid_identity,
            serde_json::json!({}),
            move |_| async move {
                callback_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(callback_ran.load(Ordering::SeqCst), 0);
}
