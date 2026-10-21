use std::time::Duration;

use async_trait::async_trait;
use tinybus::broker::Broker;
use tinybus::module::{LinkedModule, ModuleHost, PreReadyConfidentialRoute};
use tinybus::transport::memory::{MemoryBus, MemoryTransport};
use tinybus::{Connection, Error, Interface, InterfaceName, MemberName, ObjectPath};

mod fixture {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.module.PreReadyRouteFixture"],
        methods = ["GetSchema"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

struct MigrationEndpoint;

#[async_trait]
impl Interface for MigrationEndpoint {
    fn name(&self) -> InterfaceName {
        "org.tinystoragedrivers.Migration".parse().unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec!["GetSchema".parse().unwrap()]
    }

    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }

    async fn call(
        &self,
        _: &MemberName,
        _: serde_json::Value,
    ) -> tinybus::Result<serde_json::Value> {
        Ok(serde_json::json!({"schema": "ready"}))
    }
}

fn linked_module() -> tinybus::Result<LinkedModule> {
    fixture::linked_module()
}

#[tokio::test]
async fn selected_controller_can_call_allowlisted_confidential_route_before_ready() {
    let broker = Broker::new_restricted_module_control();
    let broker_task = broker.spawn(MemoryBus::new());
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    let selected = broker
        .attach_module_controller(std::sync::Arc::new(broker_transport))
        .unwrap();
    let controller = Connection::connect(Box::new(controller_transport))
        .await
        .unwrap();
    assert_eq!(controller.unique_name().as_ref(), Some(&selected));

    let host = ModuleHost::new(broker);
    host.set_pre_ready_confidential_routes(vec![PreReadyConfidentialRoute {
        path: ObjectPath::new("/org/tinystoragedrivers/migration").unwrap(),
        interface: InterfaceName::new("org.tinystoragedrivers.Migration").unwrap(),
        member: MemberName::new("GetSchema").unwrap(),
    }])
    .unwrap();

    let (setup_started_tx, setup_started_rx) = tokio::sync::oneshot::channel();
    let (finish_setup_tx, finish_setup_rx) = tokio::sync::oneshot::channel();
    let attach = tokio::spawn(async move {
        host.attach_linked_with_setup(
            linked_module().unwrap(),
            serde_json::json!({}),
            move |connection| async move {
                connection
                    .serve_at(
                        ObjectPath::new("/org/tinystoragedrivers/migration").unwrap(),
                        MigrationEndpoint,
                    )
                    .await?;
                let _ = setup_started_tx.send(());
                finish_setup_rx
                    .await
                    .map_err(|_| Error::failed("test setup gate closed"))
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), setup_started_rx)
        .await
        .expect("linked setup reached its explicit barrier")
        .expect("setup barrier sender remains live");

    let proxy = controller
        .proxy(
            "ai.tinyhumans.module.PreReadyRouteFixture",
            "/org/tinystoragedrivers/migration",
            "org.tinystoragedrivers.Migration",
        )
        .unwrap();
    let mut call = Box::pin(proxy.call_confidential::<serde_json::Value>("GetSchema", ()));
    let early = tokio::time::timeout(Duration::from_secs(1), &mut call).await;
    let _ = finish_setup_tx.send(());
    let eventual = tokio::time::timeout(Duration::from_secs(8), call)
        .await
        .expect("the queued call completes once setup finishes");
    assert!(attach.await.unwrap().is_ok());
    broker_task.abort();

    assert!(
        early.is_ok(),
        "the allowlisted call must complete before READY"
    );
    assert_eq!(early.unwrap().unwrap()["schema"], "ready");
    assert_eq!(eventual.unwrap()["schema"], "ready");
}
