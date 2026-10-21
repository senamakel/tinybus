use std::sync::Arc;
use std::time::Duration;

use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::module::{LinkedModule, ModuleHost, ModuleState};
use tinybus::transport::memory::{MemoryBus, MemoryTransport};

mod fixture {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.module.SenderLookupFixture"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

fn linked_module() -> tinybus::Result<LinkedModule> {
    fixture::linked_module()
}

#[tokio::test]
async fn sender_lookup_resolves_the_admitted_peer_during_initialization() {
    let broker = Broker::new_restricted_module_control();
    let broker_task = broker.spawn(MemoryBus::new());
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    broker
        .attach_module_controller(Arc::new(broker_transport))
        .unwrap();
    let _controller = Connection::connect(Box::new(controller_transport))
        .await
        .unwrap();

    let host = Arc::new(ModuleHost::new(broker));
    let lookup = host.sender_lookup();
    let (setup_started_tx, setup_started_rx) = tokio::sync::oneshot::channel();
    let (finish_setup_tx, finish_setup_rx) = tokio::sync::oneshot::channel();
    let attach_host = host.clone();
    let attach = tokio::spawn(async move {
        attach_host
            .attach_linked_with_setup(
                linked_module().unwrap(),
                serde_json::json!({}),
                move |_| async move {
                    let _ = setup_started_tx.send(());
                    finish_setup_rx
                        .await
                        .map_err(|_| tinybus::Error::failed("test setup gate closed"))
                },
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), setup_started_rx)
        .await
        .expect("module reached setup")
        .expect("setup barrier sender remains live");

    assert_eq!(host.list()[0].state, ModuleState::Initializing);
    let sender = lookup
        .sender_for_module("tinybus")
        .expect("the admitted initializing module has a live unique peer");
    assert!(sender.is_unique());
    assert_eq!(host.sender_for_module("tinybus"), Some(sender.clone()));

    finish_setup_tx.send(()).unwrap();
    let info = tokio::time::timeout(Duration::from_secs(2), attach)
        .await
        .expect("module setup becomes ready")
        .unwrap()
        .unwrap();
    assert!(matches!(
        info.state,
        ModuleState::Ready | ModuleState::Serving
    ));
    assert_eq!(lookup.sender_for_module("tinybus"), Some(sender));

    drop(host);
    assert!(lookup.sender_for_module("tinybus").is_none());
    broker_task.abort();
}
