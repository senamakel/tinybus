use super::*;
use crate::module::service_slot::ModuleServiceSlot;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type ShutdownHook = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

fn slot<K, S>() -> ModuleServiceSlot<K, S> {
    ModuleServiceSlot::new()
}

#[tokio::test]
async fn the_same_key_reuses_the_service_and_allows_mutating_operations() {
    let slot = slot::<&'static str, usize>();
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let first_opens = opens.clone();
    let first_closes = closes.clone();
    let first = slot
        .with(
            "alice",
            move || async move {
                first_opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, crate::Error>(4usize)
            },
            move |service| async move {
                first_closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(service);
                Ok(())
            },
            |service| {
                Box::pin(async move {
                    *service += 1;
                    Ok::<_, crate::Error>(*service)
                })
            },
        )
        .await
        .expect("first operation should open and use the service");
    assert_eq!(first, 5);

    let second_opens = opens.clone();
    let second_closes = closes.clone();
    let second = slot
        .with(
            "alice",
            move || async move {
                second_opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, crate::Error>(99usize)
            },
            move |service| async move {
                second_closes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(service);
                Ok(())
            },
            |service| {
                Box::pin(async move {
                    *service += 1;
                    Ok::<_, crate::Error>(*service)
                })
            },
        )
        .await
        .expect("matching key should reuse the installed service");

    assert_eq!(second, 6);
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(closes.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn changing_the_key_closes_the_installed_service_before_opening_its_replacement() {
    let slot = slot::<&'static str, &'static str>();
    let events = Arc::new(Mutex::new(Vec::new()));

    let opened_events = events.clone();
    let closed_events = events.clone();
    slot.with(
        "alice",
        move || async move {
            opened_events.lock().unwrap().push("open alice");
            Ok::<_, crate::Error>("alice service")
        },
        move |service| async move {
            let _ = service;
            closed_events.lock().unwrap().push("close alice");
            Err(crate::Error::failed("old service close failed"))
        },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("initial service should open");

    let opened_events = events.clone();
    slot.with(
        "bob",
        move || async move {
            opened_events.lock().unwrap().push("open bob");
            Ok::<_, crate::Error>("bob service")
        },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("replacement open should determine the result after a close error");

    assert_eq!(
        *events.lock().unwrap(),
        ["open alice", "close alice", "open bob"]
    );
}

#[tokio::test]
async fn explicit_close_returns_its_error_and_clears_the_slot() {
    let slot = slot::<u8, u8>();
    slot.with(
        1,
        || async { Ok::<_, crate::Error>(7) },
        |_| async { Err(crate::Error::failed("close failed")) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should install");

    assert!(slot.close().await.is_err());

    let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = opened.clone();
    slot.with(
        1,
        move || async move {
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, crate::Error>(8)
        },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should reopen after failed explicit close");
    assert!(opened.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn a_failed_open_leaves_the_slot_empty() {
    let slot = slot::<u8, u8>();
    assert!(
        slot.with(
            1,
            || async { Err(crate::Error::failed("open failed")) },
            |_| async { Ok(()) },
            |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
        )
        .await
        .is_err()
    );

    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = opens.clone();
    slot.with(
        1,
        move || async move {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, crate::Error>(2)
        },
        |_| async { Ok(()) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("a later open should succeed");
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn close_waits_for_the_current_operation_to_finish() {
    let slot = Arc::new(slot::<u8, u8>());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let with_slot = slot.clone();
    let operation = tokio::spawn(async move {
        with_slot
            .with(
                1,
                || async { Ok::<_, crate::Error>(7) },
                |_| async { Ok(()) },
                move |_| {
                    Box::pin(async move {
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<_, crate::Error>(())
                    })
                },
            )
            .await
    });
    entered_rx
        .await
        .expect("operation should enter before close");

    let (close_started_tx, close_started_rx) = oneshot::channel();
    let closing_slot = slot.clone();
    let close = tokio::spawn(async move {
        let _ = close_started_tx.send(());
        closing_slot.close().await
    });
    close_started_rx.await.expect("close task should start");
    tokio::task::yield_now().await;
    assert!(
        !close.is_finished(),
        "close must wait for the locked operation"
    );

    let _ = release_tx.send(());
    operation
        .await
        .expect("operation task should finish")
        .expect("operation should succeed");
    close
        .await
        .expect("close task should finish")
        .expect("close should succeed");
}

#[tokio::test]
async fn shutdown_registration_is_idempotent_and_can_be_repeated_after_cleanup_error() {
    let slot = Box::leak(Box::new(slot::<u8, u8>()));
    slot.with(
        1,
        || async { Ok::<_, crate::Error>(7) },
        |_| async { Err(crate::Error::failed("shutdown close failed")) },
        |_| Box::pin(async { Ok::<_, crate::Error>(()) }),
    )
    .await
    .expect("service should install");

    let hooks = Arc::new(Mutex::new(Vec::<ShutdownHook>::new()));
    let registered = hooks.clone();
    slot.on_shutdown(move |hook| registered.lock().unwrap().push(hook));
    let registered = hooks.clone();
    slot.on_shutdown(move |hook| registered.lock().unwrap().push(hook));
    assert_eq!(
        hooks.lock().unwrap().len(),
        1,
        "registration should be idempotent"
    );

    let hook = hooks.lock().unwrap().remove(0);
    hook().await;

    let registered = hooks.clone();
    slot.on_shutdown(move |hook| registered.lock().unwrap().push(hook));
    assert_eq!(
        hooks.lock().unwrap().len(),
        1,
        "cleanup error must reset registration"
    );
}
