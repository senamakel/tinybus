use super::*;
use crate::message::{Message, MessageKind};
use crate::ports::{Listener, Transport};
use crate::transport::memory::MemoryTransport;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

#[derive(Clone)]
struct HeldMetadataBus {
    accepted: mpsc::Sender<Box<dyn Transport>>,
    pending: Arc<AsyncMutex<mpsc::Receiver<Box<dyn Transport>>>>,
}

impl HeldMetadataBus {
    fn new() -> Self {
        let (accepted, pending) = mpsc::channel(8);
        Self {
            accepted,
            pending: Arc::new(AsyncMutex::new(pending)),
        }
    }

    async fn connect(
        &self,
        held_member: Option<&'static str>,
    ) -> (crate::Connection, Arc<Notify>, Arc<StdMutex<Vec<String>>>) {
        let (client, broker) = MemoryTransport::pair();
        let held = Arc::new(Notify::new());
        let observed = Arc::new(StdMutex::new(Vec::new()));
        self.accepted
            .send(Box::new(HoldMetadataRequest {
                inner: broker,
                held_member,
                held: held.clone(),
                observed: observed.clone(),
            }))
            .await
            .unwrap();
        (
            crate::Connection::connect(Box::new(client)).await.unwrap(),
            held,
            observed,
        )
    }
}

#[async_trait::async_trait]
impl Listener for HeldMetadataBus {
    async fn accept(&self) -> crate::Result<Option<Box<dyn Transport>>> {
        Ok(self.pending.lock().await.recv().await)
    }

    fn describe(&self) -> String {
        "test-held-metadata".to_string()
    }
}

struct HoldMetadataRequest {
    inner: MemoryTransport,
    held_member: Option<&'static str>,
    held: Arc<Notify>,
    observed: Arc<StdMutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Transport for HoldMetadataRequest {
    async fn send(&self, message: Message) -> crate::Result<()> {
        self.inner.send_brokered(message).await
    }

    async fn recv(&self) -> crate::Result<Option<Message>> {
        loop {
            let Some((message, _)) = self.inner.recv_with_provenance().await? else {
                return Ok(None);
            };
            if let Some(member) = message.header.member.as_ref() {
                self.observed
                    .lock()
                    .unwrap()
                    .push(member.as_str().to_string());
            }
            let is_held_query = self.held_member.is_some_and(|member| {
                message.header.kind == MessageKind::MethodCall
                    && message
                        .header
                        .member
                        .as_ref()
                        .is_some_and(|actual| actual.as_str() == member)
            });
            if is_held_query {
                self.held.notify_one();
                continue;
            }
            return Ok(Some(message));
        }
    }

    async fn close(&self) -> crate::Result<()> {
        self.inner.close().await
    }

    fn describe(&self) -> String {
        self.inner.describe()
    }
}

async fn client_waiting_on_metadata(
    member: &'static str,
) -> (
    ModuleClient,
    &'static ReleaseRecord,
    Arc<Notify>,
    Arc<Mutex<Vec<serde_json::Value>>>,
    Arc<Recording>,
    Arc<StdMutex<Vec<String>>>,
    tokio::task::JoinHandle<crate::Result<()>>,
) {
    let bus = HeldMetadataBus::new();
    let broker = crate::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let (service, _, _) = bus.connect(None).await;
    let (caller, held, observed) = bus.connect(Some(member)).await;
    let record = strict_record();
    let calls = Arc::new(Mutex::new(Vec::new()));
    service
        .serve_at(
            crate::ObjectPath::new(record.object_path).unwrap(),
            AttestedReceiver {
                label: "attested",
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();
    service.request_name(record.bus_name).await.unwrap();
    if member == "GetAttestation" {
        assert_eq!(
            caller.name_owner(record.bus_name).await.unwrap(),
            service.unique_name()
        );
    }

    let sink = Arc::new(Recording::default());
    let access = PreparedAccess {
        record,
        connection: caller,
        reporter: FailureReporter::new(sink.clone()),
        preparations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let client = ModuleClient::new(access).with_timeout(Duration::from_millis(5));
    (client, record, held, calls, sink, observed, broker_task)
}

async fn stalled_metadata_call_is_bounded(member: &'static str) {
    let (client, record, held, calls, sink, observed, broker_task) =
        client_waiting_on_metadata(member).await;
    let serializations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serializations_for_call = serializations.clone();
    let held_request = held.notified();
    let mut call = tokio::spawn(async move {
        client
            .call_attested::<String>(
                record.id,
                "Consume",
                SerializeCounter(serializations_for_call),
            )
            .await
    });
    let held_result = tokio::time::timeout(Duration::from_millis(100), async {
        tokio::select! {
            _ = held_request => true,
            joined = &mut call => panic!("client returned before the selected metadata query was held: {:?}; observed={:?}", joined, observed.lock().unwrap()),
        }
    })
    .await
    .expect("the selected metadata request should reach the holding transport");
    assert!(held_result);
    tokio::time::advance(Duration::from_millis(5)).await;
    let result = match tokio::time::timeout(Duration::from_millis(100), &mut call).await {
        Ok(joined) => joined.unwrap(),
        Err(_) => {
            call.abort();
            panic!("the configured ModuleClient timeout must bound metadata requests");
        }
    };

    assert_eq!(result, Err(ModuleCallError::TransportFailed));
    assert_eq!(serializations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        sink.0.lock().unwrap().as_slice(),
        &[(record.id.to_string(), Reason::TransportFailed)]
    );
    broker_task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_stalled_name_owner_lookup_uses_the_client_timeout() {
    stalled_metadata_call_is_bounded("GetNameOwner").await;
}

#[tokio::test(start_paused = true)]
async fn a_stalled_attestation_lookup_uses_the_client_timeout() {
    stalled_metadata_call_is_bounded("GetAttestation").await;
}
