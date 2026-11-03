//! Real application Link acceptance peer: HTTP caller and separate Edge event bridge.
//! This never creates a network listener or boots the Claviron system.
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;

use claviron::app::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, EventValue, link_routing_key,
};
use claviron::orbit::transport::{
    Exchange, ExchangeReply, Received, TransportClient, TransportSegment,
};
use claviron::orbit::{ApplicationEventLayout, FleetEventCodec};
use claviron::orbitive::events::{FleetEventBus, FleetEventCursor};
use claviron::orbitive::link::{LinkEvents, Routing};
use claviron::orbitive::{Fleet, NodeId};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

struct Watcher {
    bus: Arc<FleetEventBus<ApplicationEventLayout>>,
    cursor: FleetEventCursor,
    history: Vec<ApplicationEvent>,
    // Own readiness registration for the ring cursor, not a polling interval.
    ready: tokio::io::unix::AsyncFd<claviron::orbitive::core::RingEventFd>,
}

impl Watcher {
    async fn advance(&mut self) -> Result<()> {
        loop {
            let polled = self.bus.poll(&mut self.cursor);
            if polled.lagged != 0 {
                return Err("acceptance event cursor lagged".into());
            }
            if !polled.events.is_empty() {
                for record in polled.events {
                    if record.topic == APPLICATION_EVENT_TOPIC {
                        self.history.push(ApplicationEvent::decode_orbit(
                            &record.payload,
                        )?);
                    }
                }
                return Ok(());
            }
            let mut guard = self.ready.readable().await?;
            self.ready.get_ref().drain()?;
            guard.clear_ready();
        }
    }

    fn acks(
        &self,
        root: &str,
    ) -> Result<HashMap<String, usize>> {
        let mut counts = HashMap::new();
        for event in &self.history {
            if event.topic().as_str() != "example.ack" {
                continue;
            }
            let value = event.payload().to_json()?;
            if value["event"].as_str() == Some(root) {
                *counts
                    .entry(
                        value["instance"]
                            .as_str()
                            .ok_or("missing ack instance")?
                            .to_owned(),
                    )
                    .or_insert(0) += 1;
            }
        }
        Ok(counts)
    }

    async fn acknowledged(
        &mut self,
        root: &str,
        expected: &[&str],
    ) -> Result<()> {
        loop {
            let counts = self.acks(root)?;
            for (instance, count) in &counts {
                if *count != 1 || !expected.contains(&instance.as_str()) {
                    return Err(format!(
                        "duplicate/unsubscribed delivery {root}: {counts:?}"
                    )
                    .into());
                }
            }
            if counts.len() == expected.len() {
                return Ok(());
            }
            self.advance().await?;
        }
    }

    fn publish(
        &self,
        topic: &str,
    ) -> Result<String> {
        let event = ApplicationEvent::new(
            topic,
            EventValue::String(b"from-edge".to_vec()),
        )?;
        self.bus.publish(APPLICATION_EVENT_TOPIC, &event.encode_orbit())?;
        Ok(event.id().to_string())
    }
}

async fn request(
    client: &TransportClient,
    application: &str,
    path: &[u8],
    query: &[u8],
) -> Result<(ExchangeReply, Vec<u8>)> {
    let pending = client.send(
        application,
        Exchange {
            method: "GET".into(),
            scheme: "http".into(),
            path: "/".into(),
            raw_path: path.to_vec(),
            query: query.to_vec(),
            headers: vec![
                (b"host".to_vec(), b"example.test".to_vec()),
                (b"x-raw".to_vec(), vec![255]),
            ],
            ..Default::default()
        },
        false,
    )?;
    let (_upload, mut response, reply) = pending.split();
    let head = reply.head().await?;
    let mut body = Vec::new();
    response.read_to_end(&mut body).await?;
    Ok((head, body))
}

fn announce(value: &str) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: application-check <application-fleet> <edge-event-fleet> <application-name>".into());
    }
    let segment = Arc::new(TransportSegment::for_fleet(&args[0])?);
    let joined = segment.join("event-peer", "edge", 1)?;
    let fleet = Arc::new(Fleet::join_shm_as(&args[1], 1, NodeId::new(0))?);
    let bus =
        Arc::new(FleetEventBus::<ApplicationEventLayout>::with_layout(fleet)?);
    let bridge = Arc::new(LinkEvents::for_lane(
        segment.link(),
        joined.lane,
        Arc::clone(&bus),
        |role| role == "java",
        Routing::Key(link_routing_key),
    )?);
    bridge.interest(&[
        "example.ack",
        "example.local",
        "example.edge",
        "example.barrier",
    ]);
    let descriptor = bus.event_fd()?;
    // SAFETY: the owned ring descriptor retains its identity until deregistration.
    let ready = unsafe { tokio::io::unix::AsyncFd::register(descriptor) }?;
    let mut events = Watcher {
        cursor: bus.cursor_at_head(),
        bus,
        ready,
        history: Vec::new(),
    };
    let mut receiver = segment.receiver(joined.lane)?;
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let forwarding = Arc::clone(&bridge);
    let reader = tokio::spawn(async move {
        let mut frame = Vec::new();
        loop {
            let value = tokio::select! { biased; () = stopping.cancelled() => return Ok::<_, Error>(()), value = receiver.next(&mut frame) => value? };
            if value == Received::Event {
                forwarding.accept(&frame)?;
            }
        }
    });
    let stopping = stop.clone();
    let forwarding = Arc::clone(&bridge);
    let driver = tokio::spawn(async move {
        tokio::select! { () = stopping.cancelled() => Ok::<_, Error>(()), value = forwarding.drive() => { value?; Ok(()) } }
    });
    let client = TransportClient::join(&args[0], "http-peer", 1)?;
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    announce("PEER_READY")?;
    if input.next_line().await?.as_deref() != Some("run") {
        return Err("expected run".into());
    }
    let applications = client.applications();
    if applications
        .iter()
        .filter(|peer| peer.name == args[2] && peer.role == "java")
        .count()
        != 2
    {
        return Err(format!(
            "expected two independently joined Java lanes: {applications:?}"
        )
        .into());
    }
    let mut instances = HashSet::new();
    for _ in 0..8 {
        let (head, body) = request(&client, &args[2], b"/plain", b"").await?;
        if head.status != 200 || body != b"Hello, Java!" {
            return Err(format!("wrong plain reply: {head:?} {body:?}").into());
        }
        let instance = head
            .headers
            .iter()
            .find(|(name, _)| name == b"x-java-instance")
            .ok_or("no instance header")?;
        instances.insert(String::from_utf8(instance.1.clone())?);
        let cookies = head
            .headers
            .iter()
            .filter(|(name, _)| name == b"set-cookie")
            .map(|(_, value)| value.as_slice())
            .collect::<Vec<_>>();
        if cookies != [b"a=1".as_slice(), b"b=2".as_slice()] {
            return Err("duplicate headers flattened".into());
        }
        if !head
            .headers
            .iter()
            .any(|(name, value)| name == b"x-raw" && value == &[255])
        {
            return Err("header bytes changed".into());
        }
    }
    if instances.len() != 2 {
        return Err(format!(
            "requests did not reach both JVM lanes: {instances:?}"
        )
        .into());
    }
    let (head, body) =
        request(&client, &args[2], b"/a%2Fb", b"q=x%20y").await?;
    if head.status != 200 || body != b"GET http://example.test/a%2Fb?q=x%20y ff"
    {
        return Err("request metadata changed".into());
    }
    if request(&client, &args[2], b"/fail", b"").await?.0.status != 500 {
        return Err("handler exception did not become 500".into());
    }
    let pending = client.send(
        &args[2],
        Exchange {
            method: "GET".into(),
            scheme: "http".into(),
            raw_path: b"/cancel".to_vec(),
            headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
            ..Default::default()
        },
        false,
    )?;
    let (upload, response, reply) = pending.split();
    drop(response);
    drop(upload);
    if reply.head().await.is_ok() {
        return Err("idle cancellation produced a successful head".into());
    }
    // An unbridged HTTP client also advertises `edge`: it must leave before
    // the event phase, otherwise a bridge can legitimately select its lane.
    client.leave();
    let edge = events.publish("example.edge")?;
    events.acknowledged(&edge, &["one", "two"]).await?;
    announce("EDGE_CHECKS_PASSED")?;
    if input.next_line().await?.as_deref() != Some("published") {
        return Err("expected local publication acknowledgement".into());
    }
    let local = loop {
        if let Some(event) = events
            .history
            .iter()
            .find(|event| event.topic().as_str() == "example.local")
        {
            break event.id().to_string();
        }
        events.advance().await?;
    };
    events.acknowledged(&local, &["one", "two"]).await?;
    announce("FIRST_CHECKS_PASSED")?;
    if input.next_line().await?.as_deref() != Some("unsubscribed") {
        return Err("expected unsubscribe acknowledgement".into());
    }
    let after_unsubscribe = events.publish("example.edge")?;
    events.acknowledged(&after_unsubscribe, &["two"]).await?;
    let barrier = events.publish("example.barrier")?;
    events.acknowledged(&barrier, &["one", "two"]).await?;
    // The unsubscribed event was already received/acknowledged by its receiving
    // lane before the barrier was published; both subscribers then progressed.
    for (root, expected) in
        [(&edge, 2), (&local, 2), (&after_unsubscribe, 1), (&barrier, 2)]
    {
        let counts = events.acks(root)?;
        if counts.len() != expected || counts.values().any(|count| *count != 1)
        {
            return Err("event delivered twice".into());
        }
    }
    if events
        .history
        .iter()
        .filter(|event| event.id().to_string() == local)
        .count()
        != 1
    {
        return Err("local event forwarded into Edge more than once".into());
    }
    announce("APPLICATION_CHECKS_PASSED")?;
    if input.next_line().await?.as_deref() != Some("stop") {
        return Err("expected stop".into());
    }
    stop.cancel();
    reader.await??;
    driver.await??;
    segment.inbox().release_lane(joined.lane)?;
    drop(joined.hold);
    announce("PEER_DRAINED")?;
    Ok(())
}
