//! Lifecycle controls across sibling JVMs, using the existing application wire protocol.
use claviron::app::fleet::decode_link_snapshot_rows;
use claviron::orbit::link::application::{
    ApplicationClient, ApplicationSegment, ApplicationStateNotice,
    ControlCommand, ControlTarget, Exchange, FrameKind, LINK_BUFFER_BYTES,
    Peer, Received, ShutdownNotice, frame_kind,
};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
fn signal(value: &str) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}
async fn command(
    input: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
    expected: &str,
) -> Result<()> {
    if input.next_line().await?.as_deref() != Some(expected) {
        return Err(format!("expected {expected}").into());
    }
    Ok(())
}
fn control(
    name: &str,
    action: &str,
    runtime: &str,
    target: &str,
) -> ControlCommand {
    ControlCommand {
        application: name.into(),
        name: action.into(),
        payload: Vec::new(),
        target: Some(ControlTarget {
            runtime: runtime.into(),
            name: target.into(),
        }),
    }
}
fn send(
    client: &ApplicationClient,
    name: &str,
    path: &str,
    body: bool,
) -> Result<claviron::orbit::link::application::Pending> {
    Ok(client.send(
        name,
        Exchange {
            method: "GET".into(),
            scheme: "http".into(),
            raw_path: path.as_bytes().to_vec(),
            headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
            ..Default::default()
        },
        body,
    )?)
}
async fn healthy(
    client: &ApplicationClient,
    name: &str,
    generation: u64,
    instances: usize,
) -> Result<()> {
    let mut seen = HashSet::new();
    for _ in 0..(instances * 4) {
        let (_upload, mut response, reply) =
            send(client, name, "/state", false)?.split();
        if reply.head().await?.status != 200 {
            return Err("healthy response was refused".into());
        }
        let mut bytes = Vec::new();
        response.read_to_end(&mut bytes).await?;
        let body = String::from_utf8(bytes)?;
        let (tag, value) = body.split_once(':').ok_or("invalid state reply")?;
        if value.parse::<u64>()? != generation {
            return Err(format!("wrong request generation: {body}").into());
        }
        seen.insert(tag.to_owned());
    }
    if seen.len() != instances {
        return Err("healthy requests missed an instance".into());
    }
    Ok(())
}
struct Reports {
    receiver: claviron::orbitive::link::InboxReceiver,
    states: HashMap<u16, (u64, bool)>,
}
impl Reports {
    async fn wait(
        &mut self,
        peers: &[Peer],
        generation: u64,
        running: bool,
    ) -> Result<()> {
        let mut frame = Vec::new();
        while !peers.iter().all(|peer| {
            self.states.get(&(peer.lane as u16)) == Some(&(generation, running))
        }) {
            if self.receiver.next(&mut frame).await? != Received::Frame
                || frame_kind(&frame)? != FrameKind::ApplicationState
            {
                continue;
            }
            let notice = ApplicationStateNotice::decode(&frame)?;
            if notice.row_count != 1 || notice.row_index != 0 {
                return Err("unexpected status report shape".into());
            }
            let snapshot = decode_link_snapshot_rows(&[notice.payload])
                .map_err(|error| -> Error { error.into() })?;
            let placement = &snapshot
                .applications
                .first()
                .ok_or("missing application status")?
                .placements[0];
            if notice.incarnation != 17 {
                return Err("lifecycle changed membership incarnation".into());
            }
            self.states
                .insert(notice.lane, (placement.generation, placement.enabled));
        }
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let fleet = std::env::args().nth(1).ok_or("usage: lifecycle <fleet>")?;
    let segment = ApplicationSegment::for_fleet(&fleet)?;
    let observer = segment.join("status-peer", "edge", 1)?;
    let mut reports = Reports {
        receiver: segment.receiver(observer.lane)?,
        states: HashMap::new(),
    };
    let client = ApplicationClient::join(&fleet, "http-peer", 1)?;
    signal(&format!("PEER_READY {}", LINK_BUFFER_BYTES * 4))?;
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    command(&mut input, "run").await?;
    let peers = client.applications();
    let life = peers
        .iter()
        .filter(|peer| peer.name == "life")
        .cloned()
        .collect::<Vec<_>>();
    let other = peers
        .iter()
        .filter(|peer| peer.name == "other")
        .cloned()
        .collect::<Vec<_>>();
    if life.len() != 2 || other.len() != 1 {
        return Err("expected two sibling JVMs and one unrelated JVM".into());
    }
    reports.wait(&life, 1, true).await?;
    reports.wait(&other, 1, true).await?;
    healthy(&client, "life", 1, 2).await?;
    healthy(&client, "other", 1, 1).await?;
    for peer in &life {
        for invalid in [
            control("life", "stop", "javascript", "life"),
            control("life", "stop", "java", "other"),
        ] {
            segment.inbox().write(peer.lane, &invalid.encode()?)?;
        }
        let probe = ControlCommand {
            application: "life".into(),
            name: "probe".into(),
            payload: vec![0, 255],
            target: None,
        };
        segment.inbox().write(peer.lane, &probe.encode()?)?;
        segment.inbox().write(
            peer.lane,
            &ShutdownNotice { edge: "example-edge".into() }.encode()?,
        )?;
    }
    signal("CONTROL_FILTERS_SENT")?;
    command(&mut input, "filters-checked").await?;
    healthy(&client, "life", 1, 2).await?;
    let first = send(&client, "life", "/late", false)?;
    let second = send(&client, "life", "/late", false)?;
    signal("HOLDS_SENT")?;
    command(&mut input, "stop").await?;
    client.command(&control("life", "stop", "java", "life"))?;
    reports.wait(&life, 1, false).await?;
    for pending in [first, second] {
        let (_upload, _response, reply) = pending.split();
        if reply.head().await.is_ok() {
            return Err("stopped generation produced a late response".into());
        }
    }
    for _ in 0..8 {
        let (_upload, mut response, reply) =
            send(&client, "life", "/state", false)?.split();
        if reply.head().await?.status != 503 {
            return Err("stopped request did not return 503".into());
        }
        let mut bytes = Vec::new();
        response.read_to_end(&mut bytes).await?;
        if !bytes.is_empty() {
            return Err("stopped request returned a body".into());
        }
    }
    signal("STOPPED_CHECKS_PASSED")?;
    command(&mut input, "start").await?;
    client.command(&control("life", "start", "java", "life"))?;
    reports.wait(&life, 2, true).await?;
    healthy(&client, "life", 2, 2).await?;
    signal("STARTED_CHECKS_PASSED")?;
    command(&mut input, "idempotent-start").await?;
    client.command(&control("life", "start", "java", "life"))?;
    signal("IDEMPOTENT_START_SENT")?;
    command(&mut input, "blocked").await?;
    let mut blocked = Vec::new();
    for path in ["/write", "/write", "/read", "/read"] {
        let (upload, response, reply) =
            send(&client, "life", path, path == "/read")?.split();
        if reply.head().await?.status != 200 {
            return Err("blocked generation head failed".into());
        }
        blocked.push((upload, response));
    }
    signal("BLOCKED_SENT")?;
    command(&mut input, "restart").await?;
    client.command(&control("life", "restart", "java", "life"))?;
    reports.wait(&life, 3, true).await?;
    for (_upload, mut response) in blocked {
        let mut bytes = Vec::new();
        if response.read_to_end(&mut bytes).await.is_ok() {
            return Err("retired stream became successful EOF".into());
        }
    }
    healthy(&client, "life", 3, 2).await?;
    signal("RESTART_CHECKS_PASSED")?;
    command(&mut input, "local-done").await?;
    reports.wait(&life, 4, true).await?;
    healthy(&client, "life", 4, 2).await?;
    healthy(&client, "other", 1, 1).await?;
    if client.applications() != peers {
        return Err("application lifecycle changed JVM/lane membership".into());
    }
    // Leave ignored handler futures pending while closing: retired bridge work
    // must not depend on their eventual Java completion.
    let first = send(&client, "life", "/late", false)?;
    let second = send(&client, "life", "/late", false)?;
    signal("FINAL_HOLDS_SENT")?;
    command(&mut input, "final-stop").await?;
    client.command(&control("life", "stop", "java", "life"))?;
    reports.wait(&life, 4, false).await?;
    for pending in [first, second] {
        let (_upload, _response, reply) = pending.split();
        if reply.head().await.is_ok() {
            return Err("final stopped handler responded".into());
        }
    }
    signal("LIFECYCLE_CHECKS_PASSED")?;
    command(&mut input, "stop").await?;
    client.leave();
    drop(reports);
    segment.inbox().release_lane(observer.lane)?;
    drop(observer.hold);
    signal("PEER_DRAINED")?;
    Ok(())
}
