use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use claviron::app::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, EventValue, link_routing_key,
};
use claviron::orbit::transport::{EDGE_RUNTIME, TransportSegment};
use claviron::orbit::{ApplicationEventLayout, FleetEventCodec};
use claviron::orbitive::Fleet;
use claviron::orbitive::events::FleetEventBus;
use claviron::orbitive::link::{LinkEvents, Routing};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::lifecycle::{Action, TOPIC};
use super::{Application, transport};
use crate::{Handle, Result, Status, state};

struct Delivered {
    id: Box<str>,
    topic: Box<str>,
    json: Box<[u8]>,
}

pub struct EventRecord {
    value: Delivered,
    application: Arc<Application>,
    id: std::sync::atomic::AtomicU64,
}
impl Drop for EventRecord {
    fn drop(&mut self) {
        let id = self.id.load(std::sync::atomic::Ordering::Acquire);
        if id != 0 {
            if let Ok(mut active) = self.application.callbacks.lock() {
                active.remove(&id);
            }
            self.application.changed.notify_waiters();
        }
    }
}

pub(super) struct Events {
    name: Box<str>,
    application: std::sync::Weak<Application>,
    bus: Arc<FleetEventBus<ApplicationEventLayout>>,
    bridge: Arc<LinkEvents<ApplicationEventLayout>>,
    topics: Mutex<HashSet<Box<str>>>,
    send: mpsc::Sender<Delivered>,
    receive: AsyncMutex<mpsc::Receiver<Delivered>>,
    failed: watch::Sender<Option<Status>>,
}

impl Events {
    pub(super) fn new(
        fleet: Arc<Fleet>,
        name: &str,
        segment: &TransportSegment,
        lane: usize,
        capacity: usize,
        application: std::sync::Weak<Application>,
    ) -> Result<Arc<Self>> {
        let bus = Arc::new(
            FleetEventBus::<ApplicationEventLayout>::with_layout(fleet)
                .map_err(transport)?,
        );
        let bridge = Arc::new(
            LinkEvents::for_lane(
                segment.link(),
                lane,
                Arc::clone(&bus),
                |role| role == EDGE_RUNTIME,
                Routing::Key(link_routing_key),
            )
            .map_err(transport)?,
        );
        bridge.interest(&[TOPIC]);
        let (send, receive) = mpsc::channel(capacity);
        Ok(Arc::new(Self {
            name: name.into(),
            application,
            bus,
            bridge,
            topics: Mutex::default(),
            send,
            receive: AsyncMutex::new(receive),
            failed: watch::channel(None).0,
        }))
    }

    pub(super) fn start(
        self: Arc<Self>,
        tasks: &TaskTracker,
        stop: CancellationToken,
    ) -> Result<()> {
        // Capture both cursors/readiness before publishing application readiness.
        let cursor = self.bus.cursor_at_head();
        let descriptor = self.bus.event_fd().map_err(transport)?;
        // SAFETY: the owned ring descriptor keeps its fd identity until deregistration.
        let ready = unsafe { tokio::io::unix::AsyncFd::register(descriptor) }
            .map_err(transport)?;
        let controlling = Arc::clone(&self);
        let control_stop = stop.clone();
        let mut control_cursor = self.bus.cursor_at_head();
        let descriptor = self.bus.event_fd().map_err(transport)?;
        // SAFETY: this independent owned descriptor keeps its identity through deregistration.
        let control_ready =
            unsafe { tokio::io::unix::AsyncFd::register(descriptor) }
                .map_err(transport)?;
        tasks.spawn(async move {
            let result: Result<()> = async {
                loop {
                    let poll = controlling.bus.poll(&mut control_cursor);
                    if poll.lagged != 0 {
                        return Err(Status::NoCapacity);
                    }
                    for record in poll.events {
                        if record.id.node() != controlling.bus.node_id().get()
                            && record.topic == APPLICATION_EVENT_TOPIC
                        {
                            controlling.deliver_lifecycle(
                                &ApplicationEvent::decode_orbit(
                                    &record.payload,
                                )
                                .map_err(transport)?,
                            )?;
                        }
                    }
                    tokio::select! {
                        biased;
                        () = control_stop.cancelled() => return Ok(()),
                        value = control_ready.readable() => {
                            let mut guard = value.map_err(transport)?;
                            control_ready.get_ref().drain().map_err(transport)?;
                            guard.clear_ready();
                        }
                    }
                }
            }
            .await;
            if let Err(error) = result
                && let Some(application) = controlling.application.upgrade()
            {
                application.fail(error);
            }
        });
        let forwarding = Arc::clone(&self);
        let stopped = stop.clone();
        tasks.spawn(async move {
            tokio::select! {
                () = stopped.cancelled() => {},
                result = forwarding.bridge.drive() => {
                    if let Err(error) = result { forwarding.failed.send_replace(Some(transport(error))); }
                },
            }
        });
        tasks.spawn(async move {
            let mut cursor = cursor;
            let result: Result<()> = async {
                loop {
                    let poll = self.bus.poll(&mut cursor);
                    if poll.lagged != 0 { return Err(Status::NoCapacity); }
                    for record in poll.events {
                        if record.topic != APPLICATION_EVENT_TOPIC { continue; }
                        let event = ApplicationEvent::decode_orbit(&record.payload).map_err(transport)?;
                        if event.topic().as_str() == TOPIC { continue; }
                        if !self.topics.lock().map_err(|_| Status::Panic)?.contains(event.topic().as_str()) { continue; }
                        if let Some(target) = event.target()
                            && (target.runtime().as_str() != "java" || target.name().as_str() != &*self.name) { continue; }
                        let value = Delivered {
                            id: event.id().to_string().into_boxed_str(),
                            topic: event.topic().as_str().into(),
                            json: serde_json::to_vec(&event.payload().to_json().map_err(transport)?).map_err(transport)?.into_boxed_slice(),
                        };
                        tokio::select! {
                            biased;
                            () = stop.cancelled() => return Ok(()),
                            result = self.send.send(value) => { result.map_err(transport)?; },
                        }
                    }
                    tokio::select! {
                        biased;
                        () = stop.cancelled() => return Ok(()),
                        result = ready.readable() => {
                            let mut guard = result.map_err(transport)?;
                            ready.get_ref().drain().map_err(transport)?;
                            guard.clear_ready();
                        },
                    }
                }
            }.await;
            if let Err(error) = result { self.failed.send_replace(Some(error)); }
        });
        Ok(())
    }

    pub(super) fn flush(&self) -> Result<()> {
        self.bridge.pump().map_err(transport)
    }

    pub(super) fn accept(
        &self,
        frame: &[u8],
    ) -> Result<()> {
        // Delivery is ring-only, including this receiving node: no duplicate callback.
        let (topic, payload) = self.bridge.accept(frame).map_err(transport)?;
        if topic == APPLICATION_EVENT_TOPIC {
            let event =
                ApplicationEvent::decode_orbit(payload).map_err(transport)?;
            self.deliver_lifecycle(&event)?;
        }
        Ok(())
    }

    pub(crate) fn subscribe(
        &self,
        topic: &str,
        enabled: bool,
    ) -> Result<()> {
        let topic = claviron::app::EventTopic::new(topic)
            .map_err(|_| Status::InvalidArgument)?;
        let mut topics = self.topics.lock().map_err(|_| Status::Panic)?;
        if enabled {
            topics.insert(topic.as_str().into());
        } else {
            topics.remove(topic.as_str());
        }
        let mut keys =
            topics.iter().map(|value| value.as_bytes()).collect::<Vec<_>>();
        keys.push(TOPIC.as_bytes());
        self.bridge.interest(&keys);
        Ok(())
    }

    pub(super) fn publish_lifecycle(
        &self,
        action: Action,
    ) -> Result<()> {
        let target = claviron::app::ApplicationId::new(
            claviron::app::ApplicationRuntime::new("java")
                .map_err(transport)?,
            claviron::app::ApplicationName::new(&*self.name)
                .map_err(transport)?,
        );
        let event = ApplicationEvent::for_application(
            target,
            TOPIC,
            EventValue::String(action.name().as_bytes().to_vec()),
        )
        .map_err(transport)?;
        self.bus
            .publish(APPLICATION_EVENT_TOPIC, &event.encode_orbit())
            .map_err(transport)?;
        Ok(())
    }
    fn deliver_lifecycle(
        &self,
        event: &ApplicationEvent,
    ) -> Result<()> {
        if event.topic().as_str() != TOPIC
            || !event.target().is_some_and(|target| {
                target.runtime().as_str() == "java"
                    && target.name().as_str() == &*self.name
            })
        {
            return Ok(());
        }
        if let EventValue::String(bytes) = event.payload()
            && let Ok(name) = std::str::from_utf8(bytes)
            && let Some(action) = Action::from_name(name)
            && let Some(application) = self.application.upgrade()
        {
            application.apply_lifecycle(action)?;
        }
        Ok(())
    }

    pub(crate) fn publish(
        &self,
        topic: &str,
        json: &[u8],
    ) -> Result<()> {
        let value: serde_json::Value = serde_json::from_slice(json)
            .map_err(|_| Status::InvalidArgument)?;
        let event = ApplicationEvent::new(topic, EventValue::from_json(&value))
            .map_err(|_| Status::InvalidArgument)?;
        self.bus
            .publish(APPLICATION_EVENT_TOPIC, &event.encode_orbit())
            .map_err(transport)?;
        Ok(())
    }
}

impl Application {
    pub(crate) fn next_event(self: Arc<Self>) -> Result<Handle> {
        if self.stop.is_cancelled() {
            let operation = state::submit_owned(self.engine, &[])?;
            state::finish(operation, Status::End, 0)?;
            return Ok(operation);
        }
        self.submit(async move |application, cancellation| {
            let events = &application.data()?.events;
            let mut failures = events.failed.subscribe();
            let mut receive = events.receive.lock().await;
            if let Some(error) = *failures.borrow_and_update() { return Err(error); }
            tokio::select! {
                biased;
                () = application.stop.cancelled() => Ok((Status::End, 0)),
                () = cancellation.cancelled() => Ok((Status::Cancelled, 0)),
                _ = failures.changed() => Err(failures.borrow().unwrap_or(Status::Transport)),
                value = receive.recv() => match value {
                    Some(value) => {
                        let record = Arc::new(EventRecord {
                            value, application: Arc::clone(&application), id: std::sync::atomic::AtomicU64::new(0),
                        });
                        let id = state::register_event(Arc::clone(&record))?;
                        record.id.store(id, std::sync::atomic::Ordering::Release);
                        application.callbacks.lock().map_err(|_| Status::Panic)?.insert(id);
                        Ok((Status::Ok, id))
                    },
                    None => Ok((Status::End, 0)),
                },
            }
        }, false)
    }
}

impl EventRecord {
    pub(crate) fn info(&self) -> crate::EventInfo {
        crate::EventInfo {
            id: super::ffi::bytes(self.value.id.as_bytes()),
            topic: super::ffi::bytes(self.value.topic.as_bytes()),
            json: super::ffi::bytes(&self.value.json),
        }
    }
}

pub(super) fn publish_status(application: &Application) -> Result<()> {
    use claviron::app::fleet::{
        FleetApplicationListSnapshot, FleetApplicationPlacement,
        FleetApplicationStatus, encode_link_snapshot_rows,
    };
    use claviron::app::{
        ApplicationDescription, ApplicationId, ApplicationLifecycle,
        ApplicationName, ApplicationRuntime,
    };
    use claviron::orbit::service::{FleetMember, FleetMemberRole};
    let data = application.data()?;
    let state = application.lifecycle.snapshot()?;
    let member = FleetMember {
        node_id: data.lane as u16,
        role: FleetMemberRole::Standalone,
    };
    let now = SystemTime::now();
    let snapshot = FleetApplicationListSnapshot {
        captured_at: now,
        members_expected: 1,
        members_reporting: 1,
        applications: vec![FleetApplicationStatus {
            id: ApplicationId::new(
                ApplicationRuntime::new("java").map_err(transport)?,
                ApplicationName::new(&*application.settings.name)
                    .map_err(transport)?,
            ),
            placements: vec![FleetApplicationPlacement {
                description: ApplicationDescription::new("java", "http")
                    .runner("orbit-java"),
                member,
                lifecycle: if state.running != 0 {
                    ApplicationLifecycle::Running
                } else {
                    ApplicationLifecycle::Stopped
                },
                enabled: state.running != 0,
                generation: state.generation,
                pool: None,
                last_error: None,
                last_error_truncated: false,
                observed_at: now,
            }],
        }],
    };
    let rows =
        encode_link_snapshot_rows(&snapshot, member).map_err(transport)?;
    data.segment
        .publish_application_state(&application.settings.name, &rows)
        .map(|_| ())
        .map_err(transport)
}
