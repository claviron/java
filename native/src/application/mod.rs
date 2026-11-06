//! One separately hosted Java application using Claviron's application Link.

mod events;
pub mod ffi;
mod http;
pub mod invoke;
pub mod lifecycle;

use async_trait::async_trait;
use claviron::orbit::link::application::{
    ApplicationBodies, ApplicationSegment, FrameKind, Received, frame_kind,
};
use claviron::orbitive::Fleet;
use claviron::runtime::{RunContext, Runnable};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{Handle, Result, Status, state};
pub use events::EventRecord;
pub use http::Exchange;

pub(crate) struct Settings {
    pub fleet: Box<str>,
    pub name: Box<str>,
    pub incarnation: u64,
    pub max_exchanges: usize,
    pub event_capacity: usize,
    pub control_capacity: usize,
}

struct Data {
    segment: Arc<ApplicationSegment>,
    bodies: ApplicationBodies,
    events: Arc<events::Events>,
    lane: usize,
    hold: Mutex<Option<claviron::orbitive::shm::LaneHold>>,
}

pub struct Application {
    engine: Handle,
    settings: Settings,
    lifecycle: lifecycle::Lifecycle,
    _lease: state::Lease,
    data: OnceLock<Data>,
    host: OnceLock<tokio::runtime::Handle>,
    thread: Mutex<Option<JoinHandle<()>>>,
    stop: CancellationToken,
    closed: CancellationToken,
    host_stop: CancellationToken,
    io: TaskTracker,
    controls: TaskTracker,
    ready: mpsc::Sender<Handle>,
    requests: AsyncMutex<mpsc::Receiver<Handle>>,
    active: Mutex<HashSet<Handle>>,
    callbacks: Mutex<HashSet<Handle>>,
    invoke_children: Mutex<HashSet<Handle>>,
    events_stop: CancellationToken,
    changed: Notify,
    failure: Mutex<Option<Status>>,
}

fn transport(error: impl std::fmt::Display) -> Status {
    eprintln!("orbit-java: {error}");
    Status::Transport
}

impl Application {
    pub(crate) fn start(
        engine: Handle,
        settings: Settings,
    ) -> Result<Handle> {
        if settings.max_exchanges == 0
            || settings.event_capacity == 0
            || settings.control_capacity == 0
            || settings.control_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || settings.incarnation == 0
            || settings.max_exchanges > tokio::sync::Semaphore::MAX_PERMITS
            || settings.event_capacity > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(Status::InvalidArgument);
        }
        claviron::orbitive::FleetName::try_new(&*settings.fleet)
            .map_err(|_| Status::InvalidArgument)?;
        claviron::orbit::link::application::segment::check_application_name(
            &settings.name,
        )
        .map_err(|_| Status::InvalidArgument)?;
        let (ready, requests) = mpsc::channel(settings.max_exchanges);
        let owner = Arc::new(Self {
            engine,
            lifecycle: lifecycle::Lifecycle::new(settings.control_capacity),
            settings,
            _lease: state::lease(engine)?,
            data: OnceLock::new(),
            host: OnceLock::new(),
            thread: Mutex::default(),
            stop: CancellationToken::new(),
            closed: CancellationToken::new(),
            host_stop: CancellationToken::new(),
            io: TaskTracker::new(),
            controls: TaskTracker::new(),
            ready,
            requests: AsyncMutex::new(requests),
            active: Mutex::default(),
            callbacks: Mutex::default(),
            invoke_children: Mutex::default(),
            events_stop: CancellationToken::new(),
            changed: Notify::new(),
            failure: Mutex::default(),
        });
        let id = state::register_application(Arc::clone(&owner))?;
        // One startup result, not an operational work queue or timeout.
        let (started, receiving) = std::sync::mpsc::sync_channel(1);
        let application = Arc::clone(&owner);
        let thread = std::thread::Builder::new()
            .name("orbit-java-io".into())
            .spawn(move || {
                let runtime =
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = started.send(Err(transport(error)));
                            return;
                        }
                    };
                runtime.block_on(async {
                    let _ =
                        application.host.set(tokio::runtime::Handle::current());
                    let startup = Arc::new(Startup(Mutex::new(Some(started))));
                    let driver = Arc::new(Driver {
                        application: Arc::clone(&application),
                        startup: Arc::clone(&startup),
                    });
                    let context = RunContext::new(application.stop.clone());
                    if let Err(error) = driver.run((), (), context).await {
                        application.stop.cancel();
                        let ids = application
                            .active
                            .lock()
                            .expect("active exchanges")
                            .iter()
                            .copied()
                            .collect::<Vec<_>>();
                        for id in ids {
                            let _ = state::remove_exchange(id);
                        }
                        *application
                            .failure
                            .lock()
                            .expect("application failure") =
                            Some(transport(error));
                    }
                    application.stop.cancel();
                    if let Some(data) = application.data.get() {
                        let _ = data.segment.inbox().release_lane(data.lane);
                    }
                    application.events_stop.cancel();
                    application.io.close();
                    application.io.wait().await;
                    if let Some(data) = application.data.get() {
                        data.hold.lock().expect("lane hold").take();
                    }
                    startup.send(Err(Status::Transport));
                    application.closed.cancel();
                    application.host_stop.cancelled().await;
                    application.controls.close();
                    application.controls.wait().await;
                });
            })
            .map_err(transport);
        let thread = match thread {
            Ok(thread) => thread,
            Err(error) => {
                state::remove_application(id)?;
                return Err(error);
            }
        };
        *owner.thread.lock().map_err(|_| Status::Panic)? = Some(thread);
        match receiving.recv().unwrap_or(Err(Status::Transport)) {
            Ok(()) => Ok(id),
            Err(error) => {
                owner.host_stop.cancel();
                if let Some(thread) =
                    owner.thread.lock().map_err(|_| Status::Panic)?.take()
                {
                    let _ = thread.join();
                }
                state::remove_application(id)?;
                Err(error)
            }
        }
    }

    fn data(&self) -> Result<&Data> {
        self.data.get().ok_or(Status::Closed)
    }

    pub(crate) fn next(self: Arc<Self>) -> Result<Handle> {
        if self.stop.is_cancelled() {
            let operation = state::submit_owned(self.engine, &[])?;
            state::finish(operation, Status::End, 0)?;
            return Ok(operation);
        }
        self.submit(
            async move |application, cancellation| {
                let mut queue = application.requests.lock().await;
                loop {
                    let id = tokio::select! {
                        biased;
                        () = application.stop.cancelled() => return Ok((Status::End, 0)),
                        () = cancellation.cancelled() => return Ok((Status::Cancelled, 0)),
                        value = queue.recv() => match value { Some(id) => id, None => return Ok((Status::End, 0)) },
                    };
                    if state::exchange(id)?.is_current() { return Ok((Status::Ok, id)); }
                    state::remove_exchange(id)?;
                }
            },
            false,
        )
    }

    fn submit<F, Fut>(
        self: Arc<Self>,
        work: F,
        control: bool,
    ) -> Result<Handle>
    where
        F: FnOnce(Arc<Self>, CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(Status, Handle)>>
            + Send
            + 'static,
    {
        let operation = state::submit_owned(self.engine, &[])?;
        let cancellation = state::cancellation(operation)?;
        let host = self.host.get().ok_or(Status::Closed)?.clone();
        let owner = Arc::clone(&self);
        let task = async move {
            let (status, resource) = work(owner, cancellation)
                .await
                .unwrap_or_else(|error| (error, 0));
            if state::finish(operation, status, resource).is_err()
                && resource != 0
            {
                // A cancelled intake must not strand the exchange it just acquired.
                let _ = state::remove_exchange(resource)
                    .or_else(|_| state::remove_event(resource))
                    .or_else(|_| state::remove_control(resource))
                    .or_else(|_| {
                        state::invocation(resource)?.abandon();
                        state::remove_invocation(resource)
                    });
            }
        };
        if control {
            self.controls.spawn_on(task, &host);
        } else {
            self.io.spawn_on(task, &host);
        }
        Ok(operation)
    }

    pub(crate) fn withdraw(&self) {
        self.stop.cancel();
    }

    pub(crate) fn wait_closed(self: Arc<Self>) -> Result<Handle> {
        self.submit(async move |application, cancellation| {
            tokio::select! {
                () = application.closed.cancelled() => {
                    if let Some(error) = *application.failure.lock().map_err(|_| Status::Panic)? {
                        return Err(error);
                    }
                    Ok((Status::Ok, 0))
                },
                () = cancellation.cancelled() => Ok((Status::Cancelled, 0)),
            }
        }, true)
    }

    pub(crate) fn release(&self) -> Result<()> {
        if !self.closed.is_cancelled()
            || !self.active.lock().map_err(|_| Status::Panic)?.is_empty()
            || !self.callbacks.lock().map_err(|_| Status::Panic)?.is_empty()
            || !self
                .invoke_children
                .lock()
                .map_err(|_| Status::Panic)?
                .is_empty()
        {
            return Err(Status::Busy);
        }
        self.host_stop.cancel();
        if let Some(thread) =
            self.thread.lock().map_err(|_| Status::Panic)?.take()
        {
            thread.join().map_err(|_| Status::Panic)?;
        }
        Ok(())
    }
}

struct Startup(Mutex<Option<std::sync::mpsc::SyncSender<Result<()>>>>);
impl Startup {
    fn send(
        &self,
        value: Result<()>,
    ) {
        if let Some(sender) = self.0.lock().expect("startup result").take() {
            let _ = sender.send(value);
        }
    }
}

struct Driver {
    application: Arc<Application>,
    startup: Arc<Startup>,
}

#[async_trait]
impl Runnable<()> for Driver {
    async fn run(
        self: Arc<Self>,
        (): (),
        (): (),
        context: RunContext,
    ) -> claviron::runtime::Result<()> {
        self.drive(context).await.map_err(|error| {
            claviron::runtime::Error::msg(format!(
                "Java application Link: {error:?}"
            ))
        })
    }
}

impl Driver {
    async fn drive(
        &self,
        context: RunContext,
    ) -> Result<()> {
        let app = &self.application;
        let segment = Arc::new(
            ApplicationSegment::for_fleet(&app.settings.fleet)
                .map_err(transport)?,
        );
        let joined = segment
            .join(&app.settings.name, "java", app.settings.incarnation)
            .map_err(transport)?;
        let bodies = ApplicationBodies::for_segment(
            &segment,
            joined.lane,
            app.settings.incarnation,
        )
        .map_err(transport)?;
        bodies.reclaimed(&joined.reclaimed);
        let mut receiver = segment.receiver(joined.lane).map_err(transport)?;
        let fleet = Arc::new(
            Fleet::join_shm_as(
                &app.settings.fleet,
                claviron::orbit::link::application::LINK_FLEET_CAPACITY as u16,
                claviron::orbitive::NodeId::new(joined.lane as u16),
            )
            .map_err(transport)?,
        );
        let events = events::Events::new(
            fleet,
            &app.settings.name,
            &segment,
            joined.lane,
            app.settings.event_capacity,
            Arc::downgrade(app),
        )?;
        app.data
            .set(Data {
                segment: Arc::clone(&segment),
                bodies,
                events: Arc::clone(&events),
                lane: joined.lane,
                hold: Mutex::new(Some(joined.hold)),
            })
            .map_err(|_| Status::AlreadyCompleted)?;
        Arc::clone(&events).start(&app.io, app.events_stop.clone())?;
        events::publish_status(app)?;
        self.startup.send(Ok(()));
        let mut frame = Vec::new();
        loop {
            let received = tokio::select! {
                biased;
                () = context.cancelled() => break,
                value = receiver.next(&mut frame) => value.map_err(transport)?,
            };
            match received {
                Received::Event => {
                    events.accept(&frame)?;
                }
                Received::Directory(_) => {}
                Received::Frame => {
                    match frame_kind(&frame).map_err(transport)? {
                        FrameKind::Request => self.accept(&frame).await?,
                        FrameKind::Control => self.control(&frame)?,
                        FrameKind::ShutdownInitiated => {
                            let notice = claviron::orbit::link::application::ShutdownNotice::decode(&frame).map_err(transport)?;
                            app.lifecycle.notify(
                                3,
                                "".into(),
                                Box::default(),
                                notice.edge.into_boxed_str(),
                            )?;
                        }
                        _ => {}
                    }
                }
            }
        }
        segment.inbox().release_lane(joined.lane).map_err(transport)?;
        drop(receiver);
        {
            let mut queue = app.requests.lock().await;
            queue.close();
            while let Ok(id) = queue.try_recv() {
                let _ = state::remove_exchange(id);
            }
        }
        loop {
            let changed = app.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if app.active.lock().map_err(|_| Status::Panic)?.is_empty()
                && app.callbacks.lock().map_err(|_| Status::Panic)?.is_empty()
            {
                break;
            }
            changed.await;
        }
        events.flush()?;
        app.events_stop.cancel();
        app.io.close();
        app.io.wait().await;
        Ok(())
    }

    async fn accept(
        &self,
        bytes: &[u8],
    ) -> Result<()> {
        let app = &self.application;
        let frame = claviron::orbit::link::application::Exchange::decode(bytes)
            .map_err(transport)?;
        let Some(generation) = app.lifecycle.capture()? else {
            http::refuse(app, frame, 503)?;
            return Ok(());
        };
        if app.active.lock().map_err(|_| Status::Panic)?.len()
            >= app.settings.max_exchanges
            || frame.upgrade
        {
            let status = if frame.upgrade { 501 } else { 503 };
            http::refuse(app, frame, status)?;
            return Ok(());
        }
        let uri = match http::absolute_uri(&frame) {
            Ok(uri) => uri,
            Err(_) => {
                http::refuse(app, frame, 400)?;
                return Ok(());
            }
        };
        let exchange = Exchange::new(Arc::clone(app), frame, uri, generation)?;
        let id = state::register_exchange(Arc::clone(&exchange))?;
        exchange.id.store(id, std::sync::atomic::Ordering::Release);
        app.active.lock().map_err(|_| Status::Panic)?.insert(id);
        exchange.start()?;
        if app.ready.try_send(id).is_err() {
            state::remove_exchange(id)?;
        }
        Ok(())
    }

    fn control(
        &self,
        bytes: &[u8],
    ) -> Result<()> {
        let command =
            claviron::orbit::link::application::ControlCommand::decode(bytes)
                .map_err(transport)?;
        let app = &self.application;
        if command.application != *app.settings.name {
            return Ok(());
        }
        match command.name.as_str() {
            "status" if command.target.is_none() => {
                events::publish_status(app)?
            }
            "start" | "stop" | "restart" => {
                if command.target.as_ref().is_some_and(|target| {
                    target.runtime == "java"
                        && target.name == *app.settings.name
                }) {
                    app.action(
                        lifecycle::Action::from_name(&command.name)
                            .ok_or(Status::InvalidArgument)?,
                    )?;
                }
            }
            _ => app.lifecycle.notify(
                2,
                command.name.into_boxed_str(),
                command.payload.into_boxed_slice(),
                "".into(),
            )?,
        }
        Ok(())
    }
}
