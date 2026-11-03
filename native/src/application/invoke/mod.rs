//! Application-owned invokes on an isolated Link, using the existing core call protocol.
pub mod ffi;
use super::{Application, Startup, lifecycle::Generation, transport};
use crate::{Handle, Result, Status, state};
use claviron::orbit::invoke::{CallError, CallResult, Client, Failure};
use claviron::orbit::link::{Connection, invoke::Limits};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::task::Poll;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Code {
    Success = 0,
    UnknownTarget = 1,
    AmbiguousTarget = 2,
    Unavailable = 3,
    Busy = 4,
    Closed = 5,
    OutcomeUnknown = 6,
    PayloadTooLarge = 7,
    Protocol = 8,
    Transport = 9,
    RemoteBusy = 10,
    RemoteClosing = 11,
    Unsupported = 12,
    ResponseTooLarge = 13,
    Rejected = 14,
    InvalidConfig = 15,
}
fn outcome(value: CallResult<Vec<u8>>) -> (Code, Vec<u8>) {
    use CallError as E;
    match value {
        Ok(bytes) => (Code::Success, bytes),
        Err(E::Remote(Failure::Rejected(bytes))) => (Code::Rejected, bytes),
        Err(error) => (
            match error {
                E::UnknownTarget => Code::UnknownTarget,
                E::AmbiguousTarget => Code::AmbiguousTarget,
                E::Unavailable => Code::Unavailable,
                E::Busy => Code::Busy,
                E::Closed => Code::Closed,
                E::OutcomeUnknown => Code::OutcomeUnknown,
                E::PayloadTooLarge => Code::PayloadTooLarge,
                E::Protocol(_) => Code::Protocol,
                E::Transport(_) => Code::Transport,
                E::Remote(Failure::Busy) => Code::RemoteBusy,
                E::Remote(Failure::Closing) => Code::RemoteClosing,
                E::Remote(Failure::Unsupported) => Code::Unsupported,
                E::Remote(Failure::ResponseTooLarge) => Code::ResponseTooLarge,
                E::InvalidConfig(_) => Code::InvalidConfig,
                E::Remote(Failure::Rejected(_)) => unreachable!(),
            },
            Vec::new(),
        ),
    }
}
pub struct ResultRecord {
    pub code: Code,
    payload: state::Payload,
    _lease: state::Lease,
}
pub(crate) struct Settings {
    pub fleet: Box<str>,
    pub service: Box<str>,
    pub limits: Limits,
    pub routes: HashMap<Box<str>, Box<str>>,
}
struct Data {
    client: Client,
}
pub struct Invocations {
    application: Arc<Application>,
    id: AtomicU64,
    limits: Limits,
    data: OnceLock<Data>,
    send: mpsc::Sender<Handle>,
    receive: AsyncMutex<mpsc::Receiver<Handle>>,
    stop: CancellationToken,
    closed: CancellationToken,
    failure: Mutex<Option<Status>>,
}
impl Drop for Invocations {
    fn drop(&mut self) {
        let id = self.id.load(Ordering::Acquire);
        if let Ok(mut children) = self.application.invoke_children.lock() {
            children.remove(&id);
        }
        self.application.changed.notify_waiters();
    }
}
impl Invocations {
    pub(crate) fn open(
        application: Arc<Application>,
        settings: Settings,
    ) -> Result<Handle> {
        use claviron::orbit::transport::TRANSPORT_LINK_SPEC;
        if application.stop.is_cancelled() {
            return Err(Status::Closed);
        }
        // An isolated fleet prevents non-HTTP lanes entering the application event bridge's routing set.
        if settings.fleet == application.settings.fleet {
            return Err(Status::InvalidArgument);
        }
        claviron::orbitive::FleetName::try_new(&*settings.fleet)
            .map_err(|_| Status::InvalidArgument)?;
        settings
            .limits
            .validate(TRANSPORT_LINK_SPEC.inbox.payload_capacity)
            .map_err(transport)?;
        if settings.limits.accepted_calls > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(Status::InvalidArgument);
        }
        let (send, receive) = mpsc::channel(settings.limits.accepted_calls);
        let owner = Arc::new(Self {
            stop: application.stop.child_token(),
            application,
            id: 0.into(),
            limits: settings.limits,
            data: OnceLock::new(),
            send,
            receive: AsyncMutex::new(receive),
            closed: CancellationToken::new(),
            failure: Mutex::default(),
        });
        let id = state::register_invocations(Arc::clone(&owner))?;
        owner.id.store(id, Ordering::Release);
        owner
            .application
            .invoke_children
            .lock()
            .map_err(|_| Status::Panic)?
            .insert(id);
        let (started, receiving) = std::sync::mpsc::sync_channel(1);
        let startup = Arc::new(Startup(Mutex::new(Some(started))));
        let host = owner.application.host.get().ok_or(Status::Closed)?.clone();
        let binding = Arc::clone(&owner);
        let tasks = owner.application.io.clone();
        tasks.spawn_on(
            async move {
                if let Err(error) = Arc::clone(&binding)
                    .run(settings, Arc::clone(&startup))
                    .await
                {
                    *binding.failure.lock().expect("invoke failure") =
                        Some(error);
                    startup.send(Err(error));
                }
                binding.stop.cancel();
                binding.closed.cancel();
            },
            &host,
        );
        match receiving.recv().unwrap_or(Err(Status::Transport)) {
            Ok(()) => Ok(id),
            Err(error) => {
                state::remove_invocations(id)?;
                Err(error)
            }
        }
    }
    async fn run(
        self: Arc<Self>,
        settings: Settings,
        startup: Arc<Startup>,
    ) -> Result<()> {
        let connection = Connection::open(
            &settings.fleet,
            claviron::orbit::transport::TRANSPORT_LINK_SPEC,
            &settings.service,
            "invoke",
            self.application.settings.incarnation,
        )
        .map_err(transport)?;
        let (handle, driver) =
            connection.invocations(settings.limits).map_err(transport)?;
        let mut client = Client::new();
        for (target, service) in settings.routes {
            let route =
                handle.route(service.into_string()).map_err(transport)?;
            client.register(target.into_string(), route).map_err(transport)?;
        }
        self.data.set(Data { client }).map_err(|_| Status::AlreadyCompleted)?;
        let shutdown = async {
            self.stop.cancelled().await;
            let mut queue = self.receive.lock().await;
            queue.close();
            while let Ok(id) = queue.try_recv() {
                if let Ok(call) = state::invocation(id) {
                    call.abandon();
                }
                let _ = state::remove_invocation(id);
            }
        };
        let service = Arc::clone(&self);
        let driving = driver.run_until(
            shutdown,
            move |call| Arc::clone(&service).accept(call),
            |_| async { Ok(()) },
        );
        tokio::pin!(driving);
        // Publish readiness only after the core driver actually enables call admission.
        std::future::poll_fn(|cx| {
            let result = driving.as_mut().poll(cx);
            match &result {
                Poll::Pending => startup.send(Ok(())),
                Poll::Ready(Ok(())) => startup.send(Err(Status::Closed)),
                Poll::Ready(Err(_)) => startup.send(Err(Status::Transport)),
            }
            result
        })
        .await
        .map_err(transport)
    }
    async fn accept(
        self: Arc<Self>,
        call: claviron::orbit::link::invoke::Call,
    ) -> std::result::Result<Vec<u8>, Failure> {
        if self.stop.is_cancelled() {
            return Err(Failure::Closing);
        }
        let generation = self
            .application
            .lifecycle
            .capture()
            .map_err(|_| Failure::Closing)?
            .ok_or(Failure::Closing)?;
        let bytes = call.request.payload;
        let reservation =
            state::reserve_bytes(self.application.engine, bytes.len())
                .map_err(|_| Failure::Busy)?;
        let payload = reservation
            .payload(bytes.into_boxed_slice())
            .map_err(|_| Failure::Busy)?;
        let (send, receive) = oneshot::channel();
        let record = Arc::new(Invocation {
            owner: Arc::clone(&self),
            id: 0.into(),
            operation: call.request.operation.into_boxed_str(),
            payload,
            generation,
            reply: Mutex::new(Some(send)),
            terminal: watch::channel(Terminal::Open).0,
        });
        let id = state::register_invocation(Arc::clone(&record))
            .map_err(|_| Failure::Busy)?;
        record.id.store(id, Ordering::Release);
        self.application
            .callbacks
            .lock()
            .map_err(|_| Failure::Busy)?
            .insert(id);
        if self.send.try_send(id).is_err() {
            record.abandon();
            let _ = state::remove_invocation(id);
            return Err(Failure::Busy);
        }
        let result = tokio::select! {
            biased;
            ()=record.generation.cancelled.cancelled()=>Err(Failure::Closing),
            value=receive=>value.unwrap_or(Err(Failure::Closing)),
        };
        record.end(if record.generation.cancelled.is_cancelled() {
            Terminal::Cancelled
        } else {
            Terminal::Replied
        });
        result
    }
    pub(crate) fn next(self: Arc<Self>) -> Result<Handle> {
        let app = Arc::clone(&self.application);
        app.submit(async move |_application,cancel| {
            let mut queue=self.receive.lock().await;
            loop {
                let id=tokio::select! {
                    biased;
                    ()=self.stop.cancelled()=>return Ok((Status::End,0)),
                    ()=cancel.cancelled()=>return Ok((Status::Cancelled,0)),
                    value=queue.recv()=>match value {Some(id)=>id,None=>return Ok((Status::End,0))},
                };
                if state::invocation(id)?.current() {return Ok((Status::Ok,id));}
                state::invocation(id)?.abandon(); state::remove_invocation(id)?;
            }
        },false)
    }
    pub(crate) fn call(
        self: Arc<Self>,
        target: Box<str>,
        operation: Box<str>,
        bytes: &[u8],
    ) -> Result<Handle> {
        if self.stop.is_cancelled() {
            return Err(Status::Closed);
        }
        let generation =
            self.application.lifecycle.capture()?.ok_or(Status::Closed)?;
        let reservation = state::reserve_bytes(
            self.application.engine,
            self.limits.response_bytes,
        )?;
        let id = state::submit_owned(self.application.engine, bytes)?;
        let mut input = state::take_payload(id)?;
        let cancel = state::cancellation(id)?;
        let host = self.application.host.get().ok_or(Status::Closed)?.clone();
        let tasks = self.application.io.clone();
        tasks.spawn_on(async move {
            let payload=std::mem::take(&mut input.bytes).into_vec();
            let result=tokio::select! {
                biased;
                ()=cancel.cancelled()=>None,
                ()=generation.cancelled.cancelled()=>None,
                value=self.data.get().expect("invoke data").client.call(&target,operation.into_string(),payload)=>Some(value),
            };
            drop(input);
            let Some(value)=result else {let _=state::finish(id,Status::Cancelled,0);return;};
            let result: Result<Handle>=(|| {
                let (code,bytes)=outcome(value);
                let record=Arc::new(ResultRecord {code,payload:reservation.payload(bytes.into_boxed_slice())?,_lease:state::lease(self.application.engine)?});
                state::register_invoke_result(record)
            })();
            match result {
                Ok(resource)=> {if state::finish(id,Status::Ok,resource).is_err() {let _=state::remove_invoke_result(resource);}},
                Err(error)=> {let _=state::finish(id,error,0);},
            }
        },&host);
        Ok(id)
    }
    pub(crate) fn withdraw(&self) {
        self.stop.cancel();
    }
    pub(crate) fn wait_closed(self: Arc<Self>) -> Result<Handle> {
        let app = Arc::clone(&self.application);
        app.submit(async move |_application,cancel| {
            tokio::select! {
                ()=self.closed.cancelled()=> {
                    if let Some(error)=*self.failure.lock().map_err(|_|Status::Panic)? {return Err(error);}
                    Ok((Status::Ok,0))
                },
                ()=cancel.cancelled()=>Ok((Status::Cancelled,0)),
            }
        },true)
    }
    pub(crate) fn releasable(&self) -> Result<()> {
        if self.closed.is_cancelled() { Ok(()) } else { Err(Status::Busy) }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Open,
    Replied,
    Cancelled,
}
type ReplySender = oneshot::Sender<std::result::Result<Vec<u8>, Failure>>;

pub struct Invocation {
    owner: Arc<Invocations>,
    id: AtomicU64,
    operation: Box<str>,
    payload: state::Payload,
    generation: Arc<Generation>,
    reply: Mutex<Option<ReplySender>>,
    terminal: watch::Sender<Terminal>,
}
impl Drop for Invocation {
    fn drop(&mut self) {
        let id = self.id.load(Ordering::Acquire);
        if let Ok(mut active) = self.owner.application.callbacks.lock() {
            active.remove(&id);
        }
        self.owner.application.changed.notify_waiters();
    }
}
impl Invocation {
    fn end(
        &self,
        terminal: Terminal,
    ) {
        self.terminal.send_if_modified(|state| {
            if *state == Terminal::Open {
                *state = terminal;
                true
            } else {
                false
            }
        });
    }
    pub(crate) fn abandon(&self) {
        self.end(Terminal::Cancelled);
        if let Ok(mut sender) = self.reply.lock()
            && let Some(sender) = sender.take()
        {
            let _ = sender.send(Err(Failure::Closing));
        }
    }
    pub(crate) fn current(&self) -> bool {
        !self.generation.cancelled.is_cancelled()
            && *self.terminal.borrow() == Terminal::Open
    }
    pub(crate) fn respond(
        &self,
        code: u32,
        bytes: &[u8],
    ) -> Result<()> {
        if !self.current() {
            return Err(Status::Cancelled);
        }
        let result = match code {
            0 if bytes.len() > self.owner.limits.response_bytes => {
                Err(Failure::ResponseTooLarge)
            }
            0 => Ok(bytes.to_vec()),
            1 => Err(Failure::Busy),
            2 => Err(Failure::Closing),
            3 => Err(Failure::Unsupported),
            4 => Err(Failure::ResponseTooLarge),
            5 if bytes.len() <= self.owner.limits.response_bytes => {
                Err(Failure::Rejected(bytes.to_vec()))
            }
            5 => Err(Failure::ResponseTooLarge),
            _ => return Err(Status::InvalidArgument),
        };
        self.reply
            .lock()
            .map_err(|_| Status::Panic)?
            .take()
            .ok_or(Status::AlreadyCompleted)?
            .send(result)
            .map_err(|_| Status::Cancelled)?;
        self.end(Terminal::Replied);
        Ok(())
    }
    pub(crate) fn watch(self: Arc<Self>) -> Result<Handle> {
        let app = Arc::clone(&self.owner.application);
        app.submit(async move |_application,cancel| {
            let mut terminal=self.terminal.subscribe();
            loop {
                let state=*terminal.borrow_and_update();
                match state {Terminal::Replied=>return Ok((Status::Ok,0)),Terminal::Cancelled=>return Ok((Status::Cancelled,0)),Terminal::Open=>{}}
                tokio::select! {
                    ()=self.generation.cancelled.cancelled()=>return Ok((Status::Cancelled,0)),
                    ()=cancel.cancelled()=>return Ok((Status::Cancelled,0)),
                    _=terminal.changed()=>{},
                }
            }
        },false)
    }
}
#[cfg(test)]
#[path = "../../../tests/application/invoke/mod.rs"]
mod tests;
