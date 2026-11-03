//! Application generation ownership; independent of Link membership and JVM lifetime.
use super::Application;
use crate::{Handle, Result, Status, state};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

pub(super) const TOPIC: &str = "orbit-java.lifecycle.v1";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Action {
    Start = 1,
    Stop = 2,
    Restart = 3,
}
impl Action {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "start" => Some(Self::Start),
            "stop" => Some(Self::Stop),
            "restart" => Some(Self::Restart),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Snapshot {
    pub generation: u64,
    pub running: u64,
}
pub(super) struct Generation {
    pub id: u64,
    pub cancelled: CancellationToken,
}
struct State {
    generation: Arc<Generation>,
    running: bool,
}
impl State {
    fn initial() -> Self {
        Self {
            generation: Arc::new(Generation {
                id: 1,
                cancelled: CancellationToken::new(),
            }),
            running: true,
        }
    }
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            generation: self.generation.id,
            running: u64::from(self.running),
        }
    }
    fn apply(
        &mut self,
        action: Action,
    ) -> Result<Snapshot> {
        // Reserve the next identity before changing anything on exhaustion.
        let next = match action {
            Action::Restart => Some(
                self.generation.id.checked_add(1).ok_or(Status::NoCapacity)?,
            ),
            Action::Start if !self.running => Some(
                self.generation.id.checked_add(1).ok_or(Status::NoCapacity)?,
            ),
            _ => None,
        };
        if action == Action::Stop || action == Action::Restart {
            self.generation.cancelled.cancel();
        }
        if let Some(id) = next {
            self.generation = Arc::new(Generation {
                id,
                cancelled: CancellationToken::new(),
            });
        }
        self.running = action != Action::Stop;
        Ok(self.snapshot())
    }
}
pub(super) struct Control {
    pub kind: u32,
    pub action: u32,
    pub snapshot: Snapshot,
    pub name: Box<str>,
    pub payload: Box<[u8]>,
    pub edge: Box<str>,
}
pub struct ControlRecord {
    pub(super) value: Control,
    application: Arc<Application>,
    id: std::sync::atomic::AtomicU64,
}
impl Drop for ControlRecord {
    fn drop(&mut self) {
        let id = self.id.load(std::sync::atomic::Ordering::Acquire);
        if id != 0 {
            if let Ok(mut callbacks) = self.application.callbacks.lock() {
                callbacks.remove(&id);
            }
            self.application.changed.notify_waiters();
        }
    }
}
pub(super) struct Lifecycle {
    state: Mutex<State>,
    send: mpsc::Sender<Control>,
    receive: AsyncMutex<mpsc::Receiver<Control>>,
}
impl Lifecycle {
    pub fn new(capacity: usize) -> Self {
        let (send, receive) = mpsc::channel(capacity);
        Self {
            state: Mutex::new(State::initial()),
            send,
            receive: AsyncMutex::new(receive),
        }
    }
    pub fn retire(&self) {
        if let Ok(state) = self.state.lock() {
            state.generation.cancelled.cancel();
        }
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        Ok(self.state.lock().map_err(|_| Status::Panic)?.snapshot())
    }
    pub fn capture(&self) -> Result<Option<Arc<Generation>>> {
        let state = self.state.lock().map_err(|_| Status::Panic)?;
        Ok(state.running.then(|| Arc::clone(&state.generation)))
    }
    pub fn token(
        &self,
        expected: u64,
    ) -> Result<Option<CancellationToken>> {
        let state = self.state.lock().map_err(|_| Status::Panic)?;
        Ok((state.running && state.generation.id == expected)
            .then(|| state.generation.cancelled.clone()))
    }
    pub fn apply(
        &self,
        action: Action,
    ) -> Result<()> {
        // Keep notification order equal to transition order, without an unbounded queue.
        let mut state = self.state.lock().map_err(|_| Status::Panic)?;
        let snapshot = state.apply(action)?;
        let slot = self.send.try_reserve().map_err(|_| Status::NoCapacity)?;
        slot.send(Control {
            kind: 1,
            action: action as u32,
            snapshot,
            name: "".into(),
            payload: Box::default(),
            edge: "".into(),
        });
        Ok(())
    }
    pub fn notify(
        &self,
        kind: u32,
        name: Box<str>,
        payload: Box<[u8]>,
        edge: Box<str>,
    ) -> Result<()> {
        let state = self.state.lock().map_err(|_| Status::Panic)?;
        self.send
            .try_send(Control {
                kind,
                action: 0,
                snapshot: state.snapshot(),
                name,
                payload,
                edge,
            })
            .map_err(|_| Status::NoCapacity)
    }
}
impl Application {
    pub(crate) fn next_control(self: Arc<Self>) -> Result<Handle> {
        if self.stop.is_cancelled() {
            let id = state::submit_owned(self.engine, &[])?;
            state::finish(id, Status::End, 0)?;
            return Ok(id);
        }
        self.submit(async move |application, cancellation| {
            let mut receive = application.lifecycle.receive.lock().await;
            tokio::select! {
                biased;
                () = application.stop.cancelled() => Ok((Status::End, 0)),
                () = cancellation.cancelled() => Ok((Status::Cancelled, 0)),
                value = receive.recv() => {
                    let Some(value) = value else { return Ok((Status::End, 0)); };
                    let record = Arc::new(ControlRecord {value, application: Arc::clone(&application), id: 0.into()});
                    let id = state::register_control(Arc::clone(&record))?;
                    record.id.store(id, std::sync::atomic::Ordering::Release);
                    application.callbacks.lock().map_err(|_| Status::Panic)?.insert(id);
                    Ok((Status::Ok, id))
                }
            }
        }, false)
    }
    pub(crate) fn watch_generation(
        self: Arc<Self>,
        expected: u64,
    ) -> Result<Handle> {
        let token = self.lifecycle.token(expected)?;
        self.submit(async move |application, cancellation| {
            if let Some(token) = token {
                tokio::select! {
                    () = token.cancelled() => {},
                    () = application.closed.cancelled() => {},
                    () = cancellation.cancelled() => return Ok((Status::Cancelled, 0)),
                }
            }
            Ok((Status::Ok, 0))
        }, true)
    }
    pub(super) fn apply_lifecycle(
        &self,
        action: Action,
    ) -> Result<()> {
        if let Err(error) = self.lifecycle.apply(action) {
            self.fail(error);
            return Err(error);
        }
        super::events::publish_status(self)
    }
    pub(super) fn fail(
        &self,
        error: Status,
    ) {
        *self.failure.lock().expect("application failure") = Some(error);
        self.lifecycle.retire();
        self.stop.cancel();
    }
    pub(crate) fn action(
        &self,
        action: Action,
    ) -> Result<()> {
        if self.stop.is_cancelled() {
            return Err(Status::Closed);
        }
        self.data()?.events.publish_lifecycle(action)?;
        self.apply_lifecycle(action)
    }
}
#[cfg(test)]
#[path = "../../tests/application/lifecycle.rs"]
mod tests;
