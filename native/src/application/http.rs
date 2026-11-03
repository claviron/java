use claviron::orbit::transport::{Exchange as Frame, ExchangeReply, Headers};
use claviron::orbitive::stream::{ReadHalf, WriteHalf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{Application, transport};
use crate::{Handle, Result, Status, state};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Open,
    Finished,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    New,
    Open,
    Finished,
    Aborted,
}

#[derive(Clone, Copy)]
enum Kind {
    Head,
    Write,
    Finish,
    Complete,
}

struct OutputGate {
    phase: Phase,
    busy: bool,
}
impl OutputGate {
    fn reserve(
        &mut self,
        kind: Kind,
    ) -> Result<Phase> {
        if self.busy {
            return Err(Status::Busy);
        }
        if self.phase == Phase::Aborted {
            return Err(Status::Cancelled);
        }
        let next = match (self.phase, kind) {
            (Phase::New, Kind::Head) => Phase::Open,
            (Phase::New, Kind::Complete) | (Phase::Open, Kind::Finish) => {
                Phase::Finished
            }
            (Phase::Open, Kind::Write) => Phase::Open,
            _ => return Err(Status::AlreadyCompleted),
        };
        let previous = self.phase;
        self.phase = next;
        self.busy = true;
        Ok(previous)
    }
}

enum Action {
    Head { status: u16, headers: Headers },
    Write,
    Finish,
    Complete { status: u16, headers: Headers },
}
impl Action {
    fn kind(&self) -> Kind {
        match self {
            Self::Head { .. } => Kind::Head,
            Self::Write => Kind::Write,
            Self::Finish => Kind::Finish,
            Self::Complete { .. } => Kind::Complete,
        }
    }
}
struct Command {
    action: Action,
    body: state::Payload,
    operation: Handle,
    cancellation: CancellationToken,
}
struct Input {
    half: Option<ReadHalf>,
    busy: bool,
    eof: bool,
}

pub struct Exchange {
    pub(super) application: Arc<Application>,
    pub(super) id: AtomicU64,
    pub(super) frame: Frame,
    pub(super) uri: Box<str>,
    pub(super) generation: Arc<super::lifecycle::Generation>,
    response: mpsc::Sender<Command>,
    receiver: Mutex<Option<mpsc::Receiver<Command>>>,
    input: Mutex<Input>,
    output: Mutex<Option<WriteHalf>>,
    gate: Mutex<OutputGate>,
    cancel: CancellationToken,
    input_stop: CancellationToken,
    terminal: watch::Sender<Terminal>,
}

impl Drop for Exchange {
    fn drop(&mut self) {
        self.cancel.cancel();
        let id = self.id.load(Ordering::Acquire);
        if id != 0 {
            if let Ok(mut active) = self.application.active.lock() {
                active.remove(&id);
            }
            self.application.changed.notify_waiters();
        }
    }
}

pub(super) fn absolute_uri(frame: &Frame) -> Result<Box<str>> {
    if frame.scheme != "http" && frame.scheme != "https" {
        return Err(Status::InvalidArgument);
    }
    let authority = frame
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"host"))
        .map(|(_, value)| {
            std::str::from_utf8(value)
                .map(std::borrow::Cow::Borrowed)
                .map_err(|_| Status::InvalidArgument)
        })
        .transpose()?
        .or_else(|| {
            frame
                .server
                .map(|address| std::borrow::Cow::Owned(address.to_string()))
        })
        .ok_or(Status::InvalidArgument)?;
    let path = std::str::from_utf8(&frame.raw_path)
        .map_err(|_| Status::InvalidArgument)?;
    let query = std::str::from_utf8(&frame.query)
        .map_err(|_| Status::InvalidArgument)?;
    let mut uri = format!("{}://{authority}{path}", frame.scheme);
    if !query.is_empty() {
        uri.push('?');
        uri.push_str(query);
    }
    uri.parse::<::http::Uri>().map_err(|_| Status::InvalidArgument)?;
    Ok(uri.into_boxed_str())
}

impl Exchange {
    pub(super) fn new(
        application: Arc<Application>,
        frame: Frame,
        uri: Box<str>,
        generation: Arc<super::lifecycle::Generation>,
    ) -> Result<Arc<Self>> {
        let (input, output) = match frame.body {
            Some(ticket) => {
                let (input, output) = application
                    .data()?
                    .bodies
                    .accept(ticket)
                    .map_err(transport)?
                    .split();
                (Some(input), Some(output))
            }
            None => (None, None),
        };
        // The gate admits one output operation; this slot is not a chunk queue.
        let (response, receiver) = mpsc::channel(1);
        let eof = input.is_none();
        Ok(Arc::new(Self {
            application,
            generation,
            id: AtomicU64::new(0),
            frame,
            uri,
            response,
            receiver: Mutex::new(Some(receiver)),
            input: Mutex::new(Input { half: input, busy: false, eof }),
            output: Mutex::new(output),
            gate: Mutex::new(OutputGate { phase: Phase::New, busy: false }),
            cancel: CancellationToken::new(),
            input_stop: CancellationToken::new(),
            terminal: watch::channel(Terminal::Open).0,
        }))
    }

    pub(super) fn start(self: Arc<Self>) -> Result<()> {
        let host = self.application.host.get().ok_or(Status::Closed)?.clone();
        self.application.io.spawn_on(Arc::clone(&self).drive(), &host);
        Ok(())
    }

    fn validate_head(
        &self,
        status: u16,
        headers: &Headers,
    ) -> Result<()> {
        if status < 200 || ::http::StatusCode::from_u16(status).is_err() {
            return Err(Status::InvalidArgument);
        }
        for (name, value) in headers {
            ::http::HeaderName::from_bytes(name)
                .map_err(|_| Status::InvalidArgument)?;
            ::http::HeaderValue::from_bytes(value)
                .map_err(|_| Status::InvalidArgument)?;
        }
        Ok(())
    }

    pub(crate) fn respond(
        self: Arc<Self>,
        status: u16,
        headers: Headers,
        bytes: &[u8],
    ) -> Result<Handle> {
        self.validate_head(status, &headers)?;
        self.submit_output(Action::Complete { status, headers }, bytes)
    }

    pub(crate) fn head(
        self: Arc<Self>,
        status: u16,
        headers: Headers,
    ) -> Result<Handle> {
        self.validate_head(status, &headers)?;
        self.submit_output(Action::Head { status, headers }, &[])
    }

    pub(crate) fn write(
        self: Arc<Self>,
        bytes: &[u8],
    ) -> Result<Handle> {
        self.submit_output(Action::Write, bytes)
    }
    pub(crate) fn finish(self: Arc<Self>) -> Result<Handle> {
        self.submit_output(Action::Finish, &[])
    }

    fn submit_output(
        self: Arc<Self>,
        action: Action,
        bytes: &[u8],
    ) -> Result<Handle> {
        let mut gate = self.gate.lock().map_err(|_| Status::Panic)?;
        if self.cancel.is_cancelled()
            || self.generation.cancelled.is_cancelled()
        {
            return Err(Status::Cancelled);
        }
        let previous = gate.reserve(action.kind())?;
        let operation =
            match state::submit_owned(self.application.engine, bytes) {
                Ok(id) => id,
                Err(error) => {
                    gate.phase = previous;
                    gate.busy = false;
                    return Err(error);
                }
            };
        let command = Command {
            action,
            body: state::take_payload(operation)?,
            operation,
            cancellation: state::cancellation(operation)?,
        };
        if self.response.try_send(command).is_err() {
            gate.phase = Phase::Aborted;
            gate.busy = false;
            self.cancel.cancel();
            state::finish(operation, Status::Cancelled, 0)?;
        }
        Ok(operation)
    }

    pub(crate) fn read(
        self: Arc<Self>,
        capacity: usize,
    ) -> Result<Handle> {
        if capacity == 0 || capacity > isize::MAX as usize {
            return Err(Status::InvalidArgument);
        }
        let mut input = self.input.lock().map_err(|_| Status::Panic)?;
        if self.cancel.is_cancelled()
            || self.generation.cancelled.is_cancelled()
            || self.input_stop.is_cancelled()
        {
            return Err(Status::Cancelled);
        }
        if input.busy {
            return Err(Status::Busy);
        }
        if input.eof {
            let operation = state::submit_owned(self.application.engine, &[])?;
            state::finish(operation, Status::End, 0)?;
            return Ok(operation);
        }
        let (operation, mut payload) =
            state::submit_read(self.application.engine, capacity)?;
        let cancellation = state::cancellation(operation)?;
        let mut half = input.half.take().ok_or(Status::Panic)?;
        input.busy = true;
        drop(input);
        let host = self.application.host.get().ok_or(Status::Closed)?.clone();
        let tasks = self.application.io.clone();
        tasks.spawn_on(async move {
            let result = tokio::select! {
                biased;
                () = self.generation.cancelled.cancelled() => Err(Status::Cancelled),
                () = self.cancel.cancelled() => Err(Status::Cancelled),
                () = self.input_stop.cancelled() => Err(Status::Cancelled),
                () = cancellation.cancelled() => Err(Status::Cancelled),
                value = half.read(&mut payload.bytes) => value.map_err(transport),
            };
            {
                let mut input = self.input.lock().expect("request input");
                input.busy = false;
                match result {
                    Ok(0) => { input.eof = true; },
                    Ok(_) if !self.input_stop.is_cancelled() => { input.half = Some(half); },
                    _ => {},
                }
            }
            let settled = match result {
                Ok(0) => { drop(payload); state::finish(operation, Status::End, 0) },
                Ok(length) => { payload.length = length; state::finish_read(operation, payload) },
                Err(error) => { drop(payload); state::finish(operation, error, 0) },
            };
            // A cancelled read may have consumed bytes. Retire this exchange,
            // rather than pretending that retrying preserves the byte stream.
            if result.is_err() || settled.is_err() { self.abort(); }
        }, &host);
        Ok(operation)
    }

    fn publish_head(
        &self,
        status: u16,
        headers: Headers,
    ) -> Result<()> {
        let reply = ExchangeReply {
            correlation: self.frame.correlation,
            status,
            headers,
            body: self.frame.body,
        };
        self.application
            .data()?
            .segment
            .inbox()
            .write(
                self.frame.origin as usize,
                &reply.encode().map_err(transport)?,
            )
            .map_err(transport)?;
        Ok(())
    }

    async fn execute(
        &self,
        command: &mut Command,
        output: &mut Option<WriteHalf>,
    ) -> Result<bool> {
        match std::mem::replace(&mut command.action, Action::Write) {
            Action::Head { status, headers } => {
                self.publish_head(status, headers)?;
                Ok(false)
            }
            Action::Complete { status, headers } => {
                if output.is_none() && !command.body.bytes.is_empty() {
                    return Err(Status::Transport);
                }
                self.publish_head(status, headers)?;
                if let Some(output) = output {
                    output
                        .write_all(&command.body.bytes)
                        .await
                        .map_err(transport)?;
                    output.shutdown().await.map_err(transport)?;
                }
                Ok(true)
            }
            Action::Write => {
                if let Some(output) = output {
                    output
                        .write_all(&command.body.bytes)
                        .await
                        .map_err(transport)?;
                } else if !command.body.bytes.is_empty() {
                    return Err(Status::Transport);
                }
                Ok(false)
            }
            Action::Finish => {
                if let Some(output) = output {
                    output.shutdown().await.map_err(transport)?;
                }
                Ok(true)
            }
        }
    }

    async fn drive(self: Arc<Self>) {
        let mut receiver = self
            .receiver
            .lock()
            .expect("response receiver")
            .take()
            .expect("one output owner");
        let mut output = self.output.lock().expect("response output").take();
        let terminal = loop {
            let command = tokio::select! {
                biased;
                () = self.generation.cancelled.cancelled() => None,
                () = self.cancel.cancelled() => None,
                _ = async { match &mut output { Some(output) => { let _ = output.peer_closed().await; }, None => std::future::pending().await } } => None,
                value = receiver.recv() => value,
            };
            let Some(mut command) = command else {
                break Terminal::Cancelled;
            };
            let cancellation = command.cancellation.clone();
            let result = tokio::select! {
                biased;
                () = self.generation.cancelled.cancelled() => Err(Status::Cancelled),
                () = self.cancel.cancelled() => Err(Status::Cancelled),
                () = cancellation.cancelled() => Err(Status::Cancelled),
                value = self.execute(&mut command, &mut output) => value,
            };
            {
                let mut gate = self.gate.lock().expect("response gate");
                gate.busy = false;
                if result.is_err() {
                    gate.phase = Phase::Aborted;
                }
            }
            let operation = command.operation;
            drop(command);
            if state::finish(
                operation,
                result.as_ref().err().copied().unwrap_or(Status::Ok),
                0,
            )
            .is_err()
            {
                break Terminal::Cancelled;
            }
            match result {
                Ok(false) => {}
                Ok(true) => break Terminal::Finished,
                Err(_) => break Terminal::Cancelled,
            }
        };
        {
            let mut gate = self.gate.lock().expect("response gate");
            gate.phase = if terminal == Terminal::Finished {
                Phase::Finished
            } else {
                Phase::Aborted
            };
            gate.busy = false;
        }
        receiver.close();
        while let Ok(command) = receiver.try_recv() {
            let _ = state::finish(command.operation, Status::Cancelled, 0);
        }
        if terminal == Terminal::Cancelled
            && let Some(output) = &mut output
        {
            output.reset();
        }
        drop(output);
        self.input_stop.cancel();
        self.input.lock().expect("request input").half.take();
        self.terminal.send_replace(terminal);
    }

    pub(crate) fn watch(self: Arc<Self>) -> Result<Handle> {
        let application = Arc::clone(&self.application);
        application.submit(async move |_application, cancellation| {
            let mut terminal = self.terminal.subscribe();
            loop {
                match *terminal.borrow_and_update() {
                    Terminal::Finished => return Ok((Status::Ok, 0)),
                    Terminal::Cancelled => return Ok((Status::Cancelled, 0)),
                    Terminal::Open => {},
                }
                tokio::select! { _ = terminal.changed() => {}, () = cancellation.cancelled() => return Ok((Status::Cancelled, 0)) }
            }
        }, false)
    }
    pub(crate) fn is_current(&self) -> bool {
        !self.generation.cancelled.is_cancelled() && !self.cancel.is_cancelled()
    }

    pub(crate) fn abort(&self) {
        self.cancel.cancel();
    }
}

pub(super) fn refuse(
    application: &Application,
    frame: Frame,
    status: u16,
) -> Result<()> {
    let data = application.data()?;
    let mut endpoint = frame
        .body
        .map(|ticket| data.bodies.accept(ticket))
        .transpose()
        .map_err(transport)?;
    let reply = ExchangeReply {
        correlation: frame.correlation,
        status,
        headers: Vec::new(),
        body: frame.body,
    };
    let published = data
        .segment
        .inbox()
        .write(frame.origin as usize, &reply.encode().map_err(transport)?);
    if let Some(endpoint) = &mut endpoint {
        if published.is_ok() {
            endpoint.finish().map_err(transport)?;
        } else {
            endpoint.reset();
        }
    }
    published.map(|_| ()).map_err(transport)
}

#[cfg(test)]
#[path = "../../tests/application/http.rs"]
mod tests;
