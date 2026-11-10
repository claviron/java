use claviron::orbit::link::application::{
    Exchange as Frame, ExchangeReply, Headers, Message, MessageReader,
    write_message,
};
use claviron::orbitive::stream::{ReadHalf, WriteHalf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{Application, transport};
use crate::{Handle, Result, Status, state};

// WebSocket control payloads have a protocol maximum of 125 bytes, even when
// an application's text/binary limit is smaller. This is not a tuning value.
const CONTROL_BYTES: usize = 125;
/// A received message's lease starts with its kind and little-endian close code.
const MESSAGE_HEAD: usize = 3;
pub(crate) const MESSAGE_TEXT: u32 = 1;
pub(crate) const MESSAGE_BINARY: u32 = 2;
pub(crate) const MESSAGE_CLOSE: u32 = 3;

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
    Head {
        status: u16,
        headers: Headers,
    },
    Write,
    Finish,
    Complete {
        status: u16,
        headers: Headers,
    },
    /// The command's body is the text, the binary data or the close reason.
    Message {
        kind: u32,
        code: u16,
    },
}
impl Action {
    fn kind(&self) -> Kind {
        match self {
            Self::Head { .. } => Kind::Head,
            Self::Write => Kind::Write,
            Self::Finish => Kind::Finish,
            Self::Complete { .. } => Kind::Complete,
            Self::Message { kind: MESSAGE_CLOSE, .. } => Kind::Finish,
            Self::Message { .. } => Kind::Write,
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
    inline: tokio_util::bytes::Bytes,
    half: Option<ReadHalf>,
    busy: bool,
    eof: bool,
    records: Option<MessageReader>,
}

pub struct Exchange {
    _admission: Option<claviron::orbit::link::application::WorkExecution>,
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
    /// Set by acceptance: this exchange now carries Link message records.
    session_limit: OnceLock<u32>,
    /// A close the peer sent, for the output owner to answer.
    peer_close: Mutex<Option<(u16, String)>>,
    closing: Notify,
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
        admission: Option<claviron::orbit::link::application::WorkExecution>,
    ) -> Result<Arc<Self>> {
        let (input, output) = match frame.stream {
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
        let inline = match &frame.body {
            claviron::orbit::link::application::ExchangeBody::Inline(bytes) => {
                bytes.clone()
            }
            _ => tokio_util::bytes::Bytes::new(),
        };
        let eof = matches!(
            frame.body,
            claviron::orbit::link::application::ExchangeBody::Empty
        );
        Ok(Arc::new(Self {
            _admission: admission,
            application,
            generation,
            id: AtomicU64::new(0),
            frame,
            uri,
            response,
            receiver: Mutex::new(Some(receiver)),
            input: Mutex::new(Input {
                inline,
                half: input,
                busy: false,
                eof,
                records: None,
            }),
            output: Mutex::new(output),
            gate: Mutex::new(OutputGate { phase: Phase::New, busy: false }),
            cancel: CancellationToken::new(),
            input_stop: CancellationToken::new(),
            terminal: watch::channel(Terminal::Open).0,
            session_limit: OnceLock::new(),
            peer_close: Mutex::new(None),
            closing: Notify::new(),
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
        // 101 is Link's acceptance and only accept_session sends it. A success
        // status cannot answer a session: it would authorize no message stream.
        if status < 200
            || (self.frame.upgrade && status < 300)
            || ::http::StatusCode::from_u16(status).is_err()
        {
            return Err(Status::InvalidArgument);
        }
        Self::validate_headers(headers)
    }

    fn validate_headers(headers: &Headers) -> Result<()> {
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
        if self.frame.upgrade {
            return Err(Status::InvalidArgument);
        }
        self.submit_output(Action::Write, bytes)
    }

    /// Accept a message session. Native WebSocket and WebTransport handshakes
    /// stay at the edge; the 101 this sends is the Link acknowledgment.
    pub(crate) fn accept_session(
        self: Arc<Self>,
        max_message_bytes: u32,
        headers: Headers,
    ) -> Result<Handle> {
        if !self.frame.upgrade
            || self.frame.stream.is_none()
            || max_message_bytes == 0
        {
            return Err(Status::InvalidArgument);
        }
        Self::validate_headers(&headers)?;
        let operation = Arc::clone(&self)
            .submit_output(Action::Head { status: 101, headers }, &[])?;
        self.session_limit
            .set(max_message_bytes)
            .map_err(|_| Status::AlreadyCompleted)?;
        Ok(operation)
    }

    /// Queue one message. A close also ends this direction.
    pub(crate) fn send_message(
        self: Arc<Self>,
        kind: u32,
        code: u32,
        bytes: &[u8],
    ) -> Result<Handle> {
        let limit = *self.session_limit.get().ok_or(Status::InvalidArgument)?;
        let code = u16::try_from(code).map_err(|_| Status::InvalidArgument)?;
        match kind {
            MESSAGE_TEXT | MESSAGE_BINARY => {
                if bytes.len() > limit as usize {
                    return Err(Status::NoCapacity);
                }
            }
            MESSAGE_CLOSE => validate_close(code, bytes)?,
            _ => return Err(Status::InvalidArgument),
        }
        if kind != MESSAGE_BINARY {
            std::str::from_utf8(bytes).map_err(|_| Status::InvalidArgument)?;
        }
        self.submit_output(Action::Message { kind, code }, bytes)
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
        if capacity == 0 || capacity > isize::MAX as usize || self.frame.upgrade
        {
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
        let mut half = input.half.take();
        let mut inline = std::mem::take(&mut input.inline);
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
                value = async {
                    if !inline.is_empty() {
                        let length = inline.len().min(payload.bytes.len());
                        payload.bytes[..length].copy_from_slice(&inline.split_to(length));
                        Ok(length)
                    } else if matches!(self.frame.body, claviron::orbit::link::application::ExchangeBody::Stream) {
                        match half.as_mut() { Some(half) => half.read(&mut payload.bytes).await.map_err(transport), None => Ok(0) }
                    } else { Ok(0) }
                } => value,
            };
            {
                let mut input = self.input.lock().expect("request input");
                input.busy = false;
                match result {
                    Ok(0) => { input.eof = true; },
                    Ok(_) if !self.input_stop.is_cancelled() => { input.half = half; input.inline = inline; },
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

    /// One pending receive. Its lease starts with MESSAGE_HEAD, and END is the
    /// clean end of the session. Cancelling it abandons the session.
    pub(crate) fn read_message(self: Arc<Self>) -> Result<Handle> {
        let limit =
            *self.session_limit.get().ok_or(Status::InvalidArgument)? as usize;
        let mut input = self.input.lock().map_err(|_| Status::Panic)?;
        if self.cancel.is_cancelled()
            || self.generation.cancelled.is_cancelled()
        {
            return Err(Status::Cancelled);
        }
        if input.busy {
            return Err(Status::Busy);
        }
        if input.eof || self.input_stop.is_cancelled() {
            let operation = state::submit_owned(self.application.engine, &[])?;
            state::finish(operation, Status::End, 0)?;
            return Ok(operation);
        }
        let floor = limit.max(CONTROL_BYTES);
        let (operation, mut payload) =
            state::submit_read(self.application.engine, floor + MESSAGE_HEAD)?;
        let cancellation = state::cancellation(operation)?;
        let mut half = input.half.take().ok_or(Status::Panic)?;
        let mut records = input
            .records
            .take()
            .unwrap_or_else(|| MessageReader::with_limit(floor));
        input.busy = true;
        drop(input);
        let host = self.application.host.get().ok_or(Status::Closed)?.clone();
        let tasks = self.application.io.clone();
        tasks.spawn_on(async move {
            let result = tokio::select! {
                biased;
                () = self.generation.cancelled.cancelled() => Err(Status::Cancelled),
                () = self.cancel.cancelled() => Err(Status::Cancelled),
                () = cancellation.cancelled() => Err(Status::Cancelled),
                () = self.input_stop.cancelled() => Ok(None),
                value = records.read(&mut half) => value.map_err(|error| {
                    // The Link's own records are well formed, so the invalid
                    // record a consumer meets is the one over its limit.
                    if error.kind() == std::io::ErrorKind::InvalidData {
                        Status::NoCapacity
                    } else {
                        transport(error)
                    }
                }),
            };
            let result = result.and_then(|message| match message {
                Some(message) => encode_message(message, limit, &mut payload).map(Some),
                None => Ok(None),
            });
            {
                let mut input = self.input.lock().expect("request input");
                input.busy = false;
                match result {
                    Ok(None) => { input.eof = true; },
                    Ok(Some(_)) => { input.half = Some(half); input.records = Some(records); },
                    Err(_) => {},
                }
            }
            let failed = result.is_err();
            let settled = match result {
                Ok(None) => { drop(payload); state::finish(operation, Status::End, 0) },
                Ok(Some(close)) => {
                    if let Some(close) = close {
                        *self.peer_close.lock().expect("peer close") = Some(close);
                        self.closing.notify_one();
                    }
                    state::finish_read(operation, payload)
                },
                Err(error) => { drop(payload); state::finish(operation, error, 0) },
            };
            if failed || settled.is_err() { self.abort(); }
        }, &host);
        Ok(operation)
    }

    /// Answer the peer's close, then end this direction. 1005 and 1006 are
    /// observations, never codes sent on the wire.
    async fn answer_close(
        &self,
        output: &mut Option<WriteHalf>,
    ) -> Result<()> {
        let close = self.peer_close.lock().map_err(|_| Status::Panic)?.take();
        let Some(output) = output.as_mut() else { return Ok(()) };
        if let Some((code, reason)) = close
            && !matches!(code, 1005 | 1006)
        {
            let message = Message::Close { code, reason };
            write_message(output, &message).await.map_err(transport)?;
        }
        output.shutdown().await.map_err(transport)
    }

    async fn publish_head(
        &self,
        status: u16,
        headers: Headers,
        output: &mut Option<WriteHalf>,
    ) -> Result<()> {
        use claviron::orbit::link::application::Error as LinkError;

        let reply = ExchangeReply {
            correlation: self.frame.correlation,
            status,
            headers,
            body: if self.frame.stream.is_some() {
                claviron::orbit::link::application::ExchangeBody::Stream
            } else {
                claviron::orbit::link::application::ExchangeBody::Empty
            },
        };
        let bytes = reply.encode().map_err(transport)?;
        let segment = &self.application.data()?.segment;
        loop {
            match segment.publish_reply(self.frame.origin as usize, &[&bytes]) {
                Ok(()) => return Ok(()),
                Err(LinkError::ProducerBusy) => {
                    tokio::select! {
                        result = segment.record_released() => { result.map_err(transport)?; },
                        _ = async {
                            match output.as_mut() {
                                Some(output) => { let _ = output.peer_closed().await; },
                                None => std::future::pending().await,
                            }
                        }, if !self.frame.upgrade => return Err(Status::Cancelled),
                    }
                }
                Err(error) => return Err(transport(error)),
            }
        }
    }

    async fn execute(
        &self,
        command: &mut Command,
        output: &mut Option<WriteHalf>,
    ) -> Result<bool> {
        match std::mem::replace(&mut command.action, Action::Write) {
            Action::Head { status, headers } => {
                self.publish_head(status, headers, output).await?;
                Ok(false)
            }
            Action::Complete { status, headers } => {
                if output.is_none() && !command.body.bytes.is_empty() {
                    return Err(Status::Transport);
                }
                self.publish_head(status, headers, output).await?;
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
            Action::Message { kind, code } => {
                let output = output.as_mut().ok_or(Status::Transport)?;
                let bytes = std::mem::take(&mut command.body.bytes).into_vec();
                let text = |bytes| {
                    String::from_utf8(bytes).map_err(|_| Status::Transport)
                };
                let message = match kind {
                    MESSAGE_TEXT => Message::Text(text(bytes)?),
                    MESSAGE_BINARY => Message::Binary(bytes),
                    _ => Message::Close { code, reason: text(bytes)? },
                };
                write_message(output, &message).await.map_err(transport)?;
                if kind == MESSAGE_CLOSE {
                    output.shutdown().await.map_err(transport)?;
                }
                Ok(kind == MESSAGE_CLOSE)
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
                // A session's edge leaves right after its last record, which
                // the input owner has still to read: its end is not a cancel.
                _ = async { match &mut output { Some(output) => { let _ = output.peer_closed().await; }, None => std::future::pending().await } }, if !self.frame.upgrade => None,
                value = receiver.recv() => value,
                () = self.closing.notified() => {
                    // A peer already gone cannot receive the answer; the close
                    // it sent still ended this session cleanly.
                    tokio::select! {
                        biased;
                        () = self.generation.cancelled.cancelled() => {},
                        () = self.cancel.cancelled() => {},
                        _ = self.answer_close(&mut output) => {},
                    }
                    break Terminal::Finished;
                }
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

/// Fill a receive's lease; a close also returns what the output owner answers.
fn encode_message(
    message: Message,
    limit: usize,
    payload: &mut state::Payload,
) -> Result<Option<(u16, String)>> {
    let (kind, code, data, close): (u32, u16, Vec<u8>, _) = match message {
        Message::Text(text) => (MESSAGE_TEXT, 0, text.into_bytes(), None),
        Message::Binary(bytes) => (MESSAGE_BINARY, 0, bytes, None),
        Message::Close { code, reason } => {
            let answer = (code, reason.clone());
            (MESSAGE_CLOSE, code, reason.into_bytes(), Some(answer))
        }
    };
    if close.is_none() && data.len() > limit {
        return Err(Status::NoCapacity);
    }
    let length = MESSAGE_HEAD + data.len();
    let lease = payload.bytes.get_mut(..length).ok_or(Status::NoCapacity)?;
    lease[0] = kind as u8;
    lease[1..MESSAGE_HEAD].copy_from_slice(&code.to_le_bytes());
    lease[MESSAGE_HEAD..].copy_from_slice(&data);
    payload.length = length;
    Ok(close)
}

fn validate_close(
    code: u16,
    reason: &[u8],
) -> Result<()> {
    if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
        || reason.len() > CONTROL_BYTES - 2
    {
        return Err(Status::InvalidArgument);
    }
    Ok(())
}

pub(super) fn refuse(
    application: &Application,
    frame: Frame,
    status: u16,
) -> Result<()> {
    let data = application.data()?;
    let mut endpoint = frame
        .stream
        .map(|ticket| data.bodies.accept(ticket))
        .transpose()
        .map_err(transport)?;
    let reply = ExchangeReply {
        correlation: frame.correlation,
        status,
        headers: Vec::new(),
        body: if frame.stream.is_some() {
            claviron::orbit::link::application::ExchangeBody::Stream
        } else {
            claviron::orbit::link::application::ExchangeBody::Empty
        },
    };
    let published = data.segment.publish_reply(
        frame.origin as usize,
        &[&reply.encode().map_err(transport)?],
    );
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
