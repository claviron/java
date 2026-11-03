use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};

use crate::{
    ABI_VERSION, BufferView, Completion, Handle, Options, Result, Status,
};

type Registry = HashMap<Handle, Resource>;
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
enum Resource {
    Application(Arc<crate::application::Application>),
    Exchange(Arc<crate::application::Exchange>),
    Event(Arc<crate::application::EventRecord>),
    Control(Arc<crate::application::lifecycle::ControlRecord>),
    Invocations(Arc<crate::application::invoke::Invocations>),
    Invocation(Arc<crate::application::invoke::Invocation>),
    InvokeResult(Arc<crate::application::invoke::ResultRecord>),
    Engine(Arc<Engine>),
    Operation(Arc<Operation>),
    Buffer(Arc<Buffer>),
}

fn lock<T>(value: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    value.lock().map_err(|_| Status::Panic)
}

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(Mutex::default)
}

pub(crate) fn next_handle() -> Result<Handle> {
    NEXT.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
        .map_err(|_| Status::NoCapacity)
}

fn insert(
    id: Handle,
    value: Resource,
) -> Result<()> {
    lock(registry())?.insert(id, value);
    Ok(())
}

fn get(id: Handle) -> Result<Resource> {
    lock(registry())?.get(&id).cloned().ok_or(Status::InvalidHandle)
}

fn remove(id: Handle) -> Result<()> {
    // Destructors may acquire engine state; never run them under the registry lock.
    let value = lock(registry())?.remove(&id).ok_or(Status::InvalidHandle)?;
    drop(value);
    Ok(())
}

fn engine(id: Handle) -> Result<Arc<Engine>> {
    match get(id)? {
        Resource::Engine(x) => Ok(x),
        _ => Err(Status::WrongKind),
    }
}

fn operation(id: Handle) -> Result<Arc<Operation>> {
    match get(id)? {
        Resource::Operation(x) => Ok(x),
        _ => Err(Status::WrongKind),
    }
}

struct Engine {
    state: Mutex<EngineState>,
    changed: Condvar,
    max_operations: u64,
    max_bytes: u64,
}

#[derive(Default)]
struct EngineState {
    shutdown: bool,
    operations: u64,
    bytes: u64,
    buffers: u64,
    resources: u64,
    waiter: bool,
    pending: HashMap<Handle, Weak<Operation>>,
    queue: VecDeque<Completion>,
}

pub(crate) struct Payload {
    pub(crate) bytes: Box<[u8]>,
    pub(crate) length: usize,
    reserved: u64,
    engine: Arc<Engine>,
}

impl Drop for Payload {
    fn drop(&mut self) {
        if let Ok(mut state) = self.engine.state.lock() {
            state.bytes -= self.reserved;
        }
    }
}

struct Buffer {
    payload: Payload,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Ok(mut state) = self.payload.engine.state.lock() {
            state.buffers -= 1;
        }
    }
}

struct Operation {
    id: Handle,
    result_id: Handle,
    engine: Arc<Engine>,
    state: Mutex<OperationState>,
    cancellation: tokio_util::sync::CancellationToken,
}

struct OperationState {
    payload: Option<Payload>,
    terminal: bool,
    delivered: bool,
}

impl Drop for Operation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.engine.state.lock() {
            state.operations -= 1;
        }
    }
}

pub(crate) fn open(options: Options) -> Result<Handle> {
    if options.struct_size != size_of::<Options>() as u32
        || options.abi_version != ABI_VERSION
    {
        return Err(Status::Incompatible);
    }
    if options.max_operations == 0
        || options.max_operations > isize::MAX as u64
        || options.max_buffer_bytes == 0
        || options.max_buffer_bytes > isize::MAX as u64
    {
        return Err(Status::InvalidArgument);
    }
    let id = next_handle()?;
    insert(
        id,
        Resource::Engine(Arc::new(Engine {
            state: Mutex::default(),
            changed: Condvar::new(),
            max_operations: options.max_operations,
            max_bytes: options.max_buffer_bytes,
        })),
    )?;
    Ok(id)
}

/// Admit an owned payload for a native producer. No Java memory is retained.
/// This is a Rust owner API; it is not an HTTP or echo endpoint in the C ABI.
pub fn submit_owned(
    id: Handle,
    bytes: &[u8],
) -> Result<Handle> {
    submit_storage(id, bytes.len(), || bytes.into())
}

pub(crate) fn submit_read(
    id: Handle,
    capacity: usize,
) -> Result<(Handle, Payload)> {
    let operation =
        submit_storage(id, capacity, || vec![0; capacity].into_boxed_slice())?;
    Ok((operation, take_payload(operation)?))
}

fn submit_storage(
    id: Handle,
    length: usize,
    allocate: impl FnOnce() -> Box<[u8]>,
) -> Result<Handle> {
    let owner = engine(id)?;
    let mut state = lock(&owner.state)?;
    if state.shutdown {
        return Err(Status::Closed);
    }
    if state.operations == owner.max_operations
        || length as u64 > owner.max_bytes - state.bytes
    {
        return Err(Status::NoCapacity);
    }
    // Reserve both identities before admitting work; settlement cannot exhaust IDs.
    let id = next_handle()?;
    let result_id = next_handle()?;
    let payload = allocate();
    let operation = Arc::new(Operation {
        id,
        result_id,
        engine: Arc::clone(&owner),
        cancellation: tokio_util::sync::CancellationToken::new(),
        state: Mutex::new(OperationState {
            payload: Some(Payload {
                bytes: payload,
                length,
                reserved: length as u64,
                engine: Arc::clone(&owner),
            }),
            terminal: false,
            delivered: false,
        }),
    });
    state.operations += 1;
    state.bytes += length as u64;
    state.pending.insert(id, Arc::downgrade(&operation));
    insert(id, Resource::Operation(operation))?;
    Ok(id)
}

impl Operation {
    fn settle(
        &self,
        status: Status,
        keep_payload: bool,
        resource: Handle,
    ) -> Result<()> {
        self.settle_with(status, keep_payload, resource, None)
    }

    fn settle_with(
        &self,
        status: Status,
        keep_payload: bool,
        resource: Handle,
        produced: Option<Payload>,
    ) -> Result<()> {
        let mut operation = lock(&self.state)?;
        if operation.terminal {
            return Err(Status::AlreadyCompleted);
        }
        if status == Status::Cancelled {
            self.cancellation.cancel();
        }
        let payload = produced.or_else(|| operation.payload.take());
        let buffer = if status == Status::Ok && keep_payload {
            let payload = payload.ok_or(Status::Panic)?;
            lock(&self.engine.state)?.buffers += 1;
            insert(
                self.result_id,
                Resource::Buffer(Arc::new(Buffer { payload })),
            )?;
            self.result_id
        } else {
            drop(payload);
            0
        };
        operation.terminal = true;
        let mut state = lock(&self.engine.state)?;
        state.pending.remove(&self.id);
        state.queue.push_back(Completion {
            operation: self.id,
            status: status as i32,
            reserved: 0,
            buffer,
            resource,
        });
        self.engine.changed.notify_all();
        Ok(())
    }
}

/// Complete a native producer, transferring its payload to a result lease.
pub fn complete_operation(id: Handle) -> Result<()> {
    operation(id)?.settle(Status::Ok, true, 0)
}

pub(crate) fn cancel(id: Handle) -> Result<()> {
    operation(id)?.settle(Status::Cancelled, false, 0)
}

pub(crate) fn release_operation(id: Handle) -> Result<()> {
    let op = operation(id)?;
    if !lock(&op.state)?.delivered {
        return Err(Status::Busy);
    }
    remove(id)
}

pub(crate) fn wait(id: Handle) -> Result<Completion> {
    let owner = engine(id)?;
    let mut state = lock(&owner.state)?;
    if state.waiter {
        return Err(Status::Busy);
    }
    state.waiter = true;
    owner.changed.notify_all();
    let result = loop {
        if let Some(record) = state.queue.pop_front() {
            break Ok(record);
        }
        if state.shutdown && state.pending.is_empty() {
            break Err(Status::End);
        }
        state = owner.changed.wait(state).map_err(|_| Status::Panic)?;
    };
    state.waiter = false;
    drop(state);
    if let Ok(record) = &result {
        lock(&operation(record.operation)?.state)?.delivered = true;
    }
    result
}

pub(crate) fn shutdown(id: Handle) -> Result<()> {
    let owner = engine(id)?;
    let pending = {
        let mut state = lock(&owner.state)?;
        if state.resources != 0 {
            return Err(Status::Busy);
        }
        state.shutdown = true;
        state.pending.values().filter_map(Weak::upgrade).collect::<Vec<_>>()
    };
    for op in pending {
        match op.settle(Status::Cancelled, false, 0) {
            Ok(()) | Err(Status::AlreadyCompleted) => {}
            Err(error) => return Err(error),
        }
    }
    owner.changed.notify_all();
    Ok(())
}

pub(crate) fn release_engine(id: Handle) -> Result<()> {
    let owner = engine(id)?;
    let state = lock(&owner.state)?;
    if !state.shutdown
        || state.waiter
        || state.operations != 0
        || state.buffers != 0
        || state.bytes != 0
        || state.resources != 0
    {
        return Err(Status::Busy);
    }
    drop(state);
    remove(id)
}

pub(crate) fn view(id: Handle) -> Result<BufferView> {
    match get(id)? {
        Resource::Buffer(x) => Ok(BufferView {
            data: x.payload.bytes.as_ptr(),
            length: x.payload.length as u64,
        }),
        _ => Err(Status::WrongKind),
    }
}

pub(crate) fn release_buffer(id: Handle) -> Result<()> {
    match get(id)? {
        Resource::Buffer(_) => remove(id),
        _ => Err(Status::WrongKind),
    }
}

#[cfg(test)]
#[path = "../tests/state.rs"]
mod tests;

/// Pin native bridge ownership until all application resources have drained.
pub(crate) struct Lease(Arc<Engine>);
impl Drop for Lease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.resources -= 1;
        }
    }
}
pub(crate) fn lease(id: Handle) -> Result<Lease> {
    let owner = engine(id)?;
    let mut state = lock(&owner.state)?;
    if state.shutdown {
        return Err(Status::Closed);
    }
    state.resources += 1;
    drop(state);
    Ok(Lease(owner))
}
pub(crate) fn cancellation(
    id: Handle
) -> Result<tokio_util::sync::CancellationToken> {
    Ok(operation(id)?.cancellation.clone())
}
pub(crate) fn take_payload(id: Handle) -> Result<Payload> {
    lock(&operation(id)?.state)?.payload.take().ok_or(Status::AlreadyCompleted)
}
pub(crate) fn finish(
    id: Handle,
    status: Status,
    resource: Handle,
) -> Result<()> {
    operation(id)?.settle(status, false, resource)
}

pub(crate) fn finish_read(
    id: Handle,
    payload: Payload,
) -> Result<()> {
    operation(id)?.settle_with(Status::Ok, true, 0, Some(payload))
}
pub(crate) fn register_application(
    value: Arc<crate::application::Application>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::Application(value))?;
    Ok(id)
}
pub(crate) fn register_exchange(
    value: Arc<crate::application::Exchange>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::Exchange(value))?;
    Ok(id)
}
pub(crate) fn register_event(
    value: Arc<crate::application::EventRecord>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::Event(value))?;
    Ok(id)
}
pub(crate) fn application(
    id: Handle
) -> Result<Arc<crate::application::Application>> {
    match get(id)? {
        Resource::Application(x) => Ok(x),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn exchange(
    id: Handle
) -> Result<Arc<crate::application::Exchange>> {
    match get(id)? {
        Resource::Exchange(x) => Ok(x),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn event(
    id: Handle
) -> Result<Arc<crate::application::EventRecord>> {
    match get(id)? {
        Resource::Event(x) => Ok(x),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn remove_application(id: Handle) -> Result<()> {
    application(id)?;
    remove(id)
}
pub(crate) fn remove_exchange(id: Handle) -> Result<()> {
    exchange(id)?.abort();
    remove(id)
}
pub(crate) fn remove_event(id: Handle) -> Result<()> {
    event(id)?;
    remove(id)
}

pub(crate) fn register_control(
    value: Arc<crate::application::lifecycle::ControlRecord>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::Control(value))?;
    Ok(id)
}
pub(crate) fn control(
    id: Handle
) -> Result<Arc<crate::application::lifecycle::ControlRecord>> {
    match get(id)? {
        Resource::Control(value) => Ok(value),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn remove_control(id: Handle) -> Result<()> {
    control(id)?;
    remove(id)
}

/// Reserve bridge payload capacity without allocating a second result buffer.
pub(crate) struct Reservation {
    engine: Arc<Engine>,
    bytes: u64,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.engine.state.lock() {
            state.bytes -= self.bytes;
        }
    }
}
impl Reservation {
    pub fn payload(
        mut self,
        bytes: Box<[u8]>,
    ) -> Result<Payload> {
        if bytes.len() as u64 > self.bytes {
            return Err(Status::NoCapacity);
        }
        let length = bytes.len();
        let payload = Payload {
            bytes,
            length,
            reserved: self.bytes,
            engine: Arc::clone(&self.engine),
        };
        self.bytes = 0;
        Ok(payload)
    }
}
pub(crate) fn reserve_bytes(
    id: Handle,
    bytes: usize,
) -> Result<Reservation> {
    let owner = engine(id)?;
    let mut state = lock(&owner.state)?;
    if state.shutdown {
        return Err(Status::Closed);
    }
    if bytes as u64 > owner.max_bytes - state.bytes {
        return Err(Status::NoCapacity);
    }
    state.bytes += bytes as u64;
    drop(state);
    Ok(Reservation { engine: owner, bytes: bytes as u64 })
}

pub(crate) fn register_invocations(
    value: Arc<crate::application::invoke::Invocations>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::Invocations(value))?;
    Ok(id)
}
pub(crate) fn invocations(
    id: Handle
) -> Result<Arc<crate::application::invoke::Invocations>> {
    match get(id)? {
        Resource::Invocations(value) => Ok(value),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn remove_invocations(id: Handle) -> Result<()> {
    invocations(id)?;
    remove(id)
}

pub(crate) fn register_invocation(
    value: Arc<crate::application::invoke::Invocation>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::Invocation(value))?;
    Ok(id)
}
pub(crate) fn invocation(
    id: Handle
) -> Result<Arc<crate::application::invoke::Invocation>> {
    match get(id)? {
        Resource::Invocation(value) => Ok(value),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn remove_invocation(id: Handle) -> Result<()> {
    invocation(id)?;
    remove(id)
}

pub(crate) fn register_invoke_result(
    value: Arc<crate::application::invoke::ResultRecord>
) -> Result<Handle> {
    let id = next_handle()?;
    insert(id, Resource::InvokeResult(value))?;
    Ok(id)
}
pub(crate) fn invoke_result(
    id: Handle
) -> Result<Arc<crate::application::invoke::ResultRecord>> {
    match get(id)? {
        Resource::InvokeResult(value) => Ok(value),
        _ => Err(Status::WrongKind),
    }
}
pub(crate) fn remove_invoke_result(id: Handle) -> Result<()> {
    invoke_result(id)?;
    remove(id)
}
