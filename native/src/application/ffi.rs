//! Application ABI. Input views live only for the duration of their downcall.

use super::{Application, Settings};
use crate::ffi::{boundary, output};
use crate::{
    ABI_VERSION, ApplicationOptions, ByteView, ControlInfo, EventInfo, Handle,
    HeaderView, RequestInfo, Result, Status, state,
};

pub(crate) fn bytes(value: &[u8]) -> ByteView {
    ByteView { data: value.as_ptr(), length: value.len() as u64 }
}

pub(super) unsafe fn input<'a>(view: ByteView) -> Result<&'a [u8]> {
    if view.length > isize::MAX as u64
        || (view.length != 0 && view.data.is_null())
    {
        return Err(Status::InvalidArgument);
    }
    Ok(if view.length == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(view.data, view.length as usize) }
    })
}

pub(super) unsafe fn text<'a>(view: ByteView) -> Result<&'a str> {
    std::str::from_utf8(unsafe { input(view)? })
        .map_err(|_| Status::InvalidArgument)
}

#[repr(C)]
pub struct ApplicationAbi {
    size: u32,
    options_size: u32,
    options_align: u32,
    request_size: u32,
    request_align: u32,
    event_size: u32,
    event_align: u32,
    header_size: u32,
    control_size: u32,
    control_align: u32,
    state_size: u32,
    state_align: u32,
}

/// # Safety
/// All pointer/view arguments satisfy claviron_java.h's readability and lifetime rules.
pub unsafe fn abi(
    out: *mut ApplicationAbi,
    size: u32,
) -> i32 {
    boundary(|| {
        output(out)?;
        if size != size_of::<ApplicationAbi>() as u32 {
            return Err(Status::Incompatible);
        }
        unsafe {
            out.write(ApplicationAbi {
                size,
                options_size: size_of::<ApplicationOptions>() as u32,
                options_align: align_of::<ApplicationOptions>() as u32,
                request_size: size_of::<RequestInfo>() as u32,
                request_align: align_of::<RequestInfo>() as u32,
                event_size: size_of::<EventInfo>() as u32,
                event_align: align_of::<EventInfo>() as u32,
                header_size: size_of::<HeaderView>() as u32,
                control_size: size_of::<ControlInfo>() as u32,
                control_align: align_of::<ControlInfo>() as u32,
                state_size: size_of::<super::lifecycle::Snapshot>() as u32,
                state_align: align_of::<super::lifecycle::Snapshot>() as u32,
            });
        }
        Ok(())
    })
}

/// # Safety
/// All pointer/view arguments satisfy claviron_java.h's readability and lifetime rules.
pub unsafe fn join(
    engine: Handle,
    options: *const ApplicationOptions,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(options.cast_mut())?;
        output(out)?;
        let options = unsafe { options.read() };
        if options.struct_size != size_of::<ApplicationOptions>() as u32
            || options.abi_version != ABI_VERSION
        {
            return Err(Status::Incompatible);
        }
        if options.max_exchanges > isize::MAX as u64
            || options.event_capacity > isize::MAX as u64
            || options.control_capacity > isize::MAX as u64
        {
            return Err(Status::InvalidArgument);
        }
        let settings = Settings {
            fleet: unsafe { text(options.fleet)? }.into(),
            name: unsafe { text(options.name)? }.into(),
            incarnation: options.incarnation,
            max_exchanges: options.max_exchanges as usize,
            event_capacity: options.event_capacity as usize,
            control_capacity: options.control_capacity as usize,
        };
        let id = Application::start(engine, settings)?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}

/// # Safety
/// All pointer/view arguments satisfy claviron_java.h's readability and lifetime rules.
pub unsafe fn next(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::application(id)?.next()?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// All pointer/view arguments satisfy claviron_java.h's readability and lifetime rules.
pub unsafe fn closed(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::application(id)?.wait_closed()?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// Handle belongs to this library; lifetime rules follow claviron_java.h.
pub unsafe fn withdraw(id: Handle) -> i32 {
    boundary(|| {
        state::application(id)?.withdraw();
        Ok(())
    })
}

/// # Safety
/// Handle belongs to this library; lifetime rules follow claviron_java.h.
pub unsafe fn release(id: Handle) -> i32 {
    boundary(|| {
        state::application(id)?.release()?;
        state::remove_application(id)
    })
}

/// # Safety
/// Out is aligned/writable; returned views are borrowed until exchange release.
pub unsafe fn request_info(
    id: Handle,
    out: *mut RequestInfo,
) -> i32 {
    boundary(|| {
        output(out)?;
        let exchange = state::exchange(id)?;
        unsafe {
            out.write(RequestInfo {
                struct_size: size_of::<RequestInfo>() as u32,
                http_version: exchange.frame.version as u32,
                method: bytes(exchange.frame.method.as_bytes()),
                uri: bytes(exchange.uri.as_bytes()),
                header_count: exchange.frame.headers.len() as u64,
                generation: exchange.generation.id,
            });
        }
        Ok(())
    })
}

/// # Safety
/// Out holds capacity HeaderView records, disjoint from the borrowed exchange.
pub unsafe fn request_headers(
    id: Handle,
    out: *mut HeaderView,
    capacity: u64,
) -> i32 {
    boundary(|| {
        let exchange = state::exchange(id)?;
        if capacity < exchange.frame.headers.len() as u64 {
            return Err(Status::InvalidArgument);
        }
        if !exchange.frame.headers.is_empty() {
            output(out)?;
        }
        for (index, (name, value)) in exchange.frame.headers.iter().enumerate()
        {
            unsafe {
                out.add(index).write(HeaderView {
                    name: bytes(name),
                    value: bytes(value),
                });
            }
        }
        Ok(())
    })
}

/// # Safety
/// Out holds an operation handle and remains disjoint from inputs.
pub unsafe fn request_watch(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::exchange(id)?.watch()?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// Headers and body views are readable through this call; copied before return.
pub unsafe fn respond(
    id: Handle,
    status: u32,
    headers: *const HeaderView,
    count: u64,
    body: ByteView,
    out: *mut Handle,
) -> i32 {
    unsafe { response(id, status, headers, count, body, out, false) }
}

unsafe fn response(
    id: Handle,
    status: u32,
    headers: *const HeaderView,
    count: u64,
    body: ByteView,
    out: *mut Handle,
    streaming: bool,
) -> i32 {
    boundary(|| {
        output(out)?;
        let limit =
            claviron::orbit::link::application::LINK_INBOX_PAYLOAD_CAPACITY;
        if count > limit as u64 || status > u16::MAX as u32 {
            return Err(Status::InvalidArgument);
        }
        let views = if count == 0 {
            &[]
        } else {
            output(headers.cast_mut())?;
            unsafe { std::slice::from_raw_parts(headers, count as usize) }
        };
        let mut total = 0usize;
        let mut owned = Vec::with_capacity(views.len());
        for view in views {
            let name = unsafe { input(view.name)? };
            let value = unsafe { input(view.value)? };
            total = total
                .checked_add(name.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or(Status::InvalidArgument)?;
            if total > limit {
                return Err(Status::InvalidArgument);
            }
            owned.push((name.to_vec(), value.to_vec()));
        }
        let exchange = state::exchange(id)?;
        let operation = if streaming {
            exchange.head(status as u16, owned)?
        } else {
            exchange.respond(status as u16, owned, unsafe { input(body)? })?
        };
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// Headers are readable through the call; copied before return.
pub unsafe fn head(
    id: Handle,
    status: u32,
    headers: *const HeaderView,
    count: u64,
    out: *mut Handle,
) -> i32 {
    unsafe {
        response(
            id,
            status,
            headers,
            count,
            ByteView { data: std::ptr::null(), length: 0 },
            out,
            true,
        )
    }
}

/// # Safety
/// Out is a writable operation handle; capacity is an explicit positive byte budget.
pub unsafe fn read(
    id: Handle,
    capacity: u64,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        if capacity == 0 || capacity > isize::MAX as u64 {
            return Err(Status::InvalidArgument);
        }
        let operation = state::exchange(id)?.read(capacity as usize)?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// Body is readable through the call, copied before return; out is writable.
pub unsafe fn write(
    id: Handle,
    body: ByteView,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::exchange(id)?.write(unsafe { input(body)? })?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// Out is a writable operation handle.
pub unsafe fn finish(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::exchange(id)?.finish()?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// Handle belongs to this library; lifetime rules follow claviron_java.h.
pub unsafe fn request_release(id: Handle) -> i32 {
    boundary(|| {
        state::exchange(id)?.abort();
        state::remove_exchange(id)
    })
}

/// # Safety
/// Topic is readable UTF-8 through this call.
pub unsafe fn subscribe(
    id: Handle,
    topic: ByteView,
    enabled: u32,
) -> i32 {
    boundary(|| {
        let application = state::application(id)?;
        if application.stop.is_cancelled() {
            return Err(Status::Closed);
        }
        application
            .data()?
            .events
            .subscribe(unsafe { text(topic)? }, enabled != 0)
    })
}

/// # Safety
/// Out holds a writable operation handle.
pub unsafe fn next_event(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::application(id)?.next_event()?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}

/// # Safety
/// All views are readable through this call, JSON is UTF-8.
pub unsafe fn publish(
    id: Handle,
    topic: ByteView,
    json: ByteView,
) -> i32 {
    boundary(|| {
        let application = state::application(id)?;
        if application.closed.is_cancelled() {
            return Err(Status::Closed);
        }
        application
            .data()?
            .events
            .publish(unsafe { text(topic)? }, unsafe { input(json)? })
    })
}

/// # Safety
/// Out is writable; views are borrowed until event_release.
pub unsafe fn event_info(
    id: Handle,
    out: *mut EventInfo,
) -> i32 {
    boundary(|| {
        output(out)?;
        let event = state::event(id)?;
        unsafe {
            out.write(event.info());
        }
        Ok(())
    })
}

/// # Safety
/// Handle belongs to this library; lifetime rules follow claviron_java.h.
pub unsafe fn event_release(id: Handle) -> i32 {
    boundary(|| state::remove_event(id))
}

/// # Safety
/// Out holds a writable application state record.
pub unsafe fn application_state(
    id: Handle,
    out: *mut super::lifecycle::Snapshot,
) -> i32 {
    boundary(|| {
        output(out)?;
        let value = state::application(id)?.lifecycle.snapshot()?;
        unsafe {
            out.write(value);
        }
        Ok(())
    })
}
/// # Safety
/// Out holds a writable integer; the exchange handle remains owned by the caller.
pub unsafe fn request_current(
    id: Handle,
    out: *mut u32,
) -> i32 {
    boundary(|| {
        output(out)?;
        let current = state::exchange(id)?.is_current();
        unsafe {
            out.write(u32::from(current));
        }
        Ok(())
    })
}
/// # Safety
/// Application handle belongs to this library; action is an oj_lifecycle_action.
pub unsafe fn action(
    id: Handle,
    action: u32,
) -> i32 {
    boundary(|| {
        use super::lifecycle::Action;
        let action = match action {
            1 => Action::Start,
            2 => Action::Stop,
            3 => Action::Restart,
            _ => return Err(Status::InvalidArgument),
        };
        state::application(id)?.action(action)
    })
}
/// # Safety
/// Out holds a writable operation handle; expected is a captured generation identity.
pub unsafe fn watch_generation(
    id: Handle,
    expected: u64,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        if expected == 0 {
            return Err(Status::InvalidArgument);
        }
        let operation = state::application(id)?.watch_generation(expected)?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}
/// # Safety
/// Out holds a writable operation handle.
pub unsafe fn next_control(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let operation = state::application(id)?.next_control()?;
        unsafe {
            out.write(operation);
        }
        Ok(())
    })
}
/// # Safety
/// Out is writable; returned views are borrowed through control_release.
pub unsafe fn control_info(
    id: Handle,
    out: *mut ControlInfo,
) -> i32 {
    boundary(|| {
        output(out)?;
        let record = state::control(id)?;
        let value = &record.value;
        unsafe {
            out.write(ControlInfo {
                kind: value.kind,
                action: value.action,
                generation: value.snapshot.generation,
                running: value.snapshot.running,
                name: bytes(value.name.as_bytes()),
                payload: bytes(&value.payload),
                edge: bytes(value.edge.as_bytes()),
            });
        }
        Ok(())
    })
}
/// # Safety
/// Control handle belongs to this library and is no longer in use by the caller.
pub unsafe fn control_release(id: Handle) -> i32 {
    boundary(|| state::remove_control(id))
}

/// Emit a C entry point delegating to the audited application boundary.
#[macro_export]
macro_rules! application_export {
    ($name:ident, $target:ident, ($($argument:ident: $ty:ty),*)) => {
        /// # Safety
        /// All views and handles satisfy the corresponding claviron_java.h contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name($($argument: $ty),*) -> i32 {
            unsafe { $crate::application::ffi::$target($($argument),*) }
        }
    };
}

#[macro_export]
macro_rules! export_application_bindings {
    () => {
        $crate::application_export!(oj_application_state, application_state, (id: u64, out: *mut $crate::application::lifecycle::Snapshot));
        $crate::application_export!(oj_application_action, action, (id: u64, action: u32));
        $crate::application_export!(oj_generation_watch, watch_generation, (id: u64, expected: u64, out: *mut u64));
        $crate::application_export!(oj_control_next, next_control, (id: u64, out: *mut u64));
        $crate::application_export!(oj_control_info, control_info, (id: u64, out: *mut $crate::ControlInfo));
        $crate::application_export!(oj_control_release, control_release, (id: u64));
        $crate::application_export!(oj_request_current, request_current, (id: u64, out: *mut u32));
        $crate::application_export!(oj_application_abi, abi, (out: *mut $crate::application::ffi::ApplicationAbi, size: u32));
        $crate::application_export!(oj_application_join, join, (engine: u64, options: *const $crate::ApplicationOptions, out: *mut u64));
        $crate::application_export!(oj_application_next, next, (id: u64, out: *mut u64));
        $crate::application_export!(oj_application_closed, closed, (id: u64, out: *mut u64));
        $crate::application_export!(oj_application_withdraw, withdraw, (id: u64));
        $crate::application_export!(oj_application_release, release, (id: u64));
        $crate::application_export!(oj_request_info, request_info, (id: u64, out: *mut $crate::RequestInfo));
        $crate::application_export!(oj_request_headers, request_headers, (id: u64, out: *mut $crate::HeaderView, capacity: u64));
        $crate::application_export!(oj_request_watch, request_watch, (id: u64, out: *mut u64));
        $crate::application_export!(oj_request_respond, respond, (id: u64, status: u32, headers: *const $crate::HeaderView, count: u64, body: $crate::ByteView, out: *mut u64));
        $crate::application_export!(oj_request_read, read, (id: u64, capacity: u64, out: *mut u64));
        $crate::application_export!(oj_response_head, head, (id: u64, status: u32, headers: *const $crate::HeaderView, count: u64, out: *mut u64));
        $crate::application_export!(oj_response_write, write, (id: u64, body: $crate::ByteView, out: *mut u64));
        $crate::application_export!(oj_response_finish, finish, (id: u64, out: *mut u64));
        $crate::application_export!(oj_request_release, request_release, (id: u64));
        $crate::application_export!(oj_event_subscribe, subscribe, (id: u64, topic: $crate::ByteView, enabled: u32));
        $crate::application_export!(oj_event_next, next_event, (id: u64, out: *mut u64));
        $crate::application_export!(oj_event_publish, publish, (id: u64, topic: $crate::ByteView, json: $crate::ByteView));
        $crate::application_export!(oj_event_info, event_info, (id: u64, out: *mut $crate::EventInfo));
        $crate::application_export!(oj_event_release, event_release, (id: u64));
    };
}
