//! Invoke ABI: no HTTP metadata, endpoint pointers or Java heap addresses cross it.
use super::{Invocations, Settings};
use crate::application::ffi::{bytes, input, text};
use crate::ffi::{boundary, output};
use crate::{ABI_VERSION, ByteView, Handle, HeaderView, Status, state};
use std::collections::HashMap;
#[repr(C)]
pub struct Options {
    size: u32,
    version: u32,
    pending: u64,
    accepted: u64,
    operation: u64,
    request: u64,
    response: u64,
    fleet: ByteView,
    service: ByteView,
}
#[repr(C)]
pub struct Info {
    operation: ByteView,
    payload: ByteView,
    generation: u64,
}
#[repr(C)]
pub struct ResultInfo {
    code: u32,
    reserved: u32,
    payload: ByteView,
}
#[repr(C)]
pub struct Abi {
    size: u32,
    options: u32,
    options_align: u32,
    request: u32,
    request_align: u32,
    result: u32,
    result_align: u32,
    route: u32,
}
/// # Safety
/// out is aligned/writable for an Abi record.
pub unsafe fn abi(
    out: *mut Abi,
    size: u32,
) -> i32 {
    boundary(|| {
        output(out)?;
        if size != size_of::<Abi>() as u32 {
            return Err(Status::Incompatible);
        }
        unsafe {
            out.write(Abi {
                size,
                options: size_of::<Options>() as u32,
                options_align: align_of::<Options>() as u32,
                request: size_of::<Info>() as u32,
                request_align: align_of::<Info>() as u32,
                result: size_of::<ResultInfo>() as u32,
                result_align: align_of::<ResultInfo>() as u32,
                route: size_of::<HeaderView>() as u32,
            });
        }
        Ok(())
    })
}
/// # Safety
/// Options/routes are readable during this call; out is aligned and writable.
pub unsafe fn open(
    app: Handle,
    options: *const Options,
    routes: *const HeaderView,
    count: u64,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        output(options.cast_mut())?;
        let options = unsafe { options.read() };
        if options.size != size_of::<Options>() as u32
            || options.version != ABI_VERSION
        {
            return Err(Status::Incompatible);
        }
        for size in [
            options.pending,
            options.accepted,
            options.operation,
            options.request,
            options.response,
        ] {
            if size > isize::MAX as u64 {
                return Err(Status::InvalidArgument);
            }
        }
        if count > isize::MAX as u64 / size_of::<HeaderView>() as u64 {
            return Err(Status::InvalidArgument);
        }
        let entries = if count == 0 {
            &[]
        } else {
            output(routes.cast_mut())?;
            unsafe { std::slice::from_raw_parts(routes, count as usize) }
        };
        let mut targets = HashMap::with_capacity(entries.len());
        for route in entries {
            let target: Box<str> = unsafe { text(route.name)? }.into();
            let service: Box<str> = unsafe { text(route.value)? }.into();
            if target.is_empty() || targets.insert(target, service).is_some() {
                return Err(Status::InvalidArgument);
            }
        }
        let settings = Settings {
            fleet: unsafe { text(options.fleet)? }.into(),
            service: unsafe { text(options.service)? }.into(),
            limits: claviron::orbit::link::invoke::Limits {
                pending_calls: options.pending as usize,
                accepted_calls: options.accepted as usize,
                operation_bytes: options.operation as usize,
                request_bytes: options.request as usize,
                response_bytes: options.response as usize,
            },
            routes: targets,
        };
        let id = Invocations::open(state::application(app)?, settings)?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}
/// # Safety
/// out is an aligned writable handle.
pub unsafe fn next(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let id = state::invocations(id)?.next()?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}
/// # Safety
/// Input views are readable only during the call; out is a writable handle.
pub unsafe fn call(
    id: Handle,
    target: ByteView,
    operation: ByteView,
    payload: ByteView,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let id = state::invocations(id)?.call(
            unsafe { text(target)? }.into(),
            unsafe { text(operation)? }.into(),
            unsafe { input(payload)? },
        )?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}
/// # Safety
/// Binding handle belongs to this library.
pub unsafe fn withdraw(id: Handle) -> i32 {
    boundary(|| {
        state::invocations(id)?.withdraw();
        Ok(())
    })
}
/// # Safety
/// out is a writable operation handle.
pub unsafe fn closed(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let id = state::invocations(id)?.wait_closed()?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}
/// # Safety
/// Binding has drained; no caller uses its handle after release.
pub unsafe fn release(id: Handle) -> i32 {
    boundary(|| {
        state::invocations(id)?.releasable()?;
        state::remove_invocations(id)
    })
}
/// # Safety
/// out is writable; views are borrowed until invocation release.
pub unsafe fn info(
    id: Handle,
    out: *mut Info,
) -> i32 {
    boundary(|| {
        output(out)?;
        let record = state::invocation(id)?;
        unsafe {
            out.write(Info {
                operation: bytes(record.operation.as_bytes()),
                payload: bytes(&record.payload.bytes),
                generation: record.generation.id,
            });
        }
        Ok(())
    })
}
/// # Safety
/// out is an aligned writable integer.
pub unsafe fn current(
    id: Handle,
    out: *mut u32,
) -> i32 {
    boundary(|| {
        output(out)?;
        let current = state::invocation(id)?.current();
        unsafe {
            out.write(u32::from(current));
        }
        Ok(())
    })
}
/// # Safety
/// out is an aligned writable operation handle.
pub unsafe fn watch(
    id: Handle,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        let id = state::invocation(id)?.watch()?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}
/// # Safety
/// Payload is readable during this call; copied before return.
pub unsafe fn reply(
    id: Handle,
    code: u32,
    payload: ByteView,
) -> i32 {
    boundary(|| {
        state::invocation(id)?.respond(code, unsafe { input(payload)? })
    })
}
/// # Safety
/// Invocation handle belongs to this library and views are no longer in use.
pub unsafe fn invocation_release(id: Handle) -> i32 {
    boundary(|| {
        state::invocation(id)?.abandon();
        state::remove_invocation(id)
    })
}
/// # Safety
/// out is writable; views are borrowed until result release.
pub unsafe fn result_info(
    id: Handle,
    out: *mut ResultInfo,
) -> i32 {
    boundary(|| {
        output(out)?;
        let record = state::invoke_result(id)?;
        unsafe {
            out.write(ResultInfo {
                code: record.code as u32,
                reserved: 0,
                payload: bytes(&record.payload.bytes),
            });
        }
        Ok(())
    })
}
/// # Safety
/// Result views are no longer in use.
pub unsafe fn result_release(id: Handle) -> i32 {
    boundary(|| state::remove_invoke_result(id))
}
#[macro_export]
macro_rules! invoke_export {
    ($name:ident,$target:ident,($($argument:ident:$ty:ty),*))=>{
        /// # Safety
        /// Arguments obey the corresponding claviron_java.h contract.
        #[unsafe(no_mangle)] pub unsafe extern "C" fn $name($($argument:$ty),*)->i32 {
            unsafe{$crate::application::invoke::ffi::$target($($argument),*)}
        }
    };
}
#[macro_export]
macro_rules! export_invoke_bindings {()=>{
    $crate::invoke_export!(oj_invoke_abi,abi,(out:*mut $crate::application::invoke::ffi::Abi,size:u32));
    $crate::invoke_export!(oj_invoke_open,open,(app:u64,options:*const $crate::application::invoke::ffi::Options,routes:*const $crate::HeaderView,count:u64,out:*mut u64));
    $crate::invoke_export!(oj_invoke_next,next,(id:u64,out:*mut u64));
    $crate::invoke_export!(oj_invoke_call,call,(id:u64,target:$crate::ByteView,operation:$crate::ByteView,payload:$crate::ByteView,out:*mut u64));
    $crate::invoke_export!(oj_invoke_withdraw,withdraw,(id:u64));
    $crate::invoke_export!(oj_invoke_closed,closed,(id:u64,out:*mut u64));
    $crate::invoke_export!(oj_invoke_release,release,(id:u64));
    $crate::invoke_export!(oj_invocation_info,info,(id:u64,out:*mut $crate::application::invoke::ffi::Info));
    $crate::invoke_export!(oj_invocation_current,current,(id:u64,out:*mut u32));
    $crate::invoke_export!(oj_invocation_watch,watch,(id:u64,out:*mut u64));
    $crate::invoke_export!(oj_invocation_reply,reply,(id:u64,code:u32,payload:$crate::ByteView));
    $crate::invoke_export!(oj_invocation_release,invocation_release,(id:u64));
    $crate::invoke_export!(oj_invoke_result_info,result_info,(id:u64,out:*mut $crate::application::invoke::ffi::ResultInfo));
    $crate::invoke_export!(oj_invoke_result_release,result_release,(id:u64));
};}
