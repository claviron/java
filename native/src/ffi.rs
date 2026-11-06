//! C pointer contracts are declared in include/claviron_java.h.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::{
    AbiInfo, BufferView, Completion, Handle, Options, Result, Status, abi,
    state,
};

/// Convert recoverable unwinds into an ABI status; aborts are not recoverable.
pub fn boundary(body: impl FnOnce() -> Result<()>) -> i32 {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => Status::Ok as i32,
        Ok(Err(status)) => status as i32,
        Err(_) => Status::Panic as i32,
    }
}

pub(crate) fn output<T>(ptr: *mut T) -> Result<()> {
    if ptr.is_null() || !(ptr as usize).is_multiple_of(align_of::<T>()) {
        Err(Status::InvalidArgument)
    } else {
        Ok(())
    }
}

/// # Safety
/// `out` is writable, aligned and nonaliased for `size` bytes.
pub unsafe fn query(
    out: *mut AbiInfo,
    size: u32,
) -> i32 {
    boundary(|| {
        output(out)?;
        if size != size_of::<AbiInfo>() as u32 {
            return Err(Status::Incompatible);
        }
        unsafe {
            out.write(abi::info());
        }
        Ok(())
    })
}

/// # Safety
/// Options are readable and aligned; out is writable, aligned and disjoint.
pub unsafe fn open(
    options: *const Options,
    out: *mut Handle,
) -> i32 {
    boundary(|| {
        output(out)?;
        output(options.cast_mut())?;
        let id = state::open(unsafe { options.read() })?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}

/// # Safety
/// Out is writable and aligned for one completion throughout this blocking call.
pub unsafe fn wait(
    id: Handle,
    out: *mut Completion,
) -> i32 {
    boundary(|| {
        output(out)?;
        let record = state::wait(id)?;
        unsafe {
            out.write(record);
        }
        Ok(())
    })
}

/// # Safety
/// Out is writable and aligned. Caller retains the lease until Java views close.
pub unsafe fn view(
    id: Handle,
    out: *mut BufferView,
) -> i32 {
    boundary(|| {
        output(out)?;
        let value = state::view(id)?;
        unsafe {
            out.write(value);
        }
        Ok(())
    })
}

pub fn shutdown(id: Handle) -> i32 {
    boundary(|| state::shutdown(id))
}
pub fn release_engine(id: Handle) -> i32 {
    boundary(|| state::release_engine(id))
}
pub fn cancel(id: Handle) -> i32 {
    boundary(|| state::cancel(id))
}
pub fn release_operation(id: Handle) -> i32 {
    boundary(|| state::release_operation(id))
}
pub fn release_buffer(id: Handle) -> i32 {
    boundary(|| state::release_buffer(id))
}
