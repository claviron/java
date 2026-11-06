//! Controlled native producer for FFM tests, not shipped by the production addon.
use claviron_java_native::{Status, complete_operation, ffi, submit_owned};

claviron_java_native::export_bindings!();

/// # Safety
/// Input is readable for length bytes; out is aligned, writable and disjoint.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oj_fixture_submit(
    engine: u64,
    input: *const u8,
    length: u64,
    out: *mut u64,
) -> i32 {
    ffi::boundary(|| {
        if out.is_null()
            || !(out as usize).is_multiple_of(align_of::<u64>())
            || length > isize::MAX as u64
            || (length != 0 && input.is_null())
        {
            return Err(Status::InvalidArgument);
        }
        let bytes = if length == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(input, length as usize) }
        };
        let id = submit_owned(engine, bytes)?;
        unsafe {
            out.write(id);
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn oj_fixture_complete(id: u64) -> i32 {
    ffi::boundary(|| complete_operation(id))
}
