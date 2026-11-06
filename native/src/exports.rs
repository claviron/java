/// Export the reviewed C ABI in the final shared library, without another owner.
#[macro_export]
macro_rules! export_bindings {
    () => {
        $crate::export_application_bindings!();
        $crate::export_invoke_bindings!();
        /// # Safety
        /// All pointers obey the corresponding claviron_java.h contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn oj_abi_query(
            out: *mut $crate::AbiInfo,
            size: u32,
        ) -> i32 {
            unsafe { $crate::ffi::query(out, size) }
        }
        /// # Safety
        /// All pointers obey the corresponding claviron_java.h contract.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn oj_engine_open(
            options: *const $crate::Options,
            out: *mut u64,
        ) -> i32 {
            unsafe { $crate::ffi::open(options, out) }
        }
        /// # Safety
        /// Out remains writable for the entire blocking wait.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn oj_completion_wait(
            id: u64,
            out: *mut $crate::Completion,
        ) -> i32 {
            unsafe { $crate::ffi::wait(id, out) }
        }
        /// # Safety
        /// Out is writable; the returned pointer is borrowed until buffer release.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn oj_buffer_view(
            id: u64,
            out: *mut $crate::BufferView,
        ) -> i32 {
            unsafe { $crate::ffi::view(id, out) }
        }
        #[unsafe(no_mangle)]
        pub extern "C" fn oj_engine_shutdown(id: u64) -> i32 {
            $crate::ffi::shutdown(id)
        }
        #[unsafe(no_mangle)]
        pub extern "C" fn oj_engine_release(id: u64) -> i32 {
            $crate::ffi::release_engine(id)
        }
        #[unsafe(no_mangle)]
        pub extern "C" fn oj_operation_cancel(id: u64) -> i32 {
            $crate::ffi::cancel(id)
        }
        #[unsafe(no_mangle)]
        pub extern "C" fn oj_operation_release(id: u64) -> i32 {
            $crate::ffi::release_operation(id)
        }
        #[unsafe(no_mangle)]
        pub extern "C" fn oj_buffer_release(id: u64) -> i32 {
            $crate::ffi::release_buffer(id)
        }
    };
}
