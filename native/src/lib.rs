//! Owned native operations and completion delivery for a separate Java process.

mod abi;
pub mod application;
pub mod ffi;
mod state;

pub use abi::*;
pub use state::{complete_operation, submit_owned};

mod exports;
