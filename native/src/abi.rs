/// Revision of the C records and function contracts; not an Orbit wire version.
pub const ABI_VERSION: u32 = 3;
/// ABI capability bit for ownership and completions, without Link/HTTP support.
pub const FOUNDATION: u64 = 1;
/// Application HTTP transport and subscription capabilities, not raw service invoke.
pub const APPLICATION: u64 = 2;
/// Incremental request reads and backpressured response writes.
pub const STREAMING: u64 = 4;
/// Application lifecycle, generation observation and control notifications.
pub const LIFECYCLE: u64 = 8;
/// Generation-owned invokes on a separate service Link.
pub const INVOKE: u64 = 16;

pub type Handle = u64;
pub type Result<T> = std::result::Result<T, Status>;

/// Fixed C status values; positive statuses never carry a successful output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum Status {
    Ok = 0,
    InvalidArgument = 1,
    InvalidHandle = 2,
    WrongKind = 3,
    NoCapacity = 4,
    Busy = 5,
    Closed = 6,
    Cancelled = 7,
    Panic = 8,
    Incompatible = 9,
    End = 10,
    AlreadyCompleted = 11,
    Transport = 12,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct AbiInfo {
    pub struct_size: u32,
    pub abi_version: u32,
    pub capabilities: u64,
    pub options_size: u32,
    pub options_align: u32,
    pub completion_size: u32,
    pub completion_align: u32,
    pub buffer_view_size: u32,
    pub buffer_view_align: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct Options {
    pub struct_size: u32,
    pub abi_version: u32,
    pub max_operations: u64,
    pub max_buffer_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Completion {
    pub operation: Handle,
    pub status: i32,
    pub reserved: u32,
    pub buffer: Handle,
    pub resource: Handle,
}

#[repr(C)]
pub struct BufferView {
    pub data: *const u8,
    pub length: u64,
}

pub(crate) fn info() -> AbiInfo {
    AbiInfo {
        struct_size: size_of::<AbiInfo>() as u32,
        abi_version: ABI_VERSION,
        capabilities: FOUNDATION | APPLICATION | STREAMING | LIFECYCLE | INVOKE,
        options_size: size_of::<Options>() as u32,
        options_align: align_of::<Options>() as u32,
        completion_size: size_of::<Completion>() as u32,
        completion_align: align_of::<Completion>() as u32,
        buffer_view_size: size_of::<BufferView>() as u32,
        buffer_view_align: align_of::<BufferView>() as u32,
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ByteView {
    pub data: *const u8,
    pub length: u64,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct HeaderView {
    pub name: ByteView,
    pub value: ByteView,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ApplicationOptions {
    pub struct_size: u32,
    pub abi_version: u32,
    pub incarnation: u64,
    pub max_exchanges: u64,
    pub event_capacity: u64,
    pub control_capacity: u64,
    pub fleet: ByteView,
    pub name: ByteView,
}

#[repr(C)]
pub struct RequestInfo {
    pub struct_size: u32,
    pub http_version: u32,
    pub method: ByteView,
    pub uri: ByteView,
    pub header_count: u64,
    pub generation: u64,
}

#[repr(C)]
pub struct EventInfo {
    pub id: ByteView,
    pub topic: ByteView,
    pub json: ByteView,
}

#[repr(C)]
pub struct ControlInfo {
    pub kind: u32,
    pub action: u32,
    pub generation: u64,
    pub running: u64,
    pub name: ByteView,
    pub payload: ByteView,
    pub edge: ByteView,
}
