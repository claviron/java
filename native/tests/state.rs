use super::*;
use std::sync::{Barrier, mpsc};
use std::thread;

fn options(
    operations: u64,
    bytes: u64,
) -> Options {
    Options {
        struct_size: size_of::<Options>() as u32,
        abi_version: ABI_VERSION,
        max_operations: operations,
        max_buffer_bytes: bytes,
    }
}

fn close(id: Handle) {
    shutdown(id).unwrap();
    release_engine(id).unwrap();
}

#[test]
fn abi_requires_compatible_records_and_explicit_limits() {
    let mut value = options(1, 1);
    value.abi_version += 1;
    assert_eq!(open(value), Err(Status::Incompatible));
    assert_eq!(open(options(0, 1)), Err(Status::InvalidArgument));
    assert_eq!(open(options(1, 0)), Err(Status::InvalidArgument));
    assert_eq!(size_of::<crate::AbiInfo>(), 40);
    assert_eq!(size_of::<Options>(), 24);
    assert_eq!(size_of::<Completion>(), 32);
    assert_eq!(size_of::<BufferView>(), 16);
    assert_eq!(std::mem::offset_of!(Completion, buffer), 16);
}

#[test]
fn owned_bytes_survive_scratch_and_operation_release() {
    let id = open(options(1, 3)).unwrap();
    let mut input = [0, 255, 9];
    let op = submit_owned(id, &input).unwrap();
    input.fill(1);
    assert_eq!(release_operation(op), Err(Status::Busy));
    complete_operation(op).unwrap();
    assert_eq!(cancel(op), Err(Status::AlreadyCompleted));
    let record = wait(id).unwrap();
    assert_eq!(record.operation, op);
    let view = view(record.buffer).unwrap();
    assert_eq!(
        unsafe { std::slice::from_raw_parts(view.data, view.length as usize) },
        &[0, 255, 9]
    );
    release_operation(op).unwrap();
    shutdown(id).unwrap();
    assert_eq!(release_engine(id), Err(Status::Busy));
    release_buffer(record.buffer).unwrap();
    release_engine(id).unwrap();
    assert_eq!(release_operation(op), Err(Status::InvalidHandle));
    assert_eq!(release_buffer(record.buffer), Err(Status::InvalidHandle));
}

#[test]
fn credits_and_bytes_follow_different_owners() {
    let id = open(options(1, 2)).unwrap();
    let op = submit_owned(id, &[1, 2]).unwrap();
    assert_eq!(submit_owned(id, &[]), Err(Status::NoCapacity));
    complete_operation(op).unwrap();
    let record = wait(id).unwrap();
    release_operation(op).unwrap();
    assert_eq!(submit_owned(id, &[3]), Err(Status::NoCapacity));
    release_buffer(record.buffer).unwrap();
    let next = submit_owned(id, &[3]).unwrap();
    cancel(next).unwrap();
    assert_eq!(wait(id).unwrap().status, Status::Cancelled as i32);
    release_operation(next).unwrap();
    close(id);
}

#[test]
fn shutdown_delivers_all_reserved_terminals_when_full() {
    let id = open(options(2, 2)).unwrap();
    let first = submit_owned(id, &[1]).unwrap();
    let second = submit_owned(id, &[2]).unwrap();
    complete_operation(first).unwrap();
    shutdown(id).unwrap();
    assert_eq!(submit_owned(id, &[]), Err(Status::Closed));
    let a = wait(id).unwrap();
    let b = wait(id).unwrap();
    assert_eq!((a.operation, b.operation), (first, second));
    assert_eq!(b.status, Status::Cancelled as i32);
    assert!(matches!(wait(id), Err(Status::End)));
    release_operation(first).unwrap();
    release_operation(second).unwrap();
    release_buffer(a.buffer).unwrap();
    release_engine(id).unwrap();
}

#[test]
fn empty_buffer_keeps_engine_busy_and_handles_have_kinds() {
    let id = open(options(1, 1)).unwrap();
    assert_eq!(cancel(id), Err(Status::WrongKind));
    let op = submit_owned(id, &[]).unwrap();
    assert_eq!(shutdown(op), Err(Status::WrongKind));
    complete_operation(op).unwrap();
    let record = wait(id).unwrap();
    assert_eq!(view(record.buffer).unwrap().length, 0);
    release_operation(op).unwrap();
    shutdown(id).unwrap();
    assert_eq!(release_engine(id), Err(Status::Busy));
    release_buffer(record.buffer).unwrap();
    release_engine(id).unwrap();
}

#[test]
fn shutdown_wakes_an_actually_parked_waiter() {
    let id = open(options(1, 1)).unwrap();
    let owner = engine(id).unwrap();
    let worker = thread::spawn(move || wait(id));
    let mut state = owner.state.lock().unwrap();
    while !state.waiter {
        state = owner.changed.wait(state).unwrap();
    }
    drop(state);
    assert!(matches!(wait(id), Err(Status::Busy)));
    shutdown(id).unwrap();
    assert!(matches!(worker.join().unwrap(), Err(Status::End)));
    release_engine(id).unwrap();
}

#[test]
fn cancel_and_producer_race_has_exactly_one_terminal() {
    let id = open(options(1, 1)).unwrap();
    let op = submit_owned(id, &[7]).unwrap();
    let gate = Arc::new(Barrier::new(3));
    let (send, receive) = mpsc::channel();
    let a = Arc::clone(&gate);
    let s = send.clone();
    let first = thread::spawn(move || {
        a.wait();
        s.send(cancel(op)).unwrap();
    });
    let b = Arc::clone(&gate);
    let second = thread::spawn(move || {
        b.wait();
        send.send(complete_operation(op)).unwrap();
    });
    gate.wait();
    first.join().unwrap();
    second.join().unwrap();
    let outcomes = [receive.recv().unwrap(), receive.recv().unwrap()];
    assert_eq!(outcomes.iter().filter(|x| x.is_ok()).count(), 1);
    assert!(outcomes.contains(&Err(Status::AlreadyCompleted)));
    let record = wait(id).unwrap();
    release_operation(op).unwrap();
    if record.buffer != 0 {
        release_buffer(record.buffer).unwrap();
    }
    close(id);
}

#[test]
fn ffi_outputs_stay_unchanged_on_error_and_unwinds_are_statuses() {
    let mut result = 99;
    let mut bad = options(1, 1);
    bad.struct_size += 1;
    assert_eq!(
        unsafe { crate::ffi::open(&bad, &mut result) },
        Status::Incompatible as i32
    );
    assert_eq!(result, 99);
    assert_eq!(
        unsafe { crate::ffi::open(std::ptr::null(), &mut result) },
        Status::InvalidArgument as i32
    );
    assert_eq!(
        crate::ffi::boundary(|| panic!("controlled boundary probe")),
        Status::Panic as i32
    );
}

#[test]
fn short_read_lease_exposes_only_initialized_bytes_and_keeps_its_reservation() {
    let engine = open(options(2, 8)).unwrap();
    let (operation, mut payload) = submit_read(engine, 8).unwrap();
    payload.bytes[..3].copy_from_slice(&[0, 255, 7]);
    payload.length = 3;
    finish_read(operation, payload).unwrap();
    let completion = wait(engine).unwrap();
    release_operation(operation).unwrap();
    let borrowed = view(completion.buffer).unwrap();
    assert_eq!(borrowed.length, 3);
    assert_eq!(
        unsafe {
            std::slice::from_raw_parts(borrowed.data, borrowed.length as usize)
        },
        &[0, 255, 7]
    );
    assert_eq!(submit_owned(engine, &[1]), Err(Status::NoCapacity));
    release_buffer(completion.buffer).unwrap();
    close(engine);
}

#[test]
fn cancelled_read_retains_native_storage_until_its_producer_finishes() {
    let engine = open(options(2, 8)).unwrap();
    let (operation, payload) = submit_read(engine, 8).unwrap();
    cancel(operation).unwrap();
    let completion = wait(engine).unwrap();
    assert_eq!(completion.buffer, 0);
    assert_eq!(submit_owned(engine, &[1]), Err(Status::NoCapacity));
    assert_eq!(finish_read(operation, payload), Err(Status::AlreadyCompleted));
    release_operation(operation).unwrap();
    close(engine);
}

#[test]
fn released_cancelled_read_operation_cannot_leak_a_late_buffer() {
    let engine = open(options(1, 8)).unwrap();
    let (operation, payload) = submit_read(engine, 8).unwrap();
    cancel(operation).unwrap();
    wait(engine).unwrap();
    release_operation(operation).unwrap();
    assert_eq!(submit_owned(engine, &[1]), Err(Status::NoCapacity));
    assert_eq!(finish_read(operation, payload), Err(Status::InvalidHandle));
    close(engine);
}

#[test]
fn invocation_reservation_follows_owned_bytes_after_buffer_transfer() {
    let engine = open(options(1, 8)).unwrap();
    let reservation = reserve_bytes(engine, 8).unwrap();
    assert_eq!(submit_owned(engine, &[1]), Err(Status::NoCapacity));
    let payload = reservation.payload(vec![0, 255].into_boxed_slice()).unwrap();
    assert_eq!(payload.length, 2);
    drop(payload);
    let operation = submit_owned(engine, &[1]).unwrap();
    let mut owned = take_payload(operation).unwrap();
    let moved = std::mem::take(&mut owned.bytes).into_vec();
    assert_eq!(moved, vec![1]);
    // Moving storage to the protocol does not prematurely release bridge credit.
    assert!(reserve_bytes(engine, 8).is_err());
    drop(owned);
    cancel(operation).unwrap();
    wait(engine).unwrap();
    release_operation(operation).unwrap();
    close(engine);
}

#[test]
fn oversized_invocation_result_releases_its_capacity_reservation() {
    let engine = open(options(1, 2)).unwrap();
    let reservation = reserve_bytes(engine, 2).unwrap();
    assert!(matches!(
        reservation.payload(vec![1, 2, 3].into_boxed_slice()),
        Err(Status::NoCapacity)
    ));
    close(engine);
}
