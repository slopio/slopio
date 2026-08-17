//! The simulated ring, exercised without a runtime around it.
//!
//! Two things are being pinned here. The reinterpret in `Sqe::decode` is fixed
//! at compile time by offset asserts, but those only prove `Sqe` matches
//! itself — what matters is that it matches what the `io-uring` builders write,
//! so these submit real entries and read the fields back. And the wheel decides
//! completion order from a seed, so the same seed has to produce the same order
//! every run or the design is worthless.

use std::time::{Duration, Instant};

use io_uring::types::Timespec;
use io_uring::{opcode, types::Fd};
use runtime_core::deterministic_testing::{FakeRing, FakeRingHandle, IOSQE_BUFFER_SELECT};

/// Fast enough to keep the suite quick, coarse enough that buckets stay
/// distinguishable against the ~50µs floor on the sleep.
const STEP: Duration = Duration::from_millis(1);

fn ring(seed: u64) -> (FakeRing, FakeRingHandle) {
    FakeRing::builder().bucket_step(STEP).seed(seed).build()
}

fn send(fd: i32, buf: &[u8], user_data: u64) -> io_uring::squeue::Entry {
    opcode::Send::new(Fd(fd), buf.as_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data)
}

// ---------------------------------------------------------------- decoding

#[test]
fn builder_yields_a_ring_and_a_handle_sharing_one_state() {
    let (mut ring, handle) = ring(0);

    assert!(handle.inflight().is_empty());
    ring.add(&send(3, b"hi", 1));

    // Staged, not yet submitted: the driver has pushed it, the kernel has not
    // been told.
    assert!(handle.inflight().is_empty());

    ring.submit();
    assert_eq!(handle.inflight().len(), 1);
}

#[test]
fn decodes_the_fields_a_send_was_built_with() {
    let (mut ring, handle) = ring(0);
    let buf = b"hello world";

    ring.add(&send(7, buf, 0xDEAD_BEEF));
    ring.submit();

    let sqe = handle.next_inflight().expect("one op in flight");
    assert_eq!(sqe.fd, 7);
    assert_eq!(sqe.addr, buf.as_ptr() as u64);
    assert_eq!(sqe.len as usize, buf.len());
    assert_eq!(sqe.user_data, 0xDEAD_BEEF);
    assert!(!sqe.uses_buffer_select());
}

#[test]
fn decodes_a_multishot_recv_against_a_buffer_group() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::RecvMulti::new(Fd(11), 4).build().user_data(2));
    ring.submit();

    let sqe = handle.next_inflight().expect("one op in flight");
    assert_eq!(sqe.fd, 11);
    assert_eq!(sqe.buf_group, 4);
    // A provided-buffer op names a group instead of carrying a pointer.
    assert!(sqe.uses_buffer_select());
    assert_eq!(sqe.flags & IOSQE_BUFFER_SELECT, IOSQE_BUFFER_SELECT);
}

#[test]
fn async_cancel_carries_its_target_in_addr() {
    let (mut ring, handle) = ring(0);

    // The driver cancels by naming the `user_data` of the op it wants stopped;
    // the fake has to be able to read that back to know what to retire.
    ring.add(&opcode::AsyncCancel::new(0x1234).build().user_data(9));
    ring.submit();

    let sqe = handle.next_inflight().expect("one op in flight");
    assert_eq!(sqe.addr, 0x1234);
    assert_eq!(sqe.user_data, 9);
}

// ------------------------------------------------------- manual completion

#[test]
fn completing_retires_the_op_and_queues_a_completion() {
    let (mut ring, handle) = ring(0);

    ring.add(&send(3, b"x", 42));
    ring.submit();

    handle.complete(42, 1, 0);
    assert!(handle.inflight().is_empty());
    assert_eq!(handle.queued_completions(), 1);

    let completion = ring.pop_completion().expect("a completion was queued");
    assert_eq!(completion.user_data, 42);
    assert_eq!(completion.result, 1);
    assert!(ring.pop_completion().is_none());
}

#[test]
fn a_multishot_completion_leaves_the_op_in_flight() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();

    // `F_MORE`: more results are coming for this token, so the op stays.
    handle.complete_keeping(7, 128, 2);
    handle.complete_keeping(7, 64, 2);
    assert_eq!(handle.inflight().len(), 1);
    assert_eq!(handle.queued_completions(), 2);

    // The terminal one retires it.
    handle.complete(7, 0, 0);
    assert!(handle.inflight().is_empty());
}

#[test]
#[should_panic(expected = "not in flight")]
fn completing_an_unknown_token_panics() {
    let (_ring, handle) = ring(0);
    // Nothing was submitted, so this would hand the driver a slab index it
    // dereferences unchecked. Caught here instead.
    handle.complete(0x99, 0, 0);
}

#[test]
#[should_panic(expected = "not in flight")]
fn completing_the_same_op_twice_panics() {
    let (mut ring, handle) = ring(0);

    ring.add(&send(3, b"x", 5));
    ring.submit();

    handle.complete(5, 1, 0);
    handle.complete(5, 1, 0);
}

// ------------------------------------------------------------ the schedule

#[test]
fn a_scheduled_op_completes_on_its_own_after_its_bucket_delay() {
    let (mut ring, _handle) = ring(0);
    let buf = b"hello world";

    ring.add(&send(3, buf, 1));
    let started = Instant::now();
    ring.submit_and_wait();
    let waited = started.elapsed();

    let completion = ring.pop_completion().expect("the op completed on its own");
    assert_eq!(completion.user_data, 1);
    // A write-side op reports the byte count it was handed.
    assert_eq!(completion.result as usize, buf.len());

    // Bucket i delays by STEP * (i + 1), so somewhere in 1..=6 steps. Only the
    // lower bound is worth asserting — a loaded machine can overshoot freely.
    assert!(waited >= STEP, "returned after {waited:?}, before one tick");
}

#[test]
fn ops_parked_on_an_external_event_never_complete_by_themselves() {
    let (mut ring, handle) = ring(0);

    // Nobody has written to this socket, so a real kernel would leave the recv
    // outstanding indefinitely. Giving it a bucket would be a lie.
    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();

    assert_eq!(handle.parked(), 1);
    assert_eq!(handle.inflight().len(), 1);
}

#[test]
fn waking_a_parked_op_schedules_it_rather_than_completing_it() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();

    handle.wake(7, 128);
    assert_eq!(handle.parked(), 0);
    // Still in flight: the event happened, the completion has not.
    assert_eq!(handle.inflight().len(), 1);
    assert_eq!(handle.queued_completions(), 0);

    ring.submit_and_wait();
    let completion = ring.pop_completion().expect("scheduled by the wake");
    assert_eq!(completion.user_data, 7);
    assert_eq!(completion.result, 128);
}

#[test]
#[should_panic(expected = "would never wake up")]
fn blocking_with_nothing_scheduled_panics_instead_of_hanging() {
    let (mut ring, _handle) = ring(0);

    // Parked, and no wakeup timer armed — the runtime would sleep forever.
    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(1));
    ring.submit_and_wait();
}

// ------------------------------------------------------------- determinism

/// Submit `n` sends and drain them, returning the order they completed in.
fn completion_order(seed: u64, n: u64) -> Vec<u64> {
    let (mut ring, _handle) = ring(seed);
    for i in 0..n {
        ring.add(&send(3, b"x", i));
    }
    ring.submit();

    let mut order = Vec::new();
    while (order.len() as u64) < n {
        ring.submit_and_wait();
        while let Some(completion) = ring.pop_completion() {
            order.push(completion.user_data);
        }
    }
    order
}

#[test]
fn the_same_seed_reproduces_the_same_completion_order() {
    assert_eq!(
        completion_order(0xC0FFEE, 12),
        completion_order(0xC0FFEE, 12)
    );
}

#[test]
fn the_wheel_actually_reorders_against_submission_order() {
    // Otherwise the seed would be buying nothing. Checked across several seeds
    // rather than one, so the test doesn't rest on a lucky draw.
    let submitted: Vec<u64> = (0..12).collect();
    let reorders = (0..8u64).any(|seed| completion_order(seed, 12) != submitted);
    assert!(
        reorders,
        "no seed produced an order other than submission order"
    );
}

#[test]
fn a_completion_batch_arrives_in_submission_order_within_its_tick() {
    // Ops sharing a bucket share a tick, and FIFO within the slot is the whole
    // ordering rule — no timestamps are compared.
    let (mut ring, _handle) = ring(7);
    for i in 0..24 {
        ring.add(&send(3, b"x", i));
    }
    ring.submit();

    ring.submit_and_wait();
    let mut batch = Vec::new();
    while let Some(completion) = ring.pop_completion() {
        batch.push(completion.user_data);
    }

    assert!(batch.len() > 1, "expected a batch, got {}", batch.len());
    let mut sorted = batch.clone();
    sorted.sort_unstable();
    assert_eq!(batch, sorted, "a tick delivered out of submission order");
}

// ------------------------------------------------------- the wakeup timeout

#[test]
fn the_wakeup_timer_is_not_an_io_op() {
    let (mut ring, handle) = ring(0);

    let ts = Timespec::new().nsec(2_000_000);
    ring.add(&opcode::Timeout::new(&ts).count(1).build().user_data(0xFFFF));
    ring.submit();

    // It bounds how long the ring may block; it is not something in flight, and
    // it must never take a bucket.
    assert!(handle.inflight().is_empty());
    assert_eq!(handle.parked(), 0);
}

#[test]
fn the_wakeup_timer_bounds_a_wait_that_would_otherwise_be_longer() {
    // Buckets span 50..=300ms here, so a 5ms timer has to win.
    let (mut ring, _handle) = FakeRing::builder()
        .bucket_step(Duration::from_millis(50))
        .seed(0)
        .build();

    ring.add(&send(3, b"x", 1));
    let ts = Timespec::new().nsec(5_000_000);
    ring.add(&opcode::Timeout::new(&ts).count(1).build().user_data(0xFFFF));

    let started = Instant::now();
    ring.submit_and_wait();
    let waited = started.elapsed();

    assert!(
        waited < Duration::from_millis(50),
        "waited {waited:?} — the timer did not bound the wait",
    );
    let completion = ring.pop_completion().expect("the timer fired");
    assert_eq!(completion.user_data, 0xFFFF);
    assert_eq!(completion.result, -libc::ETIME);
}

#[test]
fn a_wait_ended_by_io_also_completes_the_armed_timer() {
    // The runtime arms its timer with `.count(1)`, so it completes as soon as
    // any completion event lands, not only on elapse.
    let (mut ring, _handle) = ring(0);

    ring.add(&send(3, b"x", 1));
    let ts = Timespec::new().sec(30);
    ring.add(&opcode::Timeout::new(&ts).count(1).build().user_data(0xFFFF));

    ring.submit_and_wait();

    let mut seen = Vec::new();
    while let Some(completion) = ring.pop_completion() {
        seen.push(completion.user_data);
    }
    assert!(seen.contains(&1), "the send completed");
    assert!(
        seen.contains(&0xFFFF),
        "the armed timer completed alongside it"
    );
}
