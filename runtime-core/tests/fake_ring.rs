//! The simulated ring, exercised without a runtime around it.
//!
//! Two things are being pinned here. The reinterpret in `Sqe::decode` is fixed
//! at compile time by offset asserts, but those only prove `Sqe` matches
//! itself — what matters is that it matches what the `io-uring` builders write,
//! so these submit real entries and read the fields back. And completion order
//! comes from a seed, so the same seed has to produce the same order every run
//! or the design is worthless.

use std::time::{Duration, Instant};

use io_uring::types::Timespec;
use io_uring::{cqueue, opcode, types::Fd};
use runtime_core::deterministic_testing::{
    AcceptEvent, FakeRing, FakeRingHandle, FdKind, IOSQE_BUFFER_SELECT, ReadEvent, mint,
    simulated_socket,
};
use runtime_core::io_driver::operations::sockets::SocketWrapper;

/// Fast enough to keep the suite quick, coarse enough that latency classes
/// stay distinguishable against the ~50µs floor on the sleep.
const STEP: Duration = Duration::from_millis(1);

fn ring(seed: u64) -> (FakeRing, FakeRingHandle) {
    FakeRing::builder().latency_step(STEP).seed(seed).build()
}

fn send(fd: i32, buf: &[u8], user_data: u64) -> io_uring::squeue::Entry {
    opcode::Send::new(Fd(fd), buf.as_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data)
}

fn recv(fd: i32, buf: &mut [u8], user_data: u64) -> io_uring::squeue::Entry {
    opcode::Recv::new(Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data)
}

// --------------------------------------------------- scenario declared up front

#[test]
fn a_scripted_read_completes_without_anyone_touching_the_ring() {
    let (mut ring, handle) = ring(0);
    let mut buf = [0u8; 32];

    // Declared before anything is submitted, as a test would before block_on.
    handle.on_read(11, ReadEvent::data(b"hello"));

    ring.add(&recv(11, &mut buf, 7));
    ring.submit();

    // Straight onto the timeline — nothing is waiting for a live nudge.
    assert_eq!(handle.parked(), 0);

    ring.submit_and_wait();
    let completion = ring.pop_completion().expect("the read completed");
    assert_eq!(completion.result, 5);
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn scripted_reads_are_consumed_in_order() {
    let (mut ring, handle) = ring(0);

    handle.on_read(11, ReadEvent::data(b"first"));
    handle.on_read(11, ReadEvent::data(b"second"));
    handle.on_read(11, ReadEvent::Eof);

    let mut got: Vec<Vec<u8>> = Vec::new();
    for token in 0..3u64 {
        let mut buf = [0u8; 32];
        ring.add(&recv(11, &mut buf, token));
        ring.submit_and_wait();

        let completion = ring.pop_completion().expect("a read");
        got.push(buf[..completion.result.max(0) as usize].to_vec());
    }

    assert_eq!(
        got,
        vec![b"first".to_vec(), b"second".to_vec(), Vec::new()],
        "the script was not replayed in order"
    );
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn a_multishot_accept_consumes_scripted_connections_then_parks() {
    let (mut ring, handle) = ring(0);

    handle.on_accept(3, AcceptEvent::Connection);
    handle.on_accept(3, AcceptEvent::Connection);

    ring.add(&opcode::AcceptMulti::new(Fd(3)).build().user_data(5));
    ring.submit();

    let mut fds = Vec::new();
    for _ in 0..2 {
        ring.submit_and_wait();
        let completion = ring.pop_completion().expect("a connection");
        assert!(cqueue::more(completion.flags));
        assert!(completion.result >= 0);
        fds.push(completion.result);
    }

    // The script is spent, so the op is back to waiting for something that will
    // not come — which is what a test asserting "exactly two" wants.
    assert_eq!(handle.scenario_remaining(), 0);
    assert_eq!(handle.parked(), 1);

    for fd in fds {
        assert_eq!(unsafe { libc::close(fd) }, 0);
    }
}

#[test]
fn an_accept_can_hand_back_a_descriptor_the_test_made() {
    let (mut ring, handle) = ring(0);

    // Created up front, so the test knows the number and can script what the
    // accepted connection does. Raw rather than a `SocketWrapper`: ownership
    // passes to whoever receives the completion, and two owners close it twice.
    let conn = mint(FdKind::Socket).expect("a socket");

    handle.on_accept(3, AcceptEvent::On(conn));
    handle.on_read(conn, ReadEvent::data(b"GET / HTTP/1.1\r\n"));

    ring.add(
        &opcode::Accept::new(Fd(3), std::ptr::null_mut(), std::ptr::null_mut())
            .build()
            .user_data(5),
    );
    ring.submit_and_wait();

    let accepted = ring.pop_completion().expect("a connection").result;
    assert_eq!(
        accepted, conn,
        "the accept returned the scripted descriptor"
    );

    // And now a read on that connection is scriptable — which it could not be
    // when the simulator invented the descriptor itself.
    let mut buf = [0u8; 64];
    ring.add(&recv(accepted, &mut buf, 6));
    ring.submit_and_wait();

    let n = ring.pop_completion().expect("the read").result as usize;
    assert_eq!(&buf[..n], b"GET / HTTP/1.1\r\n");

    assert_eq!(handle.scenario_remaining(), 0);
    // The simulator minted nothing here: the descriptor was the test's.
    assert!(handle.opened_fds().is_empty());

    assert_eq!(unsafe { libc::close(conn) }, 0);
}

#[test]
fn a_scripted_eof_ends_a_multishot_recv() {
    let (mut ring, handle) = ring(0);

    // Bare counts rather than `on_read`: a multishot recv draws from a provided
    // buffer ring, so there is no caller buffer to write bytes into. Scripting
    // data on one is refused, and rightly.
    handle.on_read(11, ReadEvent::Bytes(128));
    handle.on_read(11, ReadEvent::Eof); // closes the stream

    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();

    ring.submit_and_wait();
    let first = ring.pop_completion().expect("the chunk");
    assert_eq!(first.result, 128);
    assert!(cqueue::more(first.flags), "more was still coming");

    ring.submit_and_wait();
    let last = ring.pop_completion().expect("the EOF");
    assert_eq!(last.result, 0);
    assert!(
        !cqueue::more(last.flags),
        "a zero-length read is EOF, which ends the stream"
    );
    assert!(handle.inflight().is_empty(), "the op left flight");
}

#[test]
fn a_scripted_error_ends_a_multishot_accept() {
    let (mut ring, handle) = ring(0);

    handle.on_accept(3, AcceptEvent::Connection);
    handle.on_accept(3, AcceptEvent::Error(libc::ECONNABORTED));

    ring.add(&opcode::AcceptMulti::new(Fd(3)).build().user_data(5));
    ring.submit();

    ring.submit_and_wait();
    let first = ring.pop_completion().expect("a connection");
    assert!(cqueue::more(first.flags));
    let fd = first.result;

    ring.submit_and_wait();
    let last = ring.pop_completion().expect("the error");
    assert_eq!(last.result, -libc::ECONNABORTED);
    assert!(
        !cqueue::more(last.flags),
        "an error ends a multishot accept"
    );
    assert!(handle.inflight().is_empty());

    assert_eq!(unsafe { libc::close(fd) }, 0);
}

#[test]
fn a_zero_from_an_accept_is_a_descriptor_not_an_ending() {
    // The asymmetry worth pinning: `0` ends a recv but is a valid fd from an
    // accept, so the two cannot share one "non-negative keeps going" rule.
    let (mut ring, handle) = ring(0);

    handle.on_accept(3, AcceptEvent::On(0));
    ring.add(&opcode::AcceptMulti::new(Fd(3)).build().user_data(5));
    ring.submit_and_wait();

    let completion = ring.pop_completion().expect("a connection");
    assert_eq!(completion.result, 0);
    assert!(cqueue::more(completion.flags), "fd 0 is still a connection");
    assert_eq!(handle.parked(), 1, "the accept is still armed");
}

#[test]
fn a_pinned_delay_decides_the_completion_order() {
    let (mut ring, handle) = ring(0);
    let mut slow = [0u8; 8];
    let mut fast = [0u8; 8];

    // Submitted first, scheduled last: the ordering is the test's, not the
    // seed's.
    handle.on_read(11, ReadEvent::data(b"slow").after(STEP * 4));
    handle.on_read(12, ReadEvent::data(b"fast").after(STEP));

    ring.add(&recv(11, &mut slow, 1));
    ring.add(&recv(12, &mut fast, 2));
    ring.submit();

    let mut order = Vec::new();
    while order.len() < 2 {
        ring.submit_and_wait();
        while let Some(completion) = ring.pop_completion() {
            order.push(completion.user_data);
        }
    }

    assert_eq!(order, vec![2, 1], "the pinned delays decided the order");
}

/// Completion order of eight sends, behind a read that is pinned or not.
fn order_behind_a_read(pinned: bool) -> Vec<u64> {
    let (mut ring, handle) = ring(0xBEEF);
    let mut buf = [0u8; 8];

    let event = ReadEvent::data(b"x");
    if pinned {
        handle.on_read(11, event.after(STEP * 3));
    } else {
        handle.on_read(11, event);
    }

    ring.add(&recv(11, &mut buf, 100));
    for i in 0..8 {
        ring.add(&send(3, b"x", i));
    }
    ring.submit();

    let mut order = Vec::new();
    while order.len() < 9 {
        ring.submit_and_wait();
        while let Some(completion) = ring.pop_completion() {
            order.push(completion.user_data);
        }
    }
    order.retain(|ud| *ud != 100);
    order
}

#[test]
fn pinning_one_delay_leaves_every_other_op_alone() {
    // A latency is drawn even when the delay is pinned. Skipping the draw
    // would take a different number of values from the stream, so adding one
    // `after(..)` would silently reshuffle the whole test.
    assert_eq!(
        order_behind_a_read(false),
        order_behind_a_read(true),
        "pinning one op perturbed the others",
    );
}

#[test]
fn a_scripted_delay_needs_no_grid_and_has_no_ceiling() {
    // A fixed-slot design could only express a few multiples of the step. A list
    // ordered by due time takes any duration, so neither of these needs a
    // workaround.
    let (mut ring, handle) = ring(0);
    let mut late = [0u8; 8];
    let mut early = [0u8; 8];

    handle.on_read(11, ReadEvent::data(b"late").after(STEP * 9)); // past six steps
    handle.on_read(12, ReadEvent::data(b"early").after(STEP / 2)); // between two

    ring.add(&recv(11, &mut late, 1));
    ring.add(&recv(12, &mut early, 2));
    ring.submit();

    let mut order = Vec::new();
    while order.len() < 2 {
        ring.submit_and_wait();
        while let Some(completion) = ring.pop_completion() {
            order.push(completion.user_data);
        }
    }

    assert_eq!(order, vec![2, 1]);
}

#[test]
fn a_read_on_an_unscripted_descriptor_still_parks() {
    let (mut ring, handle) = ring(0);
    let mut buf = [0u8; 32];

    handle.on_read(11, ReadEvent::data(b"meant for another socket"));
    ring.add(&recv(99, &mut buf, 7));
    ring.submit();

    assert_eq!(handle.parked(), 1, "the script names fd 11, not 99");
    assert_eq!(handle.scenario_remaining(), 1);
}

#[test]
fn scenario_remaining_reports_what_was_never_claimed() {
    let (mut ring, handle) = ring(0);
    let mut buf = [0u8; 32];

    handle.on_read(11, ReadEvent::data(b"used"));
    handle.on_read(11, ReadEvent::data(b"never asked for"));
    assert_eq!(handle.scenario_remaining(), 2);

    ring.add(&recv(11, &mut buf, 7));
    ring.submit_and_wait();
    ring.pop_completion();

    assert_eq!(
        handle.scenario_remaining(),
        1,
        "an unclaimed entry means the code under test did fewer reads than scripted"
    );
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
fn a_scheduled_op_completes_on_its_own_after_its_latency() {
    let (mut ring, handle) = ring(0);
    let buf = b"hello world";

    ring.add(&send(3, buf, 1));
    let started = Instant::now();
    ring.submit_and_wait();
    let waited = started.elapsed();

    let completion = ring.pop_completion().expect("the op completed on its own");
    assert_eq!(completion.user_data, 1);
    // A write-side op reports the byte count it was handed.
    assert_eq!(completion.result as usize, buf.len());

    // An undeclared op draws one to six steps. Only the lower bound is worth
    // asserting — a loaded machine can overshoot freely.
    assert!(waited >= STEP, "returned after {waited:?}, before one tick");

    // Firing empties the slot: a single-shot leaves nothing behind.
    assert!(handle.inflight().is_empty());
}

#[test]
fn ops_parked_on_an_external_event_never_complete_by_themselves() {
    let (mut ring, handle) = ring(0);

    // Nobody has written to this socket, so a real kernel would leave the recv
    // outstanding indefinitely. Scheduling it would be a lie.
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
    // Latencies span 50..=300ms here, so a 5ms timer has to win.
    let (mut ring, _handle) = FakeRing::builder()
        .latency_step(Duration::from_millis(50))
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

// --------------------------------------------------------- minted descriptors

fn open_at(path: &std::ffi::CStr, user_data: u64) -> io_uring::squeue::Entry {
    // The path is never touched — the simulated ring reaches no filesystem — so
    // it need not exist.
    opcode::OpenAt::new(Fd(libc::AT_FDCWD), path.as_ptr())
        .build()
        .user_data(user_data)
}

#[test]
fn openat_completes_with_a_descriptor_that_really_works() {
    let (mut ring, handle) = ring(0);

    ring.add(&open_at(c"/nonexistent", 3));
    ring.submit_and_wait();

    let fd = ring.pop_completion().expect("the open completed").result;
    assert!(fd >= 0, "open returned {fd}");
    assert_eq!(handle.opened_fds(), vec![fd]);

    // The point of minting a real descriptor: it can be written, read back and
    // closed. A made-up integer would fail here — or worse, succeed against
    // some unrelated fd of the test process.
    let msg = b"round trip";
    let written = unsafe { libc::write(fd, msg.as_ptr().cast(), msg.len()) };
    assert_eq!(written, msg.len() as isize);

    let mut back = [0u8; 16];
    let read = unsafe { libc::pread(fd, back.as_mut_ptr().cast(), msg.len(), 0) };
    assert_eq!(read, msg.len() as isize);
    assert_eq!(&back[..msg.len()], msg);

    assert_eq!(
        unsafe { libc::close(fd) },
        0,
        "the descriptor did not close"
    );
}

#[test]
fn each_open_mints_a_distinct_descriptor() {
    let (mut ring, handle) = ring(0);

    for i in 0..4 {
        ring.add(&open_at(c"/nonexistent", i));
    }
    ring.submit();

    let mut opened = Vec::new();
    while opened.len() < 4 {
        ring.submit_and_wait();
        while let Some(completion) = ring.pop_completion() {
            opened.push(completion.result);
        }
    }

    let mut distinct = opened.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), 4, "descriptors were reused: {opened:?}");

    let mut reported = handle.opened_fds();
    reported.sort_unstable();
    assert_eq!(reported, distinct);

    for fd in opened {
        assert_eq!(unsafe { libc::close(fd) }, 0);
    }
}

#[test]
fn accept_mints_a_socket_the_runtime_can_actually_use() {
    let (mut ring, handle) = ring(0);

    ring.add(
        &opcode::Accept::new(Fd(3), std::ptr::null_mut(), std::ptr::null_mut())
            .build()
            .user_data(5),
    );
    ring.submit();
    // Nobody has connected yet, so it waits.
    assert_eq!(handle.parked(), 1);

    handle.accept(5);
    ring.submit_and_wait();

    let completion = ring.pop_completion().expect("a connection arrived");
    let fd = completion.result;
    assert!(fd >= 0, "accept returned {fd}");
    assert!(
        !cqueue::more(completion.flags),
        "a single-shot accept is finished, so no F_MORE"
    );
    assert_eq!(handle.opened_fds(), vec![fd]);

    // These are the syscalls the runtime makes outside the ring, and they are
    // exactly what a memfd or an AF_UNIX pair would have failed.
    let socket = SocketWrapper { fd };
    socket.set_nodelay(true).expect("TCP_NODELAY");
    socket.set_quickack(true).expect("TCP_QUICKACK");
    socket.set_reuseaddr(true).expect("SO_REUSEADDR");
    socket.local_addr().expect("getsockname");
    // Truthfully unconnected, and the kernel says so.
    assert!(socket.peer_addr().is_err(), "peer_addr should be ENOTCONN");

    // Dropping the wrapper closes it, which is only correct because it is real.
    drop(socket);
}

#[test]
fn simulated_socket_takes_every_option_the_runtime_sets() {
    let socket = simulated_socket().expect("a real unconnected TCP socket");

    socket.set_nodelay(true).expect("TCP_NODELAY");
    socket.set_keepalive(true).expect("SO_KEEPALIVE");
    socket.set_reuseaddr(true).expect("SO_REUSEADDR");
    socket.set_reuseport(true).expect("SO_REUSEPORT");
    socket.set_recv_buffer_size(8192).expect("SO_RCVBUF");
    socket.set_send_buffer_size(8192).expect("SO_SNDBUF");
    socket.set_defer_accept(1).expect("TCP_DEFER_ACCEPT");
    socket.set_quickack(true).expect("TCP_QUICKACK");
    socket.local_addr().expect("getsockname");
}

// ------------------------------------------------------------- multishot

#[test]
fn a_multishot_accept_stays_in_flight_across_connections() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::AcceptMulti::new(Fd(3)).build().user_data(5));
    ring.submit();

    let mut fds = Vec::new();
    for _ in 0..3 {
        handle.accept(5);
        ring.submit_and_wait();

        let completion = ring.pop_completion().expect("a connection arrived");
        assert_eq!(completion.user_data, 5);
        assert!(completion.result >= 0, "accept gave {}", completion.result);
        assert!(
            cqueue::more(completion.flags),
            "F_MORE should mark the op unfinished"
        );
        // Back in flight, waiting for the next connection.
        assert_eq!(handle.parked(), 1);

        fds.push(completion.result);
    }

    let mut distinct = fds.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        3,
        "connections reused a descriptor: {fds:?}"
    );

    for fd in fds {
        assert_eq!(unsafe { libc::close(fd) }, 0);
    }
}

#[test]
fn a_multishot_recv_stays_in_flight_between_results() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();

    for expected in [128, 64] {
        handle.wake(7, expected);
        ring.submit_and_wait();

        let completion = ring.pop_completion().expect("a result");
        assert_eq!(completion.result, expected);
        assert!(cqueue::more(completion.flags));
        assert_eq!(handle.parked(), 1);
    }
}

#[test]
fn a_failing_multishot_is_terminal() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();

    handle.wake(7, -libc::ECONNRESET);
    ring.submit_and_wait();

    let completion = ring.pop_completion().expect("the error");
    assert_eq!(completion.result, -libc::ECONNRESET);
    assert!(
        !cqueue::more(completion.flags),
        "an error ends a multishot — no F_MORE"
    );
    assert_eq!(handle.parked(), 0);
    assert!(handle.inflight().is_empty());
}

#[test]
fn cancelling_a_multishot_retires_it_with_ecanceled() {
    let (mut ring, handle) = ring(0);

    ring.add(&opcode::RecvMulti::new(Fd(11), 0).build().user_data(7));
    ring.submit();
    assert_eq!(handle.parked(), 1);

    ring.add(&opcode::AsyncCancel::new(7).build().user_data(99));
    ring.submit_and_wait();

    let mut seen = Vec::new();
    while let Some(completion) = ring.pop_completion() {
        seen.push((completion.user_data, completion.result, completion.flags));
    }

    // Target first and terminal — no F_MORE is what tells the driver the
    // multishot is finally done — then the cancel's own success.
    assert_eq!(seen, vec![(7, -libc::ECANCELED, 0), (99, 0, 0)]);
    assert_eq!(handle.parked(), 0);
    assert!(handle.inflight().is_empty());
}

#[test]
fn cancelling_a_scheduled_op_takes_it_off_the_wheel() {
    let (mut ring, handle) = ring(0);

    ring.add(&send(3, b"x", 7));
    ring.add(&opcode::AsyncCancel::new(7).build().user_data(99));
    ring.submit();

    // Both are on the wheel; drain until the cancel has had its say.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        ring.submit_and_wait();
        while let Some(completion) = ring.pop_completion() {
            seen.push((completion.user_data, completion.result));
        }
    }

    assert!(handle.inflight().is_empty());
    // Either the cancel landed first and the send was retired, or the send's own
    // tick came first and the cancel found nothing — both are outcomes a real
    // kernel produces, so only the end state is asserted.
    let cancel = seen.iter().find(|(ud, _)| *ud == 99).expect("the cancel");
    assert!(cancel.1 == 0 || cancel.1 == -libc::ENOENT, "got {cancel:?}");
}

#[test]
fn cancelling_an_unknown_token_reports_enoent() {
    let (mut ring, _handle) = ring(0);

    ring.add(&opcode::AsyncCancel::new(0xDEAD).build().user_data(99));
    ring.submit_and_wait();

    let completion = ring.pop_completion().expect("the cancel completed");
    assert_eq!(completion.user_data, 99);
    assert_eq!(completion.result, -libc::ENOENT);
    assert!(ring.pop_completion().is_none(), "nothing was cancelled");
}

// ------------------------------------------------------- delivering data

#[test]
fn delivering_bytes_writes_them_into_the_ops_own_buffer() {
    let (mut ring, handle) = ring(0);
    let mut buf = [0u8; 32];

    ring.add(
        &opcode::Recv::new(Fd(11), buf.as_mut_ptr(), buf.len() as u32)
            .build()
            .user_data(7),
    );
    ring.submit();

    handle.deliver(7, b"hello");
    ring.submit_and_wait();

    // A CQE carries no payload: the count comes back in `res`, the bytes come
    // back in the buffer the submitter supplied.
    let completion = ring.pop_completion().expect("the recv completed");
    assert_eq!(completion.user_data, 7);
    assert_eq!(completion.result, 5);
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(&buf[5..8], &[0, 0, 0], "wrote past the delivered bytes");
}

#[test]
fn the_buffer_is_untouched_until_the_completion_is_posted() {
    let (mut ring, handle) = ring(0);
    let mut buf = [0u8; 32];

    ring.add(
        &opcode::Recv::new(Fd(11), buf.as_mut_ptr(), buf.len() as u32)
            .build()
            .user_data(7),
    );
    ring.submit();

    // The event has happened, but the op has not completed — a kernel fills the
    // buffer when it posts the CQE, not when the socket becomes readable.
    handle.deliver(7, b"hello");
    assert_eq!(buf[0], 0, "buffer written before the op completed");

    ring.submit_and_wait();
    assert_eq!(&buf[..5], b"hello");
}

#[test]
fn a_delivery_larger_than_the_buffer_is_a_short_read() {
    let (mut ring, handle) = ring(0);
    let mut buf = [0u8; 4];

    ring.add(
        &opcode::Recv::new(Fd(11), buf.as_mut_ptr(), buf.len() as u32)
            .build()
            .user_data(7),
    );
    ring.submit();

    handle.deliver(7, b"hello world");
    ring.submit_and_wait();

    let completion = ring.pop_completion().expect("the recv completed");
    assert_eq!(completion.result, 4, "should report only what fit");
    assert_eq!(&buf, b"hell");
}

#[test]
#[should_panic(expected = "provided-buffer op")]
fn delivering_data_to_a_managed_recv_panics() {
    let (mut ring, handle) = ring(0);

    // `addr` on a BUFFER_SELECT op names no buffer, so copying there would land
    // on unrelated memory. Caught at the point the write would happen.
    ring.add(&opcode::RecvMulti::new(Fd(11), 4).build().user_data(7));
    ring.submit();

    handle.deliver(7, b"hello");
    ring.submit_and_wait();
}

#[test]
fn waking_without_data_still_reports_a_bare_result() {
    // Not every read-side completion carries bytes — an error, or an accept
    // returning a descriptor.
    let (mut ring, handle) = ring(0);

    ring.add(
        &opcode::Recv::new(Fd(11), std::ptr::null_mut(), 0)
            .build()
            .user_data(7),
    );
    ring.submit();

    handle.wake(7, -libc::ECONNRESET);
    ring.submit_and_wait();

    let completion = ring.pop_completion().expect("the recv completed");
    assert_eq!(completion.result, -libc::ECONNRESET);
}
