//! Futures dropped while their operation is still in flight.
//!
//! Every op future in the driver has a `Drop` that cancels its token and moves
//! whatever the kernel might still touch into a keepalive slot. That contract
//! is invisible against a real ring: the write lands in a heap the allocator
//! has already handed to someone else, and nothing says so. Against the
//! simulator the completion is written by Rust, so dropping a future too early
//! is an ordinary use-after-free — which is to say, something Miri will name.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p runtime-core --features simulation --test cancellation
//! ```

use std::time::Duration;

use runtime_core::deterministic_testing::{FakeRing, FakeRingHandle, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::multi_recv::MultiRecv;
use runtime_core::io_driver::operations::sockets::recv::socket_recv;
use runtime_core::io_driver::operations::sockets::send::socket_send;
use runtime_core::io_driver::operations::sockets::send_vectored_zc::socket_send_vectored_zc;
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::{RuntimeInner, spawn, time::Sleep};
use runtime_streams::StreamExt;

// Two buffers, so a ring that fails to take one back is a ring that runs dry
// on the very next read rather than a hundred reads later.
runtime_core::define_buf_ring!(CancelRing, bgid = 1, buffer_size = 64, ring_size = 2);

fn simulated() -> (FakeRing, FakeRingHandle) {
    FakeRing::builder().virtual_time().build()
}

/// Give the event loop enough passes to submit a cancel and reap what it
/// brings back. A cancel is an SQE like any other: it needs a trip through the
/// loop, and so does the completion it produces.
///
/// Sleeps rather than yields, because the simulator refuses to block a runtime
/// that has nothing scheduled to wake it — and a bare `yield_now` leaves the
/// timeline empty. Under virtual time these cost no real time at all.
async fn settle() {
    for _ in 0..2 {
        Sleep::during(Duration::from_millis(1)).await;
    }
}

#[test]
fn a_read_dropped_in_flight_leaves_nothing_behind() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    // Far enough out that the read is certainly still in flight when dropped.
    handle.on_read(
        socket.fd(),
        ReadEvent::data(b"hello").after(Duration::from_secs(10)),
    );

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            // Built, polled once so it reaches the ring, then dropped — the
            // shortest path to a cancel there is.
            let read = socket_recv(&socket, vec![0u8; 32]);
            select(read, Sleep::during(Duration::from_millis(1))).await;
            settle().await;
        },
        None,
        |run| run(),
    );

    // The op is gone from the ring rather than merely forgotten by the task.
    assert!(
        handle.inflight().is_empty(),
        "the cancel left {:?} in flight",
        handle.inflight(),
    );
}

#[test]
fn a_dropped_read_hands_its_delivery_back_to_the_scenario() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    handle.on_read(
        socket.fd(),
        ReadEvent::data(b"hello").after(Duration::from_secs(10)),
    );
    assert_eq!(handle.scenario_remaining(), 1);

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            select(
                socket_recv(&socket, vec![0u8; 32]),
                Sleep::during(Duration::from_millis(1)),
            )
            .await;
            settle().await;
        },
        None,
        |run| run(),
    );

    // A kernel does not throw away buffered bytes because the reader gave up.
    // Counted directly here rather than inferred from a second read, so a
    // scenario that quietly ate the entry cannot pass by re-scripting itself.
    assert_eq!(
        handle.scenario_remaining(),
        1,
        "the cancel consumed a delivery nobody received",
    );
}

#[test]
fn a_send_dropped_in_flight_outlives_its_own_future() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            // The buffer belongs to the future, and the future is about to
            // stop existing while the ring still holds a pointer into it.
            // Nothing here asserts — Miri is the assertion.
            //
            // Raced against an already-resolved future rather than a sleep: a
            // send has no scripted delay to give it, so the only way to catch
            // one in flight is to lose the race on the very first poll.
            let owned = vec![b'x'; 512];
            let raced = select(socket_send(&socket, owned), std::future::ready(())).await;
            assert!(
                matches!(raced, SelectResult::Second(_)),
                "the send completed before it could be dropped in flight",
            );
            settle().await;
        },
        None,
        |run| run(),
    );

    assert!(handle.inflight().is_empty());
}

#[test]
fn a_zero_copy_gather_send_dropped_in_flight_outlives_its_own_future() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            // Chunks, iovec array and msghdr must all reach the keepalive; Miri is the assertion.
            let chunks = vec![vec![b'h'; 256], vec![b'b'; 512]];
            let raced = select(
                socket_send_vectored_zc(&socket, chunks),
                std::future::ready(()),
            )
            .await;
            assert!(
                matches!(raced, SelectResult::Second(_)),
                "the send completed before it could be dropped in flight",
            );
            settle().await;
        },
        None,
        |run| run(),
    );

    assert!(handle.inflight().is_empty());
}

#[test]
fn a_multishot_dropped_in_flight_is_cancelled() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    handle.on_multishot_read(
        socket.fd(),
        [b"one".as_slice(), b"two", b"three"].map(ReadEvent::data),
    );

    let first = RuntimeInner::simulated(fake).block_on::<CancelRing, _>(
        async move {
            let mut recv = MultiRecv::<CancelRing>::new(socket);
            let chunk = recv.next().await.expect("a chunk").expect("no error");
            let bytes = chunk.as_ref().to_vec();

            // A multishot is still armed after it yields — dropping the stream
            // is the only thing that takes it off the ring.
            drop(chunk);
            drop(recv);
            settle().await;
            bytes
        },
        None,
        |run| run(),
    );

    assert_eq!(first, b"one");
    assert!(
        handle.inflight().is_empty(),
        "the multishot outlived its stream: {:?}",
        handle.inflight(),
    );
}

#[test]
fn a_stream_dropped_mid_batch_gives_the_ring_its_buffers_back() {
    // Both chunks land in one drain, so when the stream is dropped the second
    // is already sitting in the driver holding a buffer of a two-buffer ring.
    // That buffer comes back through the cancel path, which is a different
    // path from the one a delivered buffer takes.
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    handle.on_multishot_read(
        socket.fd(),
        [vec![ReadEvent::data(b"a"), ReadEvent::data(b"b")]],
    );

    let scripted = handle.clone();
    let held = RuntimeInner::simulated(fake).block_on::<CancelRing, _>(
        async move {
            let mut recv = MultiRecv::<CancelRing>::new(socket);
            let chunk = recv.next().await.expect("a chunk").expect("no error");
            drop(chunk);
            let socket = recv.into_inner();
            settle().await;

            // Scripted here rather than up front, so what follows measures the
            // ring's buffers and not whatever the cancel handed back to the
            // scenario.
            scripted.on_multishot_read(socket.fd(), [ReadEvent::data(b"c"), ReadEvent::data(b"d")]);

            // Both buffers are held at once on purpose: a ring that lost the
            // undelivered one to the cancel has a single buffer left, and the
            // second read comes back `ENOBUFS` instead of a chunk.
            let mut again = MultiRecv::<CancelRing>::new(socket);
            let mut held = Vec::new();
            for _ in 0..2 {
                match again.next().await {
                    Some(Ok(buffer)) => held.push(buffer),
                    Some(Err(error)) => panic!("the ring ran dry: {error:?}"),
                    None => panic!("the stream ended early"),
                }
            }
            held.len()
        },
        None,
        |run| run(),
    );

    assert_eq!(held, 2, "the ring should have both buffers back");
}

/// A runtime that ends with a task still parked leaks that task.
///
/// Passes on its own terms — `block_on` returns, nothing hangs — but under
/// Miri it reports two leaks: the `TypedTask` box and the buffer the parked
/// read owns. A parked task is kept alive by the waker the driver holds, not
/// by the `TaskList`, so consuming the runtime never reclaims it.
///
/// Ignored rather than deleted because the fix is a design decision and not an
/// oversight: tearing down a task whose op is in flight means cancelling and
/// draining first, which is exactly what the per-future keepalive does and
/// what no one does for a whole task at shutdown. Freeing it outright would
/// trade this leak for the use-after-free the keepalive exists to prevent.
///
/// ```text
/// cargo test --features simulation --test cancellation -- --ignored
/// ```
#[test]
#[ignore = "documents a known shutdown leak; Miri fails it by design"]
fn a_task_still_parked_on_a_read_leaks_it() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    // Nothing scripted: this read parks and never completes.
    let done = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            spawn(async move {
                let (result, _buf) = socket_recv(&socket, vec![0u8; 32]).await;
                result
            });

            // The spawned task is still on the ring when this returns, so the
            // runtime tears down around an op in flight and a buffer the ring
            // still points into.
            Sleep::during(Duration::from_millis(1)).await;
            true
        },
        None,
        |run| run(),
    );

    assert!(done);
    let _ = handle.inflight();
}
