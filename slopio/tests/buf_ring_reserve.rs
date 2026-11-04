//! A buffer ring whose pool is larger than what the kernel sees at once.
//!
//! The extra buffers wait in a reserve. The driver counts every buffer a CQE
//! carries, so it knows how many entries the ring has free and fills them from
//! the reserve before it submits.

use std::time::Duration;

use slopio::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use slopio::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use slopio::io_driver::{BufRingSpec, BufRingUsage};
use slopio::runtime::{RuntimeInner, time::Sleep};
use slopio::streams::StreamExt;

// Two buffers in the ring, two more behind them. Ring bits 3: the top of the field.
slopio::define_buf_ring!(
    Reserved,
    bgid = 3,
    buffer_size = 64,
    ring_size = 2,
    pool_size = 4
);

fn usage(in_kernel: usize, reserved: usize, in_flight: usize) -> BufRingUsage {
    BufRingUsage {
        in_kernel,
        reserved,
        in_flight,
    }
}

/// Enough loop passes to submit a cancel and reap what it brings back.
async fn settle() {
    for _ in 0..2 {
        Sleep::during(Duration::from_millis(1)).await;
    }
}

#[test]
fn held_buffers_are_replaced_from_the_reserve() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    // Four reads through a ring of two, nothing given back in between.
    handle.on_multishot_read(
        socket.fd(),
        [b"a".as_slice(), b"b", b"c", b"d"].map(ReadEvent::data),
    );

    let bids = RuntimeInner::simulated(fake).block_on::<Reserved, _>(
        async move {
            assert_eq!(Reserved::usage(), usage(2, 2, 0));

            let mut recv = MultiRecv::<Reserved>::new(socket);
            let mut held = Vec::new();
            for _ in 0..4 {
                held.push(recv.next().await.expect("a chunk").expect("no error"));
            }
            assert_eq!(Reserved::usage(), usage(0, 0, 4));
            held.iter().map(|buffer| buffer.bid()).collect::<Vec<_>>()
        },
        None,
        |run| run(),
    );

    // The ring's own two first, then the reserve's, which starts in id order.
    assert_eq!(bids, [0, 1, 2, 3]);
}

#[test]
fn a_released_buffer_fills_a_hole_and_the_next_goes_to_the_reserve() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    handle.on_multishot_read(socket.fd(), [b"a".as_slice(), b"b"].map(ReadEvent::data));

    RuntimeInner::simulated(fake).block_on::<Reserved, _>(
        async move {
            let mut recv = MultiRecv::<Reserved>::new(socket);
            let a = recv.next().await.expect("a chunk").expect("no error");
            let b = recv.next().await.expect("a chunk").expect("no error");
            // One refill ran between the two reads, and "b" left a hole since.
            assert_eq!(Reserved::usage(), usage(1, 1, 2));

            drop(a);
            assert_eq!(Reserved::usage(), usage(2, 1, 1), "the hole takes it");
            drop(b);
            assert_eq!(Reserved::usage(), usage(2, 2, 0), "a full ring does not");
        },
        None,
        |run| run(),
    );
}

#[test]
fn a_pool_held_whole_runs_dry_then_recovers() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    handle.on_multishot_read(
        socket.fd(),
        [b"a".as_slice(), b"b", b"c", b"d", b"e"].map(ReadEvent::data),
    );
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"f"), ReadEvent::Eof]);

    let got = RuntimeInner::simulated(fake).block_on::<Reserved, _>(
        async move {
            let mut recv = MultiRecv::<Reserved>::new(socket);
            let mut held = Vec::new();
            for _ in 0..4 {
                held.push(recv.next().await.expect("a chunk").expect("no error"));
            }
            match recv.next().await {
                Some(Err(CompletionError::Other(code))) => assert_eq!(code, -libc::ENOBUFS),
                other => panic!(
                    "expected ENOBUFS, got {:?}",
                    other.map(|r| r.map(|b| b.len()))
                ),
            }

            held.clear();
            assert_eq!(Reserved::usage(), usage(2, 2, 0));
            let chunk = recv.next().await.expect("a chunk").expect("no error");
            chunk.as_ref().to_vec()
        },
        None,
        |run| run(),
    );

    assert_eq!(got, b"f");
}

#[test]
fn a_stream_dropped_mid_flight_is_cancelled_and_accounted() {
    // The ring bits are all set here, so a cancel built from the bare slab
    // index would miss the op and leave it armed.
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    handle.on_multishot_read(
        socket.fd(),
        [vec![ReadEvent::data(b"a"), ReadEvent::data(b"b")]],
    );

    RuntimeInner::simulated(fake).block_on::<Reserved, _>(
        async move {
            let mut recv = MultiRecv::<Reserved>::new(socket);
            let chunk = recv.next().await.expect("a chunk").expect("no error");
            drop(chunk);
            // "b" sits undelivered in the driver, holding a buffer.
            drop(recv);
            settle().await;
            assert_eq!(Reserved::usage().in_flight, 0);
        },
        None,
        |run| run(),
    );

    assert!(
        handle.inflight().is_empty(),
        "the multishot outlived its stream: {:?}",
        handle.inflight(),
    );
}
