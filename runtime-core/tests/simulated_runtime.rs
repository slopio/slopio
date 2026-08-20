//! The runtime, running for real against a simulated ring.
//!
//! Everything the other suite proves is about the simulator in isolation. These
//! run an actual future through `block_on`, so what is under test is the driver
//! — slab, wakers, the completion routing, the event loop — with the kernel
//! swapped out from under it.

use std::time::{Duration, Instant};

use runtime_core::deterministic_testing::{FakeRing, FakeRingHandle, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::io_driver::operations::sockets::recv::socket_recv;
use runtime_core::io_driver::operations::sockets::send::socket_send;
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::{RuntimeInner, spawn, time::Sleep};
use runtime_streams::StreamExt;

// A real provided-buffer ring. `block_on::<R, _>` installs it, which on a
// simulated backend also registers the simulator as its consumer.
runtime_core::define_buf_ring!(ManagedRing, bgid = 3, buffer_size = 64, ring_size = 2);

fn simulated(seed: u64) -> (FakeRing, FakeRingHandle) {
    FakeRing::builder()
        .seed(seed)
        .latency_step(Duration::from_millis(10))
        .virtual_time()
        .build()
}

#[test]
fn a_send_completes_through_the_whole_driver() {
    let (fake, _handle) = simulated(42);
    let socket = simulated_socket().expect("a real unconnected socket");

    let (result, _buf) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { socket_send(&socket, b"hello").await },
        None,
        |run| run(),
    );

    // Nothing here was scripted: a send completes on its own, and getting the
    // byte count back means the token round-tripped through the slab and the
    // waker actually woke the task.
    assert_eq!(result.expect("the send succeeded"), 5);
}

#[test]
fn a_scripted_read_reaches_the_awaiting_task() {
    let (fake, handle) = simulated(42);
    let socket = simulated_socket().expect("a real unconnected socket");

    handle.on_read(socket.fd(), ReadEvent::data(b"hello"));

    let (result, buf) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { socket_recv(&socket, vec![0u8; 32]).await },
        None,
        |run| run(),
    );

    let n = result.expect("the read succeeded");
    assert_eq!(&buf[..n], b"hello");
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn concurrent_sends_all_complete() {
    let (fake, _handle) = simulated(7);

    let total = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let socket = simulated_socket().expect("a socket");
                    spawn(async move {
                        let (result, _) = socket_send(&socket, b"xyz").await;
                        result.expect("the send succeeded")
                    })
                })
                .collect();

            let mut total = 0;
            for handle in handles {
                total += handle.await;
            }
            total
        },
        None,
        |run| run(),
    );

    // Eight tasks, eight completions routed back to eight distinct wakers.
    assert_eq!(total, 24);
}

#[test]
fn the_same_seed_gives_the_same_completion_order() {
    fn order(seed: u64) -> Vec<usize> {
        let (fake, _handle) = simulated(seed);
        RuntimeInner::simulated(fake).block_on::<(), _>(
            async move {
                let handles: Vec<_> = (0..8)
                    .map(|i| {
                        let socket = simulated_socket().expect("a socket");
                        spawn(async move {
                            let (result, _) = socket_send(&socket, b"x").await;
                            result.expect("the send succeeded");
                            i
                        })
                    })
                    .collect();

                let mut finished = Vec::new();
                for handle in handles {
                    finished.push(handle.await);
                }
                finished
            },
            None,
            |run| run(),
        )
    }

    assert_eq!(order(0xC0FFEE), order(0xC0FFEE));
}

#[test]
fn a_timer_fires_without_any_time_actually_passing() {
    // The ring shares its clock with the timewheel, so a sleep is a jump. The
    // runtime is not told, and does not need to be.
    let (fake, handle) = simulated(1);
    let clock = handle.clock().expect("built with virtual_time");

    let started = Instant::now();
    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { Sleep::during(Duration::from_secs(30)).await },
        None,
        |run| run(),
    );
    let wall = started.elapsed();

    assert!(
        wall < Duration::from_secs(1),
        "a 30s sleep took {wall:?} of real time",
    );
    // And it was not skipped: simulated time really moved thirty seconds.
    assert!(
        clock.elapsed() >= Duration::from_secs(30),
        "the sleep resolved after only {:?} of simulated time",
        clock.elapsed(),
    );
}

/// A sleep that fails the test if it is ever reached.
async fn never_reached(duration: Duration) {
    Sleep::during(duration).await;
    panic!("the {duration:?} arm resolved first");
}

#[test]
fn select_resolves_on_whichever_sleep_is_shorter() {
    let long = Duration::from_secs(3600);
    let short = Duration::from_millis(100);

    // Both orders, so "always the second arm" cannot pass either.
    for long_first in [true, false] {
        let (fake, handle) = simulated(1);
        let clock = handle.clock().expect("built with virtual_time");

        RuntimeInner::simulated(fake).block_on::<(), _>(
            async move {
                if long_first {
                    select(never_reached(long), Sleep::during(short)).await;
                } else {
                    select(Sleep::during(short), never_reached(long)).await;
                }
            },
            None,
            |run| run(),
        );

        // The short arm was waited on rather than resolved on the spot — the
        // one thing a panic on the long arm cannot show.
        assert!(
            clock.elapsed() >= short,
            "resolved after only {:?} of simulated time",
            clock.elapsed(),
        );
    }
}

#[test]
fn work_continues_normally_after_a_select() {
    let (fake, _handle) = simulated(1);

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            select(
                never_reached(Duration::from_secs(3600)),
                Sleep::during(Duration::from_millis(100)),
            )
            .await;
            // Armed after the select, so this takes another full pass of the
            // loop — and must not queue behind the arm that lost.
            Sleep::during(Duration::from_millis(50)).await;
        },
        None,
        |run| run(),
    );
}

#[test]
fn a_managed_recv_hands_a_borrowed_buffer_to_the_task() {
    let (fake, handle) = simulated(42);
    let socket = simulated_socket().expect("a socket");
    handle.on_read(socket.fd(), ReadEvent::data(b"hello"));

    let got = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket.fd());
            let buffer = recv.next().await.expect("a chunk").expect("no error");
            buffer.as_ref().to_vec()
        },
        None,
        |run| run(),
    );

    // The bytes went into a buffer the ring provided, the CQE named it in its
    // flags, and the driver turned that into a typed BorrowedBuffer.
    assert_eq!(got, b"hello");
}

#[test]
fn a_ring_of_two_serves_more_reads_than_it_has_buffers() {
    let (fake, handle) = simulated(42);
    let socket = simulated_socket().expect("a socket");

    for chunk in [b"one".as_slice(), b"two", b"three", b"four", b"five"] {
        handle.on_read(socket.fd(), ReadEvent::data(chunk));
    }

    let got = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket.fd());
            let mut got: Vec<Vec<u8>> = Vec::new();
            for _ in 0..5 {
                let buffer = recv.next().await.expect("a chunk").expect("no error");
                got.push(buffer.as_ref().to_vec());
                // Dropped here, which recycles it.
            }
            got
        },
        None,
        |run| run(),
    );

    // Five reads through a ring of two only works if each buffer went back
    // after the task was done with it. The negative control is below.
    assert_eq!(
        got,
        vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
            b"five".to_vec()
        ]
    );
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn holding_every_buffer_drains_the_ring() {
    // The mirror of the test above: same ring, same reads, but the task keeps
    // its buffers instead of dropping them. If recycling were not what makes
    // that one pass, this one would pass too.
    let (fake, handle) = simulated(42);
    let socket = simulated_socket().expect("a socket");

    for chunk in [b"one".as_slice(), b"two", b"three"] {
        handle.on_read(socket.fd(), ReadEvent::data(chunk));
    }

    let (delivered, failure) = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket.fd());
            let mut held = Vec::new();
            let mut failure = None;

            for _ in 0..3 {
                match recv.next().await {
                    // Pushed, not dropped: this buffer never goes back.
                    Some(Ok(buffer)) => held.push(buffer),
                    Some(Err(error)) => {
                        failure = Some(error);
                        break;
                    }
                    None => break,
                }
            }
            (held.len(), failure)
        },
        None,
        |run| run(),
    );

    assert_eq!(delivered, 2, "the ring holds exactly two buffers");
    assert!(
        matches!(failure, Some(CompletionError::Other(code)) if code == -libc::ENOBUFS),
        "the third read should have run the ring dry, got {failure:?}",
    );
}

#[test]
fn a_timed_out_read_leaves_its_data_for_the_next_one() {
    let (fake, handle) = simulated(42);
    let socket = simulated_socket().expect("a socket");

    // Arrives long after the timeout below, so the first read never sees it.
    handle.on_read(
        socket.fd(),
        ReadEvent::data(b"hello").after(Duration::from_secs(10)),
    );

    let got = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            // The timeout wins, and dropping the losing read cancels it.
            let raced = select(
                socket_recv(&socket, vec![0u8; 32]),
                Sleep::during(Duration::from_millis(100)),
            )
            .await;
            assert!(
                matches!(raced, SelectResult::Second(_)),
                "the read should have timed out",
            );

            // A kernel would still have those bytes buffered, so the next read
            // collects them. The cancel must not have eaten the delivery.
            let (result, buf) = socket_recv(&socket, vec![0u8; 32]).await;
            let n = result.expect("the second read succeeded");
            buf[..n].to_vec()
        },
        None,
        |run| run(),
    );

    assert_eq!(got, b"hello");
    assert_eq!(handle.scenario_remaining(), 0);
}
