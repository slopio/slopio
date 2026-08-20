//! The runtime, running for real against a simulated ring.
//!
//! Everything the other suite proves is about the simulator in isolation. These
//! run an actual future through `block_on`, so what is under test is the driver
//! — slab, wakers, the completion routing, the event loop — with the kernel
//! swapped out from under it.

use std::time::{Duration, Instant};

use runtime_core::deterministic_testing::{FakeRing, FakeRingHandle, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::recv::socket_recv;
use runtime_core::io_driver::operations::sockets::send::socket_send;
use runtime_core::runtime::{RuntimeInner, spawn, time::Sleep};

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
