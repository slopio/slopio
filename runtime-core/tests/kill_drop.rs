//! What `Handle::kill` drops, and what that drop leaves behind.
//!
//! Both scenarios are the same shape: something woke the task, the future it
//! was awaiting still names a slot it once held, and the scheduler drops that
//! future without polling it again.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p runtime-core --features simulation --test kill_drop
//! ```

mod common;

use std::future::poll_fn;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use common::on_runtime;
use runtime_core::channels::unbounded_channel;
use runtime_core::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::managed_recv::socket_managed_recv;
use runtime_core::runtime::{
    RuntimeInner, spawn,
    time::{FIXED_5S, Sleep},
    yield_now,
};

runtime_core::define_buf_ring!(KillRing, bgid = 7, buffer_size = 64, ring_size = 2);

/// A provided-buffer recv whose completion landed, then was dropped by kill
/// before the task polled it: the buffer the CQE delivered must come back.
#[test]
fn kill_after_a_managed_recv_completed_gives_the_buffer_back() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let victim = simulated_socket().expect("a socket");
    let probe = simulated_socket().expect("a socket");

    handle.on_read(victim.fd(), ReadEvent::data(b"a"));
    handle.on_read(probe.fd(), ReadEvent::data(b"c"));
    handle.on_read(probe.fd(), ReadEvent::data(b"d"));

    let held = RuntimeInner::simulated(fake).block_on::<KillRing, _>(
        async move {
            let task = spawn(async move {
                let mut recv = socket_managed_recv::<KillRing>(&victim);
                // Submit the recv, then park elsewhere: the CQE wakes this
                // task, but the recv is never polled again before the kill.
                poll_fn(|cx| {
                    assert!(Pin::new(&mut recv).poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                Sleep::during(Duration::from_secs(10)).await;
                drop(recv);
            });
            // The read completes on the first wait; the task is woken and
            // re-polled into its sleep, holding a completed recv.
            Sleep::during(Duration::from_millis(1)).await;
            task.kill();
            Sleep::during(Duration::from_millis(1)).await;

            // Hold both buffers at once: a ring that lost one to the kill
            // answers the second read with ENOBUFS.
            let mut held = Vec::new();
            for _ in 0..2 {
                match socket_managed_recv::<KillRing>(&probe).await {
                    Ok(buffer) => held.push(buffer),
                    Err(error) => panic!("the ring ran dry: {error:?}"),
                }
            }
            held.len()
        },
        None,
        |run| run(),
    );

    assert_eq!(held, 2, "the ring should have both buffers back");
}

/// A wheel timer that fired, then was dropped by kill before the task polled
/// it: the fired slot must still be the sleep's own, and a sleep armed
/// afterwards must not collide with it.
#[test]
fn kill_after_a_wheel_timer_fired_frees_its_own_slot() {
    on_runtime(async {
        let (_send, recv) = unbounded_channel::<()>();
        let task = spawn(async move {
            let mut sleep = Sleep::during(Duration::from_millis(10));
            // Arm the timer, then park elsewhere: the wheel wakes this task,
            // but the sleep is never polled again before the kill.
            poll_fn(|cx| {
                assert!(Pin::new(&mut sleep).poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            recv.recv().await;
            drop(sleep);
        });
        // Long enough for the task's timer to fire first.
        Sleep::during(Duration::from_millis(30)).await;
        task.kill();
        yield_now().await;
        // The wheel is intact: arming and firing again works.
        Sleep::during(Duration::from_millis(10)).await;
    });
}

/// Same, on the fixed 5s store: firing pops the entry outright, so the sleep
/// dropped by kill holds a token the store no longer has. That token must be
/// rejected, not resolved against whatever was armed since.
#[test]
fn kill_after_a_fixed_5s_timer_fired_holds_a_stale_token() {
    on_runtime(async {
        let (_send, recv) = unbounded_channel::<()>();
        let task = spawn(async move {
            let mut sleep = Sleep::during(FIXED_5S);
            poll_fn(|cx| {
                assert!(Pin::new(&mut sleep).poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            recv.recv().await;
            drop(sleep);
        });
        Sleep::during(Duration::from_secs(6)).await;
        // Armed before the kill, so the stale token's slot has a new tenant
        // in the store's queues if anything were reused.
        let later = spawn(async { Sleep::during(FIXED_5S).await });
        yield_now().await;
        task.kill();
        yield_now().await;
        // The later sleep still fires, and only once.
        later.await;
    });
}

/// A receiver shared with the killed task keeps that task's waker after the
/// task is freed; the next send wakes freed memory.
///
/// Passes natively, since the freed header still reads as plausible bytes;
/// Miri names the use-after-free at `TypedTask::wake_by_ref`.
#[test]
#[ignore = "documents a known use-after-free; Miri fails it by design"]
fn kill_then_send_on_a_shared_receiver_wakes_a_freed_task() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<()>();
        let recv = Rc::new(recv);
        let handle = spawn({
            let recv = recv.clone();
            async move {
                recv.recv().await;
            }
        });
        // Task polled once: its waker is now parked in the channel.
        yield_now().await;
        handle.kill();
        // Reaped and freed; the channel still holds the waker. Only Miri can
        // see the send touch freed memory: natively it reads garbage quietly.
        yield_now().await;
        send.send(());
    });
}
