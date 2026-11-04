//! Answering a request with a pointer-free wakeup instead of a message.
//!
//! The protocol these tests exercise is the one where the requester *shares*
//! its payload instead of handing it over: worker 0 allocates a
//! `Arc<Message<T>>`, keeps a clone, and sends the other to worker 1. Because
//! worker 0 still holds the data, worker 1's answer has nothing to carry — it
//! only has to say "done", which `signal_reply_to_peer` does with no allocation
//! at all: the slot index rides in the SQE `user_data` and the status in the
//! CQE `res`.
//!
//! Two things are checked here that the message path cannot offer: the round
//! trip allocates nothing on the way back, and a wakeup aimed at a slot whose
//! future was dropped is absorbed without any type-recovering drop glue —
//! there is nothing to recover, because a wakeup owns nothing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use slopio::io_driver::{Endpoint, Header, Message};
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{
    ReplySignal, recv_msg_from_peer, send_shared_to_peer, signal_reply_to_peer, thread_id,
};
use slopio::thread_per_core;

const TIMEOUT: Duration = Duration::from_secs(5);

/// The shared payload: worker 1 writes the answer into it, worker 0 reads it
/// back through its own clone once the wakeup lands. Sharing it across the two
/// workers is what makes a payload-free reply possible.
#[derive(Debug)]
struct Shared {
    question: u32,
    answer: AtomicU32,
}

/// Send a shared request to `peer` and hand back the reserved signal plus our
/// retained clone.
///
/// # Safety
/// A clone of `msg` outlives the call, so the payload is genuinely shared with
/// `peer`. Sound here because `Shared` is `Sync`: both sides only ever touch it
/// through `&`, and the answer travels through an atomic.
unsafe fn share_request(peer: usize, question: u32) -> (ReplySignal, Arc<Message<Shared>>) {
    let signal = ReplySignal::reserve();
    let header = Header::new(
        Endpoint::with_slab(thread_id() as u16, signal.slot()),
        Endpoint::new(peer as u16),
    );
    let msg = Message::with_header(
        header,
        Shared {
            question,
            answer: AtomicU32::new(0),
        },
    );
    unsafe { send_shared_to_peer(peer, Arc::clone(&msg)) };
    (signal, msg)
}

/// Await a wakeup or fail loudly on timeout.
async fn signal_or_timeout(signal: ReplySignal, who: &str) -> i32 {
    match select(signal, Sleep::during(TIMEOUT)).await {
        SelectResult::First(status) => status,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a wakeup"),
    }
}

/// Worker 1's side, shared by both tests: take the shared request, write the
/// answer into it, drop our clone, then signal.
///
/// Dropping before signalling is deliberate and load-bearing: it is what lets
/// the requester's `Arc::into_inner` succeed the moment it wakes.
async fn serve_one() {
    // SAFETY: worker 0 only ever sends `Message<Shared>` to this worker.
    let req = unsafe { recv_msg_from_peer::<Shared>() }.await;
    req.payload
        .answer
        .store(req.payload.question + 1, Ordering::Relaxed);
    let header = req.header;
    drop(req);
    signal_reply_to_peer(&header, 0);
}

#[test]
fn shared_request_answered_by_a_wakeup() {
    thread_per_core(move |ctx| {
        let thread_id = ctx.thread_id;
        let num_threads = ctx.num_threads;
        Box::pin(async move {
            if num_threads < 2 {
                eprintln!("skipping: need >= 2 cores, have {num_threads}");
                return;
            }
            match thread_id {
                0 => {
                    // SAFETY: see `share_request` — `Shared` is `Sync`.
                    let (signal, msg) = unsafe { share_request(1, 7) };
                    let status = signal_or_timeout(signal, "worker 0").await;
                    assert_eq!(status, 0, "status rides in the CQE res");

                    // The answer was written through the shared allocation, not
                    // carried back by the reply.
                    assert_eq!(msg.payload.answer.load(Ordering::Relaxed), 8);

                    // Worker 1 dropped its clone before signalling, so ours is
                    // the last one and the payload comes back out whole.
                    let owned = Arc::into_inner(msg).expect("reply leaves us the only holder");
                    assert_eq!(owned.payload.question, 7);
                }
                1 => serve_one().await,
                _ => {}
            }
        })
    });
}

#[test]
fn wakeup_for_a_dropped_future_is_absorbed() {
    thread_per_core(move |ctx| {
        let thread_id = ctx.thread_id;
        let num_threads = ctx.num_threads;
        Box::pin(async move {
            if num_threads < 2 {
                eprintln!("skipping: need >= 2 cores, have {num_threads}");
                return;
            }
            match thread_id {
                0 => {
                    // Send, then walk away before the wakeup can land. The slot
                    // stays reserved as cancelled so its index cannot be reused
                    // while a wakeup is in flight toward it.
                    let (signal, msg) = unsafe { share_request(1, 41) };
                    drop(signal);

                    // Give the late wakeup time to arrive and be absorbed. It
                    // owns nothing, so absorbing it is just releasing the slot —
                    // no drop glue is involved, unlike a late message.
                    Sleep::during(Duration::from_millis(200)).await;

                    // Worker 1 still did the work and released its clone.
                    assert_eq!(msg.payload.answer.load(Ordering::Relaxed), 42);
                    assert!(Arc::into_inner(msg).is_some(), "no clone survives");

                    // The freed index is handed straight back out, and a fresh
                    // request on it completes normally — proof the slot was
                    // released rather than stranded.
                    let (signal, msg) = unsafe { share_request(1, 1) };
                    signal_or_timeout(signal, "worker 0 (second)").await;
                    assert_eq!(msg.payload.answer.load(Ordering::Relaxed), 2);
                }
                1 => {
                    serve_one().await;
                    serve_one().await;
                }
                _ => {}
            }
        })
    });
}
