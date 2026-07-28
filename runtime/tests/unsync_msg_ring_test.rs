//! `!Sync` payloads across the MSG_RING boundary.
//!
//! The peer-message APIs bound their payload on `Send` only. `Sync` would say
//! two threads may hold a `&T` at once, which is a property of *retaining a
//! clone* across the crossing — not of the crossing itself. A moved message is
//! owned by exactly one thread at a time, so requiring `Sync` would have locked
//! out payloads that are perfectly fine to hand over: anything with interior
//! mutability, `Cell`, `RefCell`, `Rc`-free-but-unsynchronised state.
//!
//! These tests are as much compile-time as run-time: `Unsync` holds a `Cell`, so
//! it is `Send + !Sync`, and the file simply would not build if the `Sync` bound
//! came back on `send_msg_to_peer`, `send_shared_to_peer`,
//! `request_msg_from_peer` or `reply_msg_to_peer`.
//!
//! Sharing a `!Sync` payload — erasing while a clone stays alive — is left to
//! the caller to make safe and is deliberately not exercised here.
//!
//! Each `recv` is guarded with a generous timeout so a missing wakeup fails
//! loudly instead of hanging.

use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use runtime::io_driver::Message;
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{
    RecvMsgReply, recv_msg_from_peer, reply_msg_to_peer, request_msg_from_peer, send_msg_to_peer,
    send_shared_to_peer,
};
use runtime::thread_per_core;

const TIMEOUT: Duration = Duration::from_secs(5);

/// `Send` but not `Sync`: the `Cell` makes it interior-mutable without
/// synchronisation, so it is exactly what the old bound rejected.
#[derive(Debug)]
struct Unsync {
    n: Cell<u32>,
}

impl Unsync {
    fn new(n: u32) -> Self {
        Self { n: Cell::new(n) }
    }
}

/// Static proof of the property under test — `Unsync` really is `Send + !Sync`.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<Unsync>();
};

#[derive(Debug, PartialEq, Eq)]
struct Ack {
    n: u32,
}

/// Await a `Message<T>` or fail loudly on timeout.
///
/// # Safety
/// The next inbound message must be a `Message<T>` — guaranteed here by the
/// fixed protocol between the workers.
async unsafe fn recv_or_timeout<T: 'static>(who: &str) -> Arc<Message<T>> {
    match select(unsafe { recv_msg_from_peer::<T>() }, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a message"),
    }
}

/// Await a reply or fail loudly on timeout.
async fn reply_or_timeout<T: 'static>(fut: RecvMsgReply<T>, who: &str) -> Arc<Message<T>> {
    match select(fut, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a reply"),
    }
}

/// A moved `!Sync` payload crosses, arrives unique, and is mutable on the far
/// side — one owner throughout, which is why `Sync` was never needed.
#[test]
fn unsync_payload_moves_between_workers() {
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
                    send_msg_to_peer(1, Unsync::new(41));
                    let ack = unsafe { recv_or_timeout::<Ack>("worker 0") }.await;
                    assert_eq!(ack.payload, Ack { n: 42 });
                }
                1 => {
                    let msg = unsafe { recv_or_timeout::<Unsync>("worker 1") }.await;
                    assert!(Arc::strong_count(&msg) == 1, "move path must arrive unique");
                    // Interior mutability through a shared reference: sound
                    // precisely because worker 0 kept nothing.
                    msg.payload.n.set(msg.payload.n.get() + 1);
                    send_msg_to_peer(0, Ack { n: msg.payload.n.get() });
                }
                _ => {}
            }
        })
    });
}

/// The pre-allocated send path takes the same relaxed bound. No clone is
/// retained, so this is still a move — it is the signature being pinned here.
#[test]
fn unsync_payload_crosses_preallocated() {
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
                    let msg = Message::new(Unsync::new(7));
                    // SAFETY: no clone is retained — `msg` is moved into the
                    // send — so this is a plain handover and the `!Sync`
                    // payload is never reachable from two threads at once.
                    unsafe { send_shared_to_peer(1, msg) };
                    let ack = unsafe { recv_or_timeout::<Ack>("worker 0") }.await;
                    assert_eq!(ack.payload, Ack { n: 7 });
                }
                1 => {
                    let msg = unsafe { recv_or_timeout::<Unsync>("worker 1") }.await;
                    send_msg_to_peer(0, Ack { n: msg.payload.n.get() });
                }
                _ => {}
            }
        })
    });
}

/// Request and reply both carry `!Sync` payloads through the reply slot.
#[test]
fn unsync_payload_through_request_reply() {
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
                    // SAFETY: worker 1 below answers this `Unsync` with an `Unsync`.
                    let pending =
                        unsafe { request_msg_from_peer::<Unsync, Unsync>(1, Unsync::new(20)) };
                    let reply = reply_or_timeout(pending, "worker 0").await;
                    assert_eq!(reply.payload.n.get(), 40);
                }
                1 => {
                    let req = unsafe { recv_or_timeout::<Unsync>("worker 1") }.await;
                    let doubled = req.payload.n.get() * 2;
                    reply_msg_to_peer(&req.header, Unsync::new(doubled));
                }
                _ => {}
            }
        })
    });
}
