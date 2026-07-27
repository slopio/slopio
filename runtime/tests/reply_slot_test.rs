//! Request/reply of heap-allocated messages between two workers, routed by a
//! reply slot rather than the generic inbox.
//!
//! Worker 0 issues a request to worker 1 via `request_msg_from_peer`, which
//! reserves a reply slot and stamps its index into `sender.slab`. Worker 1
//! receives the request on its generic inbox (the request carries no receiver
//! slab), then `reply_msg_to_peer` swaps the header so the reply's
//! `receiver.slab` names worker 0's slot. On arrival, the driver routes the
//! reply straight into that slot and wakes the awaiting future — the reply
//! never touches worker 0's generic inbox.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use runtime::io_driver::Message;
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{
    RecvMsgReply, recv_msg_from_peer, reply_msg_to_peer, request_msg_from_peer,
};
use runtime::thread_per_core;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
struct Req {
    n: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct Rep {
    n: u32,
}

/// Await a reply or fail loudly on timeout.
async fn reply_or_timeout<T: 'static>(fut: RecvMsgReply<T>, who: &str) -> Arc<Message<T>> {
    match select(fut, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a reply"),
    }
}

/// Await a request or fail loudly on timeout.
///
/// # Safety
/// The next inbound message must be a `Message<T>` — guaranteed here by the
/// fixed request/reply protocol between the two workers.
async unsafe fn recv_or_timeout<T: 'static>(who: &str) -> Arc<Message<T>> {
    match select(unsafe { recv_msg_from_peer::<T>() }, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a request"),
    }
}

#[test]
fn request_reply_between_two_workers() {
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
                    // SAFETY: worker 1 below answers this `Req` with a `Rep`.
                    let pending = unsafe { request_msg_from_peer::<Req, Rep>(1, Req { n: 7 }) };
                    let reply = reply_or_timeout(pending, "worker 0").await;
                    assert_eq!(reply.payload, Rep { n: 8 });
                }
                1 => {
                    let req = unsafe { recv_or_timeout::<Req>("worker 1") }.await;
                    assert_eq!(req.payload, Req { n: 7 });
                    reply_msg_to_peer(
                        &req.header,
                        Rep {
                            n: req.payload.n + 1,
                        },
                    );
                }
                _ => {}
            }
        })
    });
}

/// Counts drops of the reply destined for the *cancelled* request, so the test
/// can assert that late reply was actually freed (not leaked into the inbox).
static CANCELLED_REPLY_DROPPED: AtomicUsize = AtomicUsize::new(0);

/// A reply whose destructor records when the cancelled request's reply is freed.
#[derive(Debug)]
struct DropRep {
    n: u32,
    /// Set only on the reply to the request whose future was dropped.
    is_cancelled_target: bool,
}

impl Drop for DropRep {
    fn drop(&mut self) {
        if self.is_cancelled_target {
            CANCELLED_REPLY_DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Regression: dropping the reply future before its reply lands must mark the
/// slot `Cancelled` (not remove it), so the late reply is freed on arrival —
/// never leaked into the generic inbox, never mis-delivered into a reused slot.
///
/// Worker 0 issues R1 and immediately cancels it, then issues R2 (which forces a
/// *different* slot, since R1's is still held as `Cancelled`) and awaits it.
/// Worker 1 receives both, then replies to R1 first (the late reply into the
/// cancelled slot) and to R2 second. The proof is twofold:
///   - R2's reply carries R2's value → R1's late reply did not mis-route into
///     R2's slot;
///   - the cancelled reply's destructor ran exactly once → it was freed by the
///     `Cancelled` slot's glue, not left sitting in the inbox.
#[test]
fn cancelled_reply_future_frees_the_late_reply() {
    CANCELLED_REPLY_DROPPED.store(0, Ordering::SeqCst);
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
                    // R1: reserve a slot, send it, then drop the future →
                    // slot becomes `Cancelled` while the request is in flight.
                    // SAFETY: worker 1 answers every `Req` with a `DropRep`.
                    let f1 = unsafe { request_msg_from_peer::<Req, DropRep>(1, Req { n: 1 }) };
                    drop(f1);

                    // R2: its slot must differ from R1's (still occupied), and
                    // its reply must come back intact.
                    // SAFETY: as above.
                    let f2 = unsafe { request_msg_from_peer::<Req, DropRep>(1, Req { n: 2 }) };
                    let reply2 = reply_or_timeout(f2, "worker 0").await;
                    assert_eq!(
                        reply2.payload.n, 3,
                        "R2's reply was corrupted — the cancelled request's late \
                         reply mis-routed into R2's slot"
                    );

                    // Let R1's late reply drain into the cancelled slot and free.
                    Sleep::during(Duration::from_millis(50)).await;
                    assert_eq!(
                        CANCELLED_REPLY_DROPPED.load(Ordering::SeqCst),
                        1,
                        "the cancelled request's late reply was not freed — it \
                         leaked into the inbox instead of the Cancelled slot"
                    );
                }
                1 => {
                    // Receive both requests before replying, so the reply to the
                    // cancelled R1 is guaranteed to arrive after worker 0 has
                    // marked its slot `Cancelled`.
                    let req1 = unsafe { recv_or_timeout::<Req>("worker 1") }.await;
                    let req2 = unsafe { recv_or_timeout::<Req>("worker 1") }.await;
                    assert_eq!(req1.payload, Req { n: 1 });
                    assert_eq!(req2.payload, Req { n: 2 });

                    reply_msg_to_peer(
                        &req1.header,
                        DropRep {
                            n: 100,
                            is_cancelled_target: true,
                        },
                    );
                    reply_msg_to_peer(
                        &req2.header,
                        DropRep {
                            n: req2.payload.n + 1,
                            is_cancelled_target: false,
                        },
                    );
                }
                _ => {}
            }
        })
    });
}
