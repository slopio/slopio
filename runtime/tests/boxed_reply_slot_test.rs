//! Request/reply of heap-allocated messages between two workers, routed by a
//! reply slot rather than the generic inbox.
//!
//! Worker 0 issues a request to worker 1 via `request_boxed_from_peer`, which
//! reserves a reply slot and stamps its index into `sender.slab`. Worker 1
//! receives the request on its generic inbox (the request carries no receiver
//! slab), then `reply_boxed_to_peer` swaps the header so the reply's
//! `receiver.slab` names worker 0's slot. On arrival, the driver routes the
//! reply straight into that slot and wakes the awaiting future — the reply
//! never touches worker 0's generic inbox.

use std::time::Duration;

use runtime::io_driver::Message;
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{
    RecvBoxedReply, recv_boxed_from_peer, reply_boxed_to_peer, request_boxed_from_peer,
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
async fn reply_or_timeout<T: 'static>(fut: RecvBoxedReply<T>, who: &str) -> Box<Message<T>> {
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
async unsafe fn recv_or_timeout<T: 'static>(who: &str) -> Box<Message<T>> {
    match select(unsafe { recv_boxed_from_peer::<T>() }, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a request"),
    }
}

#[test]
fn boxed_request_reply_between_two_workers() {
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
                    let pending = request_boxed_from_peer::<Req, Rep>(1, Req { n: 7 });
                    let reply = reply_or_timeout(pending, "worker 0").await;
                    assert_eq!(reply.payload, Rep { n: 8 });
                }
                1 => {
                    let req = unsafe { recv_or_timeout::<Req>("worker 1") }.await;
                    assert_eq!(req.payload, Req { n: 7 });
                    reply_boxed_to_peer(
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
