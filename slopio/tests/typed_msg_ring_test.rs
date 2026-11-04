//! Ping-pong of heap-allocated messages between two workers.
//!
//! Worker 0 sends a heap-allocated `Ping` to worker 1; worker 1 receives it *as* a
//! `Ping` (it knows the protocol), replies with a `Pong`, and worker 0
//! receives it *as* a `Pong`. This exercises the full round-trip: allocate on
//! one worker, erase to a thin pointer, cross MSG_RING, and cast straight back
//! on the other side — no runtime type tag, the receiver knows its type.
//!
//! Every worker crosses the startup barrier via `publish_self`; workers with
//! id >= 2 return. Each `recv` is guarded with a generous timeout so a missing
//! wakeup fails loudly instead of hanging.

use std::sync::Arc;
use std::time::Duration;

use slopio::io_driver::Message;
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_msg_from_peer, send_msg_to_peer};
use slopio::thread_per_core;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
struct Ping {
    n: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct Pong {
    n: u32,
}

/// Await a `Message<T>` or fail loudly on timeout.
///
/// # Safety
/// The next inbound message must be a `Message<T>` — guaranteed here by the
/// fixed ping/pong protocol between the two workers.
async unsafe fn recv_or_timeout<T: 'static>(who: &str) -> Arc<Message<T>> {
    match select(unsafe { recv_msg_from_peer::<T>() }, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a message"),
    }
}

#[test]
fn typed_ping_pong_between_two_workers() {
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
                    send_msg_to_peer(1, Ping { n: 7 });
                    let reply = unsafe { recv_or_timeout::<Pong>("worker 0") }.await;
                    assert_eq!(reply.payload, Pong { n: 8 });
                }
                1 => {
                    let ping = unsafe { recv_or_timeout::<Ping>("worker 1") }.await;
                    assert_eq!(ping.payload, Ping { n: 7 });
                    send_msg_to_peer(
                        0,
                        Pong {
                            n: ping.payload.n + 1,
                        },
                    );
                }
                _ => {}
            }
        })
    });
}
