//! Ping-pong between two workers over MSG_RING.
//!
//! Worker 0 sends `(tag=0, data=42)` to worker 1; worker 1 echoes back
//! `(tag=1, data=43)`. Every worker crosses the startup barrier via
//! `publish_self`, so workers with id >= 2 simply return. Each side guards
//! its `recv_from_peer` with a generous timeout so a missing wakeup fails
//! loudly instead of hanging the suite.

use std::time::Duration;

use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{recv_from_peer, send_to_peer};
use runtime::thread_per_core;

const TIMEOUT: Duration = Duration::from_secs(5);

async fn recv_or_timeout(who: &str) -> runtime::io_driver::PeerMessage {
    match select(recv_from_peer(), Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a peer message"),
    }
}

#[test]
fn ping_pong_between_two_workers() {
    // `thread_per_core` blocks until every worker future completes; a panic
    // inside any worker propagates here through its `join().unwrap()`.
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
                    send_to_peer(1, 42, thread_id as u64);
                    let reply = recv_or_timeout("worker 0").await;
                    assert_eq!(reply.tag, 1, "reply should come from worker 1");
                    assert_eq!(reply.data, 43, "worker 1 should have echoed data + 1");
                }
                1 => {
                    let msg = recv_or_timeout("worker 1").await;
                    assert_eq!(msg.tag, 0, "ping should come from worker 0");
                    assert_eq!(msg.data, 42);
                    send_to_peer(0, msg.data + 1, thread_id as u64);
                }
                _ => {}
            }
        })
    });
}
