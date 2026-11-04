//! TCP server whose workers share a counter owned by a dedicated thread.
//!
//! The counter thread runs its own SQPOLL ring, on a CPU of its own, its kernel
//! SQ thread on another; the per-core workers take the CPUs left. Each
//! connection asks the counter thread for a number over MSG_RING and gets the
//! reply back, so the state lives on one thread and needs no lock.
//!
//! cargo run --release -p slopio --example topology -- 8080
//! nc 127.0.0.1 8080

use std::time::Duration;

use slopio::net::{MultiAccept, send_all};
use slopio::runtime::{recv_msg_from_peer, reply_msg_to_peer, request_msg_from_peer};
use slopio::streams::StreamExt;
use slopio::{RingOptions, Threads, Topology, spawn};

/// A worker asking for the next visitor number.
struct Visit;

struct VisitorNumber(u64);

fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .expect("usage: topology <port>");

    let mut topology = Topology::new();

    let counter = topology.group::<(), _, _>(
        Threads::Auto(1),
        RingOptions {
            sqpoll: true,
            defer_taskrun: false,
            coop_taskrun: false,
            sqpoll_idle: Duration::from_millis(50),
            ..Default::default()
        },
        |ctx| async move {
            eprintln!("counter on peer {}", ctx.thread_id);
            let mut visits = 0;
            loop {
                // SAFETY: workers only ever send this thread a `Visit`.
                let visit = unsafe { recv_msg_from_peer::<Visit>() }.await;
                visits += 1;
                reply_msg_to_peer(&visit.header, VisitorNumber(visits));
            }
        },
    );

    let counter = counter.start;
    topology.per_core::<(), _, _>(RingOptions::default(), move |ctx| async move {
        // One listener per worker; SO_REUSEPORT spreads connections across them.
        let mut accept = MultiAccept::new(port).unwrap();
        eprintln!("worker {} listening on {port}", ctx.thread_id);
        while let Some(socket) = accept.next().await {
            let Ok(socket) = socket else { continue };
            spawn(async move {
                // SAFETY: the counter answers a `Visit` with a `VisitorNumber`.
                let number =
                    unsafe { request_msg_from_peer::<Visit, VisitorNumber>(counter, Visit) }.await;
                let line = format!("hello, visitor #{}\n", number.payload.0);
                let _ = send_all(&socket, line.into_bytes()).await;
            });
        }
    });

    topology.run();
}
