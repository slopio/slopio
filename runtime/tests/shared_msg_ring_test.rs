//! Sharing a single message allocation across the MSG_RING boundary.
//!
//! `send_arc_to_peer` transfers one strong reference rather than the whole
//! allocation, so the sender can keep reading the payload after it has crossed —
//! and can fan the *same* allocation out to several peers without re-allocating
//! per peer. Two properties are exercised here:
//!
//! - the receiver observes a non-unique `Arc` while the sender's clone is alive,
//!   and can still read the payload through it;
//! - the sender's clone is still readable after the crossing.
//!
//! Ordering is pinned by an ack: worker 0 holds its clone across the await, so
//! the strong count worker 1 sees is guaranteed to include it — no race.
//!
//! Every worker crosses the startup barrier via `publish_self`; workers with
//! id >= 3 return. Each `recv` is guarded with a generous timeout so a missing
//! wakeup fails loudly instead of hanging.

use std::sync::Arc;
use std::time::Duration;

use runtime::io_driver::Message;
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{recv_boxed_from_peer, send_arc_to_peer, send_boxed_to_peer};
use runtime::thread_per_core;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
struct Shared {
    n: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct Ack {
    /// Strong count the receiver observed, and whether it could take the payload.
    strong: usize,
    unique: bool,
}

/// Await a `Message<T>` or fail loudly on timeout.
///
/// # Safety
/// The next inbound message must be a `Message<T>` — guaranteed here by the
/// fixed protocol between the workers.
async unsafe fn recv_or_timeout<T: 'static>(who: &str) -> Arc<Message<T>> {
    match select(unsafe { recv_boxed_from_peer::<T>() }, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a message"),
    }
}

#[test]
fn sender_keeps_access_to_a_shared_message() {
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
                    let msg = Message::new(Shared { n: 42 });
                    send_arc_to_peer(1, Arc::clone(&msg));

                    // Held across the await, so worker 1's count includes it.
                    let ack = unsafe { recv_or_timeout::<Ack>("worker 0") }.await;
                    assert!(
                        ack.payload.strong >= 2,
                        "receiver should see the sender's clone, saw {}",
                        ack.payload.strong
                    );
                    assert!(!ack.payload.unique, "shared message must not be takeable");

                    // The whole point: still readable after crossing.
                    assert_eq!(msg.payload, Shared { n: 42 });
                }
                1 => {
                    let msg = unsafe { recv_or_timeout::<Shared>("worker 1") }.await;
                    assert_eq!(msg.payload, Shared { n: 42 });

                    let strong = Arc::strong_count(&msg);
                    let unique = Arc::into_inner(msg).is_some();
                    send_boxed_to_peer(0, Ack { strong, unique });
                }
                _ => {}
            }
        })
    });
}

#[test]
fn fan_out_shares_one_allocation_across_peers() {
    thread_per_core(move |ctx| {
        let thread_id = ctx.thread_id;
        let num_threads = ctx.num_threads;
        Box::pin(async move {
            if num_threads < 3 {
                eprintln!("skipping: need >= 3 cores, have {num_threads}");
                return;
            }
            match thread_id {
                0 => {
                    let msg = Message::new(Shared { n: 7 });
                    for peer in 1..=2 {
                        send_arc_to_peer(peer, Arc::clone(&msg));
                    }
                    for _ in 0..2 {
                        let ack = unsafe { recv_or_timeout::<Ack>("worker 0") }.await;
                        assert!(!ack.payload.unique, "fanned-out message must be shared");
                    }
                    assert_eq!(msg.payload, Shared { n: 7 });
                }
                1 | 2 => {
                    let msg = unsafe { recv_or_timeout::<Shared>("receiver") }.await;
                    assert_eq!(msg.payload, Shared { n: 7 });

                    let strong = Arc::strong_count(&msg);
                    let unique = Arc::into_inner(msg).is_some();
                    send_boxed_to_peer(0, Ack { strong, unique });
                }
                _ => {}
            }
        })
    });
}

#[test]
fn unshared_message_arrives_unique_and_is_takeable() {
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
                    // No clone retained, so the crossing is a plain move: the
                    // receiver's `Arc` is unique and gives up its payload.
                    send_boxed_to_peer(1, Shared { n: 1 });
                    let ack = unsafe { recv_or_timeout::<Ack>("worker 0") }.await;
                    assert_eq!(ack.payload.strong, 1);
                    assert!(ack.payload.unique, "unshared message should be takeable");
                }
                1 => {
                    let msg = unsafe { recv_or_timeout::<Shared>("worker 1") }.await;
                    let strong = Arc::strong_count(&msg);
                    let taken = Arc::into_inner(msg);
                    let unique = taken.is_some();
                    assert_eq!(taken.map(|m| m.payload), Some(Shared { n: 1 }));
                    send_boxed_to_peer(0, Ack { strong, unique });
                }
                _ => {}
            }
        })
    });
}
