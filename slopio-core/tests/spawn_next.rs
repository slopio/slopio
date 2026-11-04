//! `spawn_next`, driven by a real runtime over a simulated ring.
//!
//! Each `yield_now` from the root future is one scheduler pass; a pass polls
//! only as many tasks as were queued when it started.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p slopio-core --features simulation --test spawn_next
//! ```

mod common;

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;

use common::on_runtime;
use slopio_core::runtime::{MAX_FRONT_CHAIN, spawn, spawn_next, yield_now};

type Log = Rc<RefCell<Vec<&'static str>>>;

fn record(log: &Log, name: &'static str) -> impl Future<Output = ()> + 'static {
    let log = log.clone();
    async move { log.borrow_mut().push(name) }
}

#[test]
fn spawn_next_runs_before_already_queued_tasks() {
    on_runtime(async {
        let log = Log::default();
        spawn(record(&log, "a"));
        spawn_next(record(&log, "b"));
        yield_now().await;
        assert_eq!(*log.borrow(), ["b", "a"]);
    });
}

#[test]
fn spawn_next_from_a_task_runs_right_after_it() {
    on_runtime(async {
        let log = Log::default();
        spawn({
            let log = log.clone();
            async move {
                log.borrow_mut().push("a");
                spawn_next(record(&log, "c"));
            }
        });
        spawn(record(&log, "b"));
        yield_now().await;
        // The pass budget was two: "c" took the slot, "b" moves to the next pass.
        assert_eq!(*log.borrow(), ["a", "c"]);
        yield_now().await;
        assert_eq!(*log.borrow(), ["a", "c", "b"]);
    });
}

#[test]
fn spawn_next_is_last_in_first_out() {
    on_runtime(async {
        let log = Log::default();
        spawn(record(&log, "a"));
        spawn_next(record(&log, "c1"));
        spawn_next(record(&log, "c2"));
        yield_now().await;
        assert_eq!(*log.borrow(), ["c2", "c1", "a"]);
    });
}

/// Logs `c{n}` and head-spawns `c{n+1}`, up to `last`.
fn link(log: Log, n: usize, last: usize) -> Pin<Box<dyn Future<Output = ()>>> {
    const NAMES: [&str; 6] = ["c0", "c1", "c2", "c3", "c4", "c5"];
    Box::pin(async move {
        log.borrow_mut().push(NAMES[n]);
        if n < last {
            spawn_next(link(log.clone(), n + 1, last));
        }
    })
}

#[test]
fn a_spawn_next_chain_yields_to_the_queue_past_the_cap() {
    assert_eq!(MAX_FRONT_CHAIN, 3);
    on_runtime(async {
        let log = Log::default();
        spawn(link(log.clone(), 0, 5));
        spawn(record(&log, "a"));
        yield_now().await;
        assert_eq!(*log.borrow(), ["c0", "c1"]);
        yield_now().await;
        // c3 is the third head-spawned task in a row: c4 goes behind "a".
        assert_eq!(*log.borrow(), ["c0", "c1", "c2", "c3"]);
        yield_now().await;
        // "a" reset the chain, so c4 head-spawns c5 again.
        assert_eq!(*log.borrow(), ["c0", "c1", "c2", "c3", "a", "c4"]);
        yield_now().await;
        assert_eq!(*log.borrow(), ["c0", "c1", "c2", "c3", "a", "c4", "c5"]);
    });
}
