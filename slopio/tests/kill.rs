//! `Handle::kill`, driven by a real runtime over a simulated ring.
//!
//! Each `yield_now` from the root future is one scheduler pass. The simulated
//! ring panics if the root yields while no task is queued, so every test
//! yields exactly as many times as the scenario needs.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p slopio --features simulation --test kill
//! ```

#[path = "sim_common/mod.rs"]
mod common;

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use common::on_runtime;
use slopio::Handle;
use slopio::channels::unbounded_channel;
use slopio::runtime::{spawn, yield_now};

/// Flips a shared flag when dropped, so a test can see the future go away.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn kill_drops_a_running_task() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let polls = Rc::new(Cell::new(0));
        let handle = spawn({
            let flag = DropFlag(dropped.clone());
            let polls = polls.clone();
            async move {
                let _flag = flag;
                loop {
                    polls.set(polls.get() + 1);
                    yield_now().await;
                }
            }
        });
        yield_now().await;
        assert_eq!(polls.get(), 1);
        // The task re-queued itself; kill must not queue it twice.
        handle.kill();
        assert!(
            !dropped.get(),
            "drop happens on the scheduler pass, not in kill"
        );
        yield_now().await;
        assert!(dropped.get());
        // A killed task is never polled again.
        assert_eq!(polls.get(), 1);
    });
}

#[test]
fn kill_drops_a_task_nothing_else_would_wake() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let (send, recv) = unbounded_channel::<()>();
        let handle = spawn({
            let flag = DropFlag(dropped.clone());
            async move {
                let _flag = flag;
                recv.recv().await;
            }
        });
        yield_now().await;
        assert!(!dropped.get());
        handle.kill();
        yield_now().await;
        assert!(dropped.get());
        // The receiver is gone; sending must be a no-op, not a wake of freed memory.
        send.send(());
    });
}

#[test]
fn kill_drops_an_uncollected_output() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let handle = spawn({
            let flag = DropFlag(dropped.clone());
            async move { flag }
        });
        yield_now().await;
        assert!(!dropped.get(), "output is held for the handle");
        handle.kill();
        yield_now().await;
        assert!(dropped.get());
    });
}

#[test]
fn kill_from_inside_the_task() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let (send, recv) = unbounded_channel::<Handle<()>>();
        let handle = spawn({
            let flag = DropFlag(dropped.clone());
            async move {
                let _flag = flag;
                let me = recv.recv().await.unwrap();
                me.kill();
                loop {
                    yield_now().await;
                }
            }
        });
        send.send(handle);
        // First pass: the task kills itself mid-poll and keeps running to its yield.
        yield_now().await;
        assert!(!dropped.get());
        // Second pass: the scheduler reaps it instead of polling.
        yield_now().await;
        assert!(dropped.get());
    });
}

/// Wakes the task that dropped it: parks with the task's waker, and its drop
/// glue fires that waker while the task is being killed.
struct WakeOnDrop(Rc<RefCell<Option<Waker>>>);

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        if let Some(waker) = self.0.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[test]
fn kill_survives_a_self_wake_during_drop() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let handle = spawn({
            let flag = DropFlag(dropped.clone());
            async move {
                let _flag = flag;
                let slot = Rc::new(RefCell::new(None));
                let _guard = WakeOnDrop(slot.clone());
                poll_fn(|cx| {
                    *slot.borrow_mut() = Some(cx.waker().clone());
                    Poll::<()>::Pending
                })
                .await;
            }
        });
        yield_now().await;
        handle.kill();
        // Drop glue re-queues the task, so the free is deferred to a second pass.
        yield_now().await;
        assert!(dropped.get());
        yield_now().await;
    });
}
