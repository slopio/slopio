//! When a task's allocation is freed: only once its future is gone, its
//! handle is gone, it is no longer in the run queue, and no waker points at it.
//!
//! Each `yield_now` from the root future is one scheduler pass. The simulated
//! ring panics if the root yields while no task is queued, so every test
//! yields exactly as many times as the scenario needs.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p slopio-core --features simulation --test task_free
//! ```

mod common;

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use common::on_runtime;
use slopio_core::runtime::{spawn, yield_now};
use slopio_core::task::Handle;

/// Flips a shared flag when dropped, so a test can see the allocation go away.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

/// Wakes its own task, then completes in the same poll: the task finishes
/// while it is still in the run queue.
struct WakeAndFinish;

impl Future for WakeAndFinish {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        cx.waker().wake_by_ref();
        Poll::Ready(())
    }
}

#[test]
fn detached_task_finishing_while_queued_is_freed_at_the_next_pop() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        drop(spawn({
            let flag = DropFlag(dropped.clone());
            async move {
                WakeAndFinish.await;
                flag
            }
        }));
        // Finishes here, but the self-wake left it queued: not freed yet.
        yield_now().await;
        assert!(!dropped.get());
        // The stale entry is popped and the allocation released.
        yield_now().await;
        assert!(dropped.get());
    });
}

#[test]
fn root_collecting_a_queued_task_defers_the_free() {
    on_runtime(async {
        let handle = spawn(async {
            WakeAndFinish.await;
            7
        });
        // The root is polled between passes: it collects while the task is still queued.
        assert_eq!(handle.await, 7);
        // The stale entry must pop onto live memory, then free it.
        yield_now().await;
    });
}

#[test]
fn dropping_the_handle_after_completion_frees_the_task() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let handle = spawn({
            let flag = DropFlag(dropped.clone());
            async move { flag }
        });
        yield_now().await;
        assert!(!dropped.get(), "output is held for the handle");
        drop(handle);
        assert!(dropped.get(), "nothing will poll it again: freed on detach");
    });
}

/// Drops its own task's handle, then finishes, in a single poll. `Unpin`, so
/// the task's `&mut` over it is a real unique borrow: an `async` block would
/// make the task `!Unpin` and hide an overlapping one from Miri.
struct DropOwnHandle {
    slot: Rc<Cell<Option<Handle<()>>>>,
    flag: Option<DropFlag>,
}

impl Future for DropOwnHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        drop(self.slot.take());
        drop(self.flag.take());
        Poll::Ready(())
    }
}

#[test]
fn a_task_dropping_its_own_handle_mid_poll_is_freed_once_it_finishes() {
    on_runtime(async {
        let dropped = Rc::new(Cell::new(false));
        let slot = Rc::new(Cell::new(None));
        let handle = spawn(DropOwnHandle {
            slot: slot.clone(),
            flag: Some(DropFlag(dropped.clone())),
        });
        slot.set(Some(handle));
        yield_now().await;
        assert!(dropped.get());
    });
}
