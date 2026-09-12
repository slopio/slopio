//! When a task's allocation is freed: only once its future is gone, its
//! handle is gone, and it is no longer in the run queue.
//!
//! Each `yield_now` from the root future is one scheduler pass. The simulated
//! ring panics if the root yields while no task is queued, so every test
//! yields exactly as many times as the scenario needs.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p runtime-core --features simulation --test task_free
//! ```

mod common;

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use common::on_runtime;
use runtime_core::runtime::{spawn, yield_now};

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
