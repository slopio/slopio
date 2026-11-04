//! The root future of `block_on` wakes through its own waker. A root that
//! wakes itself mid-poll, with no task ready and no IO in flight, must be
//! polled again rather than put the thread to sleep: the simulated ring
//! panics on a runtime that would never wake up.

mod common;

use std::future::poll_fn;
use std::task::Poll;

use common::on_runtime;
use slopio_core::runtime::yield_now;

#[test]
fn a_root_yield_with_nothing_else_to_run_returns() {
    on_runtime(async {
        yield_now().await;
        yield_now().await;
    });
}

#[test]
fn a_root_that_wakes_itself_is_polled_again() {
    let mut polls = 0;
    on_runtime(poll_fn(|cx| {
        polls += 1;
        if polls < 3 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }));
    assert_eq!(polls, 3);
}
