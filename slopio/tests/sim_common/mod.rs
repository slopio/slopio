//! Scaffolding shared by the channel suites.

use std::future::Future;

use slopio::deterministic_testing::FakeRing;
use slopio::runtime::RuntimeInner;

/// Run a future to completion on a runtime whose ring is simulated.
///
/// Stands in for `main_thread`, which builds a real io_uring and so can never
/// run under Miri. Nothing in these suites performs IO — the ring is here only
/// because a runtime has one — so swapping the kernel out changes nothing they
/// observe, and buys the whole file a Miri run.
pub fn on_runtime<F: Future>(future: F) -> F::Output {
    let (fake, _handle) = FakeRing::builder().virtual_time().build();
    RuntimeInner::simulated(fake).block_on::<(), _>(future, None, |run| run())
}

/// Shrink a loop count under Miri.
///
/// Miri interprets rather than executes, so the ten-thousand-message tests
/// would take minutes there instead of milliseconds. The code paths are
/// identical either way and only the number of trips through them changes;
/// a native `cargo test` still runs the full counts.
pub const fn scaled(n: usize) -> usize {
    if cfg!(miri) && n > 256 { 256 } else { n }
}
