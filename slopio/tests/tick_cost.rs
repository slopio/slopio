//! Cost of `TimeDriver::tick`, which the event loop calls once per drain pass.
//!
//! Run with `--release --ignored --nocapture`. Not an assertion — a number to
//! compare across a change.

use std::time::Instant;

use slopio::runtime::time::TimeDriver;

const ITERS: u32 = 20_000_000;

#[test]
#[ignore]
fn tick_cost() {
    let driver = TimeDriver::default();

    // Warm the vDSO pages and the branch predictor.
    for _ in 0..100_000 {
        driver.tick();
    }

    let started = Instant::now();
    for _ in 0..ITERS {
        driver.tick();
    }
    let elapsed = started.elapsed();

    println!(
        "tick: {:.2} ns/call ({elapsed:?} for {ITERS} calls)",
        elapsed.as_nanos() as f64 / f64::from(ITERS),
    );
}
