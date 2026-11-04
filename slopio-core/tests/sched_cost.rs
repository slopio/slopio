//! Cost of the scheduler's hot paths: spawn, poll, wake, free.
//!
//! Run with `--release --ignored --nocapture`. Not an assertion — a number to
//! compare across a change.

use std::time::Instant;

use slopio_core::channels::unbounded_channel;
use slopio_core::runtime::{main_thread, spawn, yield_now};

const TASKS: usize = 100_000;
const YIELDS: usize = 20;
const MESSAGES: usize = 2_000_000;

#[test]
#[ignore]
fn sched_cost() {
    main_thread(async {
        // Warm up.
        for _ in 0..1_000 {
            spawn(async { yield_now().await }).await;
        }

        // Spawn + poll + self-wake + free, in bulk.
        let started = Instant::now();
        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                spawn(async {
                    for _ in 0..YIELDS {
                        yield_now().await;
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.await;
        }
        let polls = (TASKS * (YIELDS + 1)) as f64;
        println!(
            "spawn+yield: {:.1} ns/poll ({:?} for {TASKS} tasks x {YIELDS} yields)",
            started.elapsed().as_nanos() as f64 / polls,
            started.elapsed(),
        );

        // Ping-pong through two channels: every message parks and wakes a task,
        // which is one waker clone and drop per hop.
        let (to_b, from_a) = unbounded_channel::<u32>();
        let (to_a, from_b) = unbounded_channel::<u32>();
        let started = Instant::now();
        let b = spawn(async move {
            while let Some(n) = from_a.recv().await {
                to_a.send(n);
            }
        });
        for n in 0..MESSAGES as u32 {
            to_b.send(n);
            assert_eq!(from_b.recv().await, Some(n));
        }
        drop(to_b);
        b.await;
        println!(
            "ping-pong: {:.1} ns/round trip ({:?} for {MESSAGES} messages)",
            started.elapsed().as_nanos() as f64 / MESSAGES as f64,
            started.elapsed(),
        );
    });
}
