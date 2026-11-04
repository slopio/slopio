use std::time::{Duration, Instant};

use slopio::main_thread;
use slopio::runtime::spawn;
use slopio::runtime::time::Sleep;

/// 5s is the one duration routed to a dedicated fixed-duration store instead
/// of the wheel, so it arms, fires and cancels through a different path.
#[test]
fn test_sleep_five_seconds_uses_the_fixed_store() {
    let start = Instant::now();
    main_thread(async {
        Sleep::during(Duration::from_secs(5)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(4_990),
        "Sleep finished too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(5_400),
        "Sleep took too long: {elapsed:?}"
    );
}

/// The fixed store's queues are FIFO, which only holds because every timer in
/// it runs for the same duration. Two armed at different ticks must come back
/// in the order they went in, spaced by the gap between their arms.
#[test]
fn test_concurrent_five_second_sleeps() {
    main_thread(async {
        let first = spawn(async { Instant::now() });
        let a = spawn(async {
            Sleep::during(Duration::from_secs(5)).await;
            Instant::now()
        });
        first.await;
        Sleep::during(Duration::from_millis(200)).await;
        let b = spawn(async {
            Sleep::during(Duration::from_secs(5)).await;
            Instant::now()
        });

        let (ta, tb) = (a.await, b.await);
        assert!(ta < tb, "the one armed first must fire first");
        let gap = tb - ta;
        assert!(
            gap >= Duration::from_millis(180) && gap < Duration::from_millis(320),
            "expected the 200ms arming gap to survive, got {gap:?}"
        );
    });
}

/// Dropping a 5s sleep must release its entry in the fixed store rather than
/// leave a waker behind that fires into a dead task.
#[test]
fn test_five_second_sleep_drop_cancellation() {
    let start = Instant::now();
    main_thread(async {
        let _slow = Sleep::during(Duration::from_secs(5));
        Sleep::during(Duration::from_millis(50)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "the dropped 5s sleep held the loop open: {elapsed:?}"
    );
}

#[test]
fn test_sleep_basic() {
    let start = Instant::now();
    main_thread(async {
        Sleep::during(Duration::from_millis(20)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(20),
        "Sleep finished too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(250),
        "Sleep took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_sleep_short_duration() {
    let start = Instant::now();
    main_thread(async {
        let _ = spawn(async {
            Sleep::during(Duration::from_millis(1)).await;
            println!("done");
        })
        .await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(10),
        "Sleep finished too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "Sleep took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_sleep_accuracy_1_second() {
    let start = Instant::now();
    main_thread(async {
        Sleep::during(Duration::from_secs(1)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(999),
        "Sleep finished too early: {:?}",
        elapsed
    );
}

/// Past a second the wheel parks the timer at its seconds level, so the sleep
/// only lands on time if the loop wakes for the cascade and the cascade carries
/// the sub-second remainder down with it.
#[test]
fn test_sleep_accuracy_past_the_ms_level() {
    let start = Instant::now();
    main_thread(async {
        Sleep::during(Duration::from_millis(1_500)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(1_499),
        "Sleep finished too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1_900),
        "Sleep took too long: {elapsed:?}"
    );
}

#[test]
fn test_multiple_sequential_sleeps() {
    let start = Instant::now();
    main_thread(async {
        Sleep::during(Duration::from_millis(50)).await;
        Sleep::during(Duration::from_millis(50)).await;
        Sleep::during(Duration::from_millis(50)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(150),
        "Sequential sleeps finished too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "Sequential sleeps took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_concurrent_sleeps_same_duration() {
    let start = Instant::now();
    main_thread(async {
        let h1 = spawn(async {
            Sleep::during(Duration::from_millis(100)).await;
            1
        });
        let h2 = spawn(async {
            Sleep::during(Duration::from_millis(100)).await;
            2
        });
        let h3 = spawn(async {
            Sleep::during(Duration::from_millis(100)).await;
            3
        });

        let r1 = h1.await;
        let r2 = h2.await;
        let r3 = h3.await;
        assert_eq!(r1 + r2 + r3, 6);
    });
    let elapsed = start.elapsed();

    // All should complete around 100ms (parallel), not 300ms (sequential)
    assert!(
        elapsed >= Duration::from_millis(100),
        "Concurrent sleeps finished too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(290),
        "Concurrent sleeps should run in parallel: {:?}",
        elapsed
    );
}

#[test]
fn test_concurrent_sleeps_different_durations() {
    let start = Instant::now();
    main_thread(async {
        let h1 = spawn(async {
            Sleep::during(Duration::from_millis(50)).await;
            Instant::now()
        });
        let h2 = spawn(async {
            Sleep::during(Duration::from_millis(100)).await;
            Instant::now()
        });
        let h3 = spawn(async {
            Sleep::during(Duration::from_millis(150)).await;
            Instant::now()
        });

        let t1 = h1.await;
        let t2 = h2.await;
        let t3 = h3.await;

        // Verify ordering: t1 < t2 < t3
        assert!(t1 < t2, "50ms sleep should complete before 100ms sleep");
        assert!(t2 < t3, "100ms sleep should complete before 150ms sleep");
    });
    let elapsed = start.elapsed();

    // Total time should be ~150ms (longest timer)
    assert!(
        elapsed >= Duration::from_millis(150),
        "Should wait for longest timer: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "Took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_sleep_with_computation() {
    let start = Instant::now();
    let result = main_thread(async {
        Sleep::during(Duration::from_millis(50)).await;
        let mut sum = 0;
        for i in 0..1000 {
            sum += i;
        }
        Sleep::during(Duration::from_millis(50)).await;
        sum
    });
    let elapsed = start.elapsed();

    assert_eq!(result, 499500);
    assert!(
        elapsed >= Duration::from_millis(100),
        "Sleep with computation finished too early: {:?}",
        elapsed
    );
}

#[test]
fn test_sleep_drop_cancellation() {
    // Test that dropping a Sleep future doesn't cause issues
    let start = Instant::now();
    main_thread(async {
        let quick = spawn(async {
            Sleep::during(Duration::from_millis(50)).await;
            "quick"
        });

        // This will be dropped when we return early
        let _slow = Sleep::during(Duration::from_secs(5));

        quick.await
    });
    let elapsed = start.elapsed();

    // Should complete around 50ms, not wait for the 5s timer
    assert!(
        elapsed < Duration::from_millis(200),
        "Dropped sleep should not block: {:?}",
        elapsed
    );
}

#[test]
fn test_zero_duration_sleep() {
    let start = Instant::now();
    main_thread(async {
        Sleep::during(Duration::from_millis(0)).await;
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(200),
        "Zero duration sleep took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_many_concurrent_timers() {
    let start = Instant::now();
    main_thread(async {
        let mut handles = Vec::new();
        for i in 0..100 {
            handles.push(spawn(async move {
                Sleep::during(Duration::from_millis(100)).await;
                i
            }));
        }

        let mut sum = 0;
        for h in handles {
            sum += h.await;
        }
        assert_eq!(sum, (0..100).sum::<i32>());
    });
    let elapsed = start.elapsed();

    // All 100 timers should complete around 100ms (parallel)
    assert!(
        elapsed >= Duration::from_millis(100),
        "Many timers finished too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(600),
        "Many concurrent timers should run in parallel: {:?}",
        elapsed
    );
}

#[test]
fn test_sleep_completes_while_scheduler_is_busy() {
    use slopio::yield_now;
    use std::cell::Cell;
    use std::rc::Rc;

    let start = Instant::now();
    main_thread(async {
        let stop = Rc::new(Cell::new(false));
        let spinner_stop = stop.clone();
        // Keep the scheduler loop hot: with tasks always runnable, the
        // runtime never parks and ticks the timewheel far more often than
        // its 10ms resolution. The sleep must still fire on time.
        spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !spinner_stop.get() && Instant::now() < deadline {
                yield_now().await;
            }
        });
        Sleep::during(Duration::from_millis(100)).await;
        stop.set(true);
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(100),
        "Sleep finished too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "Sleep stalled while the scheduler was busy: {:?}",
        elapsed
    );
}

/// Polled again after it fired, a sleep stays ready instead of re-arming.
#[test]
fn a_fired_sleep_stays_ready() {
    main_thread(async {
        let mut sleep = std::pin::pin!(Sleep::during(Duration::from_millis(10)));
        sleep.as_mut().await;

        let start = Instant::now();
        sleep.as_mut().await;
        assert!(
            start.elapsed() < Duration::from_millis(5),
            "the second await slept again: {:?}",
            start.elapsed()
        );
    });
}

/// `now()` is derived from the monotonic clock and re-anchored every second:
/// it stays within a pass of the system wall clock.
#[test]
fn now_tracks_the_system_wall_clock() {
    main_thread(async {
        for _ in 0..3 {
            let wall = std::time::SystemTime::now();
            let now = slopio::runtime::now();
            let gap = wall.duration_since(now).unwrap_or_else(|e| e.duration());
            assert!(gap < Duration::from_millis(50), "now() is {gap:?} off");
            Sleep::during(Duration::from_millis(600)).await;
        }
    });
}
