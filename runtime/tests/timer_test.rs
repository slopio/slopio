use std::time::{Duration, Instant};

use runtime::main_thread;
use runtime::runtime::spawn;
use runtime::runtime::time::Sleep;

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
        let _slow = Sleep::during(Duration::from_secs(10));

        quick.await
    });
    let elapsed = start.elapsed();

    // Should complete around 50ms, not wait for the 10s timer
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
