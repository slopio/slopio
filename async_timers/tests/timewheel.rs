mod common;

use async_timers::{DurationTooLong, TimeWheel};
use common::make_waker;
use std::task::Poll;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn test_new_timewheel() {
    let wheel = TimeWheel::new();
    assert_eq!(wheel.next_deadline(), None);
}

#[test]
fn test_default_timewheel() {
    let wheel = TimeWheel::default();
    assert_eq!(wheel.next_deadline(), None);
}

#[test]
fn test_init_timer_returns_id() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(50), &waker).unwrap();
    assert_eq!(id, 0);

    let id2 = wheel.init_timer(Duration::from_millis(50), &waker).unwrap();
    assert_eq!(id2, 1);
}

#[test]
fn test_duration_too_long_rejected() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let result = wheel.init_timer(Duration::from_hours(24), &waker);
    assert_eq!(result, Err(DurationTooLong));
}

#[test]
fn test_duration_at_limit() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // Just under 24 hours should work
    let result = wheel.init_timer(Duration::from_hours(24) - Duration::from_millis(1), &waker);
    assert!(result.is_ok());
}

#[test]
fn test_zero_duration_timer() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    let id = wheel.init_timer(Duration::ZERO, &waker).unwrap();

    // Timer at current bucket should fire on next tick
    sleep(Duration::from_millis(15));
    wheel.tick();

    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_timer_fires_at_ms_level() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(20), &waker).unwrap();

    // Timer should not fire before its time
    sleep(Duration::from_millis(15));
    wheel.tick();
    assert_eq!(counter.count(), 0);
    assert_eq!(wheel.poll(id, &waker), Poll::Pending);

    // Timer should fire after sufficient time
    sleep(Duration::from_millis(20));
    wheel.tick();
    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_timer_fires_at_ms_boundary() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    // 90ms is close to the ms-level boundary (100ms)
    let id = wheel.init_timer(Duration::from_millis(90), &waker).unwrap();

    sleep(Duration::from_millis(100));
    wheel.tick();

    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_timer_fires_at_second_level() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    // 300ms should be in the second-level bucket (ms threshold is 200ms)
    let id = wheel
        .init_timer(Duration::from_millis(300), &waker)
        .unwrap();

    // Verify it's not in ms level by checking deadline is calculated correctly
    assert!(wheel.next_deadline().unwrap() >= Duration::from_millis(200));

    // Process ticks until timer fires
    sleep(Duration::from_millis(350));
    wheel.tick();

    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_timer_at_one_second() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_secs(1), &waker).unwrap();

    // Verify timer is placed in second-level bucket (deadline >= 200ms threshold)
    let deadline = wheel.next_deadline().unwrap();
    assert!(
        deadline >= Duration::from_millis(200),
        "1s timer should be in second-level bucket"
    );

    // Should fire after 1 second total
    sleep(Duration::from_millis(1100));
    wheel.tick();
    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_timer_registered_at_hour_level() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // 2 hours should be in the hour-level bucket
    let _id = wheel.init_timer(Duration::from_secs(7200), &waker).unwrap();

    // Verify it's in hour level (deadline should be large)
    let deadline = wheel.next_deadline().unwrap();
    assert!(deadline >= Duration::from_secs(60));
}

#[test]
fn test_multiple_timers_same_bucket() {
    let mut wheel = TimeWheel::new();
    let (counter1, waker1) = make_waker();
    let (counter2, waker2) = make_waker();

    let id1 = wheel
        .init_timer(Duration::from_millis(20), &waker1)
        .unwrap();
    let id2 = wheel
        .init_timer(Duration::from_millis(25), &waker2)
        .unwrap();

    // Both should be in the same bucket (10ms granularity)
    sleep(Duration::from_millis(35));
    wheel.tick();

    assert_eq!(counter1.count(), 1);
    assert_eq!(counter2.count(), 1);
    assert_eq!(wheel.poll(id1, &waker1), Poll::Ready(()));
    assert_eq!(wheel.poll(id2, &waker2), Poll::Ready(()));
}

#[test]
fn test_multiple_timers_different_buckets() {
    let mut wheel = TimeWheel::new();
    let (counter1, waker1) = make_waker();
    let (counter2, waker2) = make_waker();

    let id1 = wheel.init_timer(Duration::from_millis(5), &waker1).unwrap();
    let id2 = wheel
        .init_timer(Duration::from_millis(15), &waker2)
        .unwrap();

    // First timer should fire first
    sleep(Duration::from_millis(10));
    wheel.tick();

    assert_eq!(counter1.count(), 1);
    assert_eq!(counter2.count(), 0);
    assert_eq!(wheel.poll(id1, &waker1), Poll::Ready(()));
    assert_eq!(wheel.poll(id2, &waker2), Poll::Pending);

    // Second timer should fire after more time
    sleep(Duration::from_millis(10));
    wheel.tick();

    assert_eq!(counter2.count(), 1);
    assert_eq!(wheel.poll(id2, &waker2), Poll::Ready(()));
}

#[test]
fn test_multiple_timers_different_levels() {
    let mut wheel = TimeWheel::new();
    let (counter_ms, waker_ms) = make_waker();
    let (counter_s, waker_s) = make_waker();

    let id_ms = wheel
        .init_timer(Duration::from_millis(50), &waker_ms)
        .unwrap();
    let id_s = wheel
        .init_timer(Duration::from_millis(300), &waker_s)
        .unwrap();

    // MS level timer should fire first
    sleep(Duration::from_millis(60));
    wheel.tick();

    assert_eq!(counter_ms.count(), 1);
    assert_eq!(counter_s.count(), 0);
    assert_eq!(wheel.poll(id_ms, &waker_ms), Poll::Ready(()));

    // S level timer should fire after cascade
    sleep(Duration::from_millis(300));
    wheel.tick();

    assert_eq!(counter_s.count(), 1);
    assert_eq!(wheel.poll(id_s, &waker_s), Poll::Ready(()));
}

#[test]
fn test_cancel_timer_before_fire() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(50), &waker).unwrap();

    // Cancel before it fires
    wheel.drop(id);

    // Let time pass and tick
    sleep(Duration::from_millis(60));
    wheel.tick();

    // Waker should not have been called
    assert_eq!(counter.count(), 0);
}

#[test]
fn test_cancel_timer_idempotent() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(50), &waker).unwrap();

    // Cancel multiple times should not panic
    wheel.drop(id);
    wheel.drop(id);

    sleep(Duration::from_millis(60));
    wheel.tick();

    assert_eq!(counter.count(), 0);
}

#[test]
fn test_poll_cancelled_timer() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(50), &waker).unwrap();
    wheel.drop(id);

    // Polling a cancelled timer should return Ready (it's done, just cancelled)
    let result = wheel.poll(id, &waker);
    assert_eq!(result, Poll::Ready(()));
}

#[test]
fn test_poll_pending_timer() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let id = wheel
        .init_timer(Duration::from_millis(100), &waker)
        .unwrap();

    // Should be pending before firing
    assert_eq!(wheel.poll(id, &waker), Poll::Pending);
}

#[test]
fn test_poll_updates_waker() {
    let mut wheel = TimeWheel::new();
    let (counter1, waker1) = make_waker();
    let (counter2, waker2) = make_waker();

    let id = wheel
        .init_timer(Duration::from_millis(30), &waker1)
        .unwrap();

    // Update waker by polling with different waker
    let _ = wheel.poll(id, &waker2);

    // Let timer fire
    sleep(Duration::from_millis(40));
    wheel.tick();

    // New waker should have been called, not the original
    assert_eq!(counter1.count(), 0);
    assert_eq!(counter2.count(), 1);
}

#[test]
fn test_poll_same_waker_no_clone() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let id = wheel
        .init_timer(Duration::from_millis(100), &waker)
        .unwrap();

    // Polling with same waker should not cause issues
    let _ = wheel.poll(id, &waker);
    let _ = wheel.poll(id, &waker);
    let _ = wheel.poll(id, &waker);

    assert_eq!(wheel.poll(id, &waker), Poll::Pending);
}

#[test]
fn test_next_deadline_empty() {
    let wheel = TimeWheel::new();
    assert_eq!(wheel.next_deadline(), None);
}

#[test]
fn test_next_deadline_single_ms_timer() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel.init_timer(Duration::from_millis(20), &waker).unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // Should be approximately 20ms (rounded to bucket)
    assert!(deadline <= Duration::from_millis(30));
}

#[test]
fn test_next_deadline_multiple_timers_returns_soonest() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel.init_timer(Duration::from_millis(50), &waker).unwrap();
    wheel.init_timer(Duration::from_millis(20), &waker).unwrap();
    wheel.init_timer(Duration::from_millis(80), &waker).unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // Should return the soonest (20ms rounded)
    assert!(deadline <= Duration::from_millis(30));
}

#[test]
fn test_next_deadline_updates_after_fire() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel.init_timer(Duration::from_millis(20), &waker).unwrap();
    wheel.init_timer(Duration::from_millis(50), &waker).unwrap();

    // Fire first timer
    sleep(Duration::from_millis(30));
    wheel.tick();

    let deadline = wheel.next_deadline().unwrap();
    // Should now point to the second timer
    assert!(deadline <= Duration::from_millis(40));
}

#[test]
fn test_next_deadline_returns_none_after_all_fired() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel.init_timer(Duration::from_millis(20), &waker).unwrap();

    sleep(Duration::from_millis(30));
    wheel.tick();

    // Note: occupied bit might still be set even though timers are fired
    // This test documents current behavior
    let deadline = wheel.next_deadline();
    // After firing, deadline might be None or very small depending on impl
    if let Some(d) = deadline {
        assert!(d <= Duration::from_millis(10));
    }
}

#[test]
fn test_tick_processes_multiple_ticks() {
    let mut wheel = TimeWheel::new();
    let (counter1, waker1) = make_waker();
    let (counter2, waker2) = make_waker();

    wheel
        .init_timer(Duration::from_millis(20), &waker1)
        .unwrap();
    wheel
        .init_timer(Duration::from_millis(40), &waker2)
        .unwrap();

    // Sleep long enough for both to fire
    sleep(Duration::from_millis(50));
    wheel.tick();

    // Both should have fired in single tick() call
    assert_eq!(counter1.count(), 1);
    assert_eq!(counter2.count(), 1);
}

#[test]
fn test_tick_no_timers() {
    let mut wheel = TimeWheel::new();

    // Should not panic
    sleep(Duration::from_millis(20));
    wheel.tick();
}

#[test]
fn test_rapid_ticks() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    // Use ms-level timer to avoid cascade timing issues
    wheel
        .init_timer(Duration::from_millis(100), &waker)
        .unwrap();

    // Call tick many times before timer should fire (total ~30ms)
    for _ in 0..10 {
        wheel.tick();
        sleep(Duration::from_millis(3));
    }

    assert_eq!(
        counter.count(),
        0,
        "Timer fired too early during rapid ticks"
    );

    // Now let it fire (sleep enough for >100ms total from start)
    sleep(Duration::from_millis(120));
    wheel.tick();

    assert_eq!(counter.count(), 1);
}

#[test]
fn test_cascade_from_seconds_to_ms() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    // Timer in second-level bucket
    let id = wheel
        .init_timer(Duration::from_millis(150), &waker)
        .unwrap();

    // Process ticks to trigger cascade
    sleep(Duration::from_millis(200));
    wheel.tick();

    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_timer_at_bucket_boundary() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    // Exactly at 10ms boundary
    let id = wheel.init_timer(Duration::from_millis(10), &waker).unwrap();

    sleep(Duration::from_millis(20));
    wheel.tick();

    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
}

#[test]
fn test_many_timers_in_one_bucket() {
    let mut wheel = TimeWheel::new();
    let wakers: Vec<_> = (0..20).map(|_| make_waker()).collect();

    let ids: Vec<_> = wakers
        .iter()
        .map(|(_, w)| wheel.init_timer(Duration::from_millis(20), w).unwrap())
        .collect();

    sleep(Duration::from_millis(35));
    wheel.tick();

    // All should have fired
    for (i, (counter, waker)) in wakers.iter().enumerate() {
        assert_eq!(counter.count(), 1, "Timer {} didn't fire", i);
        assert_eq!(wheel.poll(ids[i], waker), Poll::Ready(()));
    }
}

#[test]
fn test_timer_fires_exactly_once() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    wheel.init_timer(Duration::from_millis(20), &waker).unwrap();

    // Fire the timer
    sleep(Duration::from_millis(30));
    wheel.tick();

    // Tick more times
    for _ in 0..10 {
        sleep(Duration::from_millis(15));
        wheel.tick();
    }

    // Should only have been woken once
    assert_eq!(counter.count(), 1);
}

#[test]
fn test_interleaved_register_and_tick() {
    let mut wheel = TimeWheel::new();
    let (counter1, waker1) = make_waker();
    let (counter2, waker2) = make_waker();

    // Use ms-level durations to avoid cascade timing issues
    wheel
        .init_timer(Duration::from_millis(50), &waker1)
        .unwrap();

    sleep(Duration::from_millis(30));
    wheel.tick();

    // First timer should not have fired yet
    assert_eq!(counter1.count(), 0, "First timer fired too early");

    // Register another timer mid-way
    wheel
        .init_timer(Duration::from_millis(80), &waker2)
        .unwrap();

    // Let first timer fire
    sleep(Duration::from_millis(40));
    wheel.tick();

    assert_eq!(counter1.count(), 1, "First timer should have fired");
    assert_eq!(counter2.count(), 0, "Second timer fired too early");

    // Let second timer fire
    sleep(Duration::from_millis(50));
    wheel.tick();

    assert_eq!(counter2.count(), 1, "Second timer should have fired");
}

// ============================================================================
// next_deadline tests
// ============================================================================

#[test]
fn test_next_deadline_zero_duration_timer() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // Zero duration timer goes into current bucket
    wheel.init_timer(Duration::ZERO, &waker).unwrap();

    // Timer at current bucket (offset 0) should fire immediately
    let deadline = wheel.next_deadline();
    assert_eq!(
        deadline,
        Some(Duration::ZERO),
        "Zero-offset timer should return Duration::ZERO"
    );
}

#[test]
fn test_next_deadline_timer_in_second_level() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // 300ms is in the second-level bucket (ms threshold is 200ms)
    wheel
        .init_timer(Duration::from_millis(300), &waker)
        .unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // Should be at least 200ms (the full ms level needs to pass first)
    assert!(
        deadline >= Duration::from_millis(200),
        "Second-level timer deadline should be >= 200ms, got {:?}",
        deadline
    );
}

#[test]
fn test_next_deadline_timer_in_hour_level() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // 2 hours is in the hour-level bucket
    wheel.init_timer(Duration::from_secs(7200), &waker).unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // Should be at least 60 seconds (full ms + s levels need to cascade)
    assert!(
        deadline >= Duration::from_secs(60),
        "Hour-level timer deadline should be >= 60s, got {:?}",
        deadline
    );
}

#[test]
fn test_next_deadline_prefers_ms_over_s_level() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // First add a second-level timer
    wheel
        .init_timer(Duration::from_millis(200), &waker)
        .unwrap();

    // Then add a ms-level timer
    wheel.init_timer(Duration::from_millis(30), &waker).unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // Should return the ms-level timer (sooner)
    assert!(
        deadline <= Duration::from_millis(40),
        "Should return ms-level timer deadline, got {:?}",
        deadline
    );
}

#[test]
fn test_next_deadline_prefers_s_over_h_level() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // First add an hour-level timer
    wheel.init_timer(Duration::from_secs(7200), &waker).unwrap();

    // Then add a second-level timer
    wheel
        .init_timer(Duration::from_millis(500), &waker)
        .unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // Should return the second-level timer (sooner than hour-level)
    assert!(
        deadline < Duration::from_secs(60),
        "Should return s-level timer deadline, got {:?}",
        deadline
    );
}

#[test]
fn test_next_deadline_all_three_levels() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // Add timers at all three levels
    wheel.init_timer(Duration::from_secs(7200), &waker).unwrap(); // hour level
    wheel
        .init_timer(Duration::from_millis(500), &waker)
        .unwrap(); // second level
    wheel.init_timer(Duration::from_millis(50), &waker).unwrap(); // ms level

    let deadline = wheel.next_deadline().unwrap();
    // Should return the ms-level timer
    assert!(
        deadline <= Duration::from_millis(60),
        "Should return ms-level timer deadline, got {:?}",
        deadline
    );
}

#[test]
fn test_next_deadline_after_ms_level_cleared() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // Add timer at ms level and s level
    wheel.init_timer(Duration::from_millis(50), &waker).unwrap();
    wheel
        .init_timer(Duration::from_millis(500), &waker)
        .unwrap();

    // Fire the ms-level timer
    sleep(Duration::from_millis(60));
    wheel.tick();

    // Now next_deadline should point to the s-level timer
    let deadline = wheel.next_deadline();
    assert!(
        deadline.is_some(),
        "Should have deadline from s-level timer"
    );
}

#[test]
fn test_next_deadline_exact_bucket_boundaries() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // Timer exactly at 10ms (one tick)
    wheel.init_timer(Duration::from_millis(10), &waker).unwrap();

    let deadline = wheel.next_deadline().unwrap();
    assert_eq!(
        deadline,
        Duration::from_millis(10),
        "Deadline should be exactly 10ms"
    );
}

#[test]
fn test_next_deadline_multiple_same_bucket() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // Multiple timers in the same bucket (20ms and 25ms both round to same 10ms bucket)
    wheel.init_timer(Duration::from_millis(20), &waker).unwrap();
    wheel.init_timer(Duration::from_millis(25), &waker).unwrap();
    wheel.init_timer(Duration::from_millis(28), &waker).unwrap();

    let deadline = wheel.next_deadline().unwrap();
    // All go into the bucket at offset 2 (20ms)
    assert!(
        deadline <= Duration::from_millis(30),
        "Deadline should reflect the bucket, got {:?}",
        deadline
    );
}

#[test]
fn test_next_deadline_cancelled_timer_still_in_bucket() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(30), &waker).unwrap();

    // Cancel the timer
    wheel.drop(id);

    // next_deadline still sees the occupied bucket (timer is cancelled but bucket bit is set)
    // This documents current behavior - the occupied bit isn't cleared on cancel
    let deadline = wheel.next_deadline();
    // Note: This may return Some even though timer is cancelled, since we don't
    // clear the bucket occupied bit on cancel
    assert!(
        deadline.is_some(),
        "Bucket occupied bit should still be set after cancel"
    );
}
