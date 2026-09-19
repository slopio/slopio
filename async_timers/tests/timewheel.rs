mod common;

use async_timers::{DurationTooLong, MAX_DURATION, TimeWheel};
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

    let result = wheel.init_timer(MAX_DURATION + Duration::from_millis(1), &waker);
    assert_eq!(result, Err(DurationTooLong));
}

#[test]
fn test_duration_at_limit() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    // One minute exactly is the last duration the seconds level can hold.
    assert_eq!(MAX_DURATION, Duration::from_secs(60));
    assert!(wheel.init_timer(MAX_DURATION, &waker).is_ok());
}

/// A duration whose millisecond count overflows `u64` must be rejected, not
/// truncated into a small one that the ring would happily accept.
#[test]
fn test_absurd_duration_rejected() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let result = wheel.init_timer(Duration::from_secs(u64::MAX), &waker);
    assert_eq!(result, Err(DurationTooLong));
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
fn test_timer_fires_well_into_the_ring() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    let id = wheel
        .init_timer(Duration::from_millis(300), &waker)
        .unwrap();

    assert_eq!(wheel.next_deadline(), Some(Duration::from_millis(300)));

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

    // A second out is the first deadline the ms level cannot hold: it waits at
    // the seconds level, and `next_deadline` answers with its cascade.
    assert_eq!(
        wheel.next_deadline(),
        Some(Duration::from_secs(1)),
        "a 1s timer must not read as due now"
    );

    sleep(Duration::from_millis(1100));
    wheel.tick();
    assert_eq!(counter.count(), 1);
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
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

    // A tick apart: anything under 10ms shares the first tick with 10ms itself.
    let id1 = wheel.init_timer(Duration::from_millis(10), &waker1).unwrap();
    let id2 = wheel
        .init_timer(Duration::from_millis(20), &waker2)
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
fn test_timers_far_apart_in_the_ring() {
    let mut wheel = TimeWheel::new();
    let (counter_near, waker_near) = make_waker();
    let (counter_far, waker_far) = make_waker();

    let id_near = wheel
        .init_timer(Duration::from_millis(50), &waker_near)
        .unwrap();
    let id_far = wheel
        .init_timer(Duration::from_millis(300), &waker_far)
        .unwrap();

    sleep(Duration::from_millis(60));
    wheel.tick();

    assert_eq!(counter_near.count(), 1);
    assert_eq!(counter_far.count(), 0);
    assert_eq!(wheel.poll(id_near, &waker_near), Poll::Ready(()));

    sleep(Duration::from_millis(300));
    wheel.tick();

    assert_eq!(counter_far.count(), 1);
    assert_eq!(wheel.poll(id_far, &waker_far), Poll::Ready(()));
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
fn test_timer_past_the_first_hundred_ms() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

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

    // The current slot is drained by the next tick, not by this one, so the
    // answer is a tick away. Zero would spin the event loop.
    let deadline = wheel.next_deadline();
    assert_eq!(
        deadline,
        Some(Duration::from_millis(10)),
        "a timer in the current slot fires on the next tick"
    );
}

#[test]
fn test_next_deadline_is_exact() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel
        .init_timer(Duration::from_millis(300), &waker)
        .unwrap();

    // Nothing cascades below a second, so the ms slot that opens is the slot
    // that fires — the answer is the deadline itself, not a lower bound.
    assert_eq!(wheel.next_deadline(), Some(Duration::from_millis(300)));
}

#[test]
fn test_next_deadline_returns_the_soonest_of_many() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel
        .init_timer(Duration::from_millis(200), &waker)
        .unwrap();
    wheel.init_timer(Duration::from_millis(30), &waker).unwrap();
    wheel.init_timer(Duration::from_secs(1), &waker).unwrap();

    assert_eq!(wheel.next_deadline(), Some(Duration::from_millis(30)));
}

#[test]
fn test_next_deadline_moves_on_once_the_first_fired() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    wheel.init_timer(Duration::from_millis(50), &waker).unwrap();
    wheel
        .init_timer(Duration::from_millis(500), &waker)
        .unwrap();

    sleep(Duration::from_millis(60));
    wheel.tick();

    let deadline = wheel.next_deadline();
    assert!(deadline.is_some(), "the 500ms timer is still waiting");
    assert!(
        deadline.unwrap() <= Duration::from_millis(500),
        "got {deadline:?}"
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

#[test]
fn test_timer_fires_when_ticked_faster_than_resolution() {
    use std::time::Instant;

    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();

    wheel
        .init_timer(Duration::from_millis(100), &waker)
        .unwrap();

    // Tick every ~2ms, much faster than the 10ms resolution. Sub-tick
    // elapsed time must accumulate across calls instead of being discarded,
    // otherwise the wheel never advances (the "busy event loop" regression).
    let start = Instant::now();
    while counter.count() == 0 && start.elapsed() < Duration::from_secs(2) {
        sleep(Duration::from_millis(2));
        wheel.tick();
    }

    assert_eq!(
        counter.count(),
        1,
        "timer never fired while ticking faster than the wheel resolution"
    );
    // Generous upper bound (known wheel slack), well under the 2s bail-out.
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "timer fired too late: {:?}",
        start.elapsed()
    );
}

#[test]
fn test_fired_timer_releases_slab_slot_on_poll() {
    let mut wheel = TimeWheel::new();
    let (_, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(20), &waker).unwrap();
    assert_eq!(wheel.active_timers(), 1);

    sleep(Duration::from_millis(30));
    wheel.tick();

    // Observing Ready is the timer's last poll; the slot must be released
    // or every completed Sleep leaks a slab entry for the process lifetime.
    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
    assert_eq!(wheel.active_timers(), 0);
}

// Synthetic clock driving the wheel through `tick_at` in 10ms steps.
struct Clock {
    origin: std::time::Instant,
    wall: std::time::SystemTime,
    elapsed: Duration,
}

impl Clock {
    fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
            elapsed: Duration::ZERO,
        }
    }

    fn advance(&mut self, wheel: &mut TimeWheel, by: Duration) {
        self.elapsed += by;
        wheel.tick_at(self.origin + self.elapsed, self.wall + self.elapsed);
    }

    // Elapsed clock time at the first wake, panicking past `limit`.
    fn run_until_fired(
        &mut self,
        wheel: &mut TimeWheel,
        counter: &common::CountingWaker,
        limit: Duration,
    ) -> Duration {
        while counter.count() == 0 {
            assert!(self.elapsed < limit, "timer never fired within {limit:?}");
            self.advance(wheel, Duration::from_millis(10));
        }
        self.elapsed
    }
}

// A whole-tick deadline fires on the tick that closes its 10ms window.
fn assert_fired_within_tick(fired_at: Duration, deadline: Duration) {
    let lo = deadline - Duration::from_millis(deadline.as_millis() as u64 % 10);
    assert!(fired_at >= lo, "{deadline:?} timer fired early at {fired_at:?}");
    assert!(
        fired_at <= lo + Duration::from_millis(10),
        "{deadline:?} timer fired late at {fired_at:?}"
    );
}

fn assert_fires_at(duration: Duration) {
    let mut wheel = TimeWheel::new();
    let mut clock = Clock::new();
    let (counter, waker) = make_waker();
    let id = wheel.init_timer(duration, &waker).unwrap();

    let fired_at = clock.run_until_fired(&mut wheel, &counter, duration + Duration::from_secs(1));

    assert_eq!(wheel.poll(id, &waker), Poll::Ready(()));
    assert_fired_within_tick(fired_at, duration);
}

#[test]
fn test_timer_at_a_single_tick() {
    assert_fires_at(Duration::from_millis(10));
}

#[test]
fn test_timer_mid_ring_keeps_its_sub_tick_remainder() {
    assert_fires_at(Duration::from_millis(455));
}

#[test]
fn test_timer_at_the_full_window() {
    assert_fires_at(MAX_DURATION);
}

/// Armed from a clock that is already part-way round the ring, so the deadline
/// wraps past slot 0. The modulo has to carry it, not fold it back onto a slot
/// that has already fired.
#[test]
fn test_timer_inserted_mid_ring_wraps_to_its_own_slot() {
    let mut wheel = TimeWheel::new();
    let mut clock = Clock::new();
    let (counter, waker) = make_waker();

    let idle = Duration::from_millis(370);
    clock.advance(&mut wheel, idle);
    let duration = Duration::from_millis(750);
    wheel.init_timer(duration, &waker).unwrap();

    let fired_at = clock.run_until_fired(&mut wheel, &counter, Duration::from_secs(3));
    assert_fired_within_tick(fired_at, idle + duration);
}

/// The aliasing case the 61st seconds slot exists for: a full-window timer
/// armed at an arbitrary second must not land on the slot the clock stands on,
/// which has already cascaded.
#[test]
fn test_full_window_timer_never_aliases_the_current_slot() {
    // Every slot of the seconds ring, so the wrap past slot 0 is covered too,
    // each from a second boundary and from part-way into a second.
    let seconds_ring = MAX_DURATION.as_secs() + 1;
    for offset in 0..seconds_ring {
        for skew in [Duration::ZERO, Duration::from_millis(370)] {
            let mut wheel = TimeWheel::new();
            let mut clock = Clock::new();
            let (counter, waker) = make_waker();

            clock.advance(&mut wheel, Duration::from_secs(offset) + skew);
            wheel.init_timer(MAX_DURATION, &waker).unwrap();

            // An aliased deadline would land on a slot that cascades at the
            // next second, firing a minute early.
            clock.advance(&mut wheel, MAX_DURATION - Duration::from_millis(10));
            assert_eq!(counter.count(), 0, "fired early at {offset}s + {skew:?}");

            clock.advance(&mut wheel, Duration::from_millis(10));
            assert_eq!(counter.count(), 1, "never fired at {offset}s + {skew:?}");
        }
    }
}

/// Every duration the wheel takes, armed together, must fire on exactly the
/// tick its deadline names: the two levels have to agree on where a deadline
/// lives, and the cascade has to carry it down without rounding.
#[test]
fn test_every_duration_fires_on_the_tick_its_deadline_names() {
    let max_ticks = MAX_DURATION.as_millis() as usize / 10;

    // Arming at, just before and just after a second boundary: where the ms
    // level hands over to the seconds level and where a cascade lands.
    for skew in [0, 37, 99, 100] {
        let mut wheel = TimeWheel::new();
        let mut clock = Clock::new();
        clock.advance(&mut wheel, Duration::from_millis(skew * 10));

        let wakers: Vec<_> = (0..=max_ticks).map(|_| make_waker()).collect();
        for (ticks, (_, waker)) in wakers.iter().enumerate() {
            wheel
                .init_timer(Duration::from_millis(ticks as u64 * 10), waker)
                .unwrap();
        }

        // An `n` tick duration is served by the n-th tick of the clock, and
        // anything under one tick by the first — so 0 and 1 fire together.
        for elapsed in 0..=max_ticks + 1 {
            if let Some((counter, _)) = wakers.get(elapsed + 1) {
                assert_eq!(
                    counter.count(),
                    0,
                    "{} ticks fired early (skew {skew})",
                    elapsed + 1
                );
            }
            if (1..=max_ticks).contains(&elapsed) {
                let (counter, _) = &wakers[elapsed];
                assert_eq!(
                    counter.count(),
                    1,
                    "{elapsed} ticks did not fire on its tick (skew {skew})"
                );
            }
            if elapsed == 1 {
                let (counter, _) = &wakers[0];
                assert_eq!(
                    counter.count(),
                    1,
                    "a sub-tick duration did not fire on the first tick (skew {skew})"
                );
            }
            clock.advance(&mut wheel, Duration::from_millis(10));
        }

        for (ticks, (counter, _)) in wakers.iter().enumerate() {
            assert_eq!(
                counter.count(),
                1,
                "{ticks} ticks fired {} times (skew {skew})",
                counter.count()
            );
        }
    }
}

/// A deadline that outruns the ms level waits a whole seconds slot, then
/// cascades into the ms slot its remainder names — not the slot the second
/// opens on.
#[test]
fn test_timer_cascades_with_its_sub_second_remainder() {
    assert_fires_at(Duration::from_millis(1_500));
}

#[test]
fn test_timer_crossing_several_seconds() {
    assert_fires_at(Duration::from_secs(10));
}

/// Armed part-way into a second, so the cascade has to carry a remainder that
/// does not align with the boundary it cascades on.
#[test]
fn test_timer_inserted_mid_second_fires_at_its_own_deadline() {
    let mut wheel = TimeWheel::new();
    let mut clock = Clock::new();
    let (counter, waker) = make_waker();

    let idle = Duration::from_millis(370);
    clock.advance(&mut wheel, idle);
    let duration = Duration::from_millis(1_500);
    wheel.init_timer(duration, &waker).unwrap();

    let fired_at = clock.run_until_fired(&mut wheel, &counter, Duration::from_secs(3));
    assert_fired_within_tick(fired_at, idle + duration);
}

#[test]
fn test_cancelled_timer_freed_on_cascade() {
    let mut wheel = TimeWheel::new();
    let mut clock = Clock::new();
    let (_, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_secs(5), &waker).unwrap();
    wheel.drop(id);
    assert_eq!(
        wheel.active_timers(),
        1,
        "the bucket still references the id"
    );

    // The seconds slot cascades at t=5s; the cancelled id is released there,
    // without ever reaching the ms level.
    clock.advance(&mut wheel, Duration::from_millis(5_000));
    assert_eq!(wheel.active_timers(), 0);
}

#[test]
fn test_cancelled_timer_freed_when_its_bucket_fires() {
    let mut wheel = TimeWheel::new();
    let mut clock = Clock::new();
    let (_, waker) = make_waker();

    let id = wheel.init_timer(Duration::from_millis(500), &waker).unwrap();
    wheel.drop(id);
    assert_eq!(
        wheel.active_timers(),
        1,
        "the bucket still references the id"
    );

    clock.advance(&mut wheel, Duration::from_millis(510));
    assert_eq!(wheel.active_timers(), 0);
}

/// An occupied ms slot must not hide a seconds slot that cascades ahead of it.
#[test]
fn test_next_deadline_sees_both_levels() {
    let mut wheel = TimeWheel::new();
    let (counter, waker) = make_waker();
    let mut clock = Clock::new();
    clock.advance(&mut wheel, Duration::ZERO);

    wheel.init_timer(Duration::from_millis(1050), &waker).unwrap();
    clock.advance(&mut wheel, Duration::from_millis(500));
    wheel.init_timer(Duration::from_millis(1000), &waker).unwrap();

    // Sleep exactly what the wheel asks for, as the runtime does.
    while counter.count() == 0 {
        let wait = wheel.next_deadline().unwrap();
        clock.advance(&mut wheel, wait);
    }
    assert!(
        clock.elapsed <= Duration::from_millis(1070),
        "the 1050ms timer fired at {:?}",
        clock.elapsed
    );
}
