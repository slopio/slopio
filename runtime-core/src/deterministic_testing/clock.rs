//! The simulator's clock: `CLOCK_MONOTONIC`, read and slept on directly.
//!
//! Not `Instant`/`thread::sleep`, for two reasons. `Instant` doesn't expose the
//! raw value `clock_nanosleep` needs to take an *absolute* deadline, and
//! absolute deadlines are what stop a long test from accumulating the ~50µs
//! every sleep overshoots by — relative sleeps would drift by tens of
//! milliseconds over a few hundred ticks.

/// A point on `CLOCK_MONOTONIC`, in nanoseconds.
pub(crate) type Nanos = u64;

const NANOS_PER_SEC: Nanos = 1_000_000_000;

/// Read the monotonic clock.
pub(crate) fn now() -> Nanos {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `clock_gettime` writes a `timespec` through the pointer it is
    // given. The clock id is a valid constant and the destination is a live
    // local, so the write lands somewhere initialized and correctly sized.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as Nanos * NANOS_PER_SEC + ts.tv_nsec as Nanos
}

/// Block until the monotonic clock reaches `deadline`. Returns immediately if
/// it already has.
pub(crate) fn sleep_until(deadline: Nanos) {
    let ts = libc::timespec {
        tv_sec: (deadline / NANOS_PER_SEC) as libc::time_t,
        tv_nsec: (deadline % NANOS_PER_SEC) as _,
    };
    loop {
        // SAFETY: `ts` is a live, fully initialized `timespec`. The null final
        // argument is permitted and means "don't report the remaining time".
        let err = unsafe {
            libc::clock_nanosleep(
                libc::CLOCK_MONOTONIC,
                libc::TIMER_ABSTIME,
                &ts,
                std::ptr::null_mut(),
            )
        };
        // `clock_nanosleep` returns the error number directly instead of
        // setting `errno`. A signal can cut the sleep short; since the deadline
        // is absolute, reissuing simply resumes toward the same instant rather
        // than restarting a full interval.
        if err != libc::EINTR {
            return;
        }
    }
}
