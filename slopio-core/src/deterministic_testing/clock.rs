pub(crate) type Nanos = u64;

const NANOS_PER_SEC: Nanos = 1_000_000_000;

pub(crate) fn now() -> Nanos {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: the clock id is a valid constant and the destination is a live,
    // correctly sized local.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as Nanos * NANOS_PER_SEC + ts.tv_nsec as Nanos
}

pub(crate) fn sleep_until(deadline: Nanos) {
    let ts = libc::timespec {
        tv_sec: (deadline / NANOS_PER_SEC) as libc::time_t,
        tv_nsec: (deadline % NANOS_PER_SEC) as _,
    };
    loop {
        // SAFETY: `ts` is a live, initialized `timespec`; the null final
        // argument is permitted and means "don't report the remaining time".
        let err = unsafe {
            libc::clock_nanosleep(
                libc::CLOCK_MONOTONIC,
                libc::TIMER_ABSTIME,
                &ts,
                std::ptr::null_mut(),
            )
        };
        // Returns the error number directly rather than setting `errno`. The
        // deadline is absolute, so reissuing after a signal resumes toward the
        // same instant instead of restarting a full interval.
        if err != libc::EINTR {
            return;
        }
    }
}
