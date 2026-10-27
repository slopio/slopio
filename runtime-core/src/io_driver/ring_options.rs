use std::time::Duration;

/// The io_uring setup flags a worker's ring is built with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingOptions {
    /// `IORING_SETUP_DEFER_TASKRUN`: task work runs only when the thread waits
    /// for completions.
    pub defer_taskrun: bool,
    /// `IORING_SETUP_COOP_TASKRUN`: no interrupt to run task work, it waits
    /// for the next kernel entry.
    pub coop_taskrun: bool,
    /// `IORING_SETUP_SQPOLL`: a kernel thread polls the submission queue, so
    /// submitting needs no syscall while it is awake. Excludes both flags above.
    pub sqpoll: bool,
    /// How long the SQ thread spins on an empty queue before it sleeps.
    pub sqpoll_idle: Duration,
    /// NAPI busy polling, `IORING_REGISTER_NAPI` (Linux 6.9): while the thread
    /// waits for completions it polls, for up to this long, the NIC queues
    /// its sockets' packets arrive on, rather than sleep until an interrupt.
    /// The kernel learns the queues from the sockets the ring uses. `None`,
    /// the default, leaves it off: a thread that waits spins instead of
    /// sleeping, which pays only on cores given to it.
    pub napi_busy_poll: Option<Duration>,
    /// With `napi_busy_poll`: the kernel defers the queue's interrupts while
    /// the thread polls it (`prefer_busy_poll`). Takes effect only once the
    /// interface's `napi_defer_hard_irqs` and `gro_flush_timeout` are set,
    /// which are the system's to configure.
    pub napi_prefer_busy_poll: bool,
}

impl Default for RingOptions {
    fn default() -> Self {
        Self {
            defer_taskrun: true,
            coop_taskrun: true,
            sqpoll: false,
            // The kernel's own default.
            sqpoll_idle: Duration::from_secs(1),
            napi_busy_poll: None,
            napi_prefer_busy_poll: false,
        }
    }
}

impl RingOptions {
    /// Refuses the combinations the kernel would reject at setup.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.sqpoll && (self.defer_taskrun || self.coop_taskrun) {
            return Err("sqpoll excludes defer_taskrun and coop_taskrun: set them to false");
        }
        if self.napi_busy_poll == Some(Duration::ZERO) {
            return Err("napi_busy_poll: more than zero, or None to leave it off");
        }
        if self.napi_prefer_busy_poll && self.napi_busy_poll.is_none() {
            return Err("napi_prefer_busy_poll needs napi_busy_poll");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqpoll_excludes_the_task_run_flags() {
        let sqpoll = RingOptions {
            sqpoll: true,
            ..RingOptions::default()
        };
        assert!(sqpoll.check().is_err());
        assert!(
            RingOptions {
                defer_taskrun: false,
                coop_taskrun: false,
                ..sqpoll
            }
            .check()
            .is_ok()
        );
        assert!(RingOptions::default().check().is_ok());
    }

    #[test]
    fn napi_is_off_by_default_and_checked_when_on() {
        let default = RingOptions::default();
        assert_eq!(default.napi_busy_poll, None);
        assert!(!default.napi_prefer_busy_poll);

        let on = RingOptions {
            napi_busy_poll: Some(Duration::from_micros(50)),
            napi_prefer_busy_poll: true,
            ..default
        };
        assert!(on.check().is_ok());
        assert!(
            RingOptions {
                napi_busy_poll: Some(Duration::ZERO),
                ..on
            }
            .check()
            .is_err()
        );
        assert!(
            RingOptions {
                napi_busy_poll: None,
                ..on
            }
            .check()
            .is_err(),
            "prefer_busy_poll alone"
        );
    }
}
