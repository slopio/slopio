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
}

impl Default for RingOptions {
    fn default() -> Self {
        Self {
            defer_taskrun: true,
            coop_taskrun: true,
            sqpoll: false,
            // The kernel's own default.
            sqpoll_idle: Duration::from_secs(1),
        }
    }
}

impl RingOptions {
    /// Refuses the combinations the kernel would reject at setup.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.sqpoll && (self.defer_taskrun || self.coop_taskrun) {
            return Err("sqpoll excludes defer_taskrun and coop_taskrun: set them to false");
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
}
