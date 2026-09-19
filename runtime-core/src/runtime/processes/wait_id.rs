use std::fmt;
use std::future::Future;
use std::process::Child;

use io_uring::opcode;

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::OneshotToken;

/// How the kernel identifies which child(ren) to wait for.
#[repr(u32)]
#[derive(Clone, Copy)]
pub enum IdType {
    /// Wait for any child process.
    All = libc::P_ALL,
    /// Wait for the child whose PID matches.
    Pid = libc::P_PID,
    /// Wait for any child in the given process group.
    Pgid = libc::P_PGID,
    /// Wait using a pidfd instead of a raw PID.
    PidFd = libc::P_PIDFD,
}

/// Flags controlling which state changes to wait for.
/// Combine with `|`: `WaitFlags::EXITED | WaitFlags::STOPPED`.
#[derive(Clone, Copy)]
pub struct WaitFlags(i32);

impl WaitFlags {
    /// Child terminated (exited or killed by signal).
    pub const EXITED: Self = Self(libc::WEXITED);
    /// Child was stopped by a signal.
    pub const STOPPED: Self = Self(libc::WSTOPPED);
    /// Child was resumed by SIGCONT.
    pub const CONTINUED: Self = Self(libc::WCONTINUED);
    /// Don't consume the waitable state — a later wait can retrieve it again.
    pub const NOWAIT: Self = Self(libc::WNOWAIT);

    pub const fn bits(self) -> i32 {
        self.0
    }
}

impl std::ops::BitOr for WaitFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// What happened to the process.
#[derive(Debug)]
pub enum ExitCode {
    /// Process called `_exit(status)`. Contains the exit code (0–255).
    Exited(i32),
    /// Process was killed by a signal.
    Killed(i32),
    /// Process was killed by a signal and dumped core.
    Dumped(i32),
    /// Process was stopped by a signal.
    Stopped(i32),
    /// Process was resumed by SIGCONT.
    Continued,
    /// Process hit a trace trap (ptrace).
    Trapped(i32),
}

/// Exit information returned when a watched process changes state.
#[derive(Debug)]
pub struct ExitStatus {
    pub pid: u32,
    pub code: ExitCode,
}

/// Error produced when converting an `ExitStatus` that was not a clean exit.
#[derive(Debug)]
pub struct ExitError {
    pub pid: u32,
    pub code: ExitCode,
}

impl fmt::Display for ExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.code {
            ExitCode::Exited(c) => write!(f, "process {} exited with code {}", self.pid, c),
            ExitCode::Killed(sig) => write!(f, "process {} killed by signal {}", self.pid, sig),
            ExitCode::Dumped(sig) => {
                write!(
                    f,
                    "process {} killed by signal {} (core dumped)",
                    self.pid, sig
                )
            }
            ExitCode::Stopped(sig) => write!(f, "process {} stopped by signal {}", self.pid, sig),
            ExitCode::Continued => write!(f, "process {} continued", self.pid),
            ExitCode::Trapped(sig) => write!(f, "process {} trapped by signal {}", self.pid, sig),
        }
    }
}

impl std::error::Error for ExitError {}

impl ExitStatus {
    /// Returns `Ok(exit_code)` if the process exited normally (`CLD_EXITED`),
    /// otherwise returns `Err(ExitError)`.
    pub fn into_result(self) -> Result<i32, ExitError> {
        match self.code {
            ExitCode::Exited(code) => Ok(code),
            code => Err(ExitError {
                pid: self.pid,
                code,
            }),
        }
    }
}

/// Async future that watches a child process via `IORING_OP_WAITID`.
pub struct WaitProcess {
    id: u32,
    id_type: IdType,
    options: WaitFlags,
    token: Option<OneshotToken>,
    /// `Some` until a cancel hands it to the driver as keepalive.
    siginfo: Option<Box<libc::siginfo_t>>,
}

impl WaitProcess {
    /// Watch a specific PID for termination.
    pub fn from_pid(pid: u32) -> Self {
        Self::new(pid, IdType::Pid, WaitFlags::EXITED)
    }

    /// Watch a `std::process::Child`, consuming it.
    ///
    /// Takes ownership so the caller doesn't accidentally call `child.wait()`
    /// (which would race with the io_uring op).
    pub fn from_child(child: Child) -> Self {
        Self::from_pid(child.id())
    }

    /// Full control: choose id_type and flags.
    pub fn new(id: u32, id_type: IdType, options: WaitFlags) -> Self {
        Self {
            id,
            id_type,
            options,
            token: None,
            siginfo: Some(Box::new(unsafe { std::mem::zeroed() })),
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::WaitId::new(
            self.id_type as libc::idtype_t,
            self.id as libc::id_t,
            self.options.bits(),
        )
        .infop(&**self.siginfo.as_ref().expect("siginfo present") as *const libc::siginfo_t)
        .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.token = Some(token);
    }
}

impl Future for WaitProcess {
    type Output = std::io::Result<ExitStatus>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.token.is_none() {
            self.register_to_driver();
        }
        let waker = cx.waker();
        let token = self.token.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => {
                self.token = None;
                std::task::Poll::Ready(match io_result.result {
                    0.. => {
                        let info = &**self.siginfo.as_ref().expect("siginfo present");
                        let pid = unsafe { info.si_pid() } as u32;
                        let status = unsafe { info.si_status() };
                        let code = match info.si_code {
                            libc::CLD_EXITED => ExitCode::Exited(status),
                            libc::CLD_KILLED => ExitCode::Killed(status),
                            libc::CLD_DUMPED => ExitCode::Dumped(status),
                            libc::CLD_STOPPED => ExitCode::Stopped(status),
                            libc::CLD_CONTINUED => ExitCode::Continued,
                            libc::CLD_TRAPPED => ExitCode::Trapped(status),
                            other => ExitCode::Killed(other),
                        };
                        Ok(ExitStatus { pid, code })
                    }
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                })
            }
        }
    }
}

impl Drop for WaitProcess {
    fn drop(&mut self) {
        if let Some(token) = self.token {
            // The kernel writes the siginfo until the cancelled op's CQE.
            let keep = self.siginfo.take().map(|b| b as Box<dyn std::any::Any>);
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, None))
        }
    }
}
