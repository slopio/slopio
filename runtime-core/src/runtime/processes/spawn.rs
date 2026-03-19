use std::io;
use std::os::fd::AsRawFd;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use super::pipe_io::{ReadPipe, WritePipe};
use super::wait_id::{ExitStatus, WaitProcess};

/// A child process in one of two states.
///
/// ```rust,ignore
/// let proc = Process::new("cat", &[])?;
/// // proc is Process::Running — stdin/stdout/stderr available
/// let proc = proc.wait().await?;
/// // proc is Process::Finished — only stdout/stderr for draining
/// ```
pub enum Process {
    Running {
        pid: u32,
        _thread: JoinHandle<()>,
        stdin: Option<ChildStdin>,
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
    },
    Finished {
        status: ExitStatus,
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
    },
}

impl Process {
    /// Spawn a command on a dedicated OS thread.
    /// stdin, stdout, and stderr are all piped.
    pub fn new(program: &str, args: &[&str]) -> io::Result<Self> {
        let mut cmd = Command::new(program);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Self::from_command(cmd)
    }

    /// Spawn from a pre-configured `Command` on a dedicated thread.
    pub fn from_command(cmd: Command) -> io::Result<Self> {
        let (tx, rx) = mpsc::sync_channel::<io::Result<Child>>(1);

        let handle = thread::spawn(move || {
            let mut cmd = cmd;
            let _ = tx.send(cmd.spawn());
        });

        let mut child = rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "spawn thread panicked"))??;

        Ok(Self::Running {
            pid: child.id(),
            _thread: handle,
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
        })
    }

    pub fn pid(&self) -> Option<u32> {
        match self {
            Self::Running { pid, .. } => Some(*pid),
            Self::Finished { .. } => None,
        }
    }

    pub fn stdin_fd(&self) -> Option<i32> {
        match self {
            Self::Running { stdin, .. } => stdin.as_ref().map(|s| s.as_raw_fd()),
            Self::Finished { .. } => None,
        }
    }

    pub fn stdout_fd(&self) -> Option<i32> {
        match self {
            Self::Running { stdout, .. } | Self::Finished { stdout, .. } => {
                stdout.as_ref().map(|s| s.as_raw_fd())
            }
        }
    }

    pub fn stderr_fd(&self) -> Option<i32> {
        match self {
            Self::Running { stderr, .. } | Self::Finished { stderr, .. } => {
                stderr.as_ref().map(|s| s.as_raw_fd())
            }
        }
    }

    /// Async read from the child's stdout into `buf`.
    /// Returns the number of bytes read, or 0 at EOF.
    pub fn read_stdout<'a>(&self, buf: &'a mut [u8]) -> Option<ReadPipe<'a>> {
        self.stdout_fd().map(|fd| ReadPipe::new(fd, buf))
    }

    /// Async read from the child's stderr into `buf`.
    pub fn read_stderr<'a>(&self, buf: &'a mut [u8]) -> Option<ReadPipe<'a>> {
        self.stderr_fd().map(|fd| ReadPipe::new(fd, buf))
    }

    /// Async write to the child's stdin.
    /// Only available while `Running`.
    pub fn write_stdin<'a>(&self, buf: &'a [u8]) -> Option<WritePipe<'a>> {
        self.stdin_fd().map(|fd| WritePipe::new(fd, buf))
    }

    /// Close stdin, sending EOF to the child.
    /// No-op if already closed or if the process is finished.
    pub fn close_stdin(&mut self) {
        if let Self::Running { stdin, .. } = self {
            stdin.take();
        }
    }

    /// Transition from `Running` to `Finished`.
    /// Drops stdin (sends EOF to the child) and async-waits for exit.
    ///
    /// Panics if called on a `Finished` process.
    pub async fn wait(self) -> io::Result<Self> {
        match self {
            Self::Running {
                pid,
                stdout,
                stderr,
                ..
            } => {
                let status = WaitProcess::from_pid(pid).await?;
                Ok(Self::Finished {
                    status,
                    stdout,
                    stderr,
                })
            }
            Self::Finished { .. } => panic!("wait() called on a finished process"),
        }
    }
}
