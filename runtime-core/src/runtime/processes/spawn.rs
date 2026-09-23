use std::io;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use super::pipe_io::{PipeFd, ReadPipe, WritePipe};
use super::wait_id::{ExitStatus, WaitProcess};
use crate::io_driver::operations::{IoBuf, IoBufMut};

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
        stdin: Option<PipeFd>,
        stdout: Option<PipeFd>,
        stderr: Option<PipeFd>,
    },
    Finished {
        status: ExitStatus,
        stdout: Option<PipeFd>,
        stderr: Option<PipeFd>,
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
            stdin: child.stdin.take().map(PipeFd::new),
            stdout: child.stdout.take().map(PipeFd::new),
            stderr: child.stderr.take().map(PipeFd::new),
        })
    }

    pub fn pid(&self) -> Option<u32> {
        match self {
            Self::Running { pid, .. } => Some(*pid),
            Self::Finished { .. } => None,
        }
    }

    pub fn stdin(&self) -> Option<&PipeFd> {
        match self {
            Self::Running { stdin, .. } => stdin.as_ref(),
            Self::Finished { .. } => None,
        }
    }

    pub fn stdout(&self) -> Option<&PipeFd> {
        match self {
            Self::Running { stdout, .. } | Self::Finished { stdout, .. } => stdout.as_ref(),
        }
    }

    pub fn stderr(&self) -> Option<&PipeFd> {
        match self {
            Self::Running { stderr, .. } | Self::Finished { stderr, .. } => stderr.as_ref(),
        }
    }

    pub fn stdin_fd(&self) -> Option<i32> {
        match self {
            Self::Running { stdin, .. } => stdin.as_ref().map(PipeFd::fd),
            Self::Finished { .. } => None,
        }
    }

    pub fn stdout_fd(&self) -> Option<i32> {
        match self {
            Self::Running { stdout, .. } | Self::Finished { stdout, .. } => {
                stdout.as_ref().map(PipeFd::fd)
            }
        }
    }

    pub fn stderr_fd(&self) -> Option<i32> {
        match self {
            Self::Running { stderr, .. } | Self::Finished { stderr, .. } => {
                stderr.as_ref().map(PipeFd::fd)
            }
        }
    }

    /// Async read from the child's stdout into `buf`, handed back on completion.
    /// Returns the number of bytes read, or 0 at EOF.
    pub fn read_stdout<B: IoBufMut>(&self, buf: B) -> Option<ReadPipe<B>> {
        self.stdout().map(|p| p.read(buf))
    }

    /// Async read from the child's stderr into `buf`.
    pub fn read_stderr<B: IoBufMut>(&self, buf: B) -> Option<ReadPipe<B>> {
        self.stderr().map(|p| p.read(buf))
    }

    /// Async write to the child's stdin.
    /// Only available while `Running`.
    pub fn write_stdin<B: IoBuf>(&self, buf: B) -> Option<WritePipe<B>> {
        self.stdin().map(|p| p.write(buf))
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
