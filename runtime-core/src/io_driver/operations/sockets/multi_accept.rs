use std::{
    cell::Cell,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::{IntoRawFd, RawFd},
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};
use runtime_streams::Stream;
use socket2::{Domain, Protocol, Socket as S, Type};

use crate::io_driver::{
    DEFAULT_QUEUE_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult, MultishotCompletionEvent,
    UserData,
    operations::{Continuation, IoOP, MultishotToken, sockets::SocketWrapper},
    sys,
};

pub struct MultiAccept<const QUEUE_SIZE: usize = DEFAULT_QUEUE_SIZE> {
    id: Option<MultishotToken>,
    socket: SocketWrapper,
    sockets: MultiOpQueue,
    cancel_sent: bool,
    /// A fatal error was yielded: every later poll answers `None` without re-arming.
    ended: bool,
}

#[derive(Debug)]
pub enum AcceptError {
    Cancelled,
    Other(i32),
}

#[allow(unreachable_patterns)]
impl AcceptError {
    fn from_result(result: i32) -> Self {
        if result == -libc::ECANCELED {
            AcceptError::Cancelled
        } else {
            AcceptError::Other(-result)
        }
    }

    pub fn continuation(&self) -> Continuation {
        match self {
            // Pausing works by cancelling, so this is the driver's own
            // backpressure coming back to be re-armed.
            Self::Cancelled => Continuation::Continue,
            // `Other` holds a positive errno here, unlike `CompletionError`.
            Self::Other(code) => match *code {
                // ECONNABORTED: the peer left before we took the connection.
                libc::EINTR | libc::EAGAIN | libc::EWOULDBLOCK | libc::ECONNABORTED => {
                    Continuation::Continue
                }
                // Out of descriptors or memory. Another holder has to release
                // one; re-arming on the spot only burns the worker that would.
                libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM => {
                    Continuation::ContinueLater
                }
                _ => Continuation::NotContinue,
            },
        }
    }
}

/// Connections a steering thread accepted, and how many of them the kernel
/// received on the thread's own CPU: the share the steering got right.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SteeringStats {
    pub accepted: u64,
    pub on_cpu: u64,
}

thread_local!(static STEERING: Cell<SteeringStats> = const { Cell::new(SteeringStats { accepted: 0, on_cpu: 0 }) });

/// This thread's [`SteeringStats`] since it started; zero unless it steers.
pub fn steering_stats() -> SteeringStats {
    STEERING.get()
}

fn count_steered(fd: RawFd) {
    let Some(cpu) = crate::runtime::thread_options().and_then(|o| o.incoming_cpu()) else {
        return;
    };
    // Set when the handshake's last packet created the socket: the CPU that received it.
    let on_cpu = sys::incoming_cpu(fd).is_ok_and(|got| got == cpu);
    let mut stats = STEERING.get();
    stats.accepted += 1;
    stats.on_cpu += u64::from(on_cpu);
    STEERING.set(stats);
}

impl TryFrom<MultishotCompletionEvent> for SocketWrapper {
    type Error = AcceptError;

    fn try_from(value: MultishotCompletionEvent) -> Result<Self, Self::Error> {
        if value.result < 0 {
            Err(AcceptError::from_result(value.result))
        } else {
            count_steered(value.result);
            let socket = SocketWrapper::from_fd(value.result);
            let _ = socket.set_nodelay(true);
            Ok(socket)
        }
    }
}

impl MultiAccept<DEFAULT_QUEUE_SIZE> {
    /// Listen on `0.0.0.0:port`.
    pub fn new(port: u16) -> std::io::Result<Self> {
        Self::with_queue_size(port)
    }

    /// Listen on `addr`. A v6 address serves v6 only: bind a second listener
    /// for v4, or use [`MultiAccept::bind_dual_stack`].
    pub fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        Self::bind_with_queue_size(addr)
    }

    /// Listen on `[::]:port` for both families; v4 peers report a v4-mapped
    /// address such as `::ffff:127.0.0.1`.
    pub fn bind_dual_stack(port: u16) -> std::io::Result<Self> {
        Self::bind_dual_stack_with_queue_size(port)
    }
}

impl<const QUEUE_SIZE: usize> MultiAccept<QUEUE_SIZE> {
    pub fn with_queue_size(port: u16) -> std::io::Result<Self> {
        Self::bind_inner(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)), true)
    }

    pub fn bind_with_queue_size(addr: SocketAddr) -> std::io::Result<Self> {
        Self::bind_inner(addr, true)
    }

    pub fn bind_dual_stack_with_queue_size(port: u16) -> std::io::Result<Self> {
        Self::bind_inner(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)), false)
    }

    fn bind_inner(addr: SocketAddr, only_v6: bool) -> std::io::Result<Self> {
        let fd = match sys::simulated_bound_socket(addr) {
            Some(fd) => fd,
            None => {
                let domain = match addr {
                    SocketAddr::V4(_) => Domain::IPV4,
                    SocketAddr::V6(_) => Domain::IPV6,
                };
                let socket = S::new(domain, Type::STREAM, Some(Protocol::TCP))?;
                // Set before bind, or the kernel keeps its `bindv6only` default.
                if addr.is_ipv6() {
                    socket.set_only_v6(only_v6)?;
                }
                // Rebind over TIME_WAIT even when the previous owner ran under another UID.
                socket.set_reuse_address(true)?;
                socket.set_reuse_port(true)?;
                // Before listen, when the socket joins its reuseport group.
                if let Some(cpu) = crate::runtime::thread_options().and_then(|o| o.incoming_cpu()) {
                    socket.set_cpu_affinity(cpu)?;
                }
                socket.set_nonblocking(true)?;
                socket.bind(&addr.into())?;
                socket.listen(1024)?;
                socket.into_raw_fd()
            }
        };

        Ok(Self {
            id: None,
            socket: SocketWrapper::from_fd(fd),
            sockets: MultiOpQueue::new(QUEUE_SIZE),
            cancel_sent: false,
            ended: false,
        })
    }

    // Armed by the first poll, which also leaves `Init`: no CQE can land in between.
    fn ensure_registered(&mut self) {
        if self.id.is_none() {
            self.register_to_driver();
        }
    }

    pub fn socket(&self) -> &SocketWrapper {
        &self.socket
    }

    /// The bound address, including the kernel-chosen port when bound to 0.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn set_backlog(&self, backlog: i32) -> std::io::Result<()> {
        sys::listen(self.socket.fd(), backlog)
    }
}

impl<const QUEUE_SIZE: usize> Drop for MultiAccept<QUEUE_SIZE> {
    fn drop(&mut self) {
        // Close fds queued locally that the user never picked up. Driver-side
        // queue + future-arriving accept CQEs are handled by the cancel call
        // in driver_cleanup via the `AcceptCancelled` state machine.
        self.close_pending_fds();
        self.driver_cleanup();
    }
}

impl<const QUEUE_SIZE: usize> MultiAccept<QUEUE_SIZE> {
    fn close_pending_fds(&mut self) {
        while let Some(event) = self.sockets.pop() {
            if event.result >= 0 {
                sys::close(event.result);
            }
        }
    }
}

impl<const QUEUE_SIZE: usize> Unpin for MultiAccept<QUEUE_SIZE> {}

impl<const QUEUE_SIZE: usize> MultiAccept<QUEUE_SIZE> {
    /// Drain one socket or reportable error from the queue, skipping the rest.
    fn try_drain_one(&mut self) -> Poll<Option<Result<SocketWrapper, AcceptError>>> {
        while let Some(event) = self.sockets.pop() {
            match event.try_into() {
                Ok(socket) => return Poll::Ready(Some(Ok(socket))),
                Err(e) => match e.continuation() {
                    Continuation::Continue => continue,
                    // Armable still: the next poll re-arms, so the consumer
                    // decides when to come back rather than spinning here.
                    Continuation::ContinueLater => return Poll::Ready(Some(Err(e))),
                    // Reported once, then the listener is over.
                    Continuation::NotContinue => {
                        self.ended = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
            }
        }
        Poll::Pending
    }
}

impl<const QUEUE_SIZE: usize> Stream for MultiAccept<QUEUE_SIZE> {
    type Item = Result<SocketWrapper, AcceptError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let shutting_down = IO_DRIVER.with(|d| d.shutdown());

        // Never armed: nothing to cancel, nothing queued.
        if shutting_down && self.id.is_none() {
            return Poll::Ready(None);
        }
        self.ensure_registered();

        // On shutdown, ask io_uring to cancel the multishot accept (once).
        if shutting_down && !self.cancel_sent {
            self.cancel_sent = true;
            let raw = UserData::multi(self.id.as_ref().expect("registered").raw()).into_bits();
            IO_DRIVER.with(|d| d.send_multiop_cancel_event(raw));
        }

        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let this = &mut *self;

        loop {
            match IO_DRIVER.with(|d| {
                d.poll_op_multishot(
                    this.id.as_ref().expect("registered"),
                    waker,
                    &mut this.sockets,
                )
            }) {
                MultiPollResult::Pending => return Poll::Pending,
                MultiPollResult::Ready => {
                    if let Poll::Ready(result) = this.try_drain_one() {
                        return Poll::Ready(result);
                    }
                    // If shutting down and nothing left, we're done.
                    if shutting_down {
                        return Poll::Ready(None);
                    }
                    // Only skippable errors came out: ask again, the op may be over.
                }
                MultiPollResult::NeedsResubmit => {
                    // Drain any remaining sockets queued before the pause.
                    if let Poll::Ready(result) = this.try_drain_one() {
                        return Poll::Ready(result);
                    }
                    // During shutdown, don't resubmit — the stream is done.
                    if shutting_down {
                        return Poll::Ready(None);
                    }
                    let entry = this.to_entry();
                    IO_DRIVER.with(|d| {
                        d.resubmit_multishot(this.id.as_ref().expect("registered"), entry, waker)
                    });
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<const QUEUE_SIZE: usize> IoOP for MultiAccept<QUEUE_SIZE> {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.socket.fd(), QUEUE_SIZE));
        self.id = Some(token);
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::AcceptMulti::new(Fd(self.socket.fd()))
            .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
            .build()
    }

    fn driver_cleanup(&mut self) {
        // Cancel the multishot accept via the accept-aware path so any
        // already-queued or in-flight accept fds get closed instead of
        // leaked. The parked listener ref (we are its only holder) posts the
        // close after the cancel's terminal CQE.
        if let Some(token) = self.id.take() {
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_multishot_op_accept(token, parked));
        }
    }
}
