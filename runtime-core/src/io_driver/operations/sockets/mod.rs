use libc::c_int;
use std::cell::Cell;
use std::rc::Rc;

use crate::io_driver::ParkedFd;
use crate::io_driver::sys;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use self::send_control_record::{WriteControlRecord, socket_send_control_record};
use self::send_vectored::{WriteVectoredSocket, socket_send_vectored};
use self::send_vectored_fixed_zc::{WriteVectoredFixedSocketZc, socket_send_vectored_fixed_zc};
use self::send_vectored_zc::{WriteVectoredSocketZc, socket_send_vectored_zc};
use self::send_zc::{WriteSocketZc, socket_send_zc};
use crate::io_driver::PooledBuf;
use crate::io_driver::operations::IoBuf;

pub mod connect;
pub mod managed_recv;
pub mod multi_accept;
pub mod multi_recv;
pub mod recv;
pub mod send;
pub mod send_control_record;
pub mod send_fixed;
pub mod send_fixed_zc;
pub mod send_vectored;
pub mod send_vectored_fixed_zc;
pub mod send_vectored_zc;
pub mod send_zc;
pub mod split;
pub mod udp;

/// Owns the fd. The wrapper, each split half, and every prepared or in-flight
/// operation hold a strong ref, so no SQE can ever name a closed fd.
/// An op dropped mid-flight parks its ref in the cancelled slab slot
/// ([`park`]), so zero refs means no op is in flight on the fd at all and
/// the close is strictly the last thing queued for it.
#[derive(Debug)]
pub(crate) struct SocketInner {
    fd: i32,
    /// `msg_flags` for the sends on this socket that do not pass their own.
    /// See [`SocketWrapper::set_default_msg_flags`].
    msg_flags: Cell<u32>,
}

impl SocketInner {
    pub(crate) fn fd(&self) -> i32 {
        self.fd
    }
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        let fd = self.fd;
        // Close through the ring when inside the runtime; synchronously
        // otherwise (socket dropped outside the driver scope).
        crate::io_driver::IO_DRIVER.try_with(|driver| match driver {
            Some(d) => d.close_fd(fd),
            None => crate::io_driver::sys::close(fd),
        });
    }
}

#[derive(Debug)]
pub struct SocketWrapper {
    inner: Rc<SocketInner>,
}

/// The only way to build a `SocketWrapper` from a bare descriptor.
///
/// # Safety
///
/// The wrapper takes ownership: `fd` must be an open socket that nothing
/// else will close; the fd is closed when the wrapper and every operation
/// prepared on it are gone.
impl std::os::fd::FromRawFd for SocketWrapper {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        Self::from_fd(fd)
    }
}

impl SocketWrapper {
    pub(crate) fn from_fd(fd: i32) -> Self {
        Self {
            inner: Rc::new(SocketInner {
                fd,
                msg_flags: Cell::new(send_vectored::DEFAULT_MSG_FLAGS),
            }),
        }
    }

    /// A strong ref for an operation to hold until completion.
    pub(crate) fn share(&self) -> OpSocket {
        OpSocket(Some(self.inner.clone()))
    }

    /// A second wrapper over the same fd, for `into_split`. Not a `Clone`
    /// impl: user code must not duplicate exclusive handles.
    pub(crate) fn share_wrapper(&self) -> SocketWrapper {
        SocketWrapper {
            inner: self.inner.clone(),
        }
    }

    /// Same socket, not just the same fd number.
    pub(crate) fn same_socket(&self, other: &SocketWrapper) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    /// A parked ref by clone, for handles that must survive the cancellation
    /// (a multishot's owner, which `into_inner` may hand back to the user).
    pub(crate) fn park(&self) -> ParkedFd {
        ParkedFd::Socket(self.inner.clone())
    }

    /// Park by consuming the wrapper — for ops that own their socket outright.
    pub(crate) fn into_parked(self) -> ParkedFd {
        ParkedFd::Socket(self.inner)
    }

    pub fn fd(&self) -> i32 {
        self.inner.fd
    }

    /// The `msg_flags` every send on this socket uses unless it passes its own.
    /// Starts at [`DEFAULT_MSG_FLAGS`](send_vectored::DEFAULT_MSG_FLAGS).
    ///
    /// Read it to extend rather than replace what the socket was configured
    /// with: `socket.default_msg_flags() | MSG_MORE`.
    pub fn default_msg_flags(&self) -> u32 {
        self.inner.msg_flags.get()
    }

    /// Set that default. Not a `setsockopt` — it is carried per send, and is
    /// shared by both halves of an `into_split`.
    ///
    /// Set `0` after attaching the TLS ULP: `tls_sw_sendmsg` rejects
    /// `MSG_WAITALL` (and every other flag outside its allowed set) with
    /// `EOPNOTSUPP`, which would fail every send on the socket.
    pub fn set_default_msg_flags(&self, flags: u32) {
        self.inner.msg_flags.set(flags);
    }

    pub fn vectored_write<B: IoBuf>(&self, bufs: Vec<B>) -> WriteVectoredSocket<B> {
        socket_send_vectored(self, bufs)
    }

    /// Zero-copy send (`IORING_OP_SEND_ZC`): transmits directly from `buf`
    /// without copying it into socket memory, returning the buffer once the
    /// kernel's notification confirms it is free. Best for large payloads.
    pub fn send_zc<B: IoBuf>(&self, buf: B) -> WriteSocketZc<B> {
        socket_send_zc(self, buf)
    }

    /// Zero-copy gather send (`IORING_OP_SENDMSG_ZC`): every chunk goes out
    /// in one submission straight from user memory, returned once the
    /// kernel's notification confirms all of them are free.
    pub fn send_vectored_zc<B: IoBuf>(&self, bufs: Vec<B>) -> WriteVectoredSocketZc<B> {
        socket_send_vectored_zc(self, bufs)
    }

    /// Zero-copy gather send from pre-registered buffers: one submission for
    /// every chunk, and no per-request page pinning. All chunks must come from
    /// the same [`FixedSendBufPool`].
    pub fn send_vectored_fixed_zc(&self, bufs: Vec<PooledBuf>) -> WriteVectoredFixedSocketZc {
        socket_send_vectored_fixed_zc(self, bufs)
    }

    pub fn send_control_record<B: IoBuf>(
        &self,
        record_type: u8,
        payload: B,
    ) -> WriteControlRecord<B> {
        socket_send_control_record(self, record_type, payload)
    }

    pub fn set_nodelay(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_NODELAY, &(on as c_int))
    }

    pub fn set_keepalive(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, &(on as c_int))
    }

    pub fn set_reuseaddr(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_REUSEADDR, &(on as c_int))
    }

    pub fn set_reuseport(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_REUSEPORT, &(on as c_int))
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_RCVBUF, &(size as c_int))
    }

    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_SNDBUF, &(size as c_int))
    }

    pub fn set_defer_accept(&self, secs: u32) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_DEFER_ACCEPT, &(secs as c_int))
    }

    pub fn set_fastopen(&self, qlen: u32) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_FASTOPEN, &(qlen as c_int))
    }

    pub fn set_quickack(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_QUICKACK, &(on as c_int))
    }

    pub fn set_linger(&self, dur: Option<Duration>) -> io::Result<()> {
        let l = match dur {
            Some(d) => libc::linger {
                l_onoff: 1,
                l_linger: d.as_secs().min(c_int::MAX as u64) as c_int,
            },
            None => libc::linger {
                l_onoff: 0,
                l_linger: 0,
            },
        };
        self.set_opt(libc::SOL_SOCKET, libc::SO_LINGER, &l)
    }

    /// Local end of the connection (`getsockname`). One syscall per call.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        sys::local_addr(self.fd())
    }

    /// Remote end of the connection (`getpeername`). One syscall per call.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        sys::peer_addr(self.fd())
    }

    fn set_opt<T>(&self, level: c_int, name: c_int, value: &T) -> io::Result<()> {
        sys::setsockopt(self.fd(), level, name, value)
    }
}

impl SocketWrapper {
    pub fn sync_shutdown(self) {
        let _ = sys::shutdown(self.fd(), libc::SHUT_RDWR);
        // self is dropped here; the fd closes once no operation holds a ref
    }
}

/// An op's ref to its socket, movable into the cancelled slab slot. Parking
/// is unconditional: releasing at drop instead would let the wrapper's later
/// drop post the close while this op's cancel is still in flight.
#[derive(Debug)]
pub(crate) struct OpSocket(Option<Rc<SocketInner>>);

impl OpSocket {
    pub(crate) fn fd(&self) -> i32 {
        self.0.as_ref().expect("taken only in Drop").fd()
    }

    /// Move the ref out for parking. Only from `Drop` — the op must not
    /// touch the socket afterwards.
    pub(crate) fn park(&mut self) -> ParkedFd {
        ParkedFd::Socket(self.0.take().expect("parked twice"))
    }
}
