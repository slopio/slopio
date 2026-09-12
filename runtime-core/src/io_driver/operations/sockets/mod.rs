use libc::c_int;
use std::cell::Cell;
use std::rc::Rc;

use crate::io_driver::ParkedFd;
use std::io;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;

use self::send_control_record::{socket_send_control_record, WriteControlRecord};
use self::send_vectored::{socket_send_vectored, WriteVectoredSocket};
use self::send_vectored_fixed_zc::{socket_send_vectored_fixed_zc, WriteVectoredFixedSocketZc};
use self::send_vectored_zc::{socket_send_vectored_zc, WriteVectoredSocketZc};
use crate::io_driver::PooledBuf;
use self::send_zc::{socket_send_zc, WriteSocketZc};
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
            None => unsafe {
                libc::close(fd);
            },
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

    pub fn send_control_record<'a>(
        &self,
        record_type: u8,
        payload: &'a [u8],
    ) -> WriteControlRecord<'a> {
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
            None => libc::linger { l_onoff: 0, l_linger: 0 },
        };
        self.set_opt(libc::SOL_SOCKET, libc::SO_LINGER, &l)
    }

    /// Local end of the connection (`getsockname`). One syscall per call.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        getname(|sa, len| unsafe { libc::getsockname(self.fd(), sa, len) })
    }

    /// Remote end of the connection (`getpeername`). One syscall per call.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        getname(|sa, len| unsafe { libc::getpeername(self.fd(), sa, len) })
    }

    fn set_opt<T>(&self, level: c_int, name: c_int, value: &T) -> io::Result<()> {
        let ret = unsafe {
            libc::setsockopt(
                self.fd(),
                level,
                name,
                value as *const T as *const libc::c_void,
                std::mem::size_of::<T>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl SocketWrapper {
    pub fn sync_shutdown(self) {
        unsafe {
            libc::shutdown(self.fd(), libc::SHUT_RDWR);
        }
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

fn getname(
    syscall: impl FnOnce(*mut libc::sockaddr, *mut libc::socklen_t) -> c_int,
) -> io::Result<SocketAddr> {
    let mut storage: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::uninit();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let ret = syscall(
        storage.as_mut_ptr() as *mut libc::sockaddr,
        &mut len,
    );
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    let storage = unsafe { storage.assume_init() };
    match storage.ss_family as c_int {
        libc::AF_INET => {
            let sin: libc::sockaddr_in =
                unsafe { *(&storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Ok(SocketAddr::V4(SocketAddrV4::new(
                ip,
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 => {
            let sin6: libc::sockaddr_in6 =
                unsafe { *(&storage as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported address family",
        )),
    }
}
