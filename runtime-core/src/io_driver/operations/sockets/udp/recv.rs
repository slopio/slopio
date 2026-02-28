use std::{
    future::Future,
    mem::MaybeUninit,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::IO_DRIVER;

pub struct UdpRecvFrom<'a> {
    fd: i32,
    id: Option<usize>,
    buffer: &'a mut [MaybeUninit<u8>],
    // These must stay pinned for the duration of the operation
    msghdr: MaybeUninit<libc::msghdr>,
    iovec: MaybeUninit<libc::iovec>,
    addr: MaybeUninit<libc::sockaddr_in>,
}

impl<'a> UdpRecvFrom<'a> {
    pub(super) fn new(fd: i32, buffer: &'a mut [MaybeUninit<u8>]) -> Self {
        Self {
            fd,
            id: None,
            buffer,
            msghdr: MaybeUninit::uninit(),
            iovec: MaybeUninit::uninit(),
            addr: MaybeUninit::uninit(),
        }
    }

    fn register_to_driver(self: Pin<&mut Self>) {
        let this = unsafe { self.get_unchecked_mut() };

        // Initialize iovec
        this.iovec.write(libc::iovec {
            iov_base: this.buffer.as_mut_ptr().cast(),
            iov_len: this.buffer.len(),
        });

        // Initialize msghdr
        this.msghdr.write(libc::msghdr {
            msg_name: this.addr.as_mut_ptr().cast(),
            msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as u32,
            msg_iov: this.iovec.as_mut_ptr(),
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        });

        let entry = opcode::RecvMsg::new(Fd(this.fd), this.msghdr.as_mut_ptr()).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        this.id = Some(token as usize);
    }
}

pub struct UdpPacket {
    len: usize,
    addr: SocketAddr,
}

impl UdpPacket {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl<'a> Future for UdpRecvFrom<'a> {
    type Output = std::io::Result<UdpPacket>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.as_mut().register_to_driver();
        }

        let waker = cx.waker();
        let id = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
                self.id = None;
                match io_result.result {
                    len @ 0.. => {
                        let addr = unsafe { self.addr.assume_init_ref() };
                        let ip = Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr));
                        let port = u16::from_be(addr.sin_port);
                        Poll::Ready(Ok(UdpPacket {
                            len: len as usize,
                            addr: SocketAddr::V4(SocketAddrV4::new(ip, port)),
                        }))
                    }
                    err => Poll::Ready(Err(std::io::Error::from_raw_os_error(-err))),
                }
            }
        }
    }
}

impl Drop for UdpRecvFrom<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id, None))
        }
    }
}
