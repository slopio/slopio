use std::{
    future::Future,
    mem::MaybeUninit,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::IO_DRIVER;

pub struct UdpSendTo<'a> {
    fd: i32,
    id: Option<usize>,
    buffer: &'a [u8],
    msghdr: MaybeUninit<libc::msghdr>,
    iovec: MaybeUninit<libc::iovec>,
    addr: libc::sockaddr_in,
}

impl<'a> UdpSendTo<'a> {
    pub(super) fn new(fd: i32, buffer: &'a [u8], dest: SocketAddr) -> Self {
        let (ip, port) = match dest {
            SocketAddr::V4(v4) => (*v4.ip(), v4.port()),
            SocketAddr::V6(_) => panic!("IPv6 not supported"),
        };

        Self {
            fd,
            id: None,
            buffer,
            msghdr: MaybeUninit::uninit(),
            iovec: MaybeUninit::uninit(),
            addr: libc::sockaddr_in {
                sin_family: libc::AF_INET as u16,
                sin_port: port.to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from(ip).to_be(),
                },
                sin_zero: [0; 8],
            },
        }
    }

    fn register_to_driver(self: Pin<&mut Self>) {
        let this = unsafe { self.get_unchecked_mut() };

        this.iovec.write(libc::iovec {
            iov_base: this.buffer.as_ptr() as *mut _,
            iov_len: this.buffer.len(),
        });

        this.msghdr.write(libc::msghdr {
            msg_name: &mut this.addr as *mut _ as *mut _,
            msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as u32,
            msg_iov: this.iovec.as_mut_ptr(),
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        });

        let entry = opcode::SendMsg::new(Fd(this.fd), this.msghdr.as_ptr()).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        this.id = Some(token as usize);
    }
}

impl<'a> Future for UdpSendTo<'a> {
    type Output = std::io::Result<usize>;

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
                    len @ 0.. => Poll::Ready(Ok(len as usize)),
                    err => Poll::Ready(Err(std::io::Error::from_raw_os_error(-err))),
                }
            }
        }
    }
}

impl Drop for UdpSendTo<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id, None))
        }
    }
}
