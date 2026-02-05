use std::{cell::UnsafeCell, mem::MaybeUninit, net::SocketAddr};

use io_uring::{opcode, types::Fd};
use slab::Slab;

use super::completion_events::SEND_BUFFER_FLAG;

/// Maximum size for UDP packets (typical MTU minus headers)
pub const SEND_BUFFER_SIZE: usize = 1500;

/// A pre-allocated buffer for sending UDP packets via io_uring.
pub struct SendingBuffer {
    data: [MaybeUninit<u8>; SEND_BUFFER_SIZE],
    len: usize,
    addr: libc::sockaddr_in,
    iovec: MaybeUninit<libc::iovec>,
    msghdr: MaybeUninit<libc::msghdr>,
}

impl SendingBuffer {
    fn new() -> Self {
        Self {
            // SAFETY: MaybeUninit doesn't require initialization
            data: unsafe { MaybeUninit::uninit().assume_init() },
            len: 0,
            addr: libc::sockaddr_in {
                sin_family: libc::AF_INET as u16,
                sin_port: 0,
                sin_addr: libc::in_addr { s_addr: 0 },
                sin_zero: [0; 8],
            },
            iovec: MaybeUninit::uninit(),
            msghdr: MaybeUninit::uninit(),
        }
    }

    /// Returns a mutable slice to write packet data into.
    /// After writing, call `set_len` to mark how many bytes were written.
    #[inline]
    pub fn data_mut(&mut self) -> &mut [MaybeUninit<u8>; SEND_BUFFER_SIZE] {
        &mut self.data
    }

    /// Returns the data as a slice of the valid length.
    /// SAFETY: Caller must ensure `len` bytes have been initialized.
    #[inline]
    pub unsafe fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const u8, self.len) }
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        self.len = len.min(SEND_BUFFER_SIZE);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn set_dest(&mut self, dest: SocketAddr) {
        match dest {
            SocketAddr::V4(v4) => {
                self.addr.sin_family = libc::AF_INET as u16;
                self.addr.sin_port = v4.port().to_be();
                self.addr.sin_addr.s_addr = u32::from(*v4.ip()).to_be();
            }
            SocketAddr::V6(_) => panic!("IPv6 not yet supported"),
        }
    }

    #[inline]
    pub fn set_dest_raw(&mut self, ip_be: u32, port: u16) {
        self.addr.sin_family = libc::AF_INET as u16;
        self.addr.sin_port = port.to_be();
        self.addr.sin_addr.s_addr = ip_be;
    }

    fn prepare_msghdr(&mut self) {
        self.iovec.write(libc::iovec {
            iov_base: self.data.as_mut_ptr() as *mut _,
            iov_len: self.len,
        });

        self.msghdr.write(libc::msghdr {
            msg_name: &mut self.addr as *mut _ as *mut _,
            msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as u32,
            msg_iov: self.iovec.as_mut_ptr(),
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        });
    }

    #[inline]
    pub fn reset(&mut self) {
        self.len = 0;
    }

    #[inline]
    pub fn copy_from(&mut self, src: &[u8]) -> usize {
        let copy_len = src.len().min(SEND_BUFFER_SIZE);
        // SAFETY: MaybeUninit<u8> has the same layout as u8
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                self.data.as_mut_ptr() as *mut u8,
                copy_len,
            );
        }
        self.len = copy_len;
        copy_len
    }
}

/// Manages a slab of sending buffers for UDP operations.
#[derive(Default)]
pub struct SendingBufferPool {
    slab: UnsafeCell<Slab<SendingBuffer>>,
}

impl SendingBufferPool {
    pub fn new() -> Self {
        Self {
            slab: UnsafeCell::new(Slab::new()),
        }
    }

    /// Adds a packet to the pool and returns the io_uring entry ready to submit.
    /// The buffer will be automatically released when the send completes.
    pub fn add(&self, fd: i32, data: &[u8], dest: SocketAddr) -> io_uring::squeue::Entry {
        let slab = unsafe { &mut *self.slab.get() };
        let mut buffer = SendingBuffer::new();
        buffer.copy_from(data);
        buffer.set_dest(dest);
        let index = slab.insert(buffer);

        let buffer = slab.get_mut(index).unwrap();
        buffer.prepare_msghdr();
        opcode::SendMsg::new(Fd(fd), buffer.msghdr.as_ptr())
            .build()
            .user_data((index as u64) | SEND_BUFFER_FLAG)
    }

    /// Releases a buffer by its slab index.
    pub fn release(&self, index: usize) {
        let slab = unsafe { &mut *self.slab.get() };
        if slab.contains(index) {
            slab.remove(index);
        }
    }
}
