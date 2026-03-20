use std::os::unix::io::RawFd;

use crate::io_driver::{IO_DRIVER, operations::OneshotToken};

pub fn create_eventfd() -> RawFd {
    unsafe {
        let efd = libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC);
        if efd < 0 {
            panic!(
                "Failed to create eventfd: {}",
                std::io::Error::last_os_error()
            );
        }
        efd
    }
}

pub fn signal_eventfd(efd: RawFd, value: u64) {
    unsafe {
        let ret = libc::write(efd, &value as *const u64 as *const libc::c_void, 8);
        if ret < 0 {
            //if we can't notify worker threads...better panic
            panic!(
                "Failed to write to eventfd: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

pub fn register_eventfd_read(efd: RawFd) -> OneshotToken {
    IO_DRIVER.with(|driver| driver.push_signal_op(efd))
}
