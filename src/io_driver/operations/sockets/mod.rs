use libc::c_int;

pub mod managed_recv;
pub mod managed_recv_bundle;
pub mod multi_accept;
pub mod multi_recv;
pub mod recv;
pub mod send;

#[derive(Debug)]
pub struct SocketWrapper {
    pub fd: i32,
}

impl SocketWrapper {
    pub fn fd(&self) -> i32 {
        self.fd
    }
}

impl Drop for SocketWrapper {
    fn drop(&mut self) {
        unsafe {
            let fd = self.fd as c_int;
            libc::close(fd); //synchronous close to avoid fd conflicts
        }
    }
}
