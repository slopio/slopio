use libc::c_int;

pub mod multi_accept;
pub mod write;

#[derive(Debug)]
pub struct Socket {
    fd: i32,
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe {
            let fd = self.fd as c_int;
            println!("closing fd {fd}");
            libc::close(fd);
        }
    }
}
