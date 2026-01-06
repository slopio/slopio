use libc::c_int;
pub mod multi_accept;
pub mod recv;
pub mod write;

#[derive(Debug)]
pub struct Socket {
    fd: i32,
}
pub trait AsyncRead {
    type Reader<'a>;
    fn recv<'a>(&self, buffer: &'a mut [u8], len: u32) -> Self::Reader<'a>;
}

pub trait AsyncWrite {
    type Writer<'a>;
    fn send<'a>(&self, data: &'a [u8]) -> Self::Writer<'a>;
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe {
            let fd = self.fd as c_int;
            libc::close(fd);
        }
    }
}
