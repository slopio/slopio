use libc::c_int;
pub mod multi_accept;
pub mod multi_recv_bundle;
pub mod recv;
pub mod write;

#[derive(Debug)]
pub struct SocketWrapper {
    pub fd: i32,
}
pub trait AsyncRead {
    type Reader<'a>;
    fn recv<'a>(&self, buffer: &'a mut [u8], len: u32) -> Self::Reader<'a>;
}

pub trait MultiAsyncRead {
    type MultiRecv;
    fn multi_recv(&self) -> Self::MultiRecv;
}

pub trait AsyncWrite {
    type Writer<'a>;
    fn send<'a>(&self, data: &'a [u8]) -> Self::Writer<'a>;
}

impl Drop for SocketWrapper {
    fn drop(&mut self) {
        unsafe {
            let fd = self.fd as c_int;
            libc::close(fd);
        }
    }
}
