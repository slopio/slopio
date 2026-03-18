use runtime_core::net::{SocketWrapper, socket_recv_init, socket_send};
use std::io;

pub async fn send_all(socket: &SocketWrapper, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        let n = socket_send(socket, data).await?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        data = &data[n..];
    }
    Ok(())
}

pub async fn recv_exact(socket: &SocketWrapper, buf: &mut [u8]) -> io::Result<()> {
    let mut pos = 0;
    while pos < buf.len() {
        let remaining = buf.len() - pos;
        let n = socket_recv_init(socket, &mut buf[pos..], remaining).await?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        pos += n;
    }
    Ok(())
}
