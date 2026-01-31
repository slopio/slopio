use std::mem::MaybeUninit;

use rustls::server::UnbufferedServerConnection;
use rustls::unbuffered::EncodeError;

use crate::io_driver::operations::sockets::SocketWrapper;
use crate::io_driver::operations::sockets::multi_recv::BorrowedBuffer;
use crate::io_driver::operations::sockets::recv::socket_recv;
use crate::io_driver::operations::sockets::send::socket_send;
use crate::tls::tls_serv_config;

pub struct TlsStream {
    socket: SocketWrapper,
    connection: UnbufferedServerConnection,
    buffer: Box<[MaybeUninit<u8>]>,
    current_buffer: Option<BorrowedBuffer>,
}

impl TlsStream {
    pub async fn write(&mut self, bytes: &[u8]) {
        if let Ok(rustls::unbuffered::ConnectionState::WriteTraffic(mut w)) =
            self.connection.process_tls_records(&mut []).state
        {
            let out = unsafe { self.buffer.assume_init_mut() };
            let encrypted_len = w.encrypt(bytes, out).unwrap();
            socket_send(&self.socket, &out[..encrypted_len])
                .await
                .unwrap();
        }
    }

    pub async fn read(&mut self) -> Result<(), TlsStreamError> {}

    pub async fn close(&mut self) {
        if let Ok(rustls::unbuffered::ConnectionState::WriteTraffic(mut wt)) =
            self.connection.process_tls_records(&mut []).state
        {
            let out = unsafe { self.buffer.assume_init_mut() };
            let amt = wt.queue_close_notify(out).unwrap();
            socket_send(&self.socket, &out[..amt]).await.unwrap();
        }
    }
}

impl Into<TlsStream> for TlsServerConnection {
    fn into(self) -> TlsStream {
        TlsStream {
            connection: self.connection,
            buffer: self.handshake_buffer,
            socket: self.socket,
        }
    }
}

pub struct TlsServerConnection {
    socket: SocketWrapper,
    connection: UnbufferedServerConnection,
    handshake_buffer: Box<[MaybeUninit<u8>]>,
    len: usize,
}

#[derive(Debug)]
pub enum HandShakeError {
    Encoding(EncodeError),
    Rustls(rustls::Error),
}

#[derive(Debug)]
pub enum TlsStreamError {
    HandShake(HandShakeError),
    Other,
}

impl From<EncodeError> for HandShakeError {
    fn from(value: EncodeError) -> Self {
        HandShakeError::Encoding(value)
    }
}

impl From<rustls::Error> for HandShakeError {
    fn from(value: rustls::Error) -> Self {
        HandShakeError::Rustls(value)
    }
}

impl TlsServerConnection {
    pub fn new(socket: SocketWrapper) -> Self {
        let config = tls_serv_config();
        Self {
            socket,
            connection: UnbufferedServerConnection::new(config).unwrap(),
            handshake_buffer: Box::new_uninit_slice(17000),
            len: 0,
        }
    }

    async fn fill_inner_buffer(&mut self) {
        let len = socket_recv(&self.socket, &mut self.handshake_buffer, 17000)
            .await
            .unwrap();
        self.len = len;
    }

    fn reset_internal(&mut self) {
        self.len = 0;
    }

    pub async fn handshake(mut self) -> Result<TlsStream, HandShakeError> {
        self.fill_inner_buffer().await;
        loop {
            let len = self.len;
            let buffer = unsafe { self.handshake_buffer[..len].assume_init_mut() };
            match self.connection.process_tls_records(buffer).state? {
                rustls::unbuffered::ConnectionState::ReadTraffic(_) => todo!(),
                rustls::unbuffered::ConnectionState::PeerClosed => todo!(),
                rustls::unbuffered::ConnectionState::Closed => todo!(),
                rustls::unbuffered::ConnectionState::ReadEarlyData(_) => todo!(),
                rustls::unbuffered::ConnectionState::EncodeTlsData(mut encode_tls_data) => {
                    let out_buf = unsafe { self.handshake_buffer.assume_init_mut() };
                    let amt = encode_tls_data.encode(out_buf)?;
                    socket_send(&self.socket, &out_buf[..amt]).await.unwrap();
                    self.reset_internal();
                }
                rustls::unbuffered::ConnectionState::TransmitTlsData(transmit_tls_data) => {
                    transmit_tls_data.done();
                }
                rustls::unbuffered::ConnectionState::BlockedHandshake => {
                    self.fill_inner_buffer().await;
                }
                rustls::unbuffered::ConnectionState::WriteTraffic(_) => {
                    break;
                }
                _ => todo!(),
            }
        }
        Ok(self.into())
    }
}
