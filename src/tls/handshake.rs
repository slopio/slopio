use rustls::server::UnbufferedServerConnection;
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncryptError, InsufficientSizeError, UnbufferedStatus,
    WriteTraffic,
};
use std::sync::Arc;

use crate::tls::tls_serv_config;

pub struct TlsServerConnection {
    conn: UnbufferedServerConnection,
    incoming_buffer: [u8; 4096],
    // spillover: Vec<u8>,
}

impl TlsServerConnection {
    fn new() -> Self {
        let config = tls_serv_config();
        Self {
            conn: UnbufferedServerConnection::new(config).unwrap(),
            incoming_buffer: [0u8; 4096],
        }
    }

    async fn handshake(&mut self) {
        self.conn.process_tls_records(&mut self.incoming_buffer);
    }
}
