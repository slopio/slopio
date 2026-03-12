use runtime_core::channels::Sender;
use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, multi_accept::MultiAccept, recv::socket_recv_init, send::socket_send,
};
use runtime_core::runtime::spawn;
use runtime_streams::StreamExt;

use crate::server::connection::Connection;
use crate::server::stream::Handler;
use crate::server::stream::events_in::InputStreamEvent;

mod connection;
pub(crate) mod output;
pub mod stream;

pub use stream::{Stream, StreamError};

const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub(crate) const MAX_CONCURRENT_STREAMS: u32 = 100;

/// Initial SETTINGS frame with MAX_CONCURRENT_STREAMS=100
const SERVER_SETTINGS: &[u8] = &[
    0, 0, 6,    // length = 6 (one setting: id(2) + value(4))
    0x04, // type = SETTINGS
    0x00, // flags = 0
    0, 0, 0, 0, // stream_id = 0
    // SETTINGS_MAX_CONCURRENT_STREAMS (0x3) = 100
    0x00, 0x03, 0x00, 0x00, 0x00, 0x64,
];

pub struct H2Server<H> {
    port: u16,
    handler: H,
}

pub enum StreamState {
    Open,
    HalfClosedRemote,
    Closed,
}

pub struct StreamMapState {
    sender: Sender<InputStreamEvent>,
    state: StreamState,
}

impl<H: Handler + Clone + 'static> H2Server<H> {
    pub fn new(port: u16, handler: H) -> Self {
        Self { port, handler }
    }

    pub async fn run(&self) {
        let mut listener = MultiAccept::new(self.port).unwrap();

        while let Some(socket) = listener.next().await {
            let handler = self.handler.clone();
            spawn(async move { handle_connection(socket, handler).await });
        }
    }
}

async fn handle_connection<H: Handler + Clone + 'static>(
    socket: SocketWrapper,
    handler: H,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut preface_buf = [0u8; 24];
    recv_exact(&socket, &mut preface_buf).await?;
    if &preface_buf[..] != CLIENT_PREFACE {
        socket.sync_shutdown();
        return Err("invalid client preface".into());
    }
    send_all(&socket, SERVER_SETTINGS).await?;
    let mut connection = Connection::new(socket, handler);
    connection.run().await?;
    Ok(())
}

async fn recv_exact(socket: &SocketWrapper, buf: &mut [u8]) -> std::io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let remaining = buf.len() - filled;
        let n = socket_recv_init(socket, &mut buf[filled..], remaining).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed during preface",
            ));
        }
        filled += n;
    }
    Ok(())
}

async fn send_all(socket: &SocketWrapper, data: &[u8]) -> std::io::Result<()> {
    let mut sent = 0;
    while sent < data.len() {
        let n = socket_send(socket, &data[sent..]).await?;
        sent += n;
    }
    Ok(())
}
