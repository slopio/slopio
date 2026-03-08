use runtime_core::channels::{Receiver, Sender};
use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, multi_accept::MultiAccept, recv::socket_recv_init, send::socket_send,
};
use runtime_core::runtime::spawn;
use runtime_streams::StreamExt;

use crate::codec::frames::headers::Headers;
use crate::server::connection::Connection;
use crate::types::Frame;

mod connection;
mod stream;

const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Initial SETTINGS frame: empty (use defaults), no ACK
const SERVER_SETTINGS: &[u8] = &[
    0, 0, 0,    // length = 0
    0x04, // type = SETTINGS
    0x00, // flags = 0
    0, 0, 0, 0, // stream_id = 0
];

/// SETTINGS ACK frame
pub(crate) const SETTINGS_ACK: &[u8] = &[
    0, 0, 0,    // length = 0
    0x04, // type = SETTINGS
    0x01, // flags = ACK
    0, 0, 0, 0, // stream_id = 0
];

pub struct H2Server {
    port: u16,
}

pub struct StreamSendingEnd {
    window: u32,
    sender: Sender<Frame>,
}

pub struct H2Stream {
    receiver: Receiver<Frame>,
    headers: Headers,
    output: std::rc::Rc<Sender<Frame>>,
}

impl H2Stream {}

impl H2Server {
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    pub async fn run(&self) {
        let mut listener = MultiAccept::new(self.port).unwrap();

        while let Some(socket) = listener.next().await {
            spawn(async move {
                if let Err(e) = handle_connection(socket).await {
                    eprintln!("h2 connection error: {e}");
                }
            });
        }
    }
}

async fn handle_connection(socket: SocketWrapper) -> Result<(), Box<dyn std::error::Error>> {
    let mut preface_buf = [0u8; 24];
    recv_exact(&socket, &mut preface_buf).await?;
    if &preface_buf[..] != CLIENT_PREFACE {
        return Err("invalid client preface".into());
    }
    send_all(&socket, SERVER_SETTINGS).await?;
    let mut connection = Connection::new(socket);
    connection.run().await.unwrap();
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
