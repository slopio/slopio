use std::collections::HashMap;

use runtime_core::channels::{Receiver, Sender};
use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, multi_accept::MultiAccept, recv::socket_recv_init, send::socket_send,
};
use runtime_core::runtime::spawn;
use runtime_streams::StreamExt;

use crate::codec::frames::FrameDecoder;
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

pub struct Server {
    port: u16,
}

pub struct StreamSendingEnd {
    sender: Sender<Frame>,
}

pub struct H2Stream {
    receiver: Receiver<Frame>,
    window: u32,
    headers: Headers,
}

impl Server {
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
    // 1. Read client connection preface (24 bytes magic)
    let mut preface_buf = [0u8; 24];
    recv_exact(&socket, &mut preface_buf).await?;
    if &preface_buf[..] != CLIENT_PREFACE {
        return Err("invalid client preface".into());
    }

    // 2. Send server SETTINGS
    send_all(&socket, SERVER_SETTINGS).await?;
    let mut connection = Connection::new(socket);
    connection.run().await.unwrap();

    // loop {
    //     let len = buf.len();
    //     let n = socket_recv_init(&socket, &mut buf, len).await?;
    //     if n == 0 {
    //         break; // EOF
    //     }

    //     decoder.decode(&buf[..n]).map_err(|e| format!("{e:?}"))?;

    //     while let Some(frame) = decoder.frames.pop_front() {
    //         match frame {
    //             Frame::Settings { ack, ref settings } => {
    //                 if !ack {
    //                     // ACK the client's SETTINGS
    //                     println!("sending settings");
    //                     send_all(&socket, SETTINGS_ACK).await?;
    //                 }
    //             }
    //             Frame::Ping { ack, opaque } => {
    //                 if !ack {
    //                     // Respond with PING ACK
    //                     let mut pong = [0u8; 17];
    //                     pong[3] = 0x06; // type = PING
    //                     pong[4] = 0x01; // flags = ACK
    //                     // length = 8
    //                     pong[2] = 8;
    //                     pong[9..17].copy_from_slice(&opaque);
    //                     println!("sending ping");
    //                     send_all(&socket, &pong).await?;
    //                 }
    //             }
    //             Frame::Headers(ref h) => {
    //                 let stream_id = u32::from(h.stream_id());
    //                 let pseudo = h.pseudo();
    //                 eprintln!(
    //                     "stream {stream_id}: {} {} {}",
    //                     pseudo.method.as_ref().map(|m| m.as_str()).unwrap_or("?"),
    //                     pseudo.scheme.as_ref().map(|s| &**s).unwrap_or("?"),
    //                     pseudo.path.as_ref().map(|p| &**p).unwrap_or("?"),
    //                 );
    //                 if let Some(ref authority) = pseudo.authority {
    //                     eprintln!("  :authority {}", &**authority);
    //                 }
    //                 for (name, value) in h.fields() {
    //                     eprintln!("  {}: {}", name, value.to_str().unwrap_or("<binary>"));
    //                 }
    //                 // TODO: dispatch to request handler
    //             }
    //             Frame::GoAway { .. } => {
    //                 println!("go away");
    //                 break;
    //             }
    //             Frame::Data {
    //                 stream_id,
    //                 end_stream,
    //                 data,
    //             } => {
    //                 println!("received data");
    //             }
    //             Frame::RstStream {
    //                 stream_id,
    //                 error_code,
    //             } => {
    //                 println!("reset");
    //             }
    //             Frame::WindowUpdate {
    //                 stream_id,
    //                 increment,
    //             } => {
    //                 println!("received window {stream_id} {increment}");
    //             }
    //         }
    //     }
    // }

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
