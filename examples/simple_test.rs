// use runtime::tls::handshake::TlsServerConnection;
// use runtime::{io_driver::operations::sockets::multi_accept::MultiAccept, main_thread};
// use runtime_streams::StreamExt;

// const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nsalut";

// fn main() {
//     main_thread(async {
//         let mut multiaccept = MultiAccept::new(9091).unwrap();
//         while let Some(socket) = multiaccept.next().await {
//             let c = TlsServerConnection::new(socket);
//             let mut stream = c.handshake().await.expect("handshaked failed");
//             stream.read().await;
//             stream.write(RESPONSE).await;
//             stream.close().await;
//         }
//     });
// }

fn main() {}
