use bytes::Bytes;
use http::Response;
use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::runtime::spawn;
use runtime::runtime_h2::io::TokioSocket;
use runtime::runtime_h2::server;
use runtime::runtime_h2::share::{RecvStream, SendStream};
use runtime_streams::StreamExt;

fn main() {
    runtime::main_thread(async {
        let mut listener = MultiAccept::new(8080).unwrap();
        println!("h2 server listening on :8080");

        while let Some(socket) = listener.next().await {
            spawn(async move {
                let io = TokioSocket::new(socket);

                let mut h2 = match server::handshake(io).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        eprintln!("h2 handshake error: {e}");
                        return;
                    }
                };

                while let Some(result) = h2.accept().await {
                    let (req, respond) = match result {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("h2 accept error: {e}");
                            break;
                        }
                    };

                    spawn(handle_request(req, respond));
                }
            });
        }
    });
}

async fn handle_request(req: http::Request<RecvStream>, mut respond: server::SendResponse<Bytes>) {
    let (head, mut body) = req.into_parts();
    println!("{} {}", head.method, head.uri);

    // Drain request body.
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        let _ = body.flow_control().release_capacity(chunk.len());
    }

    let payload = Bytes::from_static(b"Hello from h2 runtime!\n");

    let response = Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .header("content-length", payload.len())
        .body(())
        .unwrap();

    let mut send: SendStream<Bytes> = respond.send_response(response, false).unwrap();
    send.reserve_capacity(payload.len());
    send.send_data(payload, true).unwrap();
    println!("end");
}
