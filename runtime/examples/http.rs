use runtime::net::MultiAccept;
use runtime::runtime::spawn;
use runtime_http::http1::HttpConnection;
use runtime_http::http1::response::Response;
use runtime_streams::StreamExt;

fn main() {
    runtime::main_thread(async {
        let mut listener = MultiAccept::new(8080).unwrap();
        println!("Listening on :8080");

        while let Some(socket) = listener.next().await {
            spawn(async move {
                let mut conn = HttpConnection::new(socket);
                loop {
                    match conn.recv().await {
                        Ok(Some(_req)) => {
                            let resp = Response::new(conn.arena(), 200, "OK")
                                .header("Content-Type", "text/plain")
                                .body(b"Hello from runtime!\n");
                            if conn.send(resp).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break, // clean EOF
                        Err(e) => {
                            eprintln!("parse error: {e}");
                            break;
                        }
                    }
                }
            });
        }
    });
}
