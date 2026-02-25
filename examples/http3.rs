use std::pin::Pin;
use std::time::Duration;

use runtime::{
    http::Method,
    main_thread,
    quic::{H3Server, Http3ServerConfig, HttpHandler, HttpResponse, RequestHead},
    runtime::time::Sleep,
    tracing::new_span,
};

struct Handler;

impl HttpHandler for Handler {
    fn on_headers(
        &self,
        _head: &RequestHead,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HttpResponse>> + 'static>> {
        Box::pin(async { Ok(()) })
    }

    fn no_body(
        &self,
        head: RequestHead,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            let _span = new_span("handling the request");
            Sleep::during(Duration::from_millis(50)).await;
            match (head.method, head.path()) {
                (Method::Get, "/") => HttpResponse::new(200)
                    .header("content-type", "text/plain")
                    .body("Hello from HTTP/3!"),
                _ => HttpResponse::new(404)
                    .header("content-type", "text/plain")
                    .body("Not Found"),
            }
        })
    }

    fn buffered_body(
        &self,
        head: RequestHead,
        _body: Box<[u8]>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            let _span = new_span("handling the request");
            Sleep::during(Duration::from_millis(50)).await;
            let _ = head;
            HttpResponse::new(404)
                .header("content-type", "text/plain")
                .body("Not Found")
        })
    }
}

fn main() {
    main_thread(async {
        let config = Http3ServerConfig::default();
        let server = match H3Server::build(config) {
            Ok(server) => server,
            Err(e) => {
                eprintln!("Failed to build HTTP/3 server: {e}");
                return;
            }
        };
        println!("HTTP/3 server listening on :4433");
        server.run(Handler).await;
    });
}
