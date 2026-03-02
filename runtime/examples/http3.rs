use std::time::Duration;
use std::{borrow::Cow, pin::Pin};

use runtime::tracing::event;
#[cfg(feature = "tracing")]
use runtime::tracing::{Level, new_span};
use runtime::{
    http::Method,
    main_thread,
    quic::{H3Server, Http3ServerConfig, HttpHandler, HttpResponse, RequestHead},
    runtime::time::Sleep,
    tracing::span_set_status,
    yield_now,
};

struct Handler;

impl HttpHandler for Handler {
    fn on_headers(
        &self,
        head: &RequestHead<'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HttpResponse>> + 'static>> {
        let authorized = head
            .headers
            .iter()
            .any(|&(k, v)| k == "password" && v == "uwu");
        Box::pin(async move {
            let _span = new_span("on_headers", Level::Info);
            event("accepted!");
            Sleep::during(Duration::from_millis(1)).await;
            if authorized {
                Ok(())
            } else {
                Err(HttpResponse::new(401))
            }
        })
    }

    fn no_body(
        &self,
        head: RequestHead<'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            match (head.method, head.path) {
                (Method::Get, "/") => HttpResponse::new(200)
                    .header("content-type", "text/plain")
                    .body("Hello from HTTP/3!"),
                _ => {
                    span_set_status(runtime::tracing::Status::Error {
                        description: Cow::Borrowed("404"),
                    });
                    HttpResponse::new(404)
                        .header("content-type", "text/plain")
                        .body("Not Found")
                }
            }
        })
    }

    fn buffered_body(
        &self,
        _head: RequestHead<'static>,
        body: Box<[u8]>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            let _span = new_span("on body", Level::Info).kind(runtime::tracing::SpanKind::Server);
            Sleep::during(Duration::from_millis(1)).await;
            HttpResponse::new(200)
                .header("content-type", "application/octet-stream")
                .body(body.into_vec())
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
