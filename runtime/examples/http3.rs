use std::pin::Pin;
use std::time::Duration;

use runtime::tracing::event;
#[cfg(feature = "tracing")]
use runtime::tracing::{KeyValue, Level, SpanLink, extract, new_span};
use runtime::{
    http::Method,
    main_thread,
    quic::{H3Server, Http3ServerConfig, HttpHandler, HttpResponse, RequestHead},
    runtime::time::Sleep,
    tracing::span_set_status,
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
        let header_count = head.headers.len() as i64;
        let incoming = head
            .headers
            .iter()
            .find(|&&(k, _)| k == "traceparent")
            .and_then(|&(_, v)| extract(v))
            .map(|(trace_id, span_id)| SpanLink { trace_id, span_id });
        Box::pin(async move {
            let _span = new_span("on_headers", Level::Info)
                .attr(KeyValue::new("http.authorized", authorized))
                .attr(KeyValue::new("http.request.header_count", header_count))
                .link_opt(incoming);
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
            let incoming = head
                .headers
                .iter()
                .find(|&&(k, _)| k == "traceparent")
                .and_then(|&(_, v)| extract(v))
                .map(|(trace_id, span_id)| SpanLink { trace_id, span_id });
            let _span = new_span("no_body", Level::Info)
                .attr(KeyValue::new("http.request.method", head.method.as_str()))
                .attr_dyn("http.request.path", head.path)
                .link_opt(incoming);
            match (head.method, head.path) {
                (Method::Get, "/") => {
                    span_set_status(runtime::tracing::Status::Ok);
                    HttpResponse::new(200)
                        .header("content-type", "text/plain")
                        .body("Hello from HTTP/3!")
                }
                _ => {
                    span_set_status(runtime::tracing::Status::Error("404"));
                    HttpResponse::new(404)
                        .header("content-type", "text/plain")
                        .body("Not Found")
                }
            }
        })
    }

    fn buffered_body(
        &self,
        head: RequestHead<'static>,
        body: Box<[u8]>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            let incoming = head
                .headers
                .iter()
                .find(|&&(k, _)| k == "traceparent")
                .and_then(|&(_, v)| extract(v))
                .map(|(trace_id, span_id)| SpanLink { trace_id, span_id });
            let _span = new_span("on body", Level::Info)
                .kind(runtime::tracing::SpanKind::Server)
                .attr(KeyValue::new("http.request.method", head.method.as_str()))
                .attr_dyn("http.request.path", head.path)
                .attr(KeyValue::new("http.request.body_size", body.len() as i64))
                .link_opt(incoming);
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
