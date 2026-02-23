use std::time::Duration;

use runtime::{
    http::Method,
    main_thread,
    quic::{H3Server, Http3ServerConfig, HttpRequest, HttpResponse},
    runtime::time::Sleep,
    tracing::new_span,
};

async fn handle(req: HttpRequest) -> HttpResponse {
    let _span = new_span("handling the request");
    Sleep::during(Duration::from_millis(50)).await;
    match (req.method, req.path()) {
        (Method::Get, "/") => HttpResponse::new(200)
            .header("content-type", "text/plain")
            .body("Hello from HTTP/3!"),
        _ => HttpResponse::new(404)
            .header("content-type", "text/plain")
            .body("Not Found"),
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
        server.run(handle).await;
    });
}
