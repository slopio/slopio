use runtime::{
    main_thread,
    quic::{H3Server, Http3ServerConfig, HttpRequest, HttpResponse},
};

async fn handle(req: HttpRequest) -> HttpResponse {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/") => HttpResponse::new(200)
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
