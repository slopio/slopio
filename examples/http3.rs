use runtime::{
    main_thread,
    quic::{H3Server, Http3ServerConfig},
    runtime::spawn,
    tracing::{event, new_span},
};

fn main() {
    main_thread(async {
        spawn(async {
            let span1 = new_span("first span").unwrap();
            let owo = spawn(async {
                let span2 = new_span("some second span").unwrap();
                event("hahahaha");
            });
            event("uwu");
            owo.await;
        });
        let config = Http3ServerConfig::default();
        let server = match H3Server::build(config) {
            Ok(server) => server,
            Err(e) => {
                eprintln!("Failed to build HTTP/3 server: {e}");
                return;
            }
        };
        println!("HTTP/3 server listening on :4433");
        server.run().await;
    });
}
