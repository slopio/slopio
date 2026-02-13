use runtime::{
    main_thread,
    quic::{H3Server, Http3ServerConfig},
};

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
        server.run().await;
    });
}
