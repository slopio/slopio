use runtime_core::runtime::main_thread;
use runtime_h2::server::Server;

fn main() {
    main_thread(async {
        let server = Server::new(8443);
        println!("H2 server listening on :8443");
        server.run().await;
    });
}
