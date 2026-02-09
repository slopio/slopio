use runtime::{main_thread, quic::Http3Server};

fn main() {
    main_thread(async {
        let server = Http3Server::build();
        println!("HTTP/3 server listening on :4433");
        server.run().await;
    });
}
