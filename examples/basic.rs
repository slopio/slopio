use runtime::{
    quic::{H3Server, Http3ServerConfig},
    thread_per_core,
};

fn main() {
    thread_per_core(|ctx| {
        Box::pin(async move {
            let config = Http3ServerConfig {
                thread_id: ctx.thread_id,
                num_threads: ctx.num_threads,
                ..Default::default()
            };
            let server = match H3Server::build(config) {
                Ok(server) => server,
                Err(e) => {
                    eprintln!("Failed to build HTTP/3 server: {e}");
                    return;
                }
            };
            println!("HTTP/3 server listening on :4433");
            server.run().await;
        })
    });
}
