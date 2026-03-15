use http::{HeaderMap, Method};
use runtime_core::runtime::main_thread;
use runtime_h2::client::H2Client;

fn main() {
    main_thread(async {
        let mut client = H2Client::new();
        let mut conn = client
            .connect("localhost", 8443)
            .await
            .expect("failed to connect");

        let stream = conn
            .request(Method::GET, "/", HeaderMap::new(), None)
            .expect("failed to create stream");
        let response = stream.recv_response().await.expect("request failed");

        println!("Status: {}", response.status);
        dbg!(response.headers);
        println!("Body: {}", String::from_utf8_lossy(&response.body));
    });
}
