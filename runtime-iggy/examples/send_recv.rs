// Usage: cargo run -p runtime-iggy --example send_recv -- <HOST>
// Example: cargo run -p runtime-iggy --example send_recv -- 192.168.1.10

use runtime_core::runtime::main_thread;
use runtime_iggy::{Identifier, IggyClient, IggyMessage};

const STREAM_ID: Identifier = Identifier::Numeric(0);
const TOPIC_ID: Identifier = Identifier::Numeric(0);
const PARTITION_ID: u32 = 0;

fn main() {
    let host = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let addr: std::net::SocketAddr = format!("{host}:8090").parse().expect("invalid address");

    main_thread(async move {
        println!("Connecting to {addr}...");

        let mut client = IggyClient::connect(addr, "iggy", "Secret123")
            .await
            .expect("failed to connect");

        println!("Connected and authenticated.");

        // --- SEND ---
        let messages = [
            IggyMessage::new(b"hello from runtime-iggy".to_vec()),
            IggyMessage::new(b"second message".to_vec()),
            IggyMessage::new(b"third message".to_vec()),
        ];

        client
            .send_messages(STREAM_ID, TOPIC_ID, PARTITION_ID, &messages)
            .await
            .expect("send_messages failed");

        println!("Sent {} messages.", messages.len());

        // --- POLL (only the messages we just sent) ---
        let received = client
            .poll_messages_last(STREAM_ID, TOPIC_ID, PARTITION_ID, messages.len() as u32)
            .await
            .expect("poll_messages failed");

        println!("Received {} messages:", received.len());
        for msg in &received {
            let text = std::str::from_utf8(&msg.payload).unwrap_or("<binary>");
            println!("  offset={} id={} payload={:?}", msg.offset, msg.id, text);
        }
    });
}
