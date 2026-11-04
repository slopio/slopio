//! TCP server answering `hello` to every connection, one worker per core.
//!
//! cargo run --release -p slopio --example hello -- 8080
//! nc 127.0.0.1 8080

use slopio::net::{MultiAccept, send_all};
use slopio::{spawn, thread_per_core};
use slopio_streams::StreamExt;

fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .expect("usage: hello <port>");

    thread_per_core(move |ctx| {
        Box::pin(async move {
            // One listener per worker; SO_REUSEPORT spreads connections across them.
            let mut accept = MultiAccept::new(port).unwrap();
            eprintln!("worker {} listening on {port}", ctx.thread_id);
            while let Some(socket) = accept.next().await {
                let Ok(socket) = socket else { continue };
                spawn(async move {
                    let _ = send_all(&socket, b"hello\n").await;
                });
            }
        })
    });
}
