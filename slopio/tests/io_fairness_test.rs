use std::{io::Write, time::Duration};

use slopio::net::{connect_tcp, socket_recv};

/// A task that never stops being ready must not hold IO completions back.
#[test]
fn ready_task_does_not_starve_io_completions() {
    let listener = std::net::TcpListener::bind("127.0.0.1:9196").unwrap();
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        s.write_all(b"hi").unwrap();
        std::thread::sleep(Duration::from_secs(3));
    });

    let waited = slopio::main_thread(async {
        let sock = connect_tcp("127.0.0.1:9196".parse().unwrap())
            .await
            .unwrap();
        let spinner = slopio::spawn(async {
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                slopio::yield_now().await;
            }
        });
        let start = std::time::Instant::now();
        let (res, _buf) = socket_recv(&sock, vec![0u8; 16]).await;
        res.unwrap();
        let waited = start.elapsed();
        spinner.kill();
        waited
    });

    assert!(
        waited < Duration::from_millis(500),
        "data sent at 100ms was received after {waited:?}"
    );
}
