mod common;

use common::Cv;
use runtime::main_thread;
#[cfg(feature = "test-monitor")]
use runtime::quic::monitor;
use runtime::quic::{H3Server, Http3ServerConfig};
use std::net::SocketAddr;
use std::process::Command;
#[cfg(feature = "test-monitor")]
use std::time::Duration;

fn curl_http3(port: u16, path: &str) -> (u16, String) {
    let url = format!("https://localhost:{port}{path}");
    let output = Command::new("/snap/bin/curl")
        .args([
            "--http3",
            "-k",
            "-s",
            "-o",
            "-",
            "-w",
            "\n%{http_code}",
            "--max-time",
            "5",
            &url,
        ])
        .output()
        .expect("failed to run curl");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let (body, status_line) = stdout.rsplit_once('\n').unwrap_or(("", &stdout));
    let status: u16 = status_line.trim().parse().unwrap_or(0);
    (status, body.to_string())
}

fn server_config(port: u16) -> Http3ServerConfig {
    Http3ServerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], port)),
        ..Default::default()
    }
}

fn start_server(port: u16) -> std::thread::JoinHandle<()> {
    start_server_with_config(server_config(port))
}

fn start_server_with_config(config: Http3ServerConfig) -> std::thread::JoinHandle<()> {
    let (cv_server, cv_client) = Cv::new();
    let handle = std::thread::spawn(move || {
        main_thread(async move {
            let server = H3Server::build(config).unwrap();
            cv_server.notify();
            server.run().await;
        });
    });
    cv_client.wait();
    handle
}

#[test]
fn test_http3_get_root() {
    let _handle = start_server(4501);
    let (status, body) = curl_http3(4501, "/");
    assert_eq!(status, 200);
    assert_eq!(body, "Hello from HTTP/3!");
}

#[test]
fn test_http3_not_found() {
    let _handle = start_server(4502);
    let (status, body) = curl_http3(4502, "/nonexistent");
    assert_eq!(status, 404);
    assert_eq!(body, "Not Found");
}

#[test]
#[cfg(feature = "test-monitor")]
fn test_http3_idle_timeout() {
    let config = Http3ServerConfig {
        bind_addr: SocketAddr::from(([127, 0, 0, 1], 4506)),
        idle_timeout: Duration::from_millis(500),
        ..Default::default()
    };
    let _handle = start_server_with_config(config);

    // Capture baseline — other tests may run in parallel on shared counters.
    let base_opened = monitor::connections_opened();
    let base_served = monitor::requests_served();
    let base_closed = monitor::connections_closed();

    // First request succeeds, connection established.
    let (status, body) = curl_http3(4506, "/");
    assert_eq!(status, 200);
    assert_eq!(body, "Hello from HTTP/3!");

    assert!(monitor::connections_opened() >= base_opened + 1);
    assert!(monitor::requests_served() >= base_served + 1);
    assert_eq!(monitor::connections_closed(), base_closed);

    // Wait for idle timeout to fire and QUIC close handshake to complete.
    std::thread::sleep(Duration::from_secs(2));

    assert!(monitor::connections_closed() >= base_closed + 1);

    // Server still accepts new connections after the timeout path ran.
    let (status, body) = curl_http3(4506, "/");
    assert_eq!(status, 200);
    assert_eq!(body, "Hello from HTTP/3!");

    assert!(monitor::connections_opened() >= base_opened + 2);
    assert!(monitor::requests_served() >= base_served + 2);
}

#[test]
fn test_http3_multiple_requests() {
    let _handle = start_server(4503);

    let (status1, body1) = curl_http3(4503, "/");
    assert_eq!(status1, 200);
    assert_eq!(body1, "Hello from HTTP/3!");

    let (status2, body2) = curl_http3(4503, "/missing");
    assert_eq!(status2, 404);
    assert_eq!(body2, "Not Found");
}

#[test]
fn test_http3_concurrent_connections() {
    let _handle = start_server(4504);

    let handles: Vec<_> = (0..3)
        .map(|_| std::thread::spawn(|| curl_http3(4504, "/")))
        .collect();

    for h in handles {
        let (status, body) = h.join().unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, "Hello from HTTP/3!");
    }
}

#[test]
fn test_http3_post_request() {
    let _handle = start_server(4505);

    let url = "https://localhost:4505/";
    let output = Command::new("/snap/bin/curl")
        .args([
            "--http3",
            "-k",
            "-s",
            "-X",
            "POST",
            "-d",
            "hello",
            "-o",
            "-",
            "-w",
            "\n%{http_code}",
            "--max-time",
            "5",
            url,
        ])
        .output()
        .expect("failed to run curl");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let (body, status_line) = stdout.rsplit_once('\n').unwrap_or(("", &stdout));
    let status: u16 = status_line.trim().parse().unwrap_or(0);
    assert_eq!(status, 404);
    assert_eq!(body, "Not Found");
}
