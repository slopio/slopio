mod common;

use common::Cv;
use runtime::main_thread;
#[cfg(feature = "test-monitor")]
use runtime::quic::monitor;
use runtime::quic::{H3Server, Http3ServerConfig, HttpRequest, HttpResponse};

async fn handle(req: HttpRequest) -> HttpResponse {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/") => HttpResponse::new(200)
            .header("content-type", "text/plain")
            .body("Hello from HTTP/3!"),
        _ => HttpResponse::new(404)
            .header("content-type", "text/plain")
            .body("Not Found"),
    }
}

use std::net::SocketAddr;
use std::process::Command;
#[cfg(feature = "test-monitor")]
use std::sync::Arc;
#[cfg(feature = "test-monitor")]
use std::sync::atomic::{AtomicUsize, Ordering};
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
    let join = std::thread::spawn(move || {
        main_thread(async move {
            let server = H3Server::build(config).unwrap();
            cv_server.notify();
            server.run(handle).await;
        });
    });
    cv_client.wait();
    join
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
fn test_http3_multithread_stress() {
    use runtime::quic::monitor;
    use runtime::thread_per_core;

    const PORT: u16 = 4510;
    const NUM_REQUESTS: usize = 50;

    let num_threads = std::thread::available_parallelism().unwrap().get();
    let ready_count = Arc::new(AtomicUsize::new(0));
    let (cv_server, cv_client) = Cv::new();

    let ready = ready_count.clone();
    let cv = cv_server.clone();
    let _runtime_thread = std::thread::spawn(move || {
        thread_per_core(move |ctx| {
            let ready = ready.clone();
            let cv = cv.clone();
            Box::pin(async move {
                let config = Http3ServerConfig {
                    bind_addr: SocketAddr::from(([127, 0, 0, 1], PORT)),
                    thread_id: ctx.thread_id,
                    num_threads: ctx.num_threads,
                    ..Default::default()
                };
                let server = H3Server::build(config).unwrap();
                if ready.fetch_add(1, Ordering::SeqCst) + 1 == num_threads {
                    cv.notify();
                }
                server.run(handle).await;
            })
        });
    });

    cv_client.wait();

    // Capture baselines (other tests may run in parallel).
    let base_served = monitor::requests_served();
    let base_per_thread: Vec<_> = (0..num_threads)
        .map(|tid| monitor::requests_served_by_thread(tid))
        .collect();

    // Fire many concurrent requests.
    let request_handles: Vec<_> = (0..NUM_REQUESTS)
        .map(|_| std::thread::spawn(|| curl_http3(PORT, "/")))
        .collect();

    let mut ok_count = 0;
    for h in request_handles {
        let (status, body) = h.join().unwrap();
        assert_eq!(status, 200, "expected 200, got {status}");
        assert_eq!(body, "Hello from HTTP/3!");
        ok_count += 1;
    }
    assert_eq!(ok_count, NUM_REQUESTS);

    // Verify all requests were served.
    let served = monitor::requests_served() - base_served;
    assert!(
        served >= NUM_REQUESTS,
        "expected at least {NUM_REQUESTS} served, got {served}"
    );

    // Verify that more than one thread served requests.
    let mut threads_with_requests = 0;
    for tid in 0..num_threads {
        let thread_served = monitor::requests_served_by_thread(tid) - base_per_thread[tid];
        if thread_served > 0 {
            threads_with_requests += 1;
        }
    }
    assert!(
        threads_with_requests > 1,
        "expected requests on multiple threads, but only {threads_with_requests} thread(s) served traffic"
    );
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
