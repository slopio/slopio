#![cfg(feature = "quic")]
mod common;

use std::pin::Pin;

use common::Cv;
use runtime::http::Method;
use runtime::main_thread;
#[cfg(feature = "test-monitor")]
use runtime::quic::monitor;
use runtime::quic::{H3Server, Http3ServerConfig, HttpHandler, HttpResponse, RequestHead};

struct Handler;

impl HttpHandler for Handler {
    fn on_headers(
        &self,
        _head: &RequestHead,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HttpResponse>> + 'static>> {
        Box::pin(async { Ok(()) })
    }

    fn no_body(
        &self,
        head: RequestHead<'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            match (head.method, head.path) {
                (Method::Get, "/") => HttpResponse::new(200)
                    .header("content-type", "text/plain")
                    .body("Hello from HTTP/3!"),
                _ => HttpResponse::new(404)
                    .header("content-type", "text/plain")
                    .body("Not Found"),
            }
        })
    }

    fn buffered_body(
        &self,
        head: RequestHead,
        _body: Box<[u8]>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            let _ = head;
            HttpResponse::new(404)
                .header("content-type", "text/plain")
                .body("Not Found")
        })
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

enum RequestBody {
    /// -d <data>  →  curl sets content-length automatically
    Data(String),
    /// --data-binary @-  with stdin  →  curl sets content-length
    Binary(Vec<u8>),
    /// -H "Transfer-Encoding: chunked" + -d  →  no content-length
    Chunked(String),
    /// --data-binary with a body >= 8000 bytes  →  triggers Streamed path
    Large(Vec<u8>),
}

struct CurlRequest {
    port: u16,
    path: String,
    method: Option<String>,
    headers: Vec<(String, String)>,
    body: Option<RequestBody>,
}

impl CurlRequest {
    fn get(port: u16, path: &str) -> Self {
        Self {
            port,
            path: path.to_string(),
            method: None,
            headers: vec![],
            body: None,
        }
    }

    fn post(port: u16, path: &str) -> Self {
        Self {
            port,
            path: path.to_string(),
            method: Some("POST".into()),
            headers: vec![],
            body: None,
        }
    }

    fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    /// Small text body — curl sends with content-length → Buffered path
    fn data(mut self, data: &str) -> Self {
        self.body = Some(RequestBody::Data(data.to_string()));
        self
    }

    /// Raw binary body with content-length → Buffered path
    fn binary(mut self, data: Vec<u8>) -> Self {
        self.body = Some(RequestBody::Binary(data));
        self
    }

    /// Chunked transfer (no content-length) → Streamed path
    fn chunked(mut self, data: &str) -> Self {
        self.body = Some(RequestBody::Chunked(data.to_string()));
        self
    }

    /// Body >= 8000 bytes (no content-length inferred) → Streamed path
    fn large(mut self, data: Vec<u8>) -> Self {
        self.body = Some(RequestBody::Large(data));
        self
    }

    fn send(self) -> (u16, String) {
        let (status, bytes) = self.send_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn send_bytes(self) -> (u16, Vec<u8>) {
        for attempt in 0..10 {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            let (status, bytes) = self.do_send();
            if status != 0 {
                return (status, bytes);
            }
        }
        (0, vec![])
    }

    fn do_send(&self) -> (u16, Vec<u8>) {
        let url = format!("https://localhost:{}{}", self.port, self.path);
        let mut cmd = Command::new("/snap/bin/curl");
        cmd.args([
            "--http3",
            "-k",
            "-s",
            "-o",
            "-",
            "-w",
            "\n%{http_code}",
            "--max-time",
            "5",
        ]);
        if let Some(method) = &self.method {
            cmd.args(["-X", method]);
        }
        for (k, v) in &self.headers {
            cmd.args(["-H", &format!("{k}: {v}")]);
        }
        match &self.body {
            None => {}
            Some(RequestBody::Data(s)) => {
                cmd.args(["-d", s]);
            }
            Some(RequestBody::Binary(_)) | Some(RequestBody::Large(_)) => {
                cmd.args(["--data-binary", "@-"]);
            }
            Some(RequestBody::Chunked(s)) => {
                cmd.args(["-H", "Transfer-Encoding: chunked", "-d", s]);
            }
        }
        cmd.arg(&url);
        let stdin_data: Option<Vec<u8>> = match &self.body {
            Some(RequestBody::Binary(b)) | Some(RequestBody::Large(b)) => Some(b.clone()),
            _ => None,
        };
        if stdin_data.is_some() {
            cmd.stdin(std::process::Stdio::piped());
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().expect("failed to spawn curl");
        if let Some(data) = stdin_data {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(&data).unwrap();
        }
        let output = child.wait_with_output().expect("failed to wait for curl");
        let stdout = output.stdout;
        // The last line is the http_code written by -w "\n%{http_code}"
        let split_pos = stdout.iter().rposition(|&b| b == b'\n').unwrap_or(0);
        let (body, status_line) = stdout.split_at(split_pos);
        let status: u16 = String::from_utf8_lossy(&status_line[1..])
            .trim()
            .parse()
            .unwrap_or(0);
        (status, body.to_vec())
    }
}

fn curl_http3(port: u16, path: &str) -> (u16, String) {
    CurlRequest::get(port, path).send()
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
            server.run(Handler).await;
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
                server.run(Handler).await;
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

    let (status, body) = CurlRequest::post(4505, "/").data("hello").send();
    assert_eq!(status, 404);
    assert_eq!(body, "Not Found");
}

// --- Auth handler (mirrors the http3 example) ---

struct AuthHandler;

impl HttpHandler for AuthHandler {
    fn on_headers(
        &self,
        head: &RequestHead,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HttpResponse>> + 'static>> {
        let authorized = head
            .headers
            .iter()
            .any(|&(k, v)| k == "password" && v == "uwu");
        Box::pin(async move {
            if authorized {
                Ok(())
            } else {
                Err(HttpResponse::new(401))
            }
        })
    }

    fn no_body(
        &self,
        head: RequestHead<'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            match (head.method, head.path) {
                (Method::Get, "/") => HttpResponse::new(200)
                    .header("content-type", "text/plain")
                    .body("Hello from HTTP/3!"),
                _ => HttpResponse::new(404)
                    .header("content-type", "text/plain")
                    .body("Not Found"),
            }
        })
    }

    fn buffered_body(
        &self,
        _head: RequestHead<'static>,
        _body: Box<[u8]>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async { HttpResponse::new(404).body("Not Found") })
    }
}

fn start_auth_server(port: u16) -> std::thread::JoinHandle<()> {
    let (cv_server, cv_client) = Cv::new();
    let join = std::thread::spawn(move || {
        main_thread(async move {
            let config = Http3ServerConfig {
                bind_addr: SocketAddr::from(([127, 0, 0, 1], port)),
                ..Default::default()
            };
            let server = H3Server::build(config).unwrap();
            cv_server.notify();
            server.run(AuthHandler).await;
        });
    });
    cv_client.wait();
    join
}

#[test]
fn test_auth_missing_password_returns_401() {
    let _handle = start_auth_server(4520);

    let (status, _) = CurlRequest::get(4520, "/").send();
    assert_eq!(status, 401);
}

#[test]
fn test_auth_wrong_password_returns_401() {
    let _handle = start_auth_server(4521);

    let (status, _) = CurlRequest::get(4521, "/")
        .header("password", "wrong")
        .send();
    assert_eq!(status, 401);
}

#[test]
fn test_auth_correct_password_returns_200() {
    let _handle = start_auth_server(4522);

    let (status, body) = CurlRequest::get(4522, "/").header("password", "uwu").send();
    assert_eq!(status, 200);
    assert_eq!(body, "Hello from HTTP/3!");
}

// --- Echo handler ---

struct EchoHandler;

impl HttpHandler for EchoHandler {
    fn on_headers(
        &self,
        _head: &RequestHead,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HttpResponse>> + 'static>> {
        Box::pin(async { Ok(()) })
    }

    fn no_body(
        &self,
        _head: RequestHead<'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async { HttpResponse::new(200).body("") })
    }

    fn buffered_body(
        &self,
        _head: RequestHead<'static>,
        body: Box<[u8]>,
    ) -> Pin<Box<dyn std::future::Future<Output = HttpResponse> + 'static>> {
        Box::pin(async move {
            HttpResponse::new(200)
                .header("content-type", "application/octet-stream")
                .body(body.into_vec())
        })
    }
}

fn start_echo_server(port: u16) -> std::thread::JoinHandle<()> {
    let (cv_server, cv_client) = Cv::new();
    let join = std::thread::spawn(move || {
        main_thread(async move {
            let config = Http3ServerConfig {
                bind_addr: SocketAddr::from(([127, 0, 0, 1], port)),
                ..Default::default()
            };
            let server = H3Server::build(config).unwrap();
            cv_server.notify();
            server.run(EchoHandler).await;
        });
    });
    cv_client.wait();
    join
}

#[test]
fn test_echo_body() {
    let _handle = start_echo_server(4530);

    let (status, body) = CurlRequest::post(4530, "/")
        .data("hello from the test")
        .send();
    assert_eq!(status, 200);
    assert_eq!(body, "hello from the test");
}

#[test]
fn test_echo_binary_body() {
    let _handle = start_echo_server(4531);

    let payload: Vec<u8> = (0u8..=255).collect();
    let (status, body) = CurlRequest::post(4531, "/")
        .binary(payload.clone())
        .send_bytes();
    assert_eq!(status, 200);
    assert_eq!(body, payload);
}
