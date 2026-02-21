use std::{
    fmt, io,
    net::SocketAddr,
    os::fd::RawFd,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tquic::{PacketInfo, PacketSendHandler, TlsConfig, TlsConfigSelector, h3::Http3Error};

use crate::{io_driver::IO_DRIVER, runtime::thread_id};

mod balancer;
mod process_request;
mod server;

pub use balancer::register_socket as register_balancer_socket;

#[cfg(feature = "test-monitor")]
pub mod monitor;

pub use server::H3Server;
pub use server::RequestHandler;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Http3ServerError {
    Io(io::Error),
    Quic(tquic::Error),
    Http3(Http3Error),
    Tls(String),
}

impl fmt::Display for Http3ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Quic(e) => write!(f, "QUIC error: {e}"),
            Self::Http3(e) => write!(f, "HTTP/3 error: {e}"),
            Self::Tls(e) => write!(f, "TLS error: {e}"),
        }
    }
}

impl From<io::Error> for Http3ServerError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<tquic::Error> for Http3ServerError {
    fn from(e: tquic::Error) -> Self {
        Self::Quic(e)
    }
}

impl From<Http3Error> for Http3ServerError {
    fn from(e: Http3Error) -> Self {
        Self::Http3(e)
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct Http3ServerConfig {
    pub bind_addr: SocketAddr,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub idle_timeout: Duration,
    pub max_concurrent_streams: u64,
    pub max_body_size: usize,
    pub thread_id: usize,
    pub num_threads: usize,
}

impl Default for Http3ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 4433)),
            cert_path: PathBuf::from("certs/cert.pem"),
            key_path: PathBuf::from("certs/key.pem"),
            idle_timeout: Duration::from_secs(30),
            max_concurrent_streams: 100,
            max_body_size: 8 * 1024 * 1024, // 8 MiB
            thread_id: thread_id(),
            num_threads: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP request/response types
// ---------------------------------------------------------------------------

pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

struct Tls {
    config: Arc<TlsConfig>,
}

impl Tls {
    fn new(cert_path: &Path, key_path: &Path) -> Result<Self, Http3ServerError> {
        let mut tls_config = TlsConfig::new()
            .map_err(|e| Http3ServerError::Tls(format!("failed to create TLS config: {e}")))?;
        tls_config
            .set_certificate_file(cert_path.to_str().unwrap_or(""))
            .map_err(|e| Http3ServerError::Tls(format!("failed to load certificate: {e}")))?;
        tls_config
            .set_private_key_file(key_path.to_str().unwrap_or(""))
            .map_err(|e| Http3ServerError::Tls(format!("failed to load private key: {e}")))?;
        tls_config
            .set_application_protos(vec![b"h3".to_vec()])
            .map_err(|e| Http3ServerError::Tls(format!("failed to set ALPN: {e}")))?;
        Ok(Self {
            config: Arc::new(tls_config),
        })
    }
}

impl TlsConfigSelector for Tls {
    fn get_default(&self) -> Option<Arc<TlsConfig>> {
        Some(self.config.clone())
    }

    fn select(&self, _server_name: &str) -> Option<Arc<TlsConfig>> {
        self.get_default()
    }
}

// ---------------------------------------------------------------------------
// Packet sender
// ---------------------------------------------------------------------------

struct Sender {
    fd: RawFd,
}

impl PacketSendHandler for Sender {
    fn on_packets_send(&self, pkts: &[(Vec<u8>, PacketInfo)]) -> tquic::Result<usize> {
        if pkts.is_empty() {
            return Ok(0);
        }

        IO_DRIVER.with(|driver| {
            let pool = driver.sending_buffers();

            for (data, info) in pkts {
                let entry = pool.add(self.fd, data, info.dst);
                driver.push_anon_op(entry);
            }

            Ok(pkts.len())
        })
    }
}
