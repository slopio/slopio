use std::{
    net::SocketAddr,
    os::fd::RawFd,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tquic::{PacketInfo, PacketSendHandler, TlsConfig, TlsConfigSelector};

use runtime_core::{io_driver::IO_DRIVER, runtime::thread_id};

use super::error::Http3ServerError;

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

pub(super) struct Tls {
    config: Arc<TlsConfig>,
}

impl Tls {
    pub(super) fn new(cert_path: &Path, key_path: &Path) -> Result<Self, Http3ServerError> {
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

pub(super) struct QuicOutput {
    pub(super) fd: RawFd,
}

impl PacketSendHandler for QuicOutput {
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
