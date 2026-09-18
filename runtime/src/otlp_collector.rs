use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use runtime_core::runtime::retry::{RetryError, retry};
use runtime_core::tracing::collector::otlp::protobuf_encoder::ProtoEncodeBuffer;
use runtime_core::tracing::{Collector, SpanWrapper};

use runtime_h2::client::H2Connection;

const OTLP_GRPC_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

const DEFAULT_OTLP_ENDPOINT: &str = "192.168.64.1:4317";

/// Transport-level configuration for the OTLP collector. Set once before
/// starting the runtime; subsequent calls have no effect.
///
/// This collector speaks **OTLP/gRPC only** (not OTLP/HTTP). Point it at the
/// collector's gRPC listener — typically port 4317 — not the HTTP one (4318).
pub struct OtlpConfig {
    /// gRPC endpoint of the OTLP collector (e.g. `127.0.0.1:4317`).
    ///
    /// Must be the gRPC port, **not** the OTLP/HTTP port. Pointing this at an
    /// HTTP endpoint will produce confusing connection-level errors rather
    /// than a clear protocol mismatch.
    pub endpoint: SocketAddr,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_OTLP_ENDPOINT
                .parse()
                .expect("default OTLP gRPC address is valid"),
        }
    }
}

static OTLP_CONFIG: OnceLock<OtlpConfig> = OnceLock::new();

/// Set the OTLP transport config. Call before `main_thread` / `thread_per_core`.
/// Has no effect if called more than once.
pub fn set_otlp_config(config: OtlpConfig) {
    let _ = OTLP_CONFIG.set(config);
}

fn otlp_config() -> &'static OtlpConfig {
    OTLP_CONFIG.get_or_init(OtlpConfig::default)
}

pub struct OtlpCollector {
    addr: SocketAddr,
    authority: String,
    encode_buf: ProtoEncodeBuffer,
    conn: Option<H2Connection>,
}

impl OtlpCollector {
    pub fn new(addr: SocketAddr) -> Self {
        let authority = addr.to_string();
        Self {
            addr,
            authority,
            encode_buf: ProtoEncodeBuffer::default(),
            conn: None,
        }
    }

    async fn get_connection(&mut self) -> Option<&mut H2Connection> {
        if self.conn.is_none() {
            let addr = self.addr;
            let authority = self.authority.clone();
            match retry(
                None,
                Duration::from_secs(5),
                || H2Connection::connect(addr, authority.clone()),
                |_| true,
            )
            .await
            {
                Ok(conn) => self.conn = Some(conn),
                Err(RetryError::Shutdown) => return None,
                Err(RetryError::Failed(_)) => unreachable!(),
            }
        }
        self.conn.as_mut()
    }
}

impl Collector for OtlpCollector {
    fn init() -> Self {
        Self::new(otlp_config().endpoint)
    }

    async fn process(&mut self, batch: Vec<SpanWrapper>) {
        let Some(_) = self.get_connection().await else {
            return;
        };
        self.encode_buf.encode(batch);
        let body = std::mem::take(&mut self.encode_buf.buf);

        let conn = self.conn.as_mut().unwrap();
        // Export failure drops the connection; the next batch reconnects.
        if conn.grpc_unary(OTLP_GRPC_PATH, body).await.is_err() {
            self.conn = None;
        }
    }
}
