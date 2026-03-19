use std::net::SocketAddr;
use std::time::Duration;

use runtime_core::runtime::retry::retry;
use runtime_core::tracing::collector::otlp::protobuf_encoder::ProtoEncodeBuffer;
use runtime_core::tracing::{Collector, SpanWrapper};

use runtime_h2::client::H2Connection;

const OTLP_GRPC_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

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

    async fn get_connection(&mut self) -> &mut H2Connection {
        if self.conn.is_none() {
            let addr = self.addr;
            let authority = self.authority.clone();
            let conn = retry(
                None,
                Duration::from_secs(5),
                || H2Connection::connect(addr, authority.clone()),
                |_| true,
            )
            .await
            .unwrap(); // None max means it retries forever, so it always succeeds
            self.conn = Some(conn);
        }
        self.conn.as_mut().unwrap()
    }
}

impl Collector for OtlpCollector {
    fn init() -> Self {
        let addr: SocketAddr = "192.168.64.1:4317"
            .parse()
            .expect("default OTLP gRPC address is valid");
        Self::new(addr)
    }

    async fn process(&mut self, batch: Vec<SpanWrapper>) {
        self.get_connection().await;
        self.encode_buf.encode(batch);
        let body = std::mem::take(&mut self.encode_buf.buf);

        let conn = self.conn.as_mut().unwrap();
        match conn.grpc_unary(OTLP_GRPC_PATH, body).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("OtlpCollector: grpc export failed: {e:?}");
                self.conn = None;
            }
        }
    }
}
