use std::net::SocketAddr;

use runtime_core::tracing::proto::ProtoEncodeBuffer;
use runtime_core::tracing::{Collector, SpanWrapper};

use super::H2Connection;

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

    async fn get_connection(&mut self) -> Option<&mut H2Connection> {
        if self.conn.is_none() {
            match H2Connection::connect(self.addr, self.authority.clone()).await {
                Ok(c) => self.conn = Some(c),
                Err(e) => {
                    eprintln!("OtlpCollector: h2 connect failed: {e:?}");
                    return None;
                }
            }
        }
        self.conn.as_mut()
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
        let payload = self.encode_buf.encode(batch).to_vec();

        let Some(conn) = self.get_connection().await else {
            return;
        };

        match conn.grpc_unary(OTLP_GRPC_PATH, payload).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("OtlpCollector: grpc export failed: {e:?}");
                self.conn = None;
            }
        }
    }
}
