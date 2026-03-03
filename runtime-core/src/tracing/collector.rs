use crate::{
    io_driver::operations::sockets::{SocketWrapper, connect_tcp, send::socket_send},
    tracing::{proto::ProtoEncodeBuffer, span::SpanBuf},
};

pub trait Collector: Sized {
    fn init() -> Self;
    fn process(&mut self, batch: Vec<SpanBuf>) -> impl Future<Output = ()>;
}

pub struct TraceCollector;

impl Collector for TraceCollector {
    fn init() -> Self {
        Self
    }

    async fn process(&mut self, batch: Vec<SpanBuf>) {
        for buf in &batch {
            let trace_id = u128::from_be_bytes(buf.data.trace_id.to_bytes());
            let span_id = u64::from_be_bytes(buf.data.span_id.to_bytes());
            eprintln!(
                "[trace] {:032x} {:016x} {}",
                trace_id, span_id, buf.data.name
            );
        }
    }
}

pub struct TempoExporter {
    addr: std::net::SocketAddr,
    host: String,
    encode_buf: ProtoEncodeBuffer,
}

impl TempoExporter {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        let host = addr.to_string();
        Self {
            addr,
            host,
            encode_buf: ProtoEncodeBuffer::default(),
        }
    }
}

impl Collector for TempoExporter {
    fn init() -> Self {
        let addr = "192.168.64.1:4318"
            .parse()
            .expect("default Tempo OTLP address is valid");
        Self::new(addr)
    }

    async fn process(&mut self, batch: Vec<SpanBuf>) {
        let body = self.encode_buf.encode(batch);

        let request = format!(
            "POST /v1/traces HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.host,
            body.len(),
        );

        let socket = match connect_tcp(self.addr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("TempoExporter: connect failed: {e}");
                return;
            }
        };

        if let Err(e) = send_all(&socket, request.as_bytes()).await {
            eprintln!("TempoExporter: send request failed: {e}");
            return;
        }
        if let Err(e) = send_all(&socket, body).await {
            eprintln!("TempoExporter: send body failed: {e}");
            return;
        }

        drain_response(&socket).await;
    }
}

async fn send_all(socket: &SocketWrapper, mut data: &[u8]) -> std::io::Result<()> {
    while !data.is_empty() {
        let n = socket_send(socket, data).await?;
        data = &data[n..];
    }
    Ok(())
}

async fn drain_response(socket: &SocketWrapper) {
    use crate::io_driver::operations::sockets::recv::socket_recv_init;
    let mut buf = vec![0u8; 1024];
    loop {
        let len = buf.len();
        match socket_recv_init(socket, &mut buf, len).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
}
