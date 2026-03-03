use crate::{
    io_driver::operations::sockets::{
        SocketWrapper, connect_tcp, recv::socket_recv_init, send::socket_send,
    },
    tracing::{proto::ProtoEncodeBuffer, span::SpanWrapper},
};

///the collector receives dropped spans through a channel and processses them
/// there is one collector per worker thread
pub trait Collector: Sized {
    fn init() -> Self;
    fn process(&mut self, batch: Vec<SpanWrapper>) -> impl Future<Output = ()>;
}

pub struct TraceCollector;

impl Collector for TraceCollector {
    fn init() -> Self {
        Self
    }

    async fn process(&mut self, batch: Vec<SpanWrapper>) {
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
    socket: Option<SocketWrapper>,
}

impl TempoExporter {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        let host = addr.to_string();
        Self {
            addr,
            host,
            encode_buf: ProtoEncodeBuffer::default(),
            socket: None,
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

    async fn process(&mut self, batch: Vec<SpanWrapper>) {
        // Encode first — borrows self.encode_buf but not self.socket.
        let body = self.encode_buf.encode(batch);

        let request = format!(
            "POST /v1/traces HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            self.host,
            body.len(),
        );

        // Take the cached socket or open a fresh one.
        let socket = match self.socket.take().or_else(|| {
            connect_tcp(self.addr)
                .map_err(|e| eprintln!("TempoExporter: connect failed: {e}"))
                .ok()
        }) {
            Some(s) => s,
            None => return,
        };

        if let Err(e) = send_all(&socket, request.as_bytes()).await {
            eprintln!("TempoExporter: send request failed: {e}");
            return;
        }
        if let Err(e) = send_all(&socket, body).await {
            eprintln!("TempoExporter: send body failed: {e}");
            return;
        }

        if read_response(&socket).await.is_ok() {
            self.socket = Some(socket);
        }
    }
}

async fn send_all(socket: &SocketWrapper, mut data: &[u8]) -> std::io::Result<()> {
    while !data.is_empty() {
        let n = socket_send(socket, data).await?;
        data = &data[n..];
    }
    Ok(())
}

/// Read an HTTP/1.1 response, consuming exactly the headers + body.
/// Returns `Ok(())` if the response was fully read; `Err(())` on any I/O or parse failure.
async fn read_response(socket: &SocketWrapper) -> Result<(), ()> {
    // Read until we see the end of headers ("\r\n\r\n").
    let mut raw = Vec::with_capacity(512);
    let header_end = loop {
        let before = raw.len();
        raw.resize(before + 512, 0u8);
        let n = socket_recv_init(socket, &mut raw[before..], 512)
            .await
            .map_err(|_| ())?;
        raw.truncate(before + n);
        if n == 0 {
            return Err(());
        }
        if let Some(pos) = find_header_end(&raw) {
            break pos;
        }
    };

    // Parse Content-Length from the headers slice.
    let headers_bytes = &raw[..header_end];
    let content_length = parse_content_length(headers_bytes).ok_or(())?;

    // We may have already read some body bytes past the header boundary.
    let body_start = header_end + 4; // skip \r\n\r\n
    let already_read = raw.len().saturating_sub(body_start);
    let remaining = content_length.saturating_sub(already_read);

    // Drain the remaining body bytes.
    let mut drained = 0;
    let mut buf = vec![0u8; 512];
    while drained < remaining {
        let want = (remaining - drained).min(buf.len());
        let n = socket_recv_init(socket, &mut buf, want)
            .await
            .map_err(|_| ())?;
        if n == 0 {
            return Err(());
        }
        drained += n;
    }

    Ok(())
}

/// Find the byte offset of the start of "\r\n\r\n" in `buf`, or `None`.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Extract the numeric value of the `Content-Length` header, case-insensitive.
fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(headers).ok()?;
    for line in text.split("\r\n") {
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("content-length:") {
            return rest.trim().parse().ok();
        }
    }
    None
}
