use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt, io,
    net::SocketAddr,
    os::fd::RawFd,
    path::{Path, PathBuf},
    ptr::NonNull,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;

use tquic::{
    Config, Connection, Endpoint, PacketInfo, PacketSendHandler, TlsConfig, TlsConfigSelector,
    TransportHandler,
    h3::{Header, Http3Config, Http3Error, Http3Event, connection::Http3Connection},
};

use runtime_streams::StreamExt;

use crate::runtime::spawn;
use crate::{
    io_driver::{
        IO_DRIVER,
        operations::sockets::udp::{UdpMultiRecv, UdpSocket},
    },
    quic::handler::handle_request,
};
mod handler;
mod process_request;
use process_request::Http3Request;

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

pub struct Http3ServerConfig {
    pub bind_addr: SocketAddr,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub idle_timeout: Duration,
    pub max_concurrent_streams: u64,
    pub max_body_size: usize,
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
        }
    }
}

pub struct Http3Server {
    endpoint: Endpoint,
    socket: UdpSocket,
    config: Rc<Http3ServerConfig>,
    handler: Rc<RefCell<Handler>>,
}

impl Http3Server {
    pub async fn run(mut self) {
        let mut recv = UdpMultiRecv::new(self.socket.fd());
        while let Some(datagram) = recv.next().await {
            let info = PacketInfo {
                src: datagram.addr(),
                time: Instant::now(),
                dst: self.config.bind_addr,
            };
            if let Err(e) = self.endpoint.recv(&mut datagram.payload().to_vec(), &info) {
                eprintln!("QUIC recv error: {e}");
                continue;
            }
            if let Err(e) = self.endpoint.process_connections() {
                eprintln!("QUIC process_connections error: {e}");
            }

            self.handler.borrow_mut().process_http3();
        }
    }

    pub fn build(server_config: Http3ServerConfig) -> Result<Self, Http3ServerError> {
        let udp = UdpSocket::bind(server_config.bind_addr.port())?;
        let mut config = Box::new(Config::new()?);
        let tls = Tls::new(&server_config.cert_path, &server_config.key_path)?;

        config.set_max_idle_timeout(server_config.idle_timeout.as_millis() as u64);
        config.set_initial_max_streams_bidi(server_config.max_concurrent_streams);
        config.set_tls_config_selector(Arc::new(tls));

        let server_config = Rc::new(server_config);
        let handler = Rc::new(RefCell::new(Handler::new(server_config.clone())?));
        let sender = Rc::new(Sender { fd: udp.fd() });
        let endpoint = Endpoint::new(
            config,
            true,
            Box::new(HandlerBridge(handler.clone())),
            sender,
        );

        Ok(Self {
            endpoint,
            socket: udp,
            config: server_config,
            handler,
        })
    }
}

/// HTTP/3 request representation
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// HTTP/3 response builder
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

/// Per-connection state holding the Http3Connection
pub(super) struct ConnectionState {
    h3_conn: Http3Connection,
    pub connection_streams: HashMap<u64, usize>,
    conn_ptr: Option<NonNull<Connection>>,
}

impl ConnectionState {
    pub fn send_response(&mut self, stream_id: u64, response: HttpResponse) {
        let Some(mut conn_ptr) = self.conn_ptr else {
            return;
        };
        let conn = unsafe { conn_ptr.as_mut() };

        let mut headers = vec![Header::new(
            b":status",
            response.status.to_string().as_bytes(),
        )];

        for (name, value) in &response.headers {
            headers.push(Header::new(name.as_bytes(), value.as_bytes()));
        }

        let has_body = !response.body.is_empty();

        if let Err(e) = self
            .h3_conn
            .send_headers(conn, stream_id, &headers, !has_body)
        {
            eprintln!("Failed to send headers: {e}");
            return;
        }

        if has_body
            && let Err(e) =
                self.h3_conn
                    .send_body(conn, stream_id, Bytes::from(response.body), true)
        {
            eprintln!("Failed to send body: {e}");
        }
    }
}

struct Handler {
    h3_config: Http3Config,
    connections: HashMap<u64, Rc<RefCell<ConnectionState>>>,
    connections_to_check: VecDeque<u64>,
    config: Rc<Http3ServerConfig>,
}

impl Handler {
    fn new(config: Rc<Http3ServerConfig>) -> Result<Self, Http3ServerError> {
        Ok(Self {
            h3_config: Http3Config::new()?,
            connections: HashMap::new(),
            connections_to_check: VecDeque::new(),
            config,
        })
    }

    fn process_http3(&mut self) {
        while let Some(key) = self.connections_to_check.pop_front() {
            let Some(conn_state) = self.connections.get(&key) else {
                continue;
            };
            loop {
                let mut state = conn_state.borrow_mut();
                let Some(mut conn_ptr) = state.conn_ptr else {
                    break;
                };
                let conn = unsafe { conn_ptr.as_mut() };
                match state.h3_conn.poll(conn) {
                    Ok((stream_id, Http3Event::Headers { headers, fin })) => {
                        let slab_id = process_request::insert(&headers, fin);
                        state.connection_streams.insert(stream_id, slab_id);

                        let conn_state = conn_state.clone();
                        spawn(handle_request(slab_id, stream_id, conn_state));
                    }
                    Ok((stream_id, Http3Event::Data)) => {
                        if let Some(&slab_id) = state.connection_streams.get(&stream_id) {
                            let mut buf = [0u8; 4096];
                            while let Ok(len) = state.h3_conn.recv_body(conn, stream_id, &mut buf) {
                                if len == 0 {
                                    break;
                                }
                                if !process_request::on_data(
                                    slab_id,
                                    &buf[..len],
                                    self.config.max_body_size,
                                ) {
                                    eprintln!(
                                        "Stream {stream_id}: body exceeds max size, rejecting"
                                    );
                                    state.connection_streams.remove(&stream_id);
                                    process_request::on_connection_lost(slab_id);
                                    break;
                                }
                            }
                        }
                    }
                    Ok((stream_id, Http3Event::Finished)) => {
                        if let Some(slab_id) = state.connection_streams.remove(&stream_id) {
                            process_request::on_finished(slab_id);
                        }
                    }
                    Ok((stream_id, Http3Event::Reset(err))) => {
                        eprintln!("Stream {stream_id} reset: {err}");
                        if let Some(slab_id) = state.connection_streams.remove(&stream_id) {
                            process_request::on_connection_lost(slab_id);
                        }
                    }
                    Ok((_, Http3Event::GoAway)) => {
                        eprintln!("GoAway received");
                    }
                    Ok((_, Http3Event::PriorityUpdate)) => {}
                    Err(Http3Error::Done) => break,
                    Err(e) => {
                        eprintln!("HTTP/3 poll error: {e}");
                        break;
                    }
                }
            }
        }
    }
}

struct HandlerBridge(Rc<RefCell<Handler>>);

impl TransportHandler for HandlerBridge {
    fn on_conn_created(&mut self, _conn: &mut Connection) {}

    fn on_conn_established(&mut self, conn: &mut Connection) {
        let mut handler = self.0.borrow_mut();

        match Http3Connection::new_with_quic_conn(conn, &handler.h3_config) {
            Ok(h3_conn) => {
                let key = conn as *mut Connection as u64;
                handler.connections.insert(
                    key,
                    Rc::new(RefCell::new(ConnectionState {
                        h3_conn,
                        conn_ptr: Some(NonNull::from(conn)),
                        connection_streams: HashMap::new(),
                    })),
                );
            }
            Err(e) => {
                eprintln!("Failed to create HTTP/3 connection: {e}");
            }
        }
    }

    fn on_conn_closed(&mut self, conn: &mut Connection) {
        let key = conn as *mut Connection as u64;
        let mut handler = self.0.borrow_mut();
        if let Some(conn_state) = handler.connections.remove(&key) {
            let mut state = conn_state.borrow_mut();
            state.conn_ptr = None;
            for (_, slab_id) in state.connection_streams.drain() {
                process_request::on_connection_lost(slab_id);
            }
        }
    }

    fn on_stream_created(&mut self, _conn: &mut Connection, _stream_id: u64) {}

    fn on_stream_readable(&mut self, conn: &mut Connection, _stream_id: u64) {
        let key = conn as *mut Connection as u64;
        let mut handler = self.0.borrow_mut();
        if !handler.connections_to_check.contains(&key) {
            handler.connections_to_check.push_back(key);
        }
    }
    fn on_stream_writable(&mut self, _conn: &mut Connection, _stream_id: u64) {}
    fn on_stream_closed(&mut self, conn: &mut Connection, stream_id: u64) {
        let key = conn as *mut Connection as u64;
        let handler = self.0.borrow();
        if let Some(conn_state) = handler.connections.get(&key) {
            conn_state
                .borrow_mut()
                .connection_streams
                .remove(&stream_id);
        }
    }
    fn on_new_token(&mut self, _conn: &mut Connection, _token: Vec<u8>) {}
}

struct Sender {
    fd: RawFd,
}

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
