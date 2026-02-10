use std::{
    cell::{RefCell, UnsafeCell},
    collections::{HashMap, VecDeque},
    mem::MaybeUninit,
    net::SocketAddr,
    os::fd::RawFd,
    rc::Rc,
    str::FromStr,
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;

use tquic::{
    Config, Connection, Endpoint, PacketInfo, PacketSendHandler, TlsConfig, TlsConfigSelector,
    TransportHandler,
    h3::{Header, Http3Config, Http3Error, Http3Event, connection::Http3Connection},
};

use crate::io_driver::{IO_DRIVER, operations::sockets::udp::UdpSocket};
use crate::runtime::spawn;
mod process_request;
use process_request::Http3Request;

pub struct Http3Server {
    endpoint: Endpoint,
    socket: Rc<UdpSocket>,
    handler: Rc<RefCell<Handler>>,
}

impl Http3Server {
    async fn recv(&mut self) {
        let mut buffer = [MaybeUninit::uninit(); 1500];
        let datagram = self.socket.recv_from(&mut buffer).await.unwrap();
        let data = unsafe {
            std::slice::from_raw_parts_mut(buffer.as_mut_ptr().cast::<u8>(), datagram.len())
        };
        let info = PacketInfo {
            src: datagram.addr(),
            time: Instant::now(),
            dst: SocketAddr::from_str("0.0.0.0:4433").unwrap(),
        };

        self.endpoint.recv(data, &info).unwrap();
        self.endpoint.process_connections().unwrap();

        self.handler.borrow_mut().process_http3();
    }

    pub async fn run(mut self) {
        loop {
            self.recv().await;
        }
    }

    pub fn build() -> Self {
        let udp = Rc::new(UdpSocket::bind(4433).unwrap());
        let mut config = Box::new(Config::new().unwrap());

        config.set_tls_config_selector(Arc::new(Tls {}));
        let handler = Rc::new(RefCell::new(Handler::new()));
        let sender = Rc::new(Sender { fd: udp.fd() });
        let endpoint = Endpoint::new(
            config,
            true,
            Box::new(HandlerBridge(handler.clone())),
            sender,
        );

        Self {
            endpoint,
            socket: udp,
            handler,
        }
    }
}

/// HTTP/3 request representation
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// HTTP/3 response builder
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
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
    streams: HashMap<u64, usize>,
    conn_ptr: *mut Connection,
}

impl ConnectionState {
    pub fn send_response(&mut self, stream_id: u64, response: Response) {
        let conn = unsafe { &mut *self.conn_ptr };

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
            println!("Failed to send headers: {:?}", e);
            return;
        }

        if has_body
            && let Err(e) =
                self.h3_conn
                    .send_body(conn, stream_id, Bytes::from(response.body), true)
        {
            println!("Failed to send body: {:?}", e);
        }
    }
}

struct Handler {
    h3_config: Http3Config,
    connections: HashMap<u64, Rc<UnsafeCell<ConnectionState>>>,
    connections_to_check: VecDeque<u64>,
}

impl Handler {
    fn new() -> Self {
        Self {
            h3_config: Http3Config::new().unwrap(),
            connections: HashMap::new(),
            connections_to_check: VecDeque::new(),
        }
    }

    fn process_http3(&mut self) {
        while let Some(key) = self.connections_to_check.pop_front() {
            let Some(conn_state) = self.connections.get(&key) else {
                continue;
            };
            let state = unsafe { &mut *conn_state.get() };
            let conn = unsafe { &mut *state.conn_ptr };
            loop {
                match state.h3_conn.poll(conn) {
                    Ok((stream_id, Http3Event::Headers { headers, fin })) => {
                        let slab_id = process_request::insert(&headers, fin);
                        state.streams.insert(stream_id, slab_id);

                        let conn_state = conn_state.clone();
                        spawn(async move {
                            let request = Http3Request::new(slab_id).await;
                            process_request::remove(slab_id);
                            println!("HTTP/3 request: {} {}", request.method, request.path);
                            let response = Self::handle_request(&request);
                            let state = unsafe { &mut *conn_state.get() };
                            state.send_response(stream_id, response);
                        });
                    }
                    Ok((stream_id, Http3Event::Data)) => {
                        if let Some(&slab_id) = state.streams.get(&stream_id) {
                            let mut buf = [0u8; 4096];
                            while let Ok(len) = state.h3_conn.recv_body(conn, stream_id, &mut buf) {
                                if len == 0 {
                                    break;
                                }
                                process_request::on_data(slab_id, &buf[..len]);
                            }
                        }
                    }
                    Ok((stream_id, Http3Event::Finished)) => {
                        if let Some(slab_id) = state.streams.remove(&stream_id) {
                            process_request::on_finished(slab_id);
                        }
                    }
                    Ok((stream_id, Http3Event::Reset(err))) => {
                        println!("Stream {} reset: {}", stream_id, err);
                        if let Some(slab_id) = state.streams.remove(&stream_id) {
                            process_request::remove(slab_id);
                        }
                    }
                    Ok((_, Http3Event::GoAway)) => {
                        println!("GoAway received");
                    }
                    Ok((_, Http3Event::PriorityUpdate)) => {}
                    Err(Http3Error::Done) => break,
                    Err(e) => {
                        println!("HTTP/3 error: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    fn handle_request(request: &Request) -> Response {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/") => Response::new(200)
                .header("content-type", "text/plain")
                .body("Hello from HTTP/3!"),
            _ => Response::new(404)
                .header("content-type", "text/plain")
                .body("Not Found"),
        }
    }
}

struct HandlerBridge(Rc<RefCell<Handler>>);

impl TransportHandler for HandlerBridge {
    fn on_conn_created(&mut self, _conn: &mut Connection) {}

    fn on_conn_established(&mut self, conn: &mut Connection) {
        println!("Connection established: {:?}", conn.dcid());

        let mut handler = self.0.borrow_mut();

        match Http3Connection::new_with_quic_conn(conn, &handler.h3_config) {
            Ok(h3_conn) => {
                let key = conn as *mut Connection as u64;
                handler.connections.insert(
                    key,
                    Rc::new(UnsafeCell::new(ConnectionState {
                        h3_conn,
                        conn_ptr: conn as *mut Connection,
                        streams: HashMap::new(),
                    })),
                );
            }
            Err(e) => {
                println!("Failed to create HTTP/3 connection: {:?}", e);
            }
        }
    }

    fn on_conn_closed(&mut self, conn: &mut Connection) {
        println!("Connection closed");
        let key = conn as *mut Connection as u64;
        self.0.borrow_mut().connections.remove(&key);
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
            let state = unsafe { &mut *conn_state.get() };
            if let Some(slab_id) = state.streams.remove(&stream_id) {
                process_request::remove(slab_id);
            }
        }
    }
    fn on_new_token(&mut self, _conn: &mut Connection, _token: Vec<u8>) {}
}

struct Sender {
    fd: RawFd,
}

struct Tls {}

impl TlsConfigSelector for Tls {
    fn get_default(&self) -> Option<Arc<TlsConfig>> {
        let mut tls_config = TlsConfig::new().unwrap();
        tls_config.set_certificate_file("certs/cert.pem").unwrap();
        tls_config.set_private_key_file("certs/key.pem").unwrap();
        tls_config
            .set_application_protos(vec![b"h3".to_vec()])
            .unwrap();
        Some(Arc::new(tls_config))
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
                println!(
                    "Sending {} bytes to {} (dst={})",
                    data.len(),
                    info.src,
                    info.dst
                );
                let entry = pool.add(self.fd, data, info.dst);
                driver.push_anon_op(entry);
            }

            Ok(pkts.len())
        })
    }
}
