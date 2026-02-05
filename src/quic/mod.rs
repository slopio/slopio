use std::{
    cell::RefCell, collections::HashMap, mem::MaybeUninit, net::SocketAddr, os::fd::RawFd, rc::Rc,
    str::FromStr, sync::Arc, time::Instant,
};

use bytes::Bytes;

use io_uring::opcode;
use tquic::{
    Config, Connection, Endpoint, PacketInfo, PacketSendHandler, TlsConfig, TlsConfigSelector,
    TransportHandler,
    h3::{Header, Http3Config, Http3Error, Http3Event, NameValue, connection::Http3Connection},
};

use crate::io_driver::{IO_DRIVER, operations::sockets::udp::UdpSocket};

pub struct Http3Server {
    endpoint: Endpoint,
    socket: Rc<UdpSocket>,
    handler: Rc<RefCell<Handler>>,
}

impl Http3Server {
    pub async fn recv(&mut self) {
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

        // Process HTTP/3 events for all connections
        self.handler.borrow_mut().process_http3();
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
struct ConnectionState {
    h3_conn: Http3Connection,
    conn_ptr: *mut Connection,
}

struct Handler {
    h3_config: Http3Config,
    connections: HashMap<u64, ConnectionState>,
}

impl Handler {
    fn new() -> Self {
        Self {
            h3_config: Http3Config::new().unwrap(),
            connections: HashMap::new(),
        }
    }

    fn process_http3(&mut self) {
        for (_, state) in self.connections.iter_mut() {
            let conn = unsafe { &mut *state.conn_ptr };

            loop {
                match state.h3_conn.poll(conn) {
                    Ok((stream_id, Http3Event::Headers { headers, fin })) => {
                        let request = Self::parse_request(&headers);
                        println!("HTTP/3 request: {} {}", request.method, request.path);

                        // Send response
                        let response = Self::handle_request(&request);
                        Self::send_response(&mut state.h3_conn, conn, stream_id, response);
                    }
                    Ok((stream_id, Http3Event::Data)) => {
                        let mut buf = [0u8; 4096];
                        while let Ok(len) = state.h3_conn.recv_body(conn, stream_id, &mut buf) {
                            if len == 0 {
                                break;
                            }
                            // Process body data here if needed
                        }
                    }
                    Ok((stream_id, Http3Event::Finished)) => {
                        println!("Stream {} finished", stream_id);
                    }
                    Ok((_, Http3Event::Reset(err))) => {
                        println!("Stream reset: {}", err);
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

    fn parse_request(headers: &[Header]) -> Request {
        let mut method = String::new();
        let mut path = String::new();
        let mut req_headers = Vec::new();

        for h in headers {
            let name = String::from_utf8_lossy(h.name()).to_string();
            let value = String::from_utf8_lossy(h.value()).to_string();

            match name.as_str() {
                ":method" => method = value,
                ":path" => path = value,
                _ if !name.starts_with(':') => {
                    req_headers.push((name, value));
                }
                _ => {}
            }
        }

        Request {
            method,
            path,
            headers: req_headers,
            body: Vec::new(),
        }
    }

    fn handle_request(request: &Request) -> Response {
        // Simple router - customize as needed
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/") => Response::new(200)
                .header("content-type", "text/plain")
                .body("Hello from HTTP/3!"),
            _ => Response::new(404)
                .header("content-type", "text/plain")
                .body("Not Found"),
        }
    }

    fn send_response(
        h3_conn: &mut Http3Connection,
        conn: &mut Connection,
        stream_id: u64,
        response: Response,
    ) {
        let mut headers = vec![Header::new(
            b":status",
            response.status.to_string().as_bytes(),
        )];

        for (name, value) in &response.headers {
            headers.push(Header::new(name.as_bytes(), value.as_bytes()));
        }

        let has_body = !response.body.is_empty();

        if let Err(e) = h3_conn.send_headers(conn, stream_id, &headers, !has_body) {
            println!("Failed to send headers: {:?}", e);
            return;
        }

        if has_body {
            if let Err(e) = h3_conn.send_body(conn, stream_id, Bytes::from(response.body), true) {
                println!("Failed to send body: {:?}", e);
            }
        }
    }
}

/// Bridge to forward TransportHandler events to our Handler
struct HandlerBridge(Rc<RefCell<Handler>>);

impl TransportHandler for HandlerBridge {
    fn on_conn_created(&mut self, _conn: &mut Connection) {}

    fn on_conn_established(&mut self, conn: &mut Connection) {
        println!("Connection established: {:?}", conn.dcid());

        let mut handler = self.0.borrow_mut();

        // Create HTTP/3 connection wrapping the QUIC connection
        match Http3Connection::new_with_quic_conn(conn, &handler.h3_config) {
            Ok(h3_conn) => {
                let key = conn as *mut Connection as u64;
                handler.connections.insert(
                    key,
                    ConnectionState {
                        h3_conn,
                        conn_ptr: conn as *mut Connection,
                    },
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
    fn on_stream_readable(&mut self, _conn: &mut Connection, _stream_id: u64) {}
    fn on_stream_writable(&mut self, _conn: &mut Connection, _stream_id: u64) {}
    fn on_stream_closed(&mut self, _conn: &mut Connection, _stream_id: u64) {}
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
