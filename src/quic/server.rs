use std::{
    cell::UnsafeCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
};

use bytes::Bytes;

use tquic::{
    Connection, Endpoint, TransportHandler,
    h3::{Header, Http3Config, Http3Error, Http3Event, connection::Http3Connection},
};

use crate::{
    io_driver::operations::sockets::udp::UdpSocket,
    quic::{Http3ServerConfig, HttpResponse},
};

type ConnectionId = u64;
type StreamId = u64;

struct H3State {
    handler: H3Handler,
    endpoint: Option<Endpoint>,
}

impl H3State {
    fn new() -> Self {
        Self {
            handler: H3Handler::new(),
            endpoint: None,
        }
    }
}

thread_local! {
    static H3_STATE: UnsafeCell<H3State> = UnsafeCell::new(H3State::new());
}

fn with_state<F, R>(f: F) -> R
where
    F: FnOnce(&mut H3State) -> R,
{
    H3_STATE.with(|cell| f(unsafe { &mut *cell.get() }))
}

/// Decrement in-flight counter and remove the connection if it's dead and drained.
fn task_done(connection_id: ConnectionId) {
    with_state(|state| {
        let Some(h3) = state.handler.connections.get_mut(&connection_id) else {
            return;
        };
        h3.in_flight -= 1;
        if !h3.alive && h3.in_flight == 0 {
            state.handler.connections.remove(&connection_id);
        }
    });
}

fn send_response(connection_id: ConnectionId, stream_id: StreamId, response: HttpResponse) {
    with_state(|state| {
        let Some(endpoint) = &mut state.endpoint else {
            return;
        };
        let Some(h3) = state.handler.connections.get_mut(&connection_id) else {
            return;
        };
        let Some(conn) = endpoint.conn_get_mut(connection_id) else {
            return;
        };
        h3.send_response(conn, stream_id, response);
    });
}

pub struct H3Server {
    socket: UdpSocket,
    config: Rc<Http3ServerConfig>,
}

impl H3Server {
    pub fn build(server_config: Http3ServerConfig) -> Result<Self, super::Http3ServerError> {
        let udp = UdpSocket::bind(server_config.bind_addr.port())?;

        let mut config = Box::new(tquic::Config::new()?);
        let tls = super::Tls::new(&server_config.cert_path, &server_config.key_path)?;

        config.set_max_idle_timeout(server_config.idle_timeout.as_millis() as u64);
        config.set_initial_max_streams_bidi(server_config.max_concurrent_streams);
        config.set_tls_config_selector(std::sync::Arc::new(tls));

        let server_config = Rc::new(server_config);

        with_state(|state| {
            state.handler.config = Some(server_config.clone());
            state.handler.h3_config = Http3Config::new().ok();
        });

        let sender = Rc::new(super::Sender { fd: udp.fd() });
        let endpoint = Endpoint::new(config, true, Box::new(HandlerBridge), sender);

        with_state(|state| {
            state.endpoint = Some(endpoint);
        });

        Ok(Self {
            socket: udp,
            config: server_config,
        })
    }

    pub async fn run(self) {
        use runtime_streams::StreamExt;
        use std::time::Instant;
        use tquic::PacketInfo;

        use crate::io_driver::operations::sockets::udp::UdpMultiRecv;

        let mut recv = UdpMultiRecv::new(self.socket.fd());
        loop {
            let Some(datagram) = recv.next().await else {
                break;
            };

            let info = PacketInfo {
                src: datagram.addr(),
                time: Instant::now(),
                dst: self.config.bind_addr,
            };

            with_state(|state| {
                let Some(endpoint) = &mut state.endpoint else {
                    return;
                };
                if let Err(e) = endpoint.recv(&mut datagram.payload().to_vec(), &info) {
                    eprintln!("QUIC recv error: {e}");
                    return;
                }
                if let Err(e) = endpoint.process_connections() {
                    eprintln!("QUIC process_connections error: {e}");
                }
                state.handler.process_http3(endpoint);
            });
        }
    }
}

struct H3Handler {
    h3_config: Option<Http3Config>,
    config: Option<Rc<Http3ServerConfig>>,
    connections: HashMap<ConnectionId, H3Connection>,
    connections_to_check: VecDeque<ConnectionId>,
}

impl H3Handler {
    fn new() -> Self {
        Self {
            h3_config: None,
            config: None,
            connections: HashMap::new(),
            connections_to_check: VecDeque::new(),
        }
    }

    fn process_http3(&mut self, endpoint: &mut Endpoint) {
        let H3Handler {
            connections,
            connections_to_check,
            config,
            ..
        } = self;
        let max_body_size = config.as_ref().map_or(8 * 1024 * 1024, |c| c.max_body_size);

        while let Some(conn_id) = connections_to_check.pop_front() {
            let Some(h3) = connections.get_mut(&conn_id) else {
                continue;
            };
            let Some(conn) = endpoint.conn_get_mut(conn_id) else {
                continue;
            };

            loop {
                match h3.h3_conn.poll(conn) {
                    Ok((stream_id, Http3Event::Headers { headers, fin })) => {
                        let slab_id = super::process_request::insert(&headers, fin);
                        h3.connection_streams.insert(stream_id, slab_id);
                        h3.in_flight += 1;

                        crate::runtime::spawn(handle_request(slab_id, stream_id, conn_id));
                    }
                    Ok((stream_id, Http3Event::Data)) => {
                        if let Some(&slab_id) = h3.connection_streams.get(&stream_id) {
                            let mut buf = [0u8; 4096];
                            while let Ok(len) = h3.h3_conn.recv_body(conn, stream_id, &mut buf) {
                                if len == 0 {
                                    break;
                                }
                                if !super::process_request::on_data(
                                    slab_id,
                                    &buf[..len],
                                    max_body_size,
                                ) {
                                    eprintln!(
                                        "Stream {stream_id}: body exceeds max size, rejecting"
                                    );
                                    h3.connection_streams.remove(&stream_id);
                                    super::process_request::on_connection_lost(slab_id);
                                    break;
                                }
                            }
                        }
                    }
                    Ok((stream_id, Http3Event::Finished)) => {
                        if let Some(slab_id) = h3.connection_streams.remove(&stream_id) {
                            super::process_request::on_finished(slab_id);
                        }
                    }
                    Ok((stream_id, Http3Event::Reset(err))) => {
                        eprintln!("Stream {stream_id} reset: {err}");
                        if let Some(slab_id) = h3.connection_streams.remove(&stream_id) {
                            super::process_request::on_connection_lost(slab_id);
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

struct H3Connection {
    h3_conn: Http3Connection,
    connection_id: ConnectionId,
    connection_streams: HashMap<StreamId, usize>,
    in_flight: usize,
    alive: bool,
}

impl H3Connection {
    fn try_new(conn: &mut Connection, h3_config: &Http3Config) -> Result<Self, ()> {
        let connection_id = conn.index().ok_or(())?;
        let h3_conn = Http3Connection::new_with_quic_conn(conn, h3_config).map_err(|_| ())?;
        Ok(Self {
            h3_conn,
            connection_id,
            connection_streams: HashMap::new(),
            in_flight: 0,
            alive: true,
        })
    }

    fn send_response(
        &mut self,
        conn: &mut Connection,
        stream_id: StreamId,
        response: HttpResponse,
    ) {
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

        if let Err(e) = self
            .h3_conn
            .send_body(conn, stream_id, Bytes::from(response.body), true)
            && has_body
        {
            eprintln!("Failed to send body: {e}");
        }
    }
}

async fn handle_request(slab_id: usize, stream_id: StreamId, connection_id: ConnectionId) {
    let Some(request) = super::process_request::Http3Request::new(slab_id).await else {
        super::process_request::remove(slab_id);
        task_done(connection_id);
        return;
    };
    super::process_request::remove(slab_id);

    let response = route(&request).await;
    send_response(connection_id, stream_id, response);
    task_done(connection_id);
}

async fn route(request: &super::HttpRequest) -> HttpResponse {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => HttpResponse::new(200)
            .header("content-type", "text/plain")
            .body("Hello from HTTP/3!"),
        _ => HttpResponse::new(404)
            .header("content-type", "text/plain")
            .body("Not Found"),
    }
}

struct HandlerBridge;

impl TransportHandler for HandlerBridge {
    fn on_conn_created(&mut self, _conn: &mut Connection) {}

    fn on_conn_established(&mut self, conn: &mut Connection) {
        with_state(|state| {
            let Some(h3_config) = &state.handler.h3_config else {
                return;
            };
            match H3Connection::try_new(conn, h3_config) {
                Ok(h3) => {
                    state.handler.connections.insert(h3.connection_id, h3);
                }
                Err(_) => eprintln!("Failed to create HTTP/3 connection"),
            }
        });
    }

    fn on_conn_closed(&mut self, conn: &mut Connection) {
        with_state(|state| {
            let Some(conn_index) = conn.index() else {
                return;
            };
            if let Some(h3) = state.handler.connections.get_mut(&conn_index) {
                h3.alive = false;
                for (_, slab_id) in h3.connection_streams.iter() {
                    super::process_request::on_connection_lost(*slab_id);
                }
            }
        });
    }

    fn on_stream_created(&mut self, _conn: &mut Connection, _stream_id: u64) {}

    fn on_stream_readable(&mut self, conn: &mut Connection, _stream_id: u64) {
        with_state(|state| {
            let Some(conn_index) = conn.index() else {
                return;
            };
            if !state.handler.connections_to_check.contains(&conn_index) {
                state.handler.connections_to_check.push_back(conn_index);
            }
        });
    }

    fn on_stream_writable(&mut self, _conn: &mut Connection, _stream_id: u64) {}

    fn on_stream_closed(&mut self, conn: &mut Connection, stream_id: u64) {
        with_state(|state| {
            let Some(conn_index) = conn.index() else {
                return;
            };
            if let Some(h3) = state.handler.connections.get_mut(&conn_index) {
                h3.connection_streams.remove(&stream_id);
            }
        });
    }

    fn on_new_token(&mut self, _conn: &mut Connection, _token: Vec<u8>) {}
}
