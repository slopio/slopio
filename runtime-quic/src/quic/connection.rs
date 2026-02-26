use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use tquic::{
    Connection, Endpoint, TransportHandler,
    h3::{Header, Http3Config, Http3Error, Http3Event, connection::Http3Connection},
};

use crate::quic::{
    HttpResponse,
    error::RecvBodyError,
    stream::{BodyType, StreamState, on_connection_lost, on_finished, stream_slab, take_meta},
};

use super::handler::{handle_request, task_done};
use super::server::{try_store_headers, with_state};

pub(super) type ConnectionId = u64;
pub(super) type StreamId = u64;

pub(super) struct H3Handler {
    pub(super) h3_config: Option<Http3Config>,
    pub(super) config: Option<std::rc::Rc<crate::quic::Http3ServerConfig>>,
    pub(super) connections: HashMap<ConnectionId, H3Connection>,
    pub(super) connections_to_check: VecDeque<ConnectionId>,
}

impl H3Handler {
    pub(super) fn new() -> Self {
        Self {
            h3_config: None,
            config: None,
            connections: HashMap::new(),
            connections_to_check: VecDeque::new(),
        }
    }

    pub(super) fn process_http3(&mut self, endpoint: &mut Endpoint) {
        let H3Handler {
            connections,
            connections_to_check,
            ..
        } = self;
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
                        if let Some(slab_id) = try_store_headers(&headers, fin) {
                            h3.connection_streams.insert(stream_id, slab_id);
                            h3.in_flight += 1;
                            runtime_core::runtime::spawn(handle_request(slab_id, stream_id, conn_id));
                        }
                    }
                    Ok((stream_id, Http3Event::Data)) => {
                        if let Some(&slab_id) = h3.connection_streams.get(&stream_id) {
                            match recv_data(&mut h3.h3_conn, conn, stream_id, slab_id) {
                                Err(RecvBodyError::Overflow) => {
                                    if let Some(slab_id) = h3.connection_streams.remove(&stream_id)
                                    {
                                        on_connection_lost(slab_id);
                                    }
                                    send_413(&mut h3.h3_conn, conn, stream_id);
                                }
                                Err(RecvBodyError::StreamNotFound)
                                | Err(RecvBodyError::UnexpectedBody) => {
                                    h3.connection_streams.remove(&stream_id);
                                }
                                Ok(()) => {}
                            }
                        }
                    }
                    Ok((stream_id, Http3Event::Finished)) => {
                        if let Some(slab_id) = h3.connection_streams.remove(&stream_id) {
                            on_finished(slab_id);
                        }
                    }
                    Ok((stream_id, Http3Event::Reset(err))) => {
                        eprintln!("Stream {stream_id} reset: {err}");
                        if let Some(slab_id) = h3.connection_streams.remove(&stream_id) {
                            on_connection_lost(slab_id);
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

pub(super) struct H3Connection {
    pub(super) h3_conn: Http3Connection,
    pub(super) connection_id: ConnectionId,
    pub(super) connection_streams: HashMap<StreamId, usize>,
    pub(super) in_flight: usize,
    pub(super) alive: bool,
}

impl H3Connection {
    pub(super) fn try_new(conn: &mut Connection, h3_config: &Http3Config) -> Result<Self, ()> {
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

    pub(super) fn send_response(
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

pub(super) struct HandlerBridge;

impl TransportHandler for HandlerBridge {
    fn on_conn_created(&mut self, _conn: &mut Connection) {}

    fn on_conn_established(&mut self, conn: &mut Connection) {
        use runtime_core::io_driver::IO_DRIVER;

        if IO_DRIVER.with(|d| d.shutdown()) {
            let _ = conn.close(true, 0, b"shutting down");
            return;
        }

        with_state(|state| {
            let Some(h3_config) = &state.handler.h3_config else {
                return;
            };
            match H3Connection::try_new(conn, h3_config) {
                Ok(h3) => {
                    state.handler.connections.insert(h3.connection_id, h3);
                    #[cfg(feature = "test-monitor")]
                    super::monitor::record_connection_opened();
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
                    on_connection_lost(*slab_id);
                }
                #[cfg(feature = "test-monitor")]
                super::monitor::record_connection_closed();
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

fn send_413(h3_conn: &mut Http3Connection, conn: &mut Connection, stream_id: StreamId) {
    let _ = h3_conn.send_headers(
        conn,
        stream_id,
        &[tquic::h3::Header::new(b":status", b"413")],
        true,
    );
}

fn recv_data(
    h3_conn: &mut Http3Connection,
    conn: &mut Connection,
    stream_id: StreamId,
    slab_id: usize,
) -> Result<(), RecvBodyError> {
    stream_slab(|slab| {
        let Some(stream) = slab.get_mut(slab_id) else {
            return Err(RecvBodyError::StreamNotFound);
        };
        let Some(ref mut body) = stream.body else {
            return Err(RecvBodyError::UnexpectedBody);
        };
        match body {
            BodyType::Alloc(alloc) => {
                if alloc.unfilled().is_empty() {
                    return Err(RecvBodyError::Overflow);
                }
                if let Ok(len) = h3_conn.recv_body(conn, stream_id, alloc.unfilled()) {
                    alloc.advance(len);
                }
                Ok(())
            }
            BodyType::Streamed => unimplemented!(),
        }
    })
}
