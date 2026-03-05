use std::cell::UnsafeCell;
use std::pin::Pin;
use std::task::Waker;

use bytes::Bytes;
use http::Request;
use slab::Slab;

use runtime_core::io_driver::operations::sockets::SocketWrapper;
use runtime_core::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime_core::runtime::spawn;

use crate::io::TokioSocket;
use crate::server::{self, SendResponse};
use crate::share::RecvStream;

// ---------------------------------------------------------------------------
// Connection slab
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Active { active_streams: u16 },
    Closing { active_streams: u16, waker: Waker },
    Closed,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self::Active { active_streams: 0 }
    }
}

impl ConnectionStatus {
    fn closing(&mut self, waker: Waker) {
        match self {
            ConnectionStatus::Active { active_streams } if *active_streams == 0 => {
                *self = ConnectionStatus::Closed;
            }
            ConnectionStatus::Active { active_streams } => {
                *self = ConnectionStatus::Closing {
                    active_streams: *active_streams,
                    waker,
                };
            }
            ConnectionStatus::Closing { .. } | ConnectionStatus::Closed => unreachable!(),
        }
    }

    fn incr(&mut self) {
        match self {
            ConnectionStatus::Active { active_streams } => *active_streams += 1,
            ConnectionStatus::Closing {
                active_streams,
                waker,
            } => *active_streams += 1,
            ConnectionStatus::Closed => {}
        }
    }

    fn decr(&mut self) {
        println!("decr");
        match self {
            ConnectionStatus::Active { active_streams } => *active_streams -= 1,
            ConnectionStatus::Closing {
                active_streams,
                waker,
            } if *active_streams <= 1 => {
                waker.wake_by_ref();
                println!("now closed");
                *self = Self::Closed
            }
            ConnectionStatus::Closing { active_streams, .. } => {
                *active_streams -= 1;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionId(usize);

thread_local! {
    static CONNECTIONS: UnsafeCell<Slab<ConnectionStatus>> =
        UnsafeCell::new(Slab::new());
}

fn conn_insert() -> ConnectionId {
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &mut *slab.get() };
        ConnectionId(slab.insert(ConnectionStatus::default()))
    })
}

fn conn_remove(id: ConnectionId) {
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &mut *slab.get() };
        slab.remove(id.0);
    })
}

fn conn_with<F, R>(id: ConnectionId, f: F) -> R
where
    F: FnOnce(&mut ConnectionStatus) -> R,
{
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &mut *slab.get() };
        f(&mut slab[id.0])
    })
}

pub enum Body {
    Complete {
        head: http::request::Parts,
        bytes: Bytes,
    },
    Stream {
        head: http::request::Parts,
        recv: RecvStream,
    },
    Empty {
        head: http::request::Parts,
    },
}

#[derive(Debug)]
pub enum RecvState {
    /// Raw headers just received, not yet validated.
    Headers {
        head: http::request::Parts,
        body: RecvStream,
    },
    /// Expecting END_STREAM only — no body data.
    ExpectEnd {
        head: http::request::Parts,
        body: RecvStream,
    },
    /// Expecting exactly `expected` bytes then END_STREAM.
    ExpectBody {
        head: http::request::Parts,
        body: RecvStream,
        expected: usize,
        buf: Vec<u8>,
    },
    /// Expecting bytes of unknown length until END_STREAM.
    ExpectStream {
        head: http::request::Parts,
        body: RecvStream,
    },
    /// Terminal error state.
    Error,
}

impl From<Request<RecvStream>> for RecvState {
    fn from(value: Request<RecvStream>) -> Self {
        let (head, body) = value.into_parts();
        RecvState::Headers { head, body }
    }
}

impl RecvState {
    pub fn head(&self) -> Option<&http::request::Parts> {
        match self {
            RecvState::Headers { head, .. }
            | RecvState::ExpectEnd { head, .. }
            | RecvState::ExpectBody { head, .. }
            | RecvState::ExpectStream { head, .. } => Some(head),
            RecvState::Error => None,
        }
    }

    pub async fn poll(&mut self) -> Result<Option<Body>, crate::Error> {
        match self {
            RecvState::Headers { .. } => {
                let old = std::mem::replace(self, RecvState::Error);
                let RecvState::Headers { head, body } = old else {
                    unreachable!()
                };
                if body.is_end_stream() {
                    *self = RecvState::ExpectEnd { head, body };
                } else {
                    let expected = head
                        .headers
                        .get(http::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<usize>().ok());
                    match expected {
                        Some(n) => {
                            *self = RecvState::ExpectBody {
                                head,
                                body,
                                expected: n,
                                buf: Vec::with_capacity(n),
                            }
                        }
                        None => *self = RecvState::ExpectStream { head, body },
                    };
                }
                Ok(None)
            }
            RecvState::ExpectEnd { .. } => {
                let old = std::mem::replace(self, RecvState::Error);
                let RecvState::ExpectEnd { head, body } = old else {
                    unreachable!()
                };
                if body.is_end_stream() {
                    return Ok(Some(Body::Empty { head }));
                }
                // not yet, put it back
                *self = RecvState::ExpectEnd { head, body };
                Ok(None)
            }
            RecvState::ExpectBody {
                body,
                expected,
                buf,
                ..
            } => match body.data().await {
                Some(Ok(chunk)) => {
                    let _ = body.flow_control().release_capacity(chunk.len());
                    buf.extend_from_slice(&chunk);
                    *expected = expected.saturating_sub(chunk.len());
                    if *expected == 0 {
                        let old = std::mem::replace(self, RecvState::Error);
                        let RecvState::ExpectBody { head, buf, .. } = old else {
                            unreachable!()
                        };
                        Ok(Some(Body::Complete {
                            head,
                            bytes: Bytes::from(buf),
                        }))
                    } else {
                        Ok(None)
                    }
                }
                Some(Err(e)) => {
                    *self = RecvState::Error;
                    Err(e)
                }
                None => {
                    let old = std::mem::replace(self, RecvState::Error);
                    let RecvState::ExpectBody { head, buf, .. } = old else {
                        unreachable!()
                    };
                    Ok(Some(Body::Complete {
                        head,
                        bytes: Bytes::from(buf),
                    }))
                }
            },
            RecvState::ExpectStream { .. } => {
                unimplemented!()
            }
            RecvState::Error => {
                unreachable!()
            }
        }
    }

    /// Poll until resolved, returning the complete Body.
    pub async fn recv_all(&mut self) -> Result<Body, crate::Error> {
        loop {
            if let Some(body) = self.poll().await? {
                return Ok(body);
            }
        }
    }
}

/// Accept loop: listens on `port`, spawns a task per connection,
/// spawns a task per stream with the user handler.
pub async fn serve<F, Fut>(port: u16, on_request: F) -> std::io::Result<()>
where
    F: Fn(ConnectionId, RecvState, SendResponse<Bytes>) -> Fut + Clone + 'static,
    Fut: Future,
{
    let mut acceptor = MultiAccept::new(port)?;

    loop {
        let socket = match poll_next(&mut acceptor).await {
            Some(s) => s,
            None => break,
        };

        let on_request = on_request.clone();
        spawn(handle_connection(socket, on_request));
    }
    Ok(())
}

async fn handle_connection<F, Fut>(socket: SocketWrapper, on_request: F)
where
    F: Fn(ConnectionId, RecvState, SendResponse<Bytes>) -> Fut + Clone + 'static,
    Fut: Future,
{
    let io = TokioSocket::new(socket);
    let mut conn = match server::handshake(io).await {
        Ok(c) => c,
        Err(_) => return,
    };
    let conn_id = conn_insert();

    while let Some(Ok((request, respond))) = conn.accept().await {
        conn_with(conn_id, |e| e.incr());
        let handler = on_request.clone();
        let state = request.into();
        spawn(async move {
            handler(conn_id, state, respond).await;
            conn_with(conn_id, |e| e.decr())
        });
    }

    ClosingHandler::new(conn_id, &mut conn).await;
    conn_remove(conn_id);
}

/// Helper: poll a MultiAccept stream for the next socket.
async fn poll_next(acceptor: &mut MultiAccept) -> Option<SocketWrapper> {
    use runtime_streams::Stream;
    crate::poll_fn(|cx| Pin::new(&mut *acceptor).poll_next(cx)).await
}

struct ClosingHandler<'a> {
    id: ConnectionId,
    conn: &'a mut server::Connection<TokioSocket, Bytes>,
    waiting: bool,
}

impl<'a> ClosingHandler<'a> {
    fn new(id: ConnectionId, conn: &'a mut server::Connection<TokioSocket, Bytes>) -> Self {
        Self {
            id,
            conn,
            waiting: false,
        }
    }
}

impl<'a> Future for ClosingHandler<'a> {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let _ = self.conn.poll_closed(cx);

        if !self.waiting {
            self.waiting = true;
            conn_with(self.id, |c| c.closing(cx.waker().clone()));
        }

        if conn_with(self.id, |c| matches!(c, ConnectionStatus::Closed)) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}
