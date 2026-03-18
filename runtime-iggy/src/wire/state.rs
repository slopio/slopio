use std::cell::Cell;
use std::mem;
use std::rc::Rc;
use std::time::Instant;

use runtime_core::net::SocketWrapper;

use crate::error::IggyError;

/// Observable state of the client connection, suitable for external inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    Disconnected,
    Connected,
    Authenticating,
    Authenticated,
    Shutdown,
}

impl std::fmt::Display for ClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientState::Disconnected => write!(f, "Disconnected"),
            ClientState::Connected => write!(f, "Connected"),
            ClientState::Authenticating => write!(f, "Authenticating"),
            ClientState::Authenticated => write!(f, "Authenticated"),
            ClientState::Shutdown => write!(f, "Shutdown"),
        }
    }
}

// ── per-state data ────────────────────────────────────────────────────────────

pub(crate) struct ConnectedData {
    pub socket: SocketWrapper,
}

pub(crate) struct AuthenticatedData {
    pub socket: SocketWrapper,
    pub user_id: u32,
    pub connected_at: Instant,
    /// Shared with the heartbeat task so it can update the timestamp.
    pub last_activity: Rc<Cell<Instant>>,
    /// Set to `true` while a request/response pair is in flight.
    /// The heartbeat task skips the PING if the socket is busy.
    pub socket_busy: Rc<Cell<bool>>,
    /// Cleared to `false` on disconnect/shutdown to stop the heartbeat task.
    pub alive: Rc<Cell<bool>>,
}

/// References shared with the background heartbeat task (all cheaply cloneable).
pub(crate) struct HeartbeatContext {
    pub fd: i32,
    pub alive: Rc<Cell<bool>>,
    pub socket_busy: Rc<Cell<bool>>,
    pub last_activity: Rc<Cell<Instant>>,
}

// ── state machine ─────────────────────────────────────────────────────────────

/// Owns the TCP socket inside the variant that holds it, so that the type system
/// tracks which operations are legal at each phase of the connection lifecycle.
///
/// Valid transitions:
/// ```text
/// Disconnected  ──on_connected──►  Connected
/// Connected     ──on_auth_begin──► Authenticating
/// Authenticating──on_auth_ok───►  Authenticated
/// Connected | Authenticating | Authenticated ──on_disconnect──► Disconnected
/// any ──────────────────────────────────────────────────────────► Shutdown  (terminal)
/// ```
pub(crate) enum Connection {
    Disconnected,
    Connected(ConnectedData),
    Authenticating(ConnectedData),
    Authenticated(AuthenticatedData),
    Shutdown,
}

impl Connection {
    pub fn new() -> Self {
        Connection::Disconnected
    }

    // ── state query ───────────────────────────────────────────────────────────

    pub fn state(&self) -> ClientState {
        match self {
            Connection::Disconnected => ClientState::Disconnected,
            Connection::Connected(_) => ClientState::Connected,
            Connection::Authenticating(_) => ClientState::Authenticating,
            Connection::Authenticated(_) => ClientState::Authenticated,
            Connection::Shutdown => ClientState::Shutdown,
        }
    }

    pub fn user_id(&self) -> Option<u32> {
        match self {
            Connection::Authenticated(d) => Some(d.user_id),
            _ => None,
        }
    }

    /// Returns the raw file descriptor of the live socket.
    ///
    /// The caller can build a `ManuallyDrop<SocketWrapper>` from this to perform
    /// I/O without creating a borrow on `Connection` that would conflict with
    /// subsequent state transitions.  The fd must **not** be closed by the caller.
    pub fn raw_fd(&self) -> Result<i32, IggyError> {
        match self {
            Connection::Connected(d) | Connection::Authenticating(d) => Ok(d.socket.fd),
            Connection::Authenticated(d) => Ok(d.socket.fd),
            Connection::Shutdown => Err(IggyError::InvalidState("client is shut down")),
            Connection::Disconnected => Err(IggyError::InvalidState("not connected")),
        }
    }

    /// Fails unless the client is `Authenticated`.
    pub fn require_authenticated(&self) -> Result<(), IggyError> {
        match self {
            Connection::Authenticated(_) => Ok(()),
            Connection::Shutdown => Err(IggyError::InvalidState("client is shut down")),
            _ => Err(IggyError::InvalidState("not authenticated")),
        }
    }

    // ── busy / idle tracking ──────────────────────────────────────────────────

    /// Mark the socket as busy (a request/response is in flight).
    /// The heartbeat task will not send a PING while the socket is busy.
    pub fn mark_busy(&self) {
        if let Connection::Authenticated(d) = self {
            d.socket_busy.set(true);
        }
    }

    /// Mark the socket as idle and record the current time as last activity.
    pub fn mark_idle(&self) {
        if let Connection::Authenticated(d) = self {
            d.socket_busy.set(false);
            d.last_activity.set(Instant::now());
        }
    }

    /// Clone the references needed by the heartbeat task.
    /// Returns `None` if not in the `Authenticated` state.
    pub fn heartbeat_context(&self) -> Option<HeartbeatContext> {
        match self {
            Connection::Authenticated(d) => Some(HeartbeatContext {
                fd: d.socket.fd,
                alive: Rc::clone(&d.alive),
                socket_busy: Rc::clone(&d.socket_busy),
                last_activity: Rc::clone(&d.last_activity),
            }),
            _ => None,
        }
    }

    // ── transitions ───────────────────────────────────────────────────────────

    /// TCP socket established.  Valid from: `Disconnected`.
    pub fn on_connected(&mut self, socket: SocketWrapper) -> Result<(), IggyError> {
        match self {
            Connection::Disconnected => {
                *self = Connection::Connected(ConnectedData { socket });
                Ok(())
            }
            Connection::Shutdown => Err(IggyError::InvalidState("client is shut down")),
            _ => Err(IggyError::InvalidState("already connected")),
        }
    }

    /// Login frame sent, waiting for the server response.  Valid from: `Connected`.
    pub fn on_auth_begin(&mut self) -> Result<(), IggyError> {
        let prev = mem::replace(self, Connection::Disconnected);
        match prev {
            Connection::Connected(data) => {
                *self = Connection::Authenticating(data);
                Ok(())
            }
            other => {
                *self = other;
                Err(IggyError::InvalidState("expected Connected state"))
            }
        }
    }

    /// Login succeeded.  Valid from: `Authenticating`.
    pub fn on_auth_ok(&mut self, user_id: u32) -> Result<(), IggyError> {
        let prev = mem::replace(self, Connection::Disconnected);
        match prev {
            Connection::Authenticating(ConnectedData { socket }) => {
                let now = Instant::now();
                *self = Connection::Authenticated(AuthenticatedData {
                    socket,
                    user_id,
                    connected_at: now,
                    last_activity: Rc::new(Cell::new(now)),
                    socket_busy: Rc::new(Cell::new(false)),
                    alive: Rc::new(Cell::new(true)),
                });
                Ok(())
            }
            other => {
                *self = other;
                Err(IggyError::InvalidState("expected Authenticating state"))
            }
        }
    }

    /// Drop the socket and return to `Disconnected`.
    /// No-op if already `Disconnected`; error if `Shutdown`.
    pub fn on_disconnect(&mut self) -> Result<(), IggyError> {
        match self {
            Connection::Disconnected => return Ok(()),
            Connection::Shutdown => return Err(IggyError::InvalidState("client is shut down")),
            _ => {}
        }
        if let Connection::Authenticated(d) = &*self {
            d.alive.set(false);
        }
        *self = Connection::Disconnected;
        Ok(())
    }

    /// Permanently terminate the client.  Valid from any non-`Shutdown` state.
    pub fn on_shutdown(&mut self) -> Result<(), IggyError> {
        match self {
            Connection::Shutdown => return Err(IggyError::InvalidState("already shut down")),
            _ => {}
        }
        if let Connection::Authenticated(d) = &*self {
            d.alive.set(false);
        }
        *self = Connection::Shutdown;
        Ok(())
    }
}
