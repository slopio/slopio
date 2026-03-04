use slab::Slab;
use std::cell::UnsafeCell;

use bytes::Bytes;

use crate::io::TokioSocket;
use crate::server;

pub type ServerConnection = server::Connection<TokioSocket, Bytes>;

thread_local! {
    static CONNECTIONS: UnsafeCell<Slab<ServerConnection>> =
        UnsafeCell::new(Slab::new());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionId(usize);

pub fn insert(conn: ServerConnection) -> ConnectionId {
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &mut *slab.get() };
        ConnectionId(slab.insert(conn))
    })
}

pub fn remove(id: ConnectionId) -> ServerConnection {
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &mut *slab.get() };
        slab.remove(id.0)
    })
}

pub fn with<F, R>(id: ConnectionId, f: F) -> R
where
    F: FnOnce(&mut ServerConnection) -> R,
{
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &mut *slab.get() };
        f(&mut slab[id.0])
    })
}

pub fn contains(id: ConnectionId) -> bool {
    CONNECTIONS.with(|slab| {
        let slab = unsafe { &*slab.get() };
        slab.contains(id.0)
    })
}
