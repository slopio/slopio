use super::SocketWrapper;

/// An owned handle that keeps a socket's fd open for as long as it lives.
///
/// `MultiRecv` is generic over this, so it can be armed on a whole
/// [`SocketWrapper`] (exclusive ownership) or an [`OwnedReadHalf`] (shared
/// with a write half living in another task). Deliberately not implemented
/// for [`OwnedWriteHalf`]: only one side of a split may arm a recv stream.
pub trait SocketOwner {
    fn socket(&self) -> &SocketWrapper;
}

impl SocketOwner for SocketWrapper {
    fn socket(&self) -> &SocketWrapper {
        self
    }
}

impl SocketWrapper {
    /// Split into an owned read half and write half sharing the fd, so the
    /// two directions can live in different tasks. The fd closes when the
    /// last reference — half or in-flight operation — drops;
    /// [`OwnedReadHalf::reunite`] restores the whole socket.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let other = self.share_wrapper();
        (
            OwnedReadHalf { socket: other },
            OwnedWriteHalf { socket: self },
        )
    }
}

/// Read half of a split socket: arm a `MultiRecv` on it, or pass
/// [`socket`](Self::socket) to the recv functions. Dropping it does not shut
/// the connection down; the fd closes once the write half is gone too.
#[derive(Debug)]
pub struct OwnedReadHalf {
    socket: SocketWrapper,
}

/// Write half of a split socket: pass [`socket`](Self::socket) to the send
/// functions. Unlike tokio's write half, plain drop sends nothing — the peer
/// sees no FIN until both halves are gone. Use
/// [`sync_shutdown`](Self::sync_shutdown) to half-close explicitly.
#[derive(Debug)]
pub struct OwnedWriteHalf {
    socket: SocketWrapper,
}

impl SocketOwner for OwnedReadHalf {
    fn socket(&self) -> &SocketWrapper {
        &self.socket
    }
}

impl OwnedReadHalf {
    pub fn socket(&self) -> &SocketWrapper {
        &self.socket
    }

    pub fn reunite(self, write: OwnedWriteHalf) -> Result<SocketWrapper, ReuniteError> {
        reunite(self, write)
    }
}

impl OwnedWriteHalf {
    pub fn socket(&self) -> &SocketWrapper {
        &self.socket
    }

    /// Send FIN (`shutdown(SHUT_WR)`) and consume this half. The read half
    /// keeps receiving and the fd stays open until it drops too.
    pub fn sync_shutdown(self) {
        unsafe {
            libc::shutdown(self.socket.fd(), libc::SHUT_WR);
        }
    }

    pub fn reunite(self, read: OwnedReadHalf) -> Result<SocketWrapper, ReuniteError> {
        reunite(read, self)
    }
}

fn reunite(read: OwnedReadHalf, write: OwnedWriteHalf) -> Result<SocketWrapper, ReuniteError> {
    if !read.socket.same_socket(&write.socket) {
        return Err(ReuniteError(read, write));
    }
    // Dropping the write half leaves the read half's wrapper as the
    // exclusive user handle; in-flight ops only delay the eventual close.
    Ok(read.socket)
}

/// Returned by `reunite` when the two halves come from different sockets;
/// carries both halves back in `(read, write)` order.
#[derive(Debug)]
pub struct ReuniteError(pub OwnedReadHalf, pub OwnedWriteHalf);

impl std::fmt::Display for ReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tried to reunite halves of different sockets")
    }
}

impl std::error::Error for ReuniteError {}
