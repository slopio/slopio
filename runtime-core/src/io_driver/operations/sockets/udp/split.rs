use super::UdpSocket;

/// An owned handle that keeps a UDP socket's fd open for as long as it lives.
///
/// `UdpMultiRecv` is generic over this, so it can be armed on a whole
/// [`UdpSocket`] (exclusive ownership) or a [`UdpRecvHalf`] (shared with a
/// send half living in another task). Deliberately not implemented for
/// [`UdpSendHalf`]: only one side of a split may arm a recv stream.
pub trait UdpSocketOwner {
    fn socket(&self) -> &UdpSocket;
}

impl UdpSocketOwner for UdpSocket {
    fn socket(&self) -> &UdpSocket {
        self
    }
}

impl UdpSocket {
    /// Split into an owned recv half and send half sharing the fd, so the
    /// two directions can live in different tasks. The fd closes when the
    /// last reference — half or in-flight operation — drops;
    /// [`UdpRecvHalf::reunite`] restores the whole socket.
    pub fn into_split(self) -> (UdpRecvHalf, UdpSendHalf) {
        let other = UdpSocket {
            socket: self.socket.share_wrapper(),
        };
        (UdpRecvHalf { socket: other }, UdpSendHalf { socket: self })
    }
}

/// Recv half of a split UDP socket: arm a `UdpMultiRecv` on it, or call
/// `recv_from` through [`socket`](Self::socket). The fd closes once the send
/// half is gone too.
#[derive(Debug)]
pub struct UdpRecvHalf {
    socket: UdpSocket,
}

/// Send half of a split UDP socket: call `send_to` through
/// [`socket`](Self::socket). The fd closes once the recv half is gone too.
#[derive(Debug)]
pub struct UdpSendHalf {
    socket: UdpSocket,
}

impl UdpSocketOwner for UdpRecvHalf {
    fn socket(&self) -> &UdpSocket {
        &self.socket
    }
}

impl UdpRecvHalf {
    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn reunite(self, send: UdpSendHalf) -> Result<UdpSocket, UdpReuniteError> {
        reunite(self, send)
    }
}

impl UdpSendHalf {
    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn reunite(self, recv: UdpRecvHalf) -> Result<UdpSocket, UdpReuniteError> {
        reunite(recv, self)
    }
}

fn reunite(recv: UdpRecvHalf, send: UdpSendHalf) -> Result<UdpSocket, UdpReuniteError> {
    if !recv.socket.socket.same_socket(&send.socket.socket) {
        return Err(UdpReuniteError(recv, send));
    }
    // Dropping the send half leaves the recv half's socket as the exclusive
    // user handle; in-flight ops only delay the eventual close.
    Ok(recv.socket)
}

/// Returned by `reunite` when the two halves come from different sockets;
/// carries both halves back in `(recv, send)` order.
#[derive(Debug)]
pub struct UdpReuniteError(pub UdpRecvHalf, pub UdpSendHalf);

impl std::fmt::Display for UdpReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tried to reunite halves of different sockets")
    }
}

impl std::error::Error for UdpReuniteError {}
