// Public networking API.
//
// Protocol crates (runtime-h2, runtime-http, etc.) should import
// from this module instead of reaching into `io_driver` internals.

// ── TCP ──

pub use crate::io_driver::operations::sockets::connect::{connect_tcp, ConnectTcp};
pub use crate::io_driver::operations::sockets::SocketWrapper;

// send
pub use crate::io_driver::operations::{IoBuf, IoBufMut, Slice};
pub use crate::io_driver::operations::sockets::send::{WriteSocket, send_all, socket_send};
pub use crate::io_driver::operations::sockets::send_control_record::{
    socket_send_control_record, WriteControlRecord,
};
pub use crate::io_driver::operations::sockets::send_vectored::socket_send_vectored;

// recv
pub use crate::io_driver::operations::sockets::recv::{ClassicRecv, socket_recv};
pub use crate::io_driver::operations::sockets::managed_recv::socket_managed_recv;

// streaming (multishot)
pub use crate::io_driver::operations::sockets::multi_recv::MultiRecv;
pub use crate::io_driver::operations::sockets::multi_accept::MultiAccept;

// ── UDP ──

pub use crate::io_driver::operations::sockets::udp::UdpSocket;
pub use crate::io_driver::operations::sockets::udp::UdpMultiRecv;

// ── Buffers ──

pub use crate::io_driver::{BorrowedBuffer, BufRingSet, BufRingSpec};
