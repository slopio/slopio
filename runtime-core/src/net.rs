// Public networking API.
//
// Protocol crates (runtime-h2, runtime-http, etc.) should import
// from this module instead of reaching into `io_driver` internals.

// ── TCP ──

pub use crate::io_driver::operations::sockets::connect::{connect_tcp, ConnectTcp};
pub use crate::io_driver::operations::sockets::SocketWrapper;
pub use crate::io_driver::operations::sockets::split::{
    OwnedReadHalf, OwnedWriteHalf, ReuniteError, SocketOwner,
};

// send
pub use crate::io_driver::operations::{FixedBufIndex, IoBuf, IoBufMut, Slice};
pub use crate::io_driver::operations::sockets::send::{WriteSocket, send_all, socket_send};
pub use crate::io_driver::operations::sockets::send_control_record::{
    socket_send_control_record, WriteControlRecord,
};
pub use crate::io_driver::operations::sockets::send_fixed::{
    WriteFixedSocket, socket_write_fixed,
};
pub use crate::io_driver::operations::sockets::send_fixed_zc::{
    WriteFixedSocketZc, socket_write_fixed_zc,
};
pub use crate::io_driver::operations::sockets::send_vectored::socket_send_vectored;
pub use crate::io_driver::operations::sockets::send_zc::{
    WriteSocketZc, send_all_zc, socket_send_zc,
};

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

pub use crate::io_driver::{
    BorrowedBuffer, BufRingSet, BufRingSpec, FixedBufAcquire, FixedSendBufPool, PooledBuf,
};
