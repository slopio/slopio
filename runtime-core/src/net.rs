// Public networking API.
//
// Protocol crates (runtime-h2, runtime-http, etc.) should import
// from this module instead of reaching into `io_driver` internals.

// ── TCP ──

pub use crate::io_driver::operations::sockets::SocketWrapper;
pub use crate::io_driver::operations::sockets::connect::{ConnectTcp, connect_tcp};
pub use crate::io_driver::operations::sockets::split::{
    OwnedReadHalf, OwnedWriteHalf, ReuniteError, SocketOwner,
};

// send
pub use crate::io_driver::operations::sockets::send::{WriteSocket, send_all, socket_send};
pub use crate::io_driver::operations::sockets::send_control_record::{
    WriteControlRecord, socket_send_control_record,
};
pub use crate::io_driver::operations::sockets::send_fixed::{WriteFixedSocket, socket_write_fixed};
pub use crate::io_driver::operations::sockets::send_fixed_zc::{
    WriteFixedSocketZc, socket_write_fixed_zc,
};
pub use crate::io_driver::operations::sockets::send_vectored::{
    DEFAULT_MSG_FLAGS, WriteVectoredSocket, send_vectored_all, send_vectored_all_with_flags,
    socket_send_vectored, socket_send_vectored_with_flags,
};
pub use crate::io_driver::operations::sockets::send_vectored_fixed_zc::{
    WriteVectoredFixedSocketZc, send_vectored_fixed_all_zc, socket_send_vectored_fixed_zc,
    socket_send_vectored_fixed_zc_with_flags,
};
pub use crate::io_driver::operations::sockets::send_vectored_zc::{
    WriteVectoredSocketZc, send_vectored_all_zc, socket_send_vectored_zc,
    socket_send_vectored_zc_with_flags,
};
pub use crate::io_driver::operations::sockets::send_zc::{
    WriteSocketZc, send_all_zc, socket_send_zc,
};
pub use crate::io_driver::operations::{FixedBufIndex, IoBuf, IoBufMut, Slice};

// recv
pub use crate::io_driver::operations::sockets::managed_recv::socket_managed_recv;
pub use crate::io_driver::operations::sockets::recv::{ClassicRecv, socket_peek, socket_recv};

// streaming (multishot)
pub use crate::io_driver::operations::sockets::multi_accept::{
    MultiAccept, incoming_cpu, set_incoming_cpu,
};
pub use crate::io_driver::operations::sockets::multi_recv::MultiRecv;

// ── UDP ──

pub use crate::io_driver::operations::sockets::udp::UdpMultiRecv;
pub use crate::io_driver::operations::sockets::udp::{
    UdpRecvHalf, UdpReuniteError, UdpSendHalf, UdpSocket, UdpSocketOwner,
};

// ── Buffers ──

pub use crate::io_driver::{
    BorrowedBuffer, BufRingSet, BufRingSpec, FixedSendBufPool, PooledBuf, SLAB_BUF_INDEX,
};
