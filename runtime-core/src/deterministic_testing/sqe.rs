//! A readable view over a submission queue entry.
//!
//! `io_uring::squeue::Entry` is write-only from the outside: it wraps a private
//! `io_uring_sqe` and exposes only `get_user_data`/`get_opcode`. A simulated
//! ring needs the rest — which fd, which buffer, how many bytes — so it
//! reinterprets the entry as the `repr(C)` struct it already is.
//!
//! That reinterpretation is safe to rely on because the layout is not the
//! `io-uring` crate's choice: `io_uring_sqe` is the kernel's syscall ABI, fixed
//! at 64 bytes and unchangeable without breaking every io_uring program ever
//! compiled. The asserts at the bottom of this file pin it anyway, so a
//! surprise becomes a build error rather than a fake that misreads fields.

use io_uring::squeue;

/// The kernel's `struct io_uring_sqe`, with each anonymous union collapsed to
/// the member the opcodes this runtime submits actually use.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Sqe {
    /// `IORING_OP_*`.
    pub opcode: u8,
    /// `IOSQE_*` flags — notably [`IOSQE_BUFFER_SELECT`].
    pub flags: u8,
    /// Per-opcode; carries the multishot bit for some ops.
    pub ioprio: u16,
    /// The descriptor the op acts on. For `MSG_RING`, the *target ring's* fd.
    pub fd: i32,
    /// Union of `off`/`addr2`. For `MSG_RING`, the `user_data` to post on the target.
    pub off: u64,
    /// Union of `addr`/`splice_off_in`. The buffer pointer for read/write-style
    /// ops; for `ASYNC_CANCEL`, the `user_data` being cancelled.
    pub addr: u64,
    /// Byte count for read/write-style ops. For `MSG_RING`, the `res` to post.
    pub len: u32,
    /// Union of every `*_flags` member (`rw_flags`, `msg_flags`, `cancel_flags`, …).
    pub op_flags: u32,
    /// The tagged token this runtime routes completions by.
    pub user_data: u64,
    /// Union of `buf_index`/`buf_group` — the provided-buffer group when
    /// [`IOSQE_BUFFER_SELECT`] is set, the fixed-buffer index otherwise.
    pub buf_group: u16,
    pub personality: u16,
    /// Union of `splice_fd_in`/`file_index`/…
    pub splice_fd_in: i32,
    pub addr3: u64,
    pub __pad2: u64,
}

/// `IOSQE_BUFFER_SELECT` — the op draws its buffer from the ring named by
/// [`Sqe::buf_group`]. Bit 5, per the kernel's `IOSQE_BUFFER_SELECT_BIT`.
pub const IOSQE_BUFFER_SELECT: u8 = 1 << 5;

/// `IORING_CQE_F_BUFFER` — the completion names the provided buffer its data
/// landed in, in the top bits of `flags`.
pub const IORING_CQE_F_BUFFER: u32 = 1;

/// Where the buffer id sits in a CQE's `flags`.
pub const IORING_CQE_BUFFER_SHIFT: u32 = 16;

/// `IORING_CQE_F_MORE` — more completions are still coming for this
/// `user_data`, so the op stays in flight.
pub const IORING_CQE_F_MORE: u32 = 2;

/// `IORING_ACCEPT_MULTISHOT`, in [`Sqe::ioprio`].
pub const IORING_ACCEPT_MULTISHOT: u16 = 1;

/// `IORING_RECV_MULTISHOT`, in [`Sqe::ioprio`].
pub const IORING_RECV_MULTISHOT: u16 = 2;

impl Sqe {
    /// Read an entry the runtime built as a plain struct.
    ///
    /// Copies rather than borrows: an `Sqe` is 64 bytes, and a copy frees the
    /// caller from holding a borrow on the submission queue while it decides
    /// what the op should do.
    #[inline]
    pub fn decode(entry: &squeue::Entry) -> Self {
        // SAFETY: `squeue::Entry` is `#[repr(C)] Entry(sys::io_uring_sqe)`, and
        // `Sqe` is a `#[repr(C)]` restatement of that same kernel struct with
        // its unions collapsed to one member each — identical size, alignment
        // and field offsets, all asserted below. Every byte is initialized:
        // `Entry` is only ever produced by the `io-uring` builders, which zero
        // the struct before filling it.
        unsafe { *(entry as *const squeue::Entry as *const Sqe) }
    }

    /// Whether this op draws its buffer from a provided-buffer ring.
    #[inline]
    pub fn uses_buffer_select(&self) -> bool {
        self.flags & IOSQE_BUFFER_SELECT != 0
    }
}

// The reinterpretation in `decode` rests on these. `io_uring_sqe` is the kernel
// syscall ABI, so none of them can drift without a kernel break — but pinning
// them turns a hypothetical drift into a build failure instead of a fake that
// quietly reads the wrong bytes.
const _: () = assert!(size_of::<Sqe>() == 64);
const _: () = assert!(size_of::<squeue::Entry>() == 64);
const _: () = assert!(align_of::<Sqe>() == align_of::<squeue::Entry>());
const _: () = assert!(std::mem::offset_of!(Sqe, opcode) == 0);
const _: () = assert!(std::mem::offset_of!(Sqe, flags) == 1);
const _: () = assert!(std::mem::offset_of!(Sqe, ioprio) == 2);
const _: () = assert!(std::mem::offset_of!(Sqe, fd) == 4);
const _: () = assert!(std::mem::offset_of!(Sqe, off) == 8);
const _: () = assert!(std::mem::offset_of!(Sqe, addr) == 16);
const _: () = assert!(std::mem::offset_of!(Sqe, len) == 24);
const _: () = assert!(std::mem::offset_of!(Sqe, op_flags) == 28);
const _: () = assert!(std::mem::offset_of!(Sqe, user_data) == 32);
const _: () = assert!(std::mem::offset_of!(Sqe, buf_group) == 40);
const _: () = assert!(std::mem::offset_of!(Sqe, personality) == 42);
const _: () = assert!(std::mem::offset_of!(Sqe, splice_fd_in) == 44);
const _: () = assert!(std::mem::offset_of!(Sqe, addr3) == 48);
