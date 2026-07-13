use bitfield_struct::bitfield;

/// Two-bit tag in the top bits (62..=63) of every SQE `user_data`, selecting
/// how the remaining 62 bits are interpreted. Read first on every CQE.
pub(crate) const TAG_IO: u8 = 0b00;
pub(crate) const TAG_MESSAGE: u8 = 0b01;
pub(crate) const TAG_IGNORED: u8 = 0b10;
// 0b11 reserved for future use.

/// Kind of an IO op, in bits 60..=61 — only meaningful when the tag is
/// [`TAG_IO`]. `Single` MUST stay 0 (see the invariant on [`UserData`]).
pub(crate) const IO_SINGLE: u8 = 0b00;
pub(crate) const IO_MULTI: u8 = 0b01;
pub(crate) const IO_SEND_BUFFER: u8 = 0b10;

/// Tagged `u64` stored in every io_uring SQE's `user_data`. The kernel copies
/// it verbatim into the completion, so it both routes each CQE (via `tag`) and
/// carries a slab index (IO ops) or a payload (peer messages).
///
/// Layout (LSB → MSB):
///   bits  0..=59 : slab index (IO ops)
///   bits 60..=61 : `io_kind`  (IO ops only)
///   bits 62..=63 : `tag`
///
/// INVARIANT: `TAG_IO == 0` and `IO_SINGLE == 0`, so `single(i).into_bits() ==
/// i`. `send_singleop_cancel_event` cancels by the bare slab token, which only
/// matches the submitted `user_data` because single-op encoding is the
/// identity. Changing either constant off zero silently breaks single-op
/// cancellation.
#[bitfield(u64)]
#[derive(PartialEq, Eq)]
pub(crate) struct UserData {
    #[bits(60)]
    pub index: u64,
    #[bits(2)]
    pub io_kind: u8,
    #[bits(2)]
    pub tag: u8,
}

impl UserData {
    /// Largest slab index the 60-bit field can hold. Keep in sync with the
    /// `#[bits(60)]` on `index`.
    pub const MAX_INDEX: u64 = (1u64 << 60) - 1;

    #[inline]
    #[track_caller]
    const fn check(index: u64) {
        assert!(
            index <= Self::MAX_INDEX,
            "io_uring user_data slab index exceeds index field",
        );
    }

    #[inline]
    pub const fn single(index: u64) -> Self {
        Self::check(index);
        Self::new()
            .with_index(index)
            .with_io_kind(IO_SINGLE)
            .with_tag(TAG_IO)
    }

    #[inline]
    pub const fn multi(index: u64) -> Self {
        Self::check(index);
        Self::new()
            .with_index(index)
            .with_io_kind(IO_MULTI)
            .with_tag(TAG_IO)
    }

    #[inline]
    pub const fn send_buffer_op(index: u64) -> Self {
        Self::check(index);
        Self::new()
            .with_index(index)
            .with_io_kind(IO_SEND_BUFFER)
            .with_tag(TAG_IO)
    }

    /// Standalone entry whose completion is dropped on arrival — linked
    /// timeouts, wakeup timeouts, the MSG_RING sender completion.
    #[inline]
    pub const fn ignored_only() -> Self {
        Self::new().with_tag(TAG_IGNORED)
    }

    /// `user_data` for an `AsyncCancel` SQE so its own completion is dropped.
    /// Only the tag matters; the cancelled op's kind bits are never read.
    #[inline]
    pub const fn cancel_echo(original: u64) -> Self {
        Self::from_bits(original).with_tag(TAG_IGNORED)
    }
}

// Compile-time guard for the single-op identity invariant: `send_singleop_
// cancel_event` cancels by the bare slab token, so `single(i).into_bits()` must
// equal `i`. This holds iff the tag and single-op-kind bits are both zero.
const _: () = assert!(
    TAG_IO == 0 && IO_SINGLE == 0,
    "single-op cancellation requires TAG_IO == 0 and IO_SINGLE == 0",
);
const _: () = assert!(UserData::single(0x1234_5678_9abc).into_bits() == 0x1234_5678_9abc);
const _: () = assert!(UserData::single(UserData::MAX_INDEX).into_bits() == UserData::MAX_INDEX);

/// The peer-message view of the same `user_data` u64, selected when the tag is
/// [`TAG_MESSAGE`]. The low 62 bits are a caller-defined payload; the top 2
/// bits hold the tag.
#[bitfield(u64)]
#[derive(PartialEq, Eq)]
pub(crate) struct PeerUserData {
    #[bits(62)]
    pub payload: u64,
    #[bits(2)]
    pub tag: u8,
}

impl PeerUserData {
    /// Largest payload the 62-bit field can hold.
    pub const MAX_PAYLOAD: u64 = (1u64 << 62) - 1;

    #[inline]
    #[track_caller]
    pub const fn message(payload: u64) -> Self {
        assert!(
            payload <= Self::MAX_PAYLOAD,
            "peer message payload exceeds the 62-bit field",
        );
        Self::new().with_payload(payload).with_tag(TAG_MESSAGE)
    }
}
