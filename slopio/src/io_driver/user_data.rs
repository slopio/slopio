use bitfield_struct::bitfield;

/// Two-bit tag in the top bits (62..=63) of every SQE `user_data`, selecting
/// how the remaining 62 bits are interpreted. Read first on every CQE.
pub(crate) const TAG_IO: u8 = 0b00;
pub(crate) const TAG_MESSAGE: u8 = 0b01;
pub(crate) const TAG_IGNORED: u8 = 0b10;
/// The 62-bit payload is a thin pointer to a heap `Message<T>` (see
/// [`MsgPtrUserData`]). A user-space pointer fits in 62 bits, leaving the tag
/// bits free.
pub(crate) const TAG_MSG_PTR: u8 = 0b11;

/// Kind of an IO op, in bits 60..=61 — only meaningful when the tag is
/// [`TAG_IO`]. `Single` MUST stay 0 (see the invariant on [`UserData`]).
pub(crate) const IO_SINGLE: u8 = 0b00;
pub(crate) const IO_MULTI: u8 = 0b01;
// 0b10 is free.
/// Not an IO op at all: a pointer-free reply-slot wakeup posted by a peer over
/// MSG_RING. It lives in this sub-tag space because the four top-level tags are
/// already allocated, and it fits naturally — the 60-bit `index` field carries
/// the reply-slot index, which is what `index` means for every other
/// [`TAG_IO`] value too.
///
/// Unlike [`TAG_MSG_PTR`], it carries no pointer and transfers no ownership:
/// there is nothing to reclaim on arrival, so a wakeup that finds no slot is
/// simply dropped.
pub(crate) const IO_REPLY_SIGNAL: u8 = 0b11;

/// Tagged `u64` stored in every io_uring SQE's `user_data`. The kernel copies
/// it verbatim into the completion, so it both routes each CQE (via `tag`) and
/// carries a slab index (IO ops) or a payload (peer messages).
///
/// Layout (LSB → MSB):
///   bits  0..=57 : slab index (IO ops)
///   bits 58..=59 : buffer ring of the op, read only on a CQE with `F_BUFFER`
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
    #[bits(58)]
    pub index: u64,
    #[bits(2)]
    pub ring: u8,
    #[bits(2)]
    pub io_kind: u8,
    #[bits(2)]
    pub tag: u8,
}

impl UserData {
    /// Largest slab index the 58-bit field can hold. Keep in sync with the
    /// `#[bits(58)]` on `index`.
    pub const MAX_INDEX: u64 = (1u64 << 58) - 1;
    /// How many buffer rings the `ring` field can name.
    pub const MAX_RINGS: u16 = 4;
    /// Largest token: a slab index with its ring bits on top.
    pub const MAX_TOKEN: u64 = (1u64 << 60) - 1;

    /// A slab index and the op's buffer ring, as one token value.
    #[inline]
    pub const fn token(index: u64, ring: u16) -> u64 {
        assert!(index <= Self::MAX_INDEX, "slab index exceeds index field");
        assert!(ring < Self::MAX_RINGS, "buffer ring exceeds ring field");
        index | (ring as u64) << 58
    }

    /// The slab index of a token, ring bits dropped.
    #[inline]
    pub const fn token_index(token: u64) -> usize {
        (token & Self::MAX_INDEX) as usize
    }

    #[inline]
    #[track_caller]
    const fn check(token: u64) {
        assert!(
            token <= Self::MAX_TOKEN,
            "io_uring user_data token exceeds its field",
        );
    }

    #[inline]
    pub const fn single(token: u64) -> Self {
        Self::check(token);
        Self::from_bits(token)
            .with_io_kind(IO_SINGLE)
            .with_tag(TAG_IO)
    }

    #[inline]
    pub const fn multi(token: u64) -> Self {
        Self::check(token);
        Self::from_bits(token)
            .with_io_kind(IO_MULTI)
            .with_tag(TAG_IO)
    }

    /// `user_data` for a pointer-free reply-slot wakeup aimed at `slot` on the
    /// receiving worker. See [`IO_REPLY_SIGNAL`].
    #[inline]
    pub const fn reply_signal(slot: u64) -> Self {
        Self::check(slot);
        Self::new()
            .with_index(slot)
            .with_io_kind(IO_REPLY_SIGNAL)
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
const _: () = assert!(UserData::single(UserData::MAX_TOKEN).into_bits() == UserData::MAX_TOKEN);
const _: () = assert!(UserData::multi(UserData::token(7, 3)).ring() == 3);
const _: () = assert!(UserData::multi(UserData::token(7, 3)).index() == 7);

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

/// The message-pointer view, selected when the tag is [`TAG_MSG_PTR`]. The low 62
/// bits are a thin pointer to a heap `Message<T>`; the top 2 bits hold the tag.
#[bitfield(u64)]
#[derive(PartialEq, Eq)]
pub(crate) struct MsgPtrUserData {
    #[bits(62)]
    pub ptr: u64,
    #[bits(2)]
    pub tag: u8,
}

impl MsgPtrUserData {
    #[inline]
    #[track_caller]
    pub const fn for_msg(ptr: u64) -> Self {
        assert!(
            ptr <= PeerUserData::MAX_PAYLOAD,
            "message pointer does not fit in the 62-bit field",
        );
        Self::new().with_ptr(ptr).with_tag(TAG_MSG_PTR)
    }
}
