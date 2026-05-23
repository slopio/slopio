use bitfield_struct::bitfield;

/// Tagged `u64` we store in every io_uring SQE's `user_data` field. The
/// kernel copies it verbatim into the completion, so we use it to route
/// each CQE back to its slab entry and op kind.
///
/// Layout (LSB → MSB):
///   bits  0..=60 : slab index
///   bit       61 : `send_buffer`
///   bit       62 : `ignored`   (drop this CQE on arrival)
///   bit       63 : `multi_op`
#[bitfield(u64)]
#[derive(PartialEq, Eq)]
pub(crate) struct UserData {
    #[bits(61)]
    pub index: u64,
    pub send_buffer: bool,
    pub ignored: bool,
    pub multi_op: bool,
}

impl UserData {
    /// Largest value the index field can hold without truncation. Derived
    /// by computing the flag bitmask (all flags set) and complementing it,
    /// so it stays in sync with `#[bits(...)]` and the flag field set.
    pub const MAX_INDEX: u64 = !Self::new()
        .with_send_buffer(true)
        .with_ignored(true)
        .with_multi_op(true)
        .into_bits();

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
        Self::new().with_index(index)
    }

    #[inline]
    pub const fn multi(index: u64) -> Self {
        Self::check(index);
        Self::new().with_index(index).with_multi_op(true)
    }

    #[inline]
    pub const fn send_buffer_op(index: u64) -> Self {
        Self::check(index);
        Self::new().with_index(index).with_send_buffer(true)
    }

    /// Standalone ignored entry — used for linked timeouts that carry no
    /// slab token.
    #[inline]
    pub const fn ignored_only() -> Self {
        Self::new().with_ignored(true)
    }

    /// Echo of an existing `user_data` with IGNORED set. Used as the
    /// `user_data` of an `AsyncCancel` SQE so its own completion is
    /// dropped on arrival. Preserves the original kind bits.
    #[inline]
    pub const fn cancel_echo(original: u64) -> Self {
        Self::from_bits(original).with_ignored(true)
    }
}
