use io_uring::cqueue::{self, Entry, buffer_select};
use libc::ECANCELED;

use super::{
    ErasedMessage, IO_MULTI, IO_SEND_BUFFER, MsgPtrUserData, MultiOpState,
    MultishotCompletionEvent, OpMetadata, PeerUserData, SingleOpState, TAG_IGNORED, TAG_IO,
    TAG_MESSAGE, TAG_MSG_PTR, UserData,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct CQEData {
    pub result: i32,
    pub flags: u32,
}

impl From<Entry> for CQEData {
    fn from(entry: Entry) -> Self {
        Self {
            result: entry.result(),
            flags: entry.flags(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Event {
    SingleShot(CQEData),
    MultiShot(CQEData),
    SendBuffer(i32),
    /// MSG_RING delivered by another worker. Carries no slab entry: `payload`
    /// is the 62-bit `user_data` field and `data` the CQE `res`.
    PeerMessage { payload: u64, data: i32 },
    /// A heap-allocated, type-erased message delivered by another worker. The
    /// 62-bit `user_data` was a thin pointer; we own the allocation now.
    TypedMessage(ErasedMessage),
}

#[derive(Debug)]
pub(crate) struct CompletionEvent {
    pub slab_index: usize,
    pub event: Event,
}

impl CompletionEvent {
    pub fn from_entry(entry: Entry) -> Option<Self> {
        let raw = entry.user_data();
        match UserData::from_bits(raw).tag() {
            TAG_IGNORED => None,
            TAG_MESSAGE => {
                // No slab entry: the low 62 bits are a caller-defined payload.
                let payload = PeerUserData::from_bits(raw).payload();
                Some(CompletionEvent {
                    slab_index: 0,
                    event: Event::PeerMessage {
                        payload,
                        data: entry.result(),
                    },
                })
            }
            TAG_MSG_PTR => {
                // The 62-bit field is a thin pointer to a heap `Message<T>`;
                // reclaiming it here transfers ownership to the driver.
                let ptr = MsgPtrUserData::from_bits(raw).ptr();
                let erased = unsafe { ErasedMessage::from_raw(ptr) };
                Some(CompletionEvent {
                    slab_index: 0,
                    event: Event::TypedMessage(erased),
                })
            }
            TAG_IO => {
                let ud = UserData::from_bits(raw);
                let data = CQEData::from(entry);
                let event = match ud.io_kind() {
                    IO_SEND_BUFFER => Event::SendBuffer(data.result),
                    IO_MULTI => Event::MultiShot(data),
                    _ /* IO_SINGLE */ => Event::SingleShot(data),
                };
                Some(CompletionEvent {
                    slab_index: ud.index() as usize,
                    event,
                })
            }
            _ /* reserved */ => {
                debug_assert!(false, "unknown user_data tag");
                None
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum SingleProcessResult {
    Completed,
    /// CQE for an op whose future was dropped — slab entry (and any keepalive)
    /// must be released.
    CancelComplete,
    /// CQE for a non-terminal link in a chain — slab entry should be reaped.
    LinkCompleted,
    Shutdown,
    CancelledRecycling { id: u16, bgid: u16 },
    InvalidState(&'static str),
}

#[derive(Debug)]
pub(crate) enum MultiProcessResult {
    Queued,
    WakeConsumer,
    PauseRequired(u64),
    FullyPaused,
    CancelComplete,
    /// All CQEs accounted for after an accept cancel — incoming fds (if any)
    /// were already closed by `process_multi`. Slab entry can be reaped.
    AcceptCancelComplete,
    CancelledRecycling {
        buffer_id: u16,
        bgid: u16,
    },
    InvalidState(&'static str),
}

pub(crate) fn process_single(op_state: &mut SingleOpState, data: CQEData) -> SingleProcessResult {
    match op_state {
        SingleOpState::Init => {
            SingleProcessResult::InvalidState("submitted entry shouldn't be in init")
        }
        SingleOpState::Sent(waker) => {
            if cqueue::more(data.flags) {
                // Zero-copy completion: a notification still follows, and the
                // kernel may keep reading the buffer until it lands. Stash the
                // byte count and wait — do NOT wake, the future can't make
                // progress while its buffer is pinned.
                *op_state = SingleOpState::Notif {
                    result: data.result,
                    waker: waker.clone(),
                };
            } else {
                // Normal send, or a zero-copy error / copy-fallback success:
                // no notification is coming, so this CQE is terminal.
                waker.wake_by_ref();
                *op_state = SingleOpState::Ended(OpMetadata {
                    result: data.result,
                    flags: data.flags,
                });
            }
            SingleProcessResult::Completed
        }
        SingleOpState::Notif { result, waker } => {
            // The zero-copy notification (`F_NOTIF`): the buffer is now free.
            // Deliver the byte count stashed from the completion CQE.
            waker.wake_by_ref();
            *op_state = SingleOpState::Ended(OpMetadata {
                result: *result,
                flags: data.flags,
            });
            SingleProcessResult::Completed
        }
        SingleOpState::Ended(_) => SingleProcessResult::InvalidState("received CQE for ended op"),
        SingleOpState::Signal(_) => SingleProcessResult::Shutdown,
        SingleOpState::LinkedSent(_next) => SingleProcessResult::LinkCompleted,
        SingleOpState::Cancelled { bgid, .. } => {
            if cqueue::more(data.flags) {
                // Zero-copy completion landed mid-cancel; the notification (and
                // the buffer release) is still pending. Keep the keepalive and
                // the slab entry alive until the final no-`F_MORE` CQE.
                return SingleProcessResult::Completed;
            }
            if data.result == -ECANCELED {
                SingleProcessResult::CancelComplete
            } else {
                if let Some(id) = buffer_select(data.flags)
                    && data.result > 0
                    && let Some(bgid) = bgid
                {
                    SingleProcessResult::CancelledRecycling {
                        id,
                        bgid: *bgid,
                    }
                } else {
                    //no buffers were taken from the ring or there was an error
                    SingleProcessResult::CancelComplete
                }
            }
        }
    }
}

pub(crate) fn process_multi(
    multi_op: &mut MultiOpState,
    event: MultishotCompletionEvent,
    user_data: u64,
) -> MultiProcessResult {
    match multi_op {
        MultiOpState::Init { .. } => {
            MultiProcessResult::InvalidState("received CQE for operation still in Init state")
        }

        MultiOpState::Sent {
            queue, last_cqe, ..
        } => {
            if !cqueue::more(event.flags) {
                // Terminal CQE (EOF, error): the kernel posts nothing further
                // for this token. Recorded so a later cancel can reap the
                // slab entry directly instead of waiting for a CQE that will
                // never come.
                *last_cqe = true;
            }
            let terminated = *last_cqe;
            match queue.push(event) {
                super::QueueInsertEvent::Ok => MultiProcessResult::Queued,
                super::QueueInsertEvent::WasEmpty => {
                    multi_op.wake();
                    MultiProcessResult::WakeConsumer
                }
                super::QueueInsertEvent::TooFull => {
                    if terminated {
                        // The op already stopped on its own — there is
                        // nothing left to pause, and a cancel SQE would
                        // strand the entry in `TringToPause`.
                        MultiProcessResult::Queued
                    } else {
                        multi_op.transition_to_trying_to_pause();
                        MultiProcessResult::PauseRequired(user_data)
                    }
                }
            }
        }

        MultiOpState::TringToPause {
            queue, last_cqe, ..
        } => {
            if !cqueue::more(event.flags) {
                *last_cqe = true;
            }
            let paused = *last_cqe;
            queue.push(event);

            // Whether the final CQE is our pause-cancel's -ECANCELED or the
            // op terminating on its own (EOF/error), no further CQE is
            // coming — the pause is complete either way.
            if paused {
                multi_op.transition_to_paused();
                multi_op.wake();
                MultiProcessResult::FullyPaused
            } else {
                MultiProcessResult::Queued
            }
        }

        MultiOpState::Paused { queue, .. } => {
            queue.push(event);
            MultiProcessResult::InvalidState("received CQE while paused")
        }

        MultiOpState::AcceptCancelled { last_cqe } => {
            if !cqueue::more(event.flags) {
                *last_cqe = true;
            }
            // Any positive result is a freshly-accepted fd we'll never hand
            // to the user — close it.
            if event.result > 0 {
                unsafe {
                    libc::close(event.result);
                }
            }
            if *last_cqe {
                MultiProcessResult::AcceptCancelComplete
            } else {
                MultiProcessResult::Queued
            }
        }

        MultiOpState::Cancelled {
            bgid, last_cqe, ..
        } => {
            if !cqueue::more(event.flags) {
                *last_cqe = true;
            }

            // Recycle buffer if the kernel consumed one
            let recycle = if event.result > 0 {
                buffer_select(event.flags).map(|buffer_id| (buffer_id, *bgid))
            } else {
                None
            };

            if *last_cqe {
                if let Some((buffer_id, bgid)) = recycle {
                    // This last CQE also had a buffer — recycle it, then signal complete
                    return MultiProcessResult::CancelledRecycling { buffer_id, bgid };
                }
                MultiProcessResult::CancelComplete
            } else if let Some((buffer_id, bgid)) = recycle {
                MultiProcessResult::CancelledRecycling { buffer_id, bgid }
            } else {
                MultiProcessResult::Queued
            }
        }
    }
}

