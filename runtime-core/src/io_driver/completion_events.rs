use io_uring::cqueue::{self, Entry, buffer_select};
use libc::ECANCELED;

use super::{MultiOpState, MultishotCompletionEvent, OpMetadata, SingleOpState};

pub(crate) const MULTI_OP_FLAG: u64 = 1u64 << 63;
pub(crate) const IGNORED_FLAG: u64 = 1u64 << 62;
pub(crate) const SEND_BUFFER_FLAG: u64 = 1u64 << 61;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Data {
    pub result: i32,
    pub flags: u32,
}

impl From<Entry> for Data {
    fn from(entry: Entry) -> Self {
        Self {
            result: entry.result(),
            flags: entry.flags(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Event {
    SingleShot(Data),
    MultiShot(Data),
    SendBuffer(i32),
}

#[derive(Debug)]
pub(crate) struct CompletionEvent {
    pub slab_index: usize,
    pub event: Event,
}

impl CompletionEvent {
    pub fn from_entry(entry: Entry) -> Option<Self> {
        let user_data = entry.user_data();

        if user_data & IGNORED_FLAG != 0 {
            return None;
        }

        let data = Data::from(entry);

        if user_data & SEND_BUFFER_FLAG != 0 {
            Some(CompletionEvent {
                slab_index: (user_data & !(SEND_BUFFER_FLAG)) as usize,
                event: Event::SendBuffer(data.result),
            })
        } else if user_data & MULTI_OP_FLAG != 0 {
            Some(CompletionEvent {
                slab_index: user_data as usize,
                event: Event::MultiShot(data),
            })
        } else {
            Some(CompletionEvent {
                slab_index: user_data as usize,
                event: Event::SingleShot(data),
            })
        }
    }

    pub fn clean_slab_index(&self) -> usize {
        (self.slab_index as u64 & !MULTI_OP_FLAG) as usize
    }
}

#[derive(Debug)]
pub(crate) enum SingleProcessResult {
    Completed,
    Shutdown,
    CancelledRecycling { id: u16, len: i32, bgid: u16 },
    InvalidState(&'static str),
}

#[derive(Debug)]
pub(crate) enum MultiProcessResult {
    Queued,
    WakeConsumer,
    PauseRequired(u64),
    FullyPaused,
    InvalidState(&'static str),
}

pub(crate) fn process_single(op_state: &mut SingleOpState, data: Data) -> SingleProcessResult {
    match op_state {
        SingleOpState::Init => {
            SingleProcessResult::InvalidState("submitted entry shouldn't be in init")
        }
        SingleOpState::Sent(waker) => {
            waker.wake_by_ref();
            *op_state = SingleOpState::Ended(OpMetadata {
                result: data.result,
                flags: data.flags,
            });
            SingleProcessResult::Completed
        }
        SingleOpState::Ended(_) => SingleProcessResult::InvalidState("received CQE for ended op"),
        SingleOpState::Signal(_) => SingleProcessResult::Shutdown,
        SingleOpState::Cancelled { bgid } => {
            if data.result == -ECANCELED {
                SingleProcessResult::Completed
            } else {
                if let Some(id) = buffer_select(data.flags)
                    && data.result > 0
                    && let Some(bgid) = bgid
                {
                    SingleProcessResult::CancelledRecycling {
                        id,
                        len: data.result,
                        bgid: *bgid,
                    }
                } else {
                    //no buffers were taken from the ring or there was an error
                    SingleProcessResult::Completed
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

        MultiOpState::Sent { queue, .. } => match queue.push(event) {
            super::QueueInsertEvent::Ok => MultiProcessResult::Queued,
            super::QueueInsertEvent::WasEmpty => {
                multi_op.wake();
                MultiProcessResult::WakeConsumer
            }
            super::QueueInsertEvent::TooFull => {
                multi_op.transition_to_trying_to_pause();
                MultiProcessResult::PauseRequired(user_data)
            }
        },

        MultiOpState::TringToPause {
            queue,
            ecancelled,
            last_cqe,
            ..
        } => {
            if !cqueue::more(event.flags) {
                *last_cqe = true;
            }

            if event.result == -ECANCELED {
                *ecancelled = true;
            }

            queue.push(event);

            // Check if we've fully paused
            if *ecancelled && *last_cqe {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flag_constants() {
        assert_eq!(MULTI_OP_FLAG & IGNORED_FLAG, 0);

        let combined = MULTI_OP_FLAG | IGNORED_FLAG;
        assert!(combined & MULTI_OP_FLAG != 0);
        assert!(combined & IGNORED_FLAG != 0);

        let slab_index: u64 = 42;
        let user_data = slab_index | MULTI_OP_FLAG;
        assert_eq!((user_data & !MULTI_OP_FLAG) as usize, 42);
    }
}
