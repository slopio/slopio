use std::{cell::Cell, mem};

use crate::task::{
    TypeAgnosticTask,
    handle::Handle,
    inner_parts::typed_task::{TypedTask, Vtable},
};
#[cfg(feature = "tracing")]
use crate::tracing::TaskTelemetry;

pub(crate) mod type_agnostic_task;
pub(crate) mod typed_task;

pub struct Metadata {
    detached_mode: Cell<bool>,
    /// Set by `Handle::kill`; the next scheduler pop drops the task instead of polling it.
    killed: Cell<bool>,
    pub(crate) queued: Cell<bool>,
    /// Live wakers pointing here, plus one while the scheduler polls.
    wakers: Cell<u32>,
    vtable: &'static Vtable,
    #[cfg(feature = "tracing")]
    pub(crate) tracing: TaskTelemetry,
}

enum State<T: Future> {
    Running(T),
    Finished(T::Output),
    Ended,
}

struct Data<F: Future> {
    state: State<F>,
}

impl<T: Future> State<T> {
    fn take(&mut self) -> Option<T::Output> {
        if matches!(self, State::Finished(_)) {
            let old = mem::replace(self, State::Ended);
            match old {
                State::Running(_) => unreachable!("running"),
                State::Finished(output) => Some(output),
                State::Ended => unreachable!("ended"),
            }
        } else {
            None
        }
    }
}

// `'static` is load-bearing: the task may outlive the caller's stack frame
// (it is freed by the runtime, not the spawner), so the future must not
// borrow anything shorter-lived.
pub fn new_task<F: Future + 'static>(f: F) -> (TypeAgnosticTask, Handle<F::Output>) {
    let task = TypedTask::new(f);
    let erased_type_task = task.erase_type();
    let handle = Handle::from(erased_type_task);
    (erased_type_task, handle)
}
