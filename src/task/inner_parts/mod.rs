use std::mem;

use crate::task::{
    TypeAgnosticTask,
    handle::Handle,
    inner_parts::typed_task::{TypedTask, Vtable},
};

pub(crate) mod type_agnostic_task;
pub(crate) mod typed_task;

pub struct TypeInformation {
    vtable: &'static Vtable,
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

pub fn new_task<F: Future>(f: F) -> (TypeAgnosticTask, Handle<F::Output>) {
    let mut task = TypedTask::new(f);
    let erased_type_task = task.erase_type();
    let handle = Handle::from(erased_type_task);
    (erased_type_task, handle)
}
