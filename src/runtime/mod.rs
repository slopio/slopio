use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::pin::pin;
use std::task::{Context, Waker};

use crate::io_driver::IODriver;
use crate::scoped_thread_local;
use crate::task::{Handle, TypeAgnosticTask, new_task};

pub mod join;
pub mod select;

#[derive(Default)]
pub struct RuntimeInner {
    pub tasks: TaskList,
    io_driver: IODriver,
}

impl RuntimeInner {
    pub fn block_on<F: Future>(self, f: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        self.io_driver.with(|| {
            CURRENT.set(&self.tasks, || {
                let mut pinned_f = pin!(f);
                loop {
                    self.io_driver.process_completion_queue();
                    while let Some(item) = self.tasks.pop() {
                        item.poll();
                    }
                    let pinned = pinned_f.as_mut();
                    match pinned.poll(&mut context) {
                        std::task::Poll::Ready(output) => {
                            return output;
                        }
                        std::task::Poll::Pending => {}
                    }
                    if !self.tasks.is_empty() {
                        if self.io_driver.has_pending_submissions() {
                            self.io_driver.submit_without_wait();
                        }
                        continue;
                    }
                    self.io_driver.submit_and_wait()
                }
            })
        })
    }
}

pub fn spawn<F: Future>(f: F) -> Handle<F::Output> {
    let (task, handle) = new_task(f);
    CURRENT.with(|ctx| {
        ctx.push(task);
    });
    handle
}

scoped_thread_local!(pub(crate) static CURRENT: TaskList);

#[derive(Default)]
pub struct TaskList {
    tasks: UnsafeCell<VecDeque<TypeAgnosticTask>>,
}

impl TaskList {
    pub(crate) fn push(&self, task: TypeAgnosticTask) {
        let tasks_list = unsafe { &mut *self.tasks.get() };
        tasks_list.push_back(task);
    }

    pub(crate) fn is_empty(&self) -> bool {
        let tasks_list = unsafe { &*self.tasks.get() };
        tasks_list.is_empty()
    }

    pub(crate) fn pop(&self) -> Option<TypeAgnosticTask> {
        let task_list = unsafe { &mut *self.tasks.get() };
        task_list.pop_front()
    }
}
