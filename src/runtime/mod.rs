use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::pin::pin;
use std::task::{Context, Waker};

use crate::io_driver::IODriver;
use crate::scoped_thread_local;
use crate::task::Task;

#[derive(Default)]
pub struct RuntimeInner {
    tasks: TaskList,
    io_driver: IODriver,
}

impl RuntimeInner {
    pub fn block_on<F: Future>(&mut self, f: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        self.io_driver.with(|| {
            CURRENT.set(&self.tasks, || {
                let mut pinned_f = pin!(f);
                loop {
                    self.io_driver.tick();
                    for item in self.tasks.drain() {
                        item.poll();
                    }
                    let pinned = pinned_f.as_mut();
                    match pinned.poll(&mut context) {
                        std::task::Poll::Ready(output) => {
                            return output;
                        }
                        std::task::Poll::Pending => {}
                    }
                }
            })
        })
    }
}

pub fn spawn(f: impl Future) -> Join {
    CURRENT.with(|ctx| {
        ctx.push(Task {});
    });
    Join {}
}

pub struct Join;

scoped_thread_local!(pub(crate) static CURRENT: TaskList);

#[derive(Default)]
pub struct TaskList {
    tasks: UnsafeCell<VecDeque<Task>>,
}

impl TaskList {
    fn push(&self, task: Task) {
        let tasks_list = unsafe { &mut *self.tasks.get() };
        tasks_list.push_back(task);
    }

    fn drain(&self) -> std::collections::vec_deque::Drain<'_, Task> {
        let tasks_list = unsafe { &mut *self.tasks.get() };
        tasks_list.drain(..)
    }
}
