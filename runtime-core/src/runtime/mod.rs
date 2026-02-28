use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::pin::pin;
use std::task::{Context, Waker};
use std::time::Duration;

use crate::io_driver::IODriver;
use crate::runtime::time::TimeDriver;
use crate::task::{Handle, TypeAgnosticTask, new_task};
use crate::{runtime, scoped_thread_local};

pub mod eventfd;
pub mod join;
pub mod select;
pub mod time;

scoped_thread_local!(pub(crate) static TASK_LIST: TaskList);
scoped_thread_local!(pub static TIME_DRIVER: TimeDriver);
thread_local!(static THREAD_ID: std::cell::Cell<usize> = const { std::cell::Cell::new(0) });

#[derive(Default)]
pub struct RuntimeInner {
    pub tasks: TaskList,
    io_driver: IODriver,
    time_driver: TimeDriver,
    thread_id: usize,
}

impl RuntimeInner {
    pub fn with_thread_id(thread_id: usize) -> Self {
        Self {
            thread_id,
            ..Default::default()
        }
    }

    pub fn block_on<F: Future>(
        self,
        f: F,
        event_fd: Option<RawFd>,
        wrap: impl FnOnce(Box<dyn FnOnce() -> F::Output + '_>) -> F::Output,
    ) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        set_thread_id(self.thread_id);
        self.io_driver.with(|| {
            TASK_LIST.set(&self.tasks, || {
                TIME_DRIVER.set(&self.time_driver, || {
                    let run: Box<dyn FnOnce() -> F::Output + '_> = Box::new(|| {
                        if let Some(fd) = event_fd {
                            runtime::eventfd::register_eventfd_read(fd);
                        }
                        let mut pinned_f = pin!(f);
                        loop {
                            loop {
                                self.time_driver.tick();
                                //TODO limit activity
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
                                if self.tasks.is_empty() {
                                    break;
                                }
                            }
                            match self.time_driver.next_deadline() {
                                Some(delay) if !delay.is_zero() => {
                                    self.io_driver.push_wakeup_timeout(delay);
                                    self.io_driver.submit_and_wait();
                                }
                                Some(_) => {
                                    //10 ms = next timewheel tick
                                    self.io_driver
                                        .push_wakeup_timeout(Duration::from_millis(10));
                                    self.io_driver.submit_and_wait();
                                }
                                None => {
                                    self.io_driver.submit_and_wait();
                                }
                            }
                            self.io_driver.process_completion_queue();
                        }
                    });
                    wrap(run)
                })
            })
        })
    }
}

pub fn set_thread_id(id: usize) {
    THREAD_ID.with(|cell| cell.set(id));
}

pub fn thread_id() -> usize {
    THREAD_ID.with(|cell| cell.get())
}

pub fn main_thread<F: Future>(f: F) -> F::Output {
    RuntimeInner::default().block_on(f, None, |run| run())
}

pub fn spawn<F: Future>(f: F) -> Handle<F::Output> {
    let (task, handle) = new_task(f);
    TASK_LIST.with(|ctx| {
        ctx.push(task);
    });
    handle
}

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
