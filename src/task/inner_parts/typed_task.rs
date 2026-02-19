use std::{
    cell::Cell,
    pin::{self},
    ptr::NonNull,
    task::{Context, Waker},
};

use crate::{
    runtime::TASK_LIST,
    task::inner_parts::{Data, Metadata, State, type_agnostic_task::TypeAgnosticTask},
    tracing::TracingMetadata,
};

#[repr(C)]
pub struct TypedTaskInner<F: Future> {
    header: Metadata,
    data: Data<F>,
    waker: Tail,
}

pub struct Tail {
    waker: Option<Waker>,
}

pub struct TypedTask<F: Future> {
    inner: NonNull<TypedTaskInner<F>>,
}

impl<F: Future> TypedTask<F> {
    pub fn register_waker(&mut self, waker: &Waker) {
        let inner = unsafe { self.inner.as_mut() };
        inner.waker.waker.replace(waker.clone());
    }

    fn take_output(&mut self) -> Option<F::Output> {
        let inner = unsafe { self.inner.as_mut() };
        inner.data.state.take()
    }

    pub fn detach(mut self) {
        let inner = unsafe { self.inner.as_mut() };
        inner.header.detached_mode.replace(true);
    }

    pub fn poll(mut self) {
        let inner = unsafe { self.inner.as_mut() };
        let state = &mut inner.data.state;
        match state {
            State::Running(future) => {
                let pinned = unsafe { pin::Pin::new_unchecked(future) };
                let raw_waker = crate::task::waker::raw_waker::<F>(self.inner.as_ptr() as *const _);
                let waker = unsafe { Waker::from_raw(raw_waker) };
                let mut ctx = Context::from_waker(&waker);
                match pinned.poll(&mut ctx) {
                    std::task::Poll::Ready(r) => {
                        *state = State::Finished(r);
                        let registered_waker = &inner.waker.waker;
                        if !inner.header.detached_mode.get() {
                            if let Some(waker) = registered_waker {
                                waker.wake_by_ref();
                            }
                        } else {
                            self.free();
                        }
                    }
                    std::task::Poll::Pending => {}
                }
            }
            State::Finished(_) => unreachable!("finished"),
            State::Ended => unreachable!("ended!"),
        }
    }

    pub fn wake_by_val(self) {
        let erased = self.erase_type();
        TASK_LIST.with(|ctx| {
            ctx.push(erased);
        });
    }

    pub fn wake_by_ref(&self) {
        let erased = self.erase_type();
        TASK_LIST.with(|ctx| {
            ctx.push(erased);
        });
    }

    pub fn free(self) {
        drop(unsafe { Box::from_raw(self.inner.as_ptr()) });
    }

    pub fn erase_type(&self) -> TypeAgnosticTask {
        TypeAgnosticTask::new(unsafe {
            NonNull::new_unchecked(self.inner.as_ptr() as *mut _ as *mut Metadata)
        })
    }

    pub(crate) fn from_raw(raw: NonNull<Metadata>) -> Self {
        Self { inner: raw.cast() }
    }

    pub fn new(future: F) -> Self {
        let ptr = Box::into_raw(Box::new(TypedTaskInner::<F> {
            header: Metadata {
                vtable: table::<F>(),
                detached_mode: Cell::new(false),
                tracing: TracingMetadata::default(),
            },
            data: Data {
                state: State::Running(future),
            },
            waker: Tail { waker: None },
        }));
        Self {
            inner: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

#[derive(Debug)]
pub struct Vtable {
    pub poll: unsafe fn(NonNull<Metadata>),
    pub free: unsafe fn(NonNull<Metadata>),
    pub get_output: unsafe fn(NonNull<Metadata>, *mut (), &Waker),
    pub detach: unsafe fn(NonNull<Metadata>),
}

fn table<F: Future>() -> &'static Vtable {
    &Vtable {
        poll: poll::<F>,
        free: free::<F>,
        get_output: get_output::<F>,
        detach: detach::<F>,
    }
}

unsafe fn poll<F: Future>(ptr: NonNull<Metadata>) {
    let typed_task = TypedTask::<F>::from_raw(ptr);
    typed_task.poll();
}

unsafe fn detach<F: Future>(ptr: NonNull<Metadata>) {
    let typed_task = TypedTask::<F>::from_raw(ptr);
    typed_task.detach();
}

unsafe fn free<F: Future>(ptr: NonNull<Metadata>) {
    let typed_task = TypedTask::<F>::from_raw(ptr);
    typed_task.free();
}

unsafe fn get_output<F: Future>(ptr: NonNull<Metadata>, dest: *mut (), waker: &Waker) {
    let mut typed_task = TypedTask::<F>::from_raw(ptr);
    typed_task.register_waker(waker);
    if let Some(result) = typed_task.take_output() {
        let p = dest as *mut std::task::Poll<F::Output>;
        unsafe { *p = std::task::Poll::Ready(result) };
    }
}
