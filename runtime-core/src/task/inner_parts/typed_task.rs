use std::{
    cell::Cell,
    mem::ManuallyDrop,
    pin::{self},
    ptr::NonNull,
    task::{Context, Waker},
};

#[cfg(feature = "tracing")]
use crate::tracing::{TaskTelemetry, current_trace_id};
use crate::{
    runtime::TASK_LIST,
    task::inner_parts::{Data, Metadata, State, type_agnostic_task::TypeAgnosticTask},
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
        match &mut inner.waker.waker {
            Some(held) if held.will_wake(waker) => {}
            slot => *slot = Some(waker.clone()),
        }
    }

    fn take_output(&mut self) -> Option<F::Output> {
        let inner = unsafe { self.inner.as_mut() };
        inner.data.state.take()
    }

    /// The handle is gone: free now if nothing else can reach us.
    pub fn detach(self) {
        // Tail only: the task may be dropping its own handle mid-poll.
        let tail = unsafe { &mut (*self.inner.as_ptr()).waker };
        // Nobody awaits the output anymore: release the awaiting task's waker.
        let awaiter = tail.waker.take();
        let header = unsafe { &(*self.inner.as_ptr()).header };
        header.detached_mode.set(true);
        self.free_if_unreachable();
        drop(awaiter);
    }

    pub fn poll(self) {
        let ptr = self.inner.as_ptr();
        // Disjoint borrows: wakers and `detach` reach the header and tail mid-poll.
        let header = unsafe { &(*ptr).header };
        let state = unsafe { &mut (*ptr).data.state };
        header.queued.set(false);
        // One ref for the whole poll: nothing can free the task under us.
        header.wakers.set(header.wakers.get() + 1);
        if header.killed.get() {
            Self::reap(state);
        } else if let State::Running(future) = state {
            let pinned = unsafe { pin::Pin::new_unchecked(future) };
            let raw_waker = crate::task::waker::raw_waker::<F>(ptr as *const _);
            // Never dropped: its ref is the one taken above.
            let waker = ManuallyDrop::new(unsafe { Waker::from_raw(raw_waker) });
            let mut ctx = Context::from_waker(&waker);
            if let std::task::Poll::Ready(r) = pinned.poll(&mut ctx) {
                *state = State::Finished(r);
                // Taken and woken by value: the awaiter only needs waking once.
                if !header.detached_mode.get()
                    && let Some(waker) = unsafe { &mut (*ptr).waker }.waker.take()
                {
                    waker.wake();
                }
            }
        }
        self.release();
    }

    /// Kill path: drops the future, or an output nobody will collect.
    #[cold]
    #[inline(never)]
    fn reap(state: &mut State<F>) {
        *state = State::Ended;
    }

    pub fn wake_by_val(self) {
        self.wake_by_ref();
        self.release();
    }

    pub fn wake_by_ref(&self) {
        // Header only: the future's bytes may be under a protected `&mut` mid-poll.
        let header = unsafe { &(*self.inner.as_ptr()).header };
        if header.queued.get() {
            return;
        }
        header.queued.set(true);
        let erased = self.erase_type();
        TASK_LIST.with(|ctx| {
            ctx.push(erased);
        });
    }

    /// One more waker points here.
    pub(crate) fn retain(&self) {
        let header = unsafe { &(*self.inner.as_ptr()).header };
        header.wakers.set(header.wakers.get() + 1);
    }

    /// One waker fewer; frees the task once nothing can reach it.
    pub(crate) fn release(self) {
        let header = unsafe { &(*self.inner.as_ptr()).header };
        header.wakers.set(header.wakers.get() - 1);
        self.free_if_unreachable();
    }

    /// Header first: `state` is read only when no poll is in progress.
    fn free_if_unreachable(self) {
        let header = unsafe { &(*self.inner.as_ptr()).header };
        if header.wakers.get() != 0 || header.queued.get() || !header.detached_mode.get() {
            return;
        }
        let inner = unsafe { self.inner.as_ref() };
        if !matches!(inner.data.state, State::Running(_)) {
            self.free();
        }
    }

    fn free(self) {
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
                detached_mode: Cell::new(false),
                killed: Cell::new(false),
                queued: Cell::new(true),
                wakers: Cell::new(0),
                vtable: table::<F>(),
                #[cfg(feature = "tracing")]
                tracing: match current_trace_id() {
                    Some(trace_id) => TaskTelemetry::with_trace_id(trace_id),
                    None => TaskTelemetry::default(),
                },
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
    pub get_output: unsafe fn(NonNull<Metadata>, *mut (), &Waker),
    pub detach: unsafe fn(NonNull<Metadata>),
}

fn table<F: Future>() -> &'static Vtable {
    &Vtable {
        poll: poll::<F>,
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

unsafe fn get_output<F: Future>(ptr: NonNull<Metadata>, dest: *mut (), waker: &Waker) {
    let mut typed_task = TypedTask::<F>::from_raw(ptr);
    match typed_task.take_output() {
        Some(result) => {
            let p = dest as *mut std::task::Poll<F::Output>;
            unsafe { *p = std::task::Poll::Ready(result) };
        }
        // Only a handle that still waits needs to be woken later.
        None => typed_task.register_waker(waker),
    }
}
