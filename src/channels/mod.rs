use std::{cell::UnsafeCell, collections::VecDeque, future::poll_fn, rc::Rc, task::Waker};

struct SharedState<T> {
    data: VecDeque<T>,
    waker: Option<Waker>,
    part_dropped: bool,
}

impl<T> Default for SharedState<T> {
    fn default() -> Self {
        Self {
            data: VecDeque::default(),
            waker: Default::default(),
            part_dropped: false,
        }
    }
}

pub struct Sender<T> {
    inner: Rc<UnsafeCell<SharedState<T>>>,
}

pub struct Receiver<T> {
    inner: Rc<UnsafeCell<SharedState<T>>>,
}

impl<T> Sender<T> {
    pub fn send(&self, element: T) {
        let inner = unsafe { &mut *self.inner.get() };
        if inner.part_dropped {
            panic!()
        } else {
            inner.data.push_front(element);
        }
        if let Some(ref waker) = inner.waker
            && inner.data.len() == 1
        {
            println!("waking");
            waker.wake_by_ref();
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.part_dropped = true;
    }
}

pub fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Rc::new(UnsafeCell::new(SharedState::<T>::default()));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.part_dropped = true;
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> impl Future<Output = Option<T>> {
        let inner = unsafe { &mut *self.inner.get() };
        poll_fn(move |cx| {
            inner.waker.replace(cx.waker().clone());
            match inner.data.pop_back() {
                Some(e) => std::task::Poll::Ready(Some(e)),
                None => {
                    if inner.part_dropped {
                        std::task::Poll::Ready(None)
                    } else {
                        std::task::Poll::Pending
                    }
                }
            }
        })
    }
}
