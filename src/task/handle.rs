use std::marker::PhantomData;

use crate::task::TypeAgnosticTask;

#[derive(Debug)]
pub struct Handle<T> {
    task: TypeAgnosticTask,
    _data: PhantomData<T>,
}
impl<T> Unpin for Handle<T> {}

impl<T> From<TypeAgnosticTask> for Handle<T> {
    fn from(task: TypeAgnosticTask) -> Self {
        Self {
            task,
            _data: PhantomData,
        }
    }
}

impl<T> Drop for Handle<T> {
    fn drop(&mut self) {
        self.task.detach();
    }
}

impl<T> Future for Handle<T> {
    type Output = T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut ret = std::task::Poll::Pending;
        let ptr = std::ptr::from_mut(&mut ret) as *mut ();
        self.task.get_ouput(ptr, cx.waker());
        ret
    }
}
