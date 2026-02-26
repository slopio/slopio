use std::marker::PhantomData;

use crate::task::TypeAgnosticTask;

#[derive(Debug)]
pub struct Handle<T> {
    task: Option<TypeAgnosticTask>,
    _data: PhantomData<T>,
}
impl<T> Unpin for Handle<T> {}

impl<T> From<TypeAgnosticTask> for Handle<T> {
    fn from(task: TypeAgnosticTask) -> Self {
        Self {
            task: Some(task),
            _data: PhantomData,
        }
    }
}

impl<T> Drop for Handle<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task {
            task.detach();
        }
    }
}

impl<T> Future for Handle<T> {
    type Output = T;

    #[allow(clippy::collapsible_if)]
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut ret = std::task::Poll::Pending;
        let ptr = std::ptr::from_mut(&mut ret) as *mut ();
        unsafe {
            self.task.unwrap().get_ouput(ptr, cx.waker());
        }
        if ret.is_ready() {
            if let Some(task) = self.task.take() {
                task.free();
            }
        }
        ret
    }
}
