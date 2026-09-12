use std::{
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable},
};

use crate::task::inner_parts::{Metadata, typed_task::TypedTask};

/// A waker over `header`. Counts nothing: the caller owns the ref it stands for.
pub(super) fn raw_waker<T>(header: *const Metadata) -> RawWaker
where
    T: Future,
{
    let ptr = header as *const ();
    let vtable = &RawWakerVTable::new(
        clone_waker::<T>,
        wake_by_val::<T>,
        wake_by_ref::<T>,
        drop_waker::<T>,
    );
    RawWaker::new(ptr, vtable)
}

fn task<T: Future>(ptr: *const ()) -> TypedTask<T> {
    TypedTask::<T>::from_raw(unsafe { NonNull::new_unchecked(ptr as *mut Metadata) })
}

unsafe fn clone_waker<T>(ptr: *const ()) -> RawWaker
where
    T: Future,
{
    task::<T>(ptr).retain();
    raw_waker::<T>(ptr as *const Metadata)
}

unsafe fn drop_waker<T>(ptr: *const ())
where
    T: Future,
{
    task::<T>(ptr).release();
}

unsafe fn wake_by_val<T>(ptr: *const ())
where
    T: Future,
{
    task::<T>(ptr).wake_by_val();
}

unsafe fn wake_by_ref<T>(ptr: *const ())
where
    T: Future,
{
    task::<T>(ptr).wake_by_ref();
}
