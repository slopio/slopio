use std::{
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable},
};

use crate::task::inner_parts::{Metadata, typed_task::TypedTask};

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

unsafe fn clone_waker<T>(ptr: *const ()) -> RawWaker
where
    T: Future,
{
    let header = ptr as *const Metadata;
    raw_waker::<T>(header)
}

unsafe fn drop_waker<T>(_ptr: *const ())
where
    T: Future,
{
}

unsafe fn wake_by_val<T>(ptr: *const ())
where
    T: Future,
{
    let ptr = unsafe { NonNull::new_unchecked(ptr as *mut Metadata) };
    let typed_task = TypedTask::<T>::from_raw(ptr);
    typed_task.wake_by_val()
}

// Wake without consuming the waker
unsafe fn wake_by_ref<T>(ptr: *const ())
where
    T: Future,
{
    let ptr = unsafe { NonNull::new_unchecked(ptr as *mut Metadata) };
    let mut typed_task = TypedTask::<T>::from_raw(ptr);
    typed_task.wake_by_ref();
}
