use std::rc::Rc;

use crate::io_driver::ParkedFd;

/// Owns a file or pipe fd. The owning handle and every prepared or in-flight
/// operation hold a strong ref; an op dropped mid-flight parks its ref in the
/// cancelled slab slot, so the close is strictly the last thing queued for it.
#[derive(Debug)]
pub(crate) struct FdInner {
    fd: i32,
}

impl FdInner {
    /// Takes ownership of `fd`: nothing else may close it.
    pub(crate) fn new(fd: i32) -> Rc<Self> {
        Rc::new(Self { fd })
    }

    pub(crate) fn fd(&self) -> i32 {
        self.fd
    }
}

impl Drop for FdInner {
    fn drop(&mut self) {
        crate::io_driver::close_owned_fd(self.fd);
    }
}

/// An op's ref to its fd, movable into the cancelled slab slot.
#[derive(Debug)]
pub(crate) struct OpFd(Option<Rc<FdInner>>);

impl OpFd {
    pub(crate) fn new(inner: &Rc<FdInner>) -> Self {
        Self(Some(inner.clone()))
    }

    pub(crate) fn fd(&self) -> i32 {
        self.0.as_ref().expect("taken only in Drop").fd()
    }

    /// Move the ref out for parking. Only from `Drop`.
    pub(crate) fn park(&mut self) -> ParkedFd {
        ParkedFd::File(self.0.take().expect("parked twice"))
    }
}
