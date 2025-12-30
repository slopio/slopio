use super::Op;
use io_uring::{opcode, squeue::Entry, types};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

pub struct Open {
    path: CString,
    flags: i32,
    mode: libc::mode_t,
}

impl Open {
    pub fn new<P: AsRef<Path>>(path: P, flags: i32, mode: libc::mode_t) -> Self {
        let path_bytes = path.as_ref().as_os_str().as_bytes();
        let path = CString::new(path_bytes).expect("path contains null byte");

        Self { path, flags, mode }
    }
}

impl Op for Open {
    fn into_entry(self) -> Entry {
        opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), self.path.as_ptr())
            .flags(self.flags)
            .mode(self.mode)
            .build()
    }
}
