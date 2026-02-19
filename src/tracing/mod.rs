use std::cell::UnsafeCell;
use std::ffi::CString;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::io_driver::IO_DRIVER;

// Same bit as IGNORED_FLAG in io_driver — CQEs with this user_data are discarded.
const IGNORED_FLAG: u64 = 1u64 << 62;
use crate::runtime::thread_id;
use crate::scoped_thread_local;

const BUFFER_SIZE: usize = 64 * 1024;

/// One buffer that can be handed to io_uring while the other is being written into.
struct TraceBuf {
    data: Box<[u8; BUFFER_SIZE]>,
    len: usize,
}
pub struct TracingMetadata {
    id: u64,
}

impl TracingMetadata {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl TraceBuf {
    fn new() -> Self {
        Self {
            data: Box::new([0u8; BUFFER_SIZE]),
            len: 0,
        }
    }

    fn remaining(&self) -> usize {
        BUFFER_SIZE - self.len
    }

    fn append(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.data[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }

    fn reset(&mut self) {
        self.len = 0;
    }
}

/// Per-thread double-buffered trace manager.
/// Call `TraceManager::init()` once per thread before tracing.
/// Call `TraceManager::trace(s)` to append a trace entry.
/// Call `TraceManager::flush()` to submit whatever is left to io_uring.
pub struct TraceManager {
    /// The two buffers (active index is `active`).
    bufs: UnsafeCell<[TraceBuf; 2]>,
    /// Which buffer index is currently being written into.
    active: UnsafeCell<usize>,
    /// File descriptor for `logs-{thread_id}`.
    fd: i32,
}

impl TraceManager {
    /// Opens `logs-{thread_id}` and returns a new `TraceManager`.
    pub fn init() -> Self {
        let tid = thread_id();
        let path = CString::new(format!("logs-{tid}")).expect("CString::new");
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
                0o644,
            )
        };
        if fd < 0 {
            panic!(
                "TraceManager: failed to open logs-{tid}: {}",
                std::io::Error::last_os_error()
            );
        }
        Self {
            bufs: UnsafeCell::new([TraceBuf::new(), TraceBuf::new()]),
            active: UnsafeCell::new(0),
            fd,
        }
    }

    /// Append `s` to the active buffer.
    /// If `s` would overflow the active buffer, the current buffer is
    /// submitted to io_uring first and the write continues into the other
    /// (now-idle) buffer.
    pub fn trace(&self, s: &str) {
        if s.len() > BUFFER_SIZE {
            // Can't fit even in a fresh buffer; drop silently.
            return;
        }
        let bufs = unsafe { &mut *self.bufs.get() };
        let active = unsafe { &mut *self.active.get() };

        if bufs[*active].remaining() < s.len() {
            self.submit_active_buffer(bufs, active);
        }

        bufs[*active].append(s);
    }

    /// Flush the active buffer to io_uring (useful on shutdown).
    pub fn flush(&self) {
        let bufs = unsafe { &mut *self.bufs.get() };
        let active = unsafe { &mut *self.active.get() };
        if bufs[*active].len > 0 {
            self.submit_active_buffer(bufs, active);
        }
    }

    /// Submit the active buffer to io_uring as a fire-and-forget write,
    /// then swap active to the other buffer and reset it for new traces.
    fn submit_active_buffer(&self, bufs: &mut [TraceBuf; 2], active: &mut usize) {
        let len = bufs[*active].len;
        if len == 0 {
            return;
        }
        let ptr = bufs[*active].data.as_ptr();
        let entry = opcode::Write::new(Fd(self.fd), ptr, len as u32)
            .build()
            .user_data(IGNORED_FLAG);
        IO_DRIVER.with(|d| d.push_anon_op(entry));

        // Swap to the other buffer and clear it for new data.
        *active ^= 1;
        bufs[*active].reset();
    }
}

scoped_thread_local!(pub(crate) static TRACE_MANAGER: TraceManager);

thread_local!(static CURRENT_SPAN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) });

pub fn current_span() -> u64 {
    CURRENT_SPAN.with(|c| c.get())
}

pub fn set_span(span: u64) {
    CURRENT_SPAN.with(|c| c.set(span));
}

/// Append a trace string on the current thread.
/// No-op if called outside of a runtime context.
pub fn trace(s: &str) {
    TRACE_MANAGER.try_with(|opt| {
        if let Some(manager) = opt {
            manager.trace(s);
        }
    });
}

impl Drop for TraceManager {
    fn drop(&mut self) {
        // Best-effort flush — io_uring may not be live at this point.
        IO_DRIVER.try_with(|opt| {
            if opt.is_some() {
                self.flush();
            }
        });
        unsafe { libc::close(self.fd) };
    }
}
