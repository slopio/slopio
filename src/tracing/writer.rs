use std::cell::UnsafeCell;
use std::ffi::CString;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::io_driver::IO_DRIVER;
use crate::runtime::thread_id;
use crate::tracing::TraceWriter;
use crate::tracing::span::SpanMetadata;

// Same bit as IGNORED_FLAG in io_driver — CQEs with this user_data are discarded.
const IGNORED_FLAG: u64 = 1u64 << 62;

pub(super) const BUFFER_SIZE: usize = 64 * 1024;

pub(super) struct TraceBuf {
    pub data: Box<[u8; BUFFER_SIZE]>,
    pub len: usize,
}

impl TraceBuf {
    pub fn new() -> Self {
        Self {
            data: Box::new([0u8; BUFFER_SIZE]),
            len: 0,
        }
    }

    pub fn remaining(&self) -> usize {
        BUFFER_SIZE - self.len
    }

    pub fn append(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.data[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }

    pub fn reset(&mut self) {
        self.len = 0;
    }
}

/// Per-thread double-buffered trace writer.
pub struct TraceCollector {
    bufs: UnsafeCell<[TraceBuf; 2]>,
    active: UnsafeCell<usize>,
    pub fd: i32,
}

impl TraceCollector {
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

    pub(super) fn push_ended_span(&self, span: SpanMetadata) {
        let msg = match span.parent_span_id {
            Some(parent) => format!(
                "span end trace_id={:032x} span_id={:016x} parent_id={:016x} name={}\n",
                span.trace_id, span.span_id, parent, span.name
            ),
            None => format!(
                "span end trace_id={:032x} span_id={:016x} name={}\n",
                span.trace_id, span.span_id, span.name
            ),
        };
        self.write_str(&msg);
    }

    pub fn trace(&self, s: &str) {
        self.write_str(s);
        self.write_str("\n");
    }

    fn write_str(&self, s: &str) {
        let bufs = unsafe { &mut *self.bufs.get() };
        let active = unsafe { &mut *self.active.get() };
        if s.len() > bufs[*active].remaining() {
            self.submit_active_buffer(bufs, active);
        }
        bufs[*active].append(s);
    }

    fn flush_buffer(&self) {
        let bufs = unsafe { &mut *self.bufs.get() };
        let active = unsafe { &mut *self.active.get() };
        if bufs[*active].len > 0 {
            self.submit_active_buffer(bufs, active);
        }
    }

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
        *active ^= 1;
        bufs[*active].reset();
    }
}

impl crate::tracing::TraceWriter for TraceCollector {
    fn write_span(&self, span: crate::tracing::span::SpanMetadata) {
        self.push_ended_span(span);
    }

    fn flush(&self) {
        self.flush_buffer();
    }
}

impl Drop for TraceCollector {
    fn drop(&mut self) {
        IO_DRIVER.try_with(|opt| {
            if opt.is_some() {
                self.flush();
            }
        });
        unsafe { libc::close(self.fd) };
    }
}
