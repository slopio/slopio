use std::fs::{File, OpenOptions, read_dir};
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::tracing::collector::Collector;
use crate::tracing::span::SpanWrapper;
use crate::tracing::task::thread_id;
use crate::tracing::types::{SpanKind, Status, Value};

// ─── Config ───────────────────────────────────────────────────────────────────

/// Configuration for the local-debug collector. Set once before starting the
/// runtime via [`set_local_debug_config`]; subsequent calls have no effect.
pub struct LocalDebugConfig {
    /// Directory where per-worker trace files are written. Created if missing.
    pub dir: PathBuf,
    /// Rotate the current file once it reaches this many bytes.
    pub max_file_size: u64,
    /// Rotate the current file after this elapsed time, regardless of size.
    pub rotate_every: Duration,
    /// Hard cap on total bytes on disk for this worker's files. Older files
    /// are pruned on rotation to stay under this cap. Always rounded up to at
    /// least `max_file_size` so the file we just rotated out isn't immediately
    /// reaped before the next one opens.
    pub per_worker_size_cap: u64,
    /// Prefix for trace filenames: `{prefix}-core{NNN}-{unix_seconds}.jsonl`.
    pub file_prefix: &'static str,
}

impl Default for LocalDebugConfig {
    fn default() -> Self {
        Self {
            dir: std::env::temp_dir().join("runtime-traces"),
            max_file_size: 64 * 1024 * 1024,
            rotate_every: Duration::from_secs(3600),
            per_worker_size_cap: 1024 * 1024 * 1024,
            file_prefix: "traces",
        }
    }
}

static LOCAL_DEBUG_CONFIG: OnceLock<LocalDebugConfig> = OnceLock::new();

/// Set the local-debug collector config. Call before runtime startup.
/// Has no effect if called more than once.
pub fn set_local_debug_config(config: LocalDebugConfig) {
    let _ = LOCAL_DEBUG_CONFIG.set(config);
}

fn local_debug_config() -> &'static LocalDebugConfig {
    LOCAL_DEBUG_CONFIG.get_or_init(LocalDebugConfig::default)
}

// ─── Collector ────────────────────────────────────────────────────────────────

/// Writes spans as JSONL to per-worker files under [`LocalDebugConfig::dir`].
/// One file per worker thread, rotated by size or elapsed time. Older files
/// are pruned to stay under [`LocalDebugConfig::per_worker_size_cap`].
///
/// Intended for **local debugging**, not production: writes are synchronous
/// (`std::fs::File::write_all`), no fsync, no shipping pipeline.
pub struct LocalDebugCollector {
    thread_id: usize,
    file: Option<File>,
    current_size: u64,
    opened_at: Instant,
    encode_buf: Vec<u8>,
    config: &'static LocalDebugConfig,
}

impl Collector for LocalDebugCollector {
    fn init() -> Self {
        let config = local_debug_config();
        if let Err(e) = std::fs::create_dir_all(&config.dir) {
            eprintln!(
                "LocalDebugCollector: failed to create {}: {}",
                config.dir.display(),
                e
            );
        }
        Self {
            thread_id: thread_id().unwrap_or(0),
            file: None,
            current_size: 0,
            opened_at: Instant::now(),
            encode_buf: Vec::with_capacity(8192),
            config,
        }
    }

    async fn process(&mut self, batch: Vec<SpanWrapper>) {
        if self.needs_rotation() {
            self.rotate();
        }
        if self.file.is_none() {
            self.open_file();
        }

        self.encode_buf.clear();
        for span in &batch {
            encode_jsonl(&mut self.encode_buf, span);
        }
        if self.encode_buf.is_empty() {
            return;
        }

        if let Some(file) = self.file.as_mut() {
            match file.write_all(&self.encode_buf) {
                Ok(()) => self.current_size += self.encode_buf.len() as u64,
                Err(e) => eprintln!("LocalDebugCollector: write failed: {}", e),
            }
        }
    }
}

impl LocalDebugCollector {
    fn needs_rotation(&self) -> bool {
        self.file.is_some()
            && (self.current_size >= self.config.max_file_size
                || self.opened_at.elapsed() >= self.config.rotate_every)
    }

    fn rotate(&mut self) {
        self.file = None;
        self.prune_old_files();
    }

    fn open_file(&mut self) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let path = self.config.dir.join(format!(
            "{}-core{:03}-{}.jsonl",
            self.config.file_prefix, self.thread_id, ts
        ));
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => {
                self.file = Some(f);
                self.current_size = 0;
                self.opened_at = Instant::now();
            }
            Err(e) => {
                eprintln!(
                    "LocalDebugCollector: failed to open {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }

    fn prune_old_files(&self) {
        let prefix = format!("{}-core{:03}-", self.config.file_prefix, self.thread_id);
        let mut entries: Vec<(PathBuf, u64, SystemTime)> = match read_dir(&self.config.dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    let path = e.path();
                    let name = path.file_name()?.to_str()?;
                    if !name.starts_with(&prefix) || !name.ends_with(".jsonl") {
                        return None;
                    }
                    let meta = e.metadata().ok()?;
                    let modified = meta.modified().ok()?;
                    Some((path, meta.len(), modified))
                })
                .collect(),
            Err(_) => return,
        };

        // Guard against silly configs: keep at least one full file's worth.
        let cap = self
            .config
            .per_worker_size_cap
            .max(self.config.max_file_size);
        let mut remaining: u64 = entries.iter().map(|(_, sz, _)| *sz).sum();
        if remaining <= cap {
            return;
        }

        entries.sort_by_key(|(_, _, mt)| *mt);
        for (path, size, _) in entries {
            if remaining <= cap {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                remaining = remaining.saturating_sub(size);
            }
        }
    }
}

// ─── JSONL encoding ───────────────────────────────────────────────────────────

fn encode_jsonl(out: &mut Vec<u8>, span: &SpanWrapper) {
    let trace_id = u128::from_be_bytes(span.data.trace_id.to_bytes());
    let span_id = u64::from_be_bytes(span.data.span_id.to_bytes());
    let parent_id = u64::from_be_bytes(span.data.parent_span_id.to_bytes());
    let start_ns = span
        .data
        .start_time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let end_ns = span
        .data
        .end_time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    out.push(b'{');
    let _ = write!(
        out,
        r#""trace_id":"{:032x}","span_id":"{:016x}""#,
        trace_id, span_id
    );
    if parent_id != 0 {
        let _ = write!(out, r#","parent_span_id":"{:016x}""#, parent_id);
    }
    out.extend_from_slice(br#","name":""#);
    write_escaped(out, span.data.name.as_bytes());
    out.push(b'"');
    let _ = write!(out, r#","kind":"{}""#, span_kind_str(span.data.span_kind));
    let _ = write!(out, r#","start_unix_nano":{}"#, start_ns);
    let _ = write!(out, r#","end_unix_nano":{}"#, end_ns);

    match span.data.status {
        Status::Unset => {}
        Status::Ok => out.extend_from_slice(br#","status":"OK""#),
        Status::Error(msg) => {
            out.extend_from_slice(br#","status":"ERROR","status_message":""#);
            write_escaped(out, msg.as_bytes());
            out.push(b'"');
        }
    }

    if !span.data.attributes.is_empty() {
        out.extend_from_slice(br#","attributes":["#);
        for (i, kv) in span.data.attributes.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(br#"{"key":""#);
            write_escaped(out, kv.key.as_bytes());
            out.extend_from_slice(br#"","value":"#);
            write_value(out, kv.value);
            out.push(b'}');
        }
        out.push(b']');
    }

    if !span.data.events.events.is_empty() {
        out.extend_from_slice(br#","events":["#);
        for (i, ev) in span.data.events.events.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(br#"{"name":""#);
            write_escaped(out, ev.name.as_bytes());
            out.push(b'"');
            if let Ok(d) = ev.timestamp.duration_since(UNIX_EPOCH) {
                let _ = write!(out, r#","time_unix_nano":{}"#, d.as_nanos());
            }
            out.push(b'}');
        }
        out.push(b']');
    }

    out.push(b'}');
    out.push(b'\n');
}

fn span_kind_str(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "Internal",
        SpanKind::Server => "Server",
        SpanKind::Client => "Client",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
    }
}

fn write_value(out: &mut Vec<u8>, v: Value) {
    match v {
        Value::Bool(b) => {
            let _ = write!(out, "{}", b);
        }
        Value::I64(i) => {
            let _ = write!(out, "{}", i);
        }
        Value::F64(f) => {
            let _ = write!(out, "{}", f);
        }
        Value::Str(s) => {
            out.push(b'"');
            write_escaped(out, s.as_bytes());
            out.push(b'"');
        }
    }
}

fn write_escaped(out: &mut Vec<u8>, s: &[u8]) {
    for &b in s {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0..=0x1F => {
                let _ = write!(out, "\\u{:04x}", b);
            }
            _ => out.push(b),
        }
    }
}
