use std::time::SystemTime;

use smallvec::SmallVec;

// ─── Trace / Span IDs ────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub const INVALID: Self = Self([0; 16]);

    pub fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl From<u128> for TraceId {
    fn from(v: u128) -> Self {
        Self(v.to_be_bytes())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct SpanId([u8; 8]);

impl SpanId {
    pub const INVALID: Self = Self([0; 8]);

    pub fn to_bytes(self) -> [u8; 8] {
        self.0
    }
}

impl From<u64> for SpanId {
    fn from(v: u64) -> Self {
        Self(v.to_be_bytes())
    }
}

// ─── SpanKind / Status ───────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Default)]
pub enum SpanKind {
    #[default]
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// All string variants are `&'static str` — either truly static or arena-allocated via `SpanBuf`.
#[derive(Clone, Copy, Debug, Default)]
pub enum Status {
    #[default]
    Unset,
    Ok,
    Error(&'static str),
}

// ─── Attributes ──────────────────────────────────────────────────────────────

/// All variants are `Copy`. Dynamic strings must be arena-allocated via `SpanBuf::attr_dyn`.
#[derive(Clone, Copy, Debug)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(&'static str),
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<&'static str> for Value {
    fn from(v: &'static str) -> Self {
        Self::Str(v)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct KeyValue {
    pub key: &'static str,
    pub value: Value,
}

impl KeyValue {
    pub fn new(key: &'static str, value: impl Into<Value>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

// ─── Events ──────────────────────────────────────────────────────────────────

/// `name` is `&'static str` — either truly static or arena-allocated via `SpanBuf::event_dyn`.
#[derive(Clone, Copy, Debug)]
pub struct SpanEvent {
    pub name: &'static str,
    pub timestamp: SystemTime,
}

impl SpanEvent {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            timestamp: SystemTime::now(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SpanEvents {
    pub events: SmallVec<[SpanEvent; 4]>,
    pub dropped_count: u32,
}

impl IntoIterator for SpanEvents {
    type Item = SpanEvent;
    type IntoIter = smallvec::IntoIter<[SpanEvent; 4]>;

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

// ─── Links ───────────────────────────────────────────────────────────────────

/// A reference to another span, used to express relationships that are not
/// parent-child (e.g. a consumer span linking back to the producer span).
#[derive(Clone, Copy, Debug)]
pub struct SpanLink {
    pub trace_id: TraceId,
    pub span_id: SpanId,
}

// ─── SpanData ────────────────────────────────────────────────────────────────

/// All data for a completed span. Contains no heap allocations —
/// all strings are `&'static str` (truly static or arena-allocated via [`SpanBuf`]).
#[derive(Clone, Debug)]
pub struct SpanData {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// [`SpanId::INVALID`] when there is no parent.
    pub parent_span_id: SpanId,
    pub parent_span_is_remote: bool,
    pub name: &'static str,
    pub span_kind: SpanKind,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub attributes: SmallVec<[KeyValue; 8]>,
    pub dropped_attributes_count: u32,
    pub events: SpanEvents,
    pub links: SmallVec<[SpanLink; 2]>,
    pub dropped_links_count: u32,
    pub status: Status,
}
