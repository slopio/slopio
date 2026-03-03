use std::time::SystemTime;

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

#[derive(Clone, Debug, Default)]
pub enum Status {
    #[default]
    Unset,
    Ok,
    Error(String),
}

// ─── Attributes ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(&'static str),
    String(String),
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

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: SystemTime,
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

impl SpanEvent {
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timestamp: SystemTime::now(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SpanEvents {
    pub events: Vec<SpanEvent>,
    pub dropped_count: u32,
}

impl IntoIterator for SpanEvents {
    type Item = SpanEvent;
    type IntoIter = std::vec::IntoIter<SpanEvent>;

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

// ─── SpanData ────────────────────────────────────────────────────────────────

/// All data for a completed span.
///
/// `trace_flags` is always `0x01` (SAMPLED) and `trace_state` is always empty.
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
    pub attributes: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub events: SpanEvents,
    pub status: Status,
}
