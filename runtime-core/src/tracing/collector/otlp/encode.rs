use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::tracing::span::SpanWrapper;
use crate::tracing::types::{
    KeyValue, SpanData, SpanEvent, SpanId, SpanKind, SpanLink, Status, Value,
};

// ─── Wire format primitives ─────────────────────────────────────────────────

const VARINT: u32 = 0;
const FIXED64: u32 = 1;
const LEN: u32 = 2;
const FIXED32: u32 = 5;

fn encode_varint(buf: &mut Vec<u8>, mut val: u64) {
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

#[inline]
fn encode_tag(buf: &mut Vec<u8>, field: u32, wire_type: u32) {
    encode_varint(buf, ((field << 3) | wire_type) as u64);
}

fn encode_bytes(buf: &mut Vec<u8>, field: u32, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    encode_tag(buf, field, LEN);
    encode_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

#[inline]
fn encode_str(buf: &mut Vec<u8>, field: u32, s: &str) {
    encode_bytes(buf, field, s.as_bytes());
}

fn encode_uint32(buf: &mut Vec<u8>, field: u32, val: u32) {
    if val == 0 {
        return;
    }
    encode_tag(buf, field, VARINT);
    encode_varint(buf, val as u64);
}

fn encode_int32(buf: &mut Vec<u8>, field: u32, val: i32) {
    if val == 0 {
        return;
    }
    encode_tag(buf, field, VARINT);
    encode_varint(buf, val as u64);
}

fn encode_fixed64(buf: &mut Vec<u8>, field: u32, val: u64) {
    if val == 0 {
        return;
    }
    encode_tag(buf, field, FIXED64);
    buf.extend_from_slice(&val.to_le_bytes());
}

fn encode_fixed32(buf: &mut Vec<u8>, field: u32, val: u32) {
    if val == 0 {
        return;
    }
    encode_tag(buf, field, FIXED32);
    buf.extend_from_slice(&val.to_le_bytes());
}

/// Write tag + length prefix, then fill content via `inner`.
/// Uses a length-prefix placeholder patched after the fact.
fn encode_submsg(buf: &mut Vec<u8>, field: u32, inner: impl FnOnce(&mut Vec<u8>)) {
    encode_tag(buf, field, LEN);
    // Reserve a single byte for the length; patch it after.
    let len_pos = buf.len();
    buf.push(0); // placeholder
    let start = buf.len();
    inner(buf);
    let content_len = buf.len() - start;
    if content_len < 0x80 {
        // Single-byte varint — patch in place.
        buf[len_pos] = content_len as u8;
    } else {
        // Multi-byte varint — encode to scratch then splice.
        let mut varint_buf = [0u8; 10];
        let mut n = 0;
        let mut v = content_len as u64;
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                varint_buf[n] = byte;
                n += 1;
                break;
            }
            varint_buf[n] = byte | 0x80;
            n += 1;
        }
        // Replace the single placeholder byte with the actual varint.
        buf[len_pos] = varint_buf[0];
        if n > 1 {
            buf.splice(len_pos + 1..len_pos + 1, varint_buf[1..n].iter().copied());
        }
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64
}

const CONTEXT_HAS_IS_REMOTE: u32 = 256;
const CONTEXT_IS_REMOTE: u32 = 512;

fn span_flags(parent_is_remote: bool) -> u32 {
    let mut f = 1u32; // SAMPLED
    f |= CONTEXT_HAS_IS_REMOTE;
    if parent_is_remote {
        f |= CONTEXT_IS_REMOTE;
    }
    f
}

fn span_kind(k: SpanKind) -> i32 {
    match k {
        SpanKind::Internal => 1,
        SpanKind::Server => 2,
        SpanKind::Client => 3,
        SpanKind::Producer => 4,
        SpanKind::Consumer => 5,
    }
}

fn status_code(s: &Status) -> i32 {
    match s {
        Status::Unset => 0,
        Status::Ok => 1,
        Status::Error(_) => 2,
    }
}

// ─── AnyValue / KeyValue ────────────────────────────────────────────────────

// AnyValue: string_value=1, bool_value=2, int_value=3, double_value=4
fn encode_any_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Str(s) => encode_str(buf, 1, s),
        Value::Bool(v) => {
            encode_tag(buf, 2, VARINT);
            encode_varint(buf, *v as u64);
        }
        Value::I64(v) => {
            encode_tag(buf, 3, VARINT);
            encode_varint(buf, *v as u64);
        }
        Value::F64(v) => {
            encode_tag(buf, 4, FIXED64);
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

// KeyValue: key=1(string), value=2(AnyValue)
fn encode_kv(buf: &mut Vec<u8>, kv: &KeyValue) {
    encode_str(buf, 1, kv.key);
    encode_submsg(buf, 2, |b| encode_any_value(b, &kv.value));
}

// ─── Resource attributes ────────────────────────────────────────────────────

pub struct ResourceAttr {
    pub key: &'static str,
    pub value: ResourceAttrValue,
}

pub enum ResourceAttrValue {
    Str(&'static str),
    Owned(String),
    Int(i64),
}

fn encode_resource_attr(buf: &mut Vec<u8>, attr: &ResourceAttr) {
    encode_str(buf, 1, attr.key);
    encode_submsg(buf, 2, |b| match &attr.value {
        ResourceAttrValue::Str(s) => encode_str(b, 1, s),
        ResourceAttrValue::Owned(s) => encode_str(b, 1, s),
        ResourceAttrValue::Int(v) => {
            encode_tag(b, 3, VARINT);
            encode_varint(b, *v as u64);
        }
    });
}

// ─── Span sub-messages ──────────────────────────────────────────────────────

// Event: time_unix_nano=1(fixed64), name=2(string), attributes=3(repeated KV), dropped=4(uint32)
fn encode_event(buf: &mut Vec<u8>, e: &SpanEvent) {
    encode_fixed64(buf, 1, to_nanos(e.timestamp));
    encode_str(buf, 2, e.name);
    for kv in &e.attributes {
        encode_submsg(buf, 3, |b| encode_kv(b, kv));
    }
}

// Link: trace_id=1(bytes), span_id=2(bytes), flags=6(fixed32)
fn encode_link(buf: &mut Vec<u8>, link: &SpanLink) {
    encode_bytes(buf, 1, &link.trace_id.to_bytes());
    encode_bytes(buf, 2, &link.span_id.to_bytes());
    encode_fixed32(buf, 6, 1); // SAMPLED
}

// Status: message=2(string), code=3(int32)
fn encode_status(buf: &mut Vec<u8>, s: &Status) {
    if let Status::Error(msg) = s {
        encode_str(buf, 2, msg);
    }
    encode_int32(buf, 3, status_code(s));
}

// ─── Span ───────────────────────────────────────────────────────────────────

fn encode_span(buf: &mut Vec<u8>, s: &SpanData) {
    encode_bytes(buf, 1, &s.trace_id.to_bytes());
    encode_bytes(buf, 2, &s.span_id.to_bytes());
    if s.parent_span_id != SpanId::INVALID {
        encode_bytes(buf, 4, &s.parent_span_id.to_bytes());
    }
    encode_str(buf, 5, s.name);
    encode_int32(buf, 6, span_kind(s.span_kind));
    encode_fixed64(buf, 7, to_nanos(s.start_time));
    encode_fixed64(buf, 8, to_nanos(s.end_time));
    for kv in &s.attributes {
        encode_submsg(buf, 9, |b| encode_kv(b, kv));
    }
    encode_uint32(buf, 10, s.dropped_attributes_count);
    for event in &s.events.events {
        encode_submsg(buf, 11, |b| encode_event(b, event));
    }
    encode_uint32(buf, 12, s.events.dropped_count);
    for link in &s.links {
        encode_submsg(buf, 13, |b| encode_link(b, link));
    }
    encode_uint32(buf, 14, s.dropped_links_count);
    encode_submsg(buf, 15, |b| encode_status(b, &s.status));
    encode_fixed32(buf, 16, span_flags(s.parent_span_is_remote));
}

// ─── ExportTraceServiceRequest ──────────────────────────────────────────────

/// Encode spans directly into an OTLP ExportTraceServiceRequest.
/// Appends to `buf` — caller is responsible for clearing/pre-filling.
/// Reads `&SpanData` fields by reference — no intermediate owned structs.
pub fn encode_export_request(
    buf: &mut Vec<u8>,
    spans: Vec<SpanWrapper>,
    resource_attrs: &[ResourceAttr],
) {
    // ExportTraceServiceRequest.resource_spans = field 1 (ResourceSpans)
    encode_submsg(buf, 1, |rs| {
        // ResourceSpans.resource = field 1 (Resource)
        encode_submsg(rs, 1, |res| {
            // Resource.attributes = field 1 (repeated KeyValue)
            for attr in resource_attrs {
                encode_submsg(res, 1, |b| encode_resource_attr(b, attr));
            }
        });
        // ResourceSpans.scope_spans = field 2 (ScopeSpans)
        encode_submsg(rs, 2, |ss| {
            // ScopeSpans.scope = field 1 (InstrumentationScope)
            encode_submsg(ss, 1, |scope| {
                encode_str(scope, 1, "my runtime");
            });
            // ScopeSpans.spans = field 2 (repeated Span)
            for span_wrapper in spans {
                encode_submsg(ss, 2, |b| encode_span(b, &span_wrapper.data));
            }
        });
    });
}
