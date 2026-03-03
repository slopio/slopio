use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::tracing::otlp::proto::common::{
    AnyValue, InstrumentationScope, KeyValue as ProtoKeyValue, any_value,
};
use crate::tracing::otlp::proto::resource::Resource as ProtoResource;
use crate::tracing::otlp::proto::trace::{
    ResourceSpans, ScopeSpans, Span, SpanFlags, Status as ProtoStatus, span, status,
};
use crate::tracing::span::SpanWrapper;
use crate::tracing::types::{KeyValue, SpanData, SpanId, SpanKind, SpanLink, Status, Value};

// ─── Helpers ─────────────────────────────────────────────────────────────────

pub(crate) fn to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

fn build_span_flags(parent_is_remote: bool) -> u32 {
    // trace_flags = SAMPLED (0x01)
    let mut flags = 1u32;
    flags |= SpanFlags::ContextHasIsRemoteMask as u32;
    if parent_is_remote {
        flags |= SpanFlags::ContextIsRemoteMask as u32;
    }
    flags
}

// ─── Attribute conversion ────────────────────────────────────────────────────

impl From<Value> for AnyValue {
    fn from(value: Value) -> Self {
        AnyValue {
            value: Some(match value {
                Value::Bool(v) => any_value::Value::BoolValue(v),
                Value::I64(v) => any_value::Value::IntValue(v),
                Value::F64(v) => any_value::Value::DoubleValue(v),
                Value::Str(v) => any_value::Value::StringValue(v.to_owned()),
            }),
        }
    }
}

fn attrs_to_proto(kvs: impl IntoIterator<Item = KeyValue>) -> Vec<ProtoKeyValue> {
    kvs.into_iter()
        .map(|kv| ProtoKeyValue {
            key: kv.key.to_owned(),
            value: Some(kv.value.into()),
        })
        .collect()
}

// ─── SpanKind / Status ───────────────────────────────────────────────────────

impl From<SpanKind> for span::SpanKind {
    fn from(k: SpanKind) -> Self {
        match k {
            SpanKind::Client => span::SpanKind::Client,
            SpanKind::Server => span::SpanKind::Server,
            SpanKind::Internal => span::SpanKind::Internal,
            SpanKind::Producer => span::SpanKind::Producer,
            SpanKind::Consumer => span::SpanKind::Consumer,
        }
    }
}

impl From<Status> for status::StatusCode {
    fn from(s: Status) -> Self {
        match s {
            Status::Ok => status::StatusCode::Ok,
            Status::Unset => status::StatusCode::Unset,
            Status::Error(_) => status::StatusCode::Error,
        }
    }
}

// ─── Link conversion ─────────────────────────────────────────────────────────

fn link_to_proto(link: SpanLink) -> span::Link {
    span::Link {
        trace_id: link.trace_id.to_bytes().to_vec(),
        span_id: link.span_id.to_bytes().to_vec(),
        trace_state: String::new(),
        attributes: vec![],
        dropped_attributes_count: 0,
        flags: 1, // SAMPLED
    }
}

// ─── SpanData → proto Span ───────────────────────────────────────────────────

impl From<SpanData> for Span {
    fn from(s: SpanData) -> Self {
        let kind: span::SpanKind = s.span_kind.into();
        let status_code = status::StatusCode::from(s.status);
        let status_message = match s.status {
            Status::Error(msg) => msg.to_owned(),
            _ => String::new(),
        };

        Span {
            trace_id: s.trace_id.to_bytes().to_vec(),
            span_id: s.span_id.to_bytes().to_vec(),
            trace_state: String::new(),
            parent_span_id: {
                if s.parent_span_id != SpanId::INVALID {
                    s.parent_span_id.to_bytes().to_vec()
                } else {
                    vec![]
                }
            },
            flags: build_span_flags(s.parent_span_is_remote),
            name: s.name.to_owned(),
            kind: kind as i32,
            start_time_unix_nano: to_nanos(s.start_time),
            end_time_unix_nano: to_nanos(s.end_time),
            dropped_attributes_count: s.dropped_attributes_count,
            attributes: attrs_to_proto(s.attributes),
            dropped_events_count: s.events.dropped_count,
            events: s
                .events
                .into_iter()
                .map(|e| span::Event {
                    time_unix_nano: to_nanos(e.timestamp),
                    name: e.name.to_owned(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                })
                .collect(),
            dropped_links_count: s.dropped_links_count,
            links: s.links.into_iter().map(link_to_proto).collect(),
            status: Some(ProtoStatus {
                code: status_code.into(),
                message: status_message,
            }),
        }
    }
}

// ─── Grouping ────────────────────────────────────────────────────────────────

/// Encode a batch of spans into OTLP `ResourceSpans`.
///
/// Each `SpanBuf` is partially moved: `buf.data` is consumed by `From<SpanData>` (which
/// copies arena strings to owned `String`s), then `buf.arena` drops at end of the closure.
/// Arena-allocated attribute strings remain valid throughout the `From<SpanData>` call
/// because the arena is still alive in the same `SpanBuf`.
pub fn group_spans_by_resource_and_scope(
    spans: Vec<SpanWrapper>,
    resource_attrs: &[ProtoKeyValue],
) -> Vec<ResourceSpans> {
    vec![ResourceSpans {
        resource: Some(ProtoResource {
            attributes: resource_attrs.to_vec(),
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "my runtime".to_string(),
                version: String::new(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            schema_url: String::new(),
            spans: spans.into_iter().map(|buf| buf.data.into()).collect(),
        }],
        schema_url: String::new(),
    }]
}
