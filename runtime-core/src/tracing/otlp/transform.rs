use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::tracing::types::{KeyValue, SpanData, SpanId, SpanKind, Status, Value};
use crate::tracing::otlp::proto::common::{
    AnyValue, InstrumentationScope, KeyValue as ProtoKeyValue, any_value,
};
use crate::tracing::otlp::proto::resource::Resource as ProtoResource;
use crate::tracing::otlp::proto::trace::{
    ResourceSpans, ScopeSpans, Span, SpanFlags, Status as ProtoStatus, span, status,
};

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
                Value::String(v) => any_value::Value::StringValue(v),
            }),
        }
    }
}

fn attrs_to_proto(kvs: Vec<KeyValue>) -> Vec<ProtoKeyValue> {
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

impl From<&Status> for status::StatusCode {
    fn from(s: &Status) -> Self {
        match s {
            Status::Ok => status::StatusCode::Ok,
            Status::Unset => status::StatusCode::Unset,
            Status::Error(_) => status::StatusCode::Error,
        }
    }
}

// ─── SpanData → proto Span ───────────────────────────────────────────────────

impl From<SpanData> for Span {
    fn from(s: SpanData) -> Self {
        let kind: span::SpanKind = s.span_kind.into();
        let status_code = status::StatusCode::from(&s.status);
        let status_message = match &s.status {
            Status::Error(msg) => msg.clone(),
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
                    name: e.name,
                    attributes: attrs_to_proto(e.attributes),
                    dropped_attributes_count: e.dropped_attributes_count,
                })
                .collect(),
            dropped_links_count: 0,
            links: vec![],
            status: Some(ProtoStatus {
                code: status_code.into(),
                message: status_message,
            }),
        }
    }
}

// ─── Grouping ────────────────────────────────────────────────────────────────

/// All spans share a single instrumentation scope ("my runtime"), so no grouping is needed.
pub fn group_spans_by_resource_and_scope(
    spans: Vec<SpanData>,
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
            spans: spans.into_iter().map(Into::into).collect(),
        }],
        schema_url: String::new(),
    }]
}
