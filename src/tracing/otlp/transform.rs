// Vendored from opentelemetry-proto 0.31.0 transform/common.rs and transform/trace.rs,
// stripped of feature gates, serde, metrics, logs, and tonic transport.
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{Link, SpanId, SpanKind};
use opentelemetry::{Array, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SpanData;

use crate::tracing::otlp::proto::common::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, any_value,
};
use crate::tracing::otlp::proto::resource::Resource as ProtoResource;
use crate::tracing::otlp::proto::trace::{
    ResourceSpans, ScopeSpans, Span, SpanFlags, Status, span, status,
};

// --- common helpers ---

pub(crate) fn to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

/// Wrapper around `Vec<KeyValue>` for conversion from opentelemetry attributes.
#[derive(Default, Debug)]
pub struct Attributes(pub Vec<KeyValue>);

impl<I: IntoIterator<Item = opentelemetry::KeyValue>> From<I> for Attributes {
    fn from(kvs: I) -> Self {
        Attributes(
            kvs.into_iter()
                .map(|api_kv| KeyValue {
                    key: api_kv.key.as_str().to_string(),
                    value: Some(api_kv.value.into()),
                })
                .collect(),
        )
    }
}

impl From<Value> for AnyValue {
    fn from(value: Value) -> Self {
        AnyValue {
            value: match value {
                Value::Bool(val) => Some(any_value::Value::BoolValue(val)),
                Value::I64(val) => Some(any_value::Value::IntValue(val)),
                Value::F64(val) => Some(any_value::Value::DoubleValue(val)),
                Value::String(val) => Some(any_value::Value::StringValue(val.to_string())),
                Value::Array(array) => Some(any_value::Value::ArrayValue(match array {
                    Array::Bool(vals) => array_into_proto(vals),
                    Array::I64(vals) => array_into_proto(vals),
                    Array::F64(vals) => array_into_proto(vals),
                    Array::String(vals) => array_into_proto(vals),
                    _ => unreachable!("nonexistent array type"),
                })),
                _ => unreachable!("nonexistent value type"),
            },
        }
    }
}

fn array_into_proto<T: Into<Value>>(vals: Vec<T>) -> ArrayValue {
    ArrayValue {
        values: vals.into_iter().map(|v| AnyValue::from(v.into())).collect(),
    }
}

fn resource_attributes(resource: &Resource) -> Attributes {
    resource
        .iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k.clone(), v.clone()))
        .collect::<Vec<_>>()
        .into()
}

#[derive(Debug, Default)]
pub struct ResourceAttributesWithSchema {
    pub attributes: Attributes,
    pub schema_url: Option<String>,
}

impl From<&Resource> for ResourceAttributesWithSchema {
    fn from(resource: &Resource) -> Self {
        ResourceAttributesWithSchema {
            attributes: resource_attributes(resource),
            schema_url: resource.schema_url().map(ToString::to_string),
        }
    }
}

impl
    From<(
        opentelemetry::InstrumentationScope,
        Option<std::borrow::Cow<'static, str>>,
    )> for InstrumentationScope
{
    fn from(
        data: (
            opentelemetry::InstrumentationScope,
            Option<std::borrow::Cow<'static, str>>,
        ),
    ) -> Self {
        let (library, target) = data;
        if let Some(t) = target {
            InstrumentationScope {
                name: t.to_string(),
                version: String::new(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }
        } else {
            InstrumentationScope {
                name: library.name().to_owned(),
                version: library.version().map(ToOwned::to_owned).unwrap_or_default(),
                attributes: Attributes::from(library.attributes().cloned()).0,
                dropped_attributes_count: 0,
            }
        }
    }
}

impl
    From<(
        &opentelemetry::InstrumentationScope,
        Option<std::borrow::Cow<'static, str>>,
    )> for InstrumentationScope
{
    fn from(
        data: (
            &opentelemetry::InstrumentationScope,
            Option<std::borrow::Cow<'static, str>>,
        ),
    ) -> Self {
        let (library, target) = data;
        if let Some(t) = target {
            InstrumentationScope {
                name: t.to_string(),
                version: String::new(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }
        } else {
            InstrumentationScope {
                name: library.name().to_owned(),
                version: library.version().map(ToOwned::to_owned).unwrap_or_default(),
                attributes: Attributes::from(library.attributes().cloned()).0,
                dropped_attributes_count: 0,
            }
        }
    }
}

// --- trace transform ---

fn build_span_flags(parent_span_is_remote: bool, base_flags: u32) -> u32 {
    let mut flags = base_flags;
    flags |= SpanFlags::ContextHasIsRemoteMask as u32;
    if parent_span_is_remote {
        flags |= SpanFlags::ContextIsRemoteMask as u32;
    }
    flags
}

impl From<SpanKind> for span::SpanKind {
    fn from(span_kind: SpanKind) -> Self {
        match span_kind {
            SpanKind::Client => span::SpanKind::Client,
            SpanKind::Consumer => span::SpanKind::Consumer,
            SpanKind::Internal => span::SpanKind::Internal,
            SpanKind::Producer => span::SpanKind::Producer,
            SpanKind::Server => span::SpanKind::Server,
        }
    }
}

impl From<&opentelemetry::trace::Status> for status::StatusCode {
    fn from(status: &opentelemetry::trace::Status) -> Self {
        match status {
            opentelemetry::trace::Status::Ok => status::StatusCode::Ok,
            opentelemetry::trace::Status::Unset => status::StatusCode::Unset,
            opentelemetry::trace::Status::Error { .. } => status::StatusCode::Error,
        }
    }
}

impl From<Link> for span::Link {
    fn from(link: Link) -> Self {
        span::Link {
            trace_id: link.span_context.trace_id().to_bytes().to_vec(),
            span_id: link.span_context.span_id().to_bytes().to_vec(),
            trace_state: link.span_context.trace_state().header(),
            attributes: Attributes::from(link.attributes).0,
            dropped_attributes_count: link.dropped_attributes_count,
            flags: build_span_flags(
                link.span_context.is_remote(),
                link.span_context.trace_flags().to_u8() as u32,
            ),
        }
    }
}

impl From<SpanData> for Span {
    fn from(source_span: SpanData) -> Self {
        let span_kind: span::SpanKind = source_span.span_kind.into();
        Span {
            trace_id: source_span.span_context.trace_id().to_bytes().to_vec(),
            span_id: source_span.span_context.span_id().to_bytes().to_vec(),
            trace_state: source_span.span_context.trace_state().header(),
            parent_span_id: {
                if source_span.parent_span_id != SpanId::INVALID {
                    source_span.parent_span_id.to_bytes().to_vec()
                } else {
                    vec![]
                }
            },
            flags: build_span_flags(
                source_span.parent_span_is_remote,
                source_span.span_context.trace_flags().to_u8() as u32,
            ),
            name: source_span.name.into_owned(),
            kind: span_kind as i32,
            start_time_unix_nano: to_nanos(source_span.start_time),
            end_time_unix_nano: to_nanos(source_span.end_time),
            dropped_attributes_count: source_span.dropped_attributes_count,
            attributes: Attributes::from(source_span.attributes).0,
            dropped_events_count: source_span.events.dropped_count,
            events: source_span
                .events
                .into_iter()
                .map(|event| span::Event {
                    time_unix_nano: to_nanos(event.timestamp),
                    name: event.name.into(),
                    attributes: Attributes::from(event.attributes).0,
                    dropped_attributes_count: event.dropped_attributes_count,
                })
                .collect(),
            dropped_links_count: source_span.links.dropped_count,
            links: source_span.links.into_iter().map(Into::into).collect(),
            status: Some(Status {
                code: status::StatusCode::from(&source_span.status).into(),
                message: match source_span.status {
                    opentelemetry::trace::Status::Error { description } => description.to_string(),
                    _ => Default::default(),
                },
            }),
        }
    }
}

pub fn group_spans_by_resource_and_scope(
    spans: Vec<SpanData>,
    resource: &ResourceAttributesWithSchema,
) -> Vec<ResourceSpans> {
    let scope_map = spans.iter().fold(
        HashMap::new(),
        |mut scope_map: HashMap<&opentelemetry::InstrumentationScope, Vec<&SpanData>>, span| {
            scope_map
                .entry(&span.instrumentation_scope)
                .or_default()
                .push(span);
            scope_map
        },
    );

    let scope_spans = scope_map
        .into_iter()
        .map(|(instrumentation, span_records)| ScopeSpans {
            scope: Some((instrumentation, None).into()),
            schema_url: instrumentation
                .schema_url()
                .map(ToOwned::to_owned)
                .unwrap_or_default(),
            spans: span_records
                .into_iter()
                .map(|span_data| span_data.clone().into())
                .collect(),
        })
        .collect();

    vec![ResourceSpans {
        resource: Some(ProtoResource {
            attributes: resource.attributes.0.clone(),
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_spans,
        schema_url: resource.schema_url.clone().unwrap_or_default(),
    }]
}
