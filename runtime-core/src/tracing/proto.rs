use prost::Message;

use crate::tracing::otlp::proto::collector::trace::ExportTraceServiceRequest;
use crate::tracing::otlp::proto::common::{AnyValue, KeyValue as ProtoKeyValue, any_value};
use crate::tracing::otlp::transform::group_spans_by_resource_and_scope;
use crate::tracing::span::SpanWrapper;

const ENCODE_BUFFER_CAPACITY: usize = 64 * 1024;

pub struct ProtoEncodeBuffer {
    buf: Vec<u8>,
    resource_attrs: Vec<ProtoKeyValue>,
}

fn str_attr(key: &str, value: &str) -> ProtoKeyValue {
    ProtoKeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

fn int_attr(key: &str, value: i64) -> ProtoKeyValue {
    ProtoKeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

impl Default for ProtoEncodeBuffer {
    fn default() -> Self {
        let cfg = crate::tracing::resource_config();
        let mut resource_attrs = vec![str_attr("service.name", cfg.service_name)];
        if !cfg.service_version.is_empty() {
            resource_attrs.push(str_attr("service.version", cfg.service_version));
        }
        if let Some(ref h) = cfg.host_name {
            resource_attrs.push(str_attr("host.name", h));
        }
        if let Some(id) = crate::tracing::thread_id() {
            resource_attrs.push(int_attr("service.instance.id", id as i64));
        }
        Self {
            buf: Vec::with_capacity(ENCODE_BUFFER_CAPACITY),
            resource_attrs,
        }
    }
}

impl ProtoEncodeBuffer {
    pub fn encode(&mut self, batch: Vec<SpanWrapper>) -> &[u8] {
        let resource_spans = group_spans_by_resource_and_scope(batch, &self.resource_attrs);
        let request = ExportTraceServiceRequest { resource_spans };
        self.buf.clear();
        self.buf.reserve(request.encoded_len());
        request
            .encode(&mut self.buf)
            .expect("encoding OTLP protobuf failed");
        &self.buf
    }
}
