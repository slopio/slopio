use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SpanData;
use prost::Message;

use crate::tracing::otlp::proto::collector::trace::ExportTraceServiceRequest;
use crate::tracing::otlp::transform::{ResourceAttributesWithSchema, group_spans_by_resource_and_scope};

const ENCODE_BUFFER_CAPACITY: usize = 64 * 1024;

pub struct ProtoEncodeBuffer {
    buf: Vec<u8>,
    resource: ResourceAttributesWithSchema,
}

impl Default for ProtoEncodeBuffer {
    fn default() -> Self {
        let cfg = crate::tracing::resource_config();
        let mut attrs = vec![KeyValue::new("service.name", cfg.service_name)];
        if !cfg.service_version.is_empty() {
            attrs.push(KeyValue::new("service.version", cfg.service_version));
        }
        if let Some(ref h) = cfg.host_name {
            attrs.push(KeyValue::new("host.name", h.clone()));
        }
        if let Some(id) = crate::tracing::thread_id() {
            attrs.push(KeyValue::new("service.instance.id", id as i64));
        }
        let resource = attrs
            .into_iter()
            .fold(Resource::builder_empty(), |b, kv| b.with_attribute(kv))
            .build();
        Self {
            buf: Vec::with_capacity(ENCODE_BUFFER_CAPACITY),
            resource: (&resource).into(),
        }
    }
}

impl ProtoEncodeBuffer {
    pub fn encode(&mut self, batch: Vec<SpanData>) -> &[u8] {
        let resource_spans = group_spans_by_resource_and_scope(batch, &self.resource);
        let request = ExportTraceServiceRequest { resource_spans };
        self.buf.clear();
        self.buf.reserve(request.encoded_len());
        request
            .encode(&mut self.buf)
            .expect("encoding OTLP protobuf failed");
        &self.buf
    }
}
