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
        let resource = Resource::builder_empty()
            .with_attribute(KeyValue::new("service.name", "my-runtime"))
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
