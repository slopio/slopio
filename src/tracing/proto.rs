use opentelemetry::KeyValue;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema;
use opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SpanData;
use prost::Message;

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

#[cfg(test)]
mod tests {
    use opentelemetry::TraceId;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use prost::Message;

    use super::*;
    use crate::tracing::span::{new_span_data, random_trace_id};

    fn make_span(trace_id: TraceId, name: &'static str) -> SpanData {
        new_span_data(trace_id, None, name)
    }

    #[test]
    fn empty_batch_encodes_to_valid_proto() {
        let mut buf = ProtoEncodeBuffer::default();
        let bytes = buf.encode(vec![]);
        let decoded = ExportTraceServiceRequest::decode(bytes).unwrap();
        let total_spans: usize = decoded
            .resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .map(|ss| ss.spans.len())
            .sum();
        assert_eq!(total_spans, 0);
    }

    #[test]
    fn single_span_round_trips() {
        let trace_id = random_trace_id();
        let span = make_span(trace_id, "my-span");

        let mut buf = ProtoEncodeBuffer::default();
        let bytes = buf.encode(vec![span]);

        let decoded = ExportTraceServiceRequest::decode(bytes).unwrap();
        assert_eq!(decoded.resource_spans.len(), 1);

        let scope_spans = &decoded.resource_spans[0].scope_spans;
        assert_eq!(scope_spans.len(), 1);
        assert_eq!(scope_spans[0].spans.len(), 1);
        assert_eq!(scope_spans[0].spans[0].name, "my-span");
    }

    #[test]
    fn multiple_spans_same_trace_grouped_together() {
        let trace_id = random_trace_id();
        let spans = vec![
            make_span(trace_id, "span-a"),
            make_span(trace_id, "span-b"),
            make_span(trace_id, "span-c"),
        ];

        let mut buf = ProtoEncodeBuffer::default();
        let bytes = buf.encode(spans);

        let decoded = ExportTraceServiceRequest::decode(bytes).unwrap();
        let total_spans: usize = decoded
            .resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .map(|ss| ss.spans.len())
            .sum();
        assert_eq!(total_spans, 3);
    }

    #[test]
    fn span_trace_id_is_preserved() {
        let trace_id = random_trace_id();
        let span = make_span(trace_id, "check-id");

        let mut buf = ProtoEncodeBuffer::default();
        let bytes = buf.encode(vec![span]);

        let decoded = ExportTraceServiceRequest::decode(bytes).unwrap();
        let proto_span = &decoded.resource_spans[0].scope_spans[0].spans[0];

        assert_eq!(proto_span.trace_id, trace_id.to_bytes().to_vec());
    }

    #[test]
    fn buffer_can_encode_multiple_batches() {
        let mut buf = ProtoEncodeBuffer::default();

        for i in 0..5u32 {
            let name = if i % 2 == 0 { "even" } else { "odd" };
            let span = make_span(random_trace_id(), name);
            let bytes = buf.encode(vec![span]);
            // Each call should produce non-empty output
            assert!(!bytes.is_empty());
        }
    }

    #[test]
    fn encoded_bytes_are_non_empty_for_span() {
        let span = make_span(random_trace_id(), "non-empty");
        let mut buf = ProtoEncodeBuffer::default();
        let bytes = buf.encode(vec![span]);
        assert!(!bytes.is_empty());
    }
}
