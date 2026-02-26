// Vendored from opentelemetry-proto 0.31.0, stripped of serde/schemars/tonic/gRPC features.
use crate::tracing::otlp::proto::trace::ResourceSpans;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExportTraceServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_spans: ::prost::alloc::vec::Vec<ResourceSpans>,
}
