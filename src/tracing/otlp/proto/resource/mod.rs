// Vendored from opentelemetry-proto 0.31.0, stripped of serde/schemars/tonic features.
use crate::tracing::otlp::proto::common::{EntityRef, KeyValue};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(uint32, tag = "2")]
    pub dropped_attributes_count: u32,
    #[prost(message, repeated, tag = "3")]
    pub entity_refs: ::prost::alloc::vec::Vec<EntityRef>,
}
