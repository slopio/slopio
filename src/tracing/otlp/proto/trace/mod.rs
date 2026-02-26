// Vendored from opentelemetry-proto 0.31.0, stripped of serde/schemars/tonic features.
use crate::tracing::otlp::proto::common::{InstrumentationScope, KeyValue};
use crate::tracing::otlp::proto::resource::Resource;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceSpans {
    #[prost(message, optional, tag = "1")]
    pub resource: ::core::option::Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_spans: ::prost::alloc::vec::Vec<ScopeSpans>,
    #[prost(string, tag = "3")]
    pub schema_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopeSpans {
    #[prost(message, optional, tag = "1")]
    pub scope: ::core::option::Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
    #[prost(string, tag = "3")]
    pub schema_url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    #[prost(bytes = "vec", tag = "1")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "3")]
    pub trace_state: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "4")]
    pub parent_span_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(fixed32, tag = "16")]
    pub flags: u32,
    #[prost(string, tag = "5")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "span::SpanKind", tag = "6")]
    pub kind: i32,
    #[prost(fixed64, tag = "7")]
    pub start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    pub end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(uint32, tag = "10")]
    pub dropped_attributes_count: u32,
    #[prost(message, repeated, tag = "11")]
    pub events: ::prost::alloc::vec::Vec<span::Event>,
    #[prost(uint32, tag = "12")]
    pub dropped_events_count: u32,
    #[prost(message, repeated, tag = "13")]
    pub links: ::prost::alloc::vec::Vec<span::Link>,
    #[prost(uint32, tag = "14")]
    pub dropped_links_count: u32,
    #[prost(message, optional, tag = "15")]
    pub status: ::core::option::Option<Status>,
}
pub mod span {
    use crate::tracing::otlp::proto::common::KeyValue;

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Event {
        #[prost(fixed64, tag = "1")]
        pub time_unix_nano: u64,
        #[prost(string, tag = "2")]
        pub name: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: ::prost::alloc::vec::Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "4")]
        pub attributes: ::prost::alloc::vec::Vec<KeyValue>,
        #[prost(uint32, tag = "5")]
        pub dropped_attributes_count: u32,
        #[prost(fixed32, tag = "6")]
        pub flags: u32,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SpanKind {
        Unspecified = 0,
        Internal = 1,
        Server = 2,
        Client = 3,
        Producer = 4,
        Consumer = 5,
    }
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Status {
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    #[prost(enumeration = "status::StatusCode", tag = "3")]
    pub code: i32,
}
pub mod status {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum StatusCode {
        Unset = 0,
        Ok = 1,
        Error = 2,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SpanFlags {
    DoNotUse = 0,
    TraceFlagsMask = 255,
    ContextHasIsRemoteMask = 256,
    ContextIsRemoteMask = 512,
}
