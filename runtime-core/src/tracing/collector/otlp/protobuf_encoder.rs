use super::encode::{ResourceAttr, ResourceAttrValue, encode_export_request};
use crate::tracing::span::SpanWrapper;

const ENCODE_BUFFER_CAPACITY: usize = 64 * 1024;

pub struct ProtoEncodeBuffer {
    pub buf: Vec<u8>,
    resource_attrs: Vec<ResourceAttr>,
}

impl Default for ProtoEncodeBuffer {
    fn default() -> Self {
        let cfg = crate::tracing::resource_config();
        let mut resource_attrs = vec![ResourceAttr {
            key: "service.name",
            value: ResourceAttrValue::Str(cfg.service_name),
        }];
        if !cfg.service_version.is_empty() {
            resource_attrs.push(ResourceAttr {
                key: "service.version",
                value: ResourceAttrValue::Str(cfg.service_version),
            });
        }
        if let Some(ref h) = cfg.host_name {
            resource_attrs.push(ResourceAttr {
                key: "host.name",
                value: ResourceAttrValue::Owned(h.clone()),
            });
        }
        if let Some(id) = crate::tracing::thread_id() {
            resource_attrs.push(ResourceAttr {
                key: "service.instance.id",
                value: ResourceAttrValue::Int(id as i64),
            });
        }
        Self {
            buf: Vec::with_capacity(ENCODE_BUFFER_CAPACITY),
            resource_attrs,
        }
    }
}

const GRPC_HEADER_LEN: usize = 5;

impl ProtoEncodeBuffer {
    /// Encode a batch of spans as a gRPC-framed protobuf message.
    /// Returns bytes ready to send as an H2 DATA frame:
    /// `[0x00][length: u32 BE][protobuf payload...]`
    pub fn encode(&mut self, batch: Vec<SpanWrapper>) -> &[u8] {
        self.buf.clear();
        // Reserve 5 bytes for the gRPC frame header, filled after encoding.
        self.buf.extend_from_slice(&[0u8; GRPC_HEADER_LEN]);
        encode_export_request(&mut self.buf, batch, &self.resource_attrs);
        let proto_len = (self.buf.len() - GRPC_HEADER_LEN) as u32;
        // byte 0 = 0x00 (no compression), already zero
        self.buf[1..5].copy_from_slice(&proto_len.to_be_bytes());
        &self.buf
    }
}
