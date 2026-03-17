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

/// 9 bytes H2 DATA frame header + 5 bytes gRPC frame header.
/// The first 9 bytes are filled by the H2 layer via `send_data_preframed`.
const FRAME_HEADER_LEN: usize = 14;
const GRPC_HEADER_OFFSET: usize = 9;

impl ProtoEncodeBuffer {
    /// Encode a batch of spans into a buffer with space for both the H2 frame
    /// header (9 bytes, filled later by the H2 layer) and the gRPC frame header.
    /// Layout: `[9 bytes reserved][0x00][grpc_len: u32 BE][protobuf payload...]`
    pub fn encode(&mut self, batch: Vec<SpanWrapper>) -> &[u8] {
        self.buf.clear();
        // Reserve 14 bytes: 9 for H2 frame header + 5 for gRPC frame header.
        self.buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
        encode_export_request(&mut self.buf, batch, &self.resource_attrs);
        let proto_len = (self.buf.len() - FRAME_HEADER_LEN) as u32;
        // byte 9 = 0x00 (no compression), already zero
        self.buf[GRPC_HEADER_OFFSET + 1..GRPC_HEADER_OFFSET + 5]
            .copy_from_slice(&proto_len.to_be_bytes());
        &self.buf
    }
}
