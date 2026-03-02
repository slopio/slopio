use crate::types::Identifier;

/// Encode a numeric identifier: [kind=1][len=4][id: u32 LE] = 6 bytes
pub fn encode_identifier(id: Identifier) -> [u8; 6] {
    let Identifier::Numeric(n) = id;
    let mut buf = [0u8; 6];
    buf[0] = 1; // kind = Numeric
    buf[1] = 4; // length = 4 bytes
    buf[2..6].copy_from_slice(&n.to_le_bytes());
    buf
}

/// Encode a PartitionId partitioning: [kind=2][len=4][id: u32 LE] = 6 bytes
pub fn encode_partitioning(partition_id: u32) -> [u8; 6] {
    let mut buf = [0u8; 6];
    buf[0] = 2; // kind = PartitionId
    buf[1] = 4; // length = 4 bytes
    buf[2..6].copy_from_slice(&partition_id.to_le_bytes());
    buf
}

/// Build a complete TCP request frame:
/// [payload_size: u32 LE][cmd: u32 LE][payload]
/// payload_size = payload.len() + 4 (includes the 4-byte command code)
pub fn encode_request(cmd: u32, payload: &[u8]) -> Vec<u8> {
    let size = (payload.len() as u32) + 4;
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&size.to_le_bytes());
    frame.extend_from_slice(&cmd.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Decode 8-byte response header → (status, payload_length)
pub fn decode_response_header(buf: &[u8; 8]) -> (u32, u32) {
    let status = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let length = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    (status, length)
}
