use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::time::{Duration, UNIX_EPOCH};

use runtime_core::io_driver::IO_DRIVER;
use runtime_core::net::{SocketWrapper, connect_tcp};
use runtime_core::runtime::{now, spawn, time::Sleep};

use crate::{
    error::IggyError,
    types::{Identifier, IggyMessage, ReceivedMessage},
    wire::{
        codec::{decode_response_header, encode_identifier, encode_partitioning, encode_request},
        state::{ClientState, Connection},
        transport::{recv_exact, send_all},
    },
};

const CMD_PING: u32 = 1;
const CMD_LOGIN: u32 = 38;
const CMD_SEND_MESSAGES: u32 = 101;
const CMD_POLL_MESSAGES: u32 = 100;
const CMD_STORE_OFFSET: u32 = 121;

/// Size of IggyMessageHeader on the wire (bytes).
const IGGY_MESSAGE_HEADER_SIZE: usize = 64;

/// Size of one index entry in a send_messages payload (bytes).
const INDEX_SIZE: usize = 16;

/// Default keep-alive interval if none is provided.
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);

pub struct IggyClient {
    connection: Connection,
}

impl IggyClient {
    pub async fn connect(
        addr: SocketAddr,
        username: &str,
        password: &str,
    ) -> Result<Self, IggyError> {
        Self::connect_with_heartbeat(addr, username, password, DEFAULT_HEARTBEAT).await
    }

    pub async fn connect_with_heartbeat(
        addr: SocketAddr,
        username: &str,
        password: &str,
        heartbeat_interval: Duration,
    ) -> Result<Self, IggyError> {
        let socket = connect_tcp(addr)?;

        let mut client = IggyClient {
            connection: Connection::new(),
        };
        client.connection.on_connected(socket)?;
        client.login(username, password).await?;

        // Spawn the background heartbeat task.
        if let Some(ctx) = client.connection.heartbeat_context() {
            spawn(async move {
                loop {
                    Sleep::during(heartbeat_interval).await;
                    if !ctx.alive.get() || IO_DRIVER.with(|d| d.shutdown()) {
                        break;
                    }
                    if ctx.socket_busy.get() {
                        continue;
                    }
                    // Socket is idle — send a PING.  If it fails, stop.
                    let sock = ManuallyDrop::new(SocketWrapper { fd: ctx.fd });
                    let frame = encode_request(CMD_PING, &[]);
                    if send_all(&sock, &frame).await.is_err() {
                        ctx.alive.set(false);
                        break;
                    }
                    let mut hdr = [0u8; 8];
                    if recv_exact(&sock, &mut hdr).await.is_err() {
                        ctx.alive.set(false);
                        break;
                    }
                    // PING response has no body regardless of status.
                    ctx.last_activity.set(std::time::Instant::now());
                }
            });
        }

        Ok(client)
    }

    // ── public API ────────────────────────────────────────────────────────────

    pub fn state(&self) -> ClientState {
        self.connection.state()
    }

    pub fn user_id(&self) -> Option<u32> {
        self.connection.user_id()
    }

    pub async fn send_messages(
        &mut self,
        stream: Identifier,
        topic: Identifier,
        partition_id: u32,
        messages: &[IggyMessage],
    ) -> Result<(), IggyError> {
        self.connection.require_authenticated()?;
        if messages.is_empty() {
            return Ok(());
        }

        let stream_id = encode_identifier(stream);
        let topic_id = encode_identifier(topic);
        let partitioning = encode_partitioning(partition_id);

        let metadata_len: u32 = (stream_id.len() + topic_id.len() + partitioning.len() + 4) as u32;

        let mut payload = Vec::new();
        payload.extend_from_slice(&metadata_len.to_le_bytes());
        payload.extend_from_slice(&stream_id);
        payload.extend_from_slice(&topic_id);
        payload.extend_from_slice(&partitioning);
        payload.extend_from_slice(&(messages.len() as u32).to_le_bytes());

        let index_start = payload.len();
        payload.resize(payload.len() + INDEX_SIZE * messages.len(), 0);

        let mut cumulative_size: u32 = 0;
        let origin_ts = current_micros();

        for (i, msg) in messages.iter().enumerate() {
            let payload_len = msg.payload.len() as u32;

            payload.extend_from_slice(&[0u8; 8]); // checksum = 0 (server computes)
            payload.extend_from_slice(&msg.id.to_le_bytes()); // id (0 = auto-assign)
            payload.extend_from_slice(&[0u8; 8]); // offset = 0 (server assigns)
            payload.extend_from_slice(&[0u8; 8]); // timestamp = 0 (server assigns)
            payload.extend_from_slice(&origin_ts.to_le_bytes()); // origin_timestamp
            payload.extend_from_slice(&0u32.to_le_bytes()); // user_headers_length = 0
            payload.extend_from_slice(&payload_len.to_le_bytes());
            payload.extend_from_slice(&[0u8; 8]); // reserved = 0

            payload.extend_from_slice(&msg.payload);

            cumulative_size += IGGY_MESSAGE_HEADER_SIZE as u32 + payload_len;
            let idx = index_start + i * INDEX_SIZE;
            payload[idx + 4..idx + 8].copy_from_slice(&cumulative_size.to_le_bytes());
        }

        self.send_and_receive(CMD_SEND_MESSAGES, &payload).await?;
        Ok(())
    }

    pub async fn poll_messages(
        &mut self,
        stream: Identifier,
        topic: Identifier,
        partition_id: u32,
        offset: u64,
        count: u32,
        auto_commit: bool,
    ) -> Result<Vec<ReceivedMessage>, IggyError> {
        self.connection.require_authenticated()?;

        let consumer_id = encode_identifier(Identifier::Numeric(1));
        let stream_id = encode_identifier(stream);
        let topic_id = encode_identifier(topic);

        let mut payload = Vec::new();
        payload.push(1u8); // consumer_kind = Single
        payload.extend_from_slice(&consumer_id);
        payload.extend_from_slice(&stream_id);
        payload.extend_from_slice(&topic_id);
        payload.push(1u8); // partition_id present
        payload.extend_from_slice(&partition_id.to_le_bytes());
        payload.push(1u8); // polling_kind = Offset (code 1)
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(&count.to_le_bytes());
        payload.push(auto_commit as u8);

        let resp = self.send_and_receive(CMD_POLL_MESSAGES, &payload).await?;
        parse_poll_response(&resp)
    }

    /// Poll the last `count` messages from the partition (PollingKind::Last = 4).
    pub async fn poll_messages_last(
        &mut self,
        stream: Identifier,
        topic: Identifier,
        partition_id: u32,
        count: u32,
    ) -> Result<Vec<ReceivedMessage>, IggyError> {
        self.connection.require_authenticated()?;

        let consumer_id = encode_identifier(Identifier::Numeric(1));
        let stream_id = encode_identifier(stream);
        let topic_id = encode_identifier(topic);

        let mut payload = Vec::new();
        payload.push(1u8); // consumer_kind = Single
        payload.extend_from_slice(&consumer_id);
        payload.extend_from_slice(&stream_id);
        payload.extend_from_slice(&topic_id);
        payload.push(1u8); // partition_id present
        payload.extend_from_slice(&partition_id.to_le_bytes());
        payload.push(4u8); // polling_kind = Last
        payload.extend_from_slice(&(count as u64).to_le_bytes()); // value = count for Last
        payload.extend_from_slice(&count.to_le_bytes());
        payload.push(0u8); // no auto_commit

        let resp = self.send_and_receive(CMD_POLL_MESSAGES, &payload).await?;
        parse_poll_response(&resp)
    }

    pub async fn store_offset(
        &mut self,
        stream: Identifier,
        topic: Identifier,
        partition_id: u32,
        consumer_id: u32,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.connection.require_authenticated()?;

        let consumer_id_bytes = encode_identifier(Identifier::Numeric(consumer_id));
        let stream_id = encode_identifier(stream);
        let topic_id = encode_identifier(topic);

        let mut payload = Vec::new();
        payload.push(1u8); // consumer_kind = Single
        payload.extend_from_slice(&consumer_id_bytes);
        payload.extend_from_slice(&stream_id);
        payload.extend_from_slice(&topic_id);
        payload.extend_from_slice(&partition_id.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());

        self.send_and_receive(CMD_STORE_OFFSET, &payload).await?;
        Ok(())
    }

    // ── internal helpers ──────────────────────────────────────────────────────

    async fn login(&mut self, username: &str, password: &str) -> Result<(), IggyError> {
        self.connection.on_auth_begin()?;

        let u = username.as_bytes();
        let p = password.as_bytes();

        let mut payload = Vec::with_capacity(2 + u.len() + p.len() + 8);
        payload.push(u.len() as u8);
        payload.extend_from_slice(u);
        payload.push(p.len() as u8);
        payload.extend_from_slice(p);
        payload.extend_from_slice(&0u32.to_le_bytes()); // version_len = 0
        payload.extend_from_slice(&0u32.to_le_bytes()); // context_len = 0

        let frame = encode_request(CMD_LOGIN, &payload);
        let sock = self.io_socket()?;

        send_all(&sock, &frame).await?;

        let mut header = [0u8; 8];
        recv_exact(&sock, &mut header).await?;
        let (status, length) = decode_response_header(&header);

        if length > 0 {
            let mut resp_payload = vec![0u8; length as usize];
            recv_exact(&sock, &mut resp_payload).await?;
        }

        if status != 0 {
            return Err(IggyError::Server(status));
        }

        self.connection.on_auth_ok(0)?;
        Ok(())
    }

    /// Send a framed request and read back the response payload atomically.
    /// Marks the socket busy for the duration so the heartbeat task won't
    /// inject a PING in between.
    async fn send_and_receive(&mut self, cmd: u32, payload: &[u8]) -> Result<Vec<u8>, IggyError> {
        self.connection.mark_busy();

        let frame = encode_request(cmd, payload);
        let sock = self.io_socket()?;
        let result = send_and_receive_on(&sock, &frame).await;

        self.connection.mark_idle();
        result
    }

    /// Build a `ManuallyDrop<SocketWrapper>` from the current raw fd so we can
    /// perform I/O without holding a borrow on `self.connection`.
    fn io_socket(&self) -> Result<ManuallyDrop<SocketWrapper>, IggyError> {
        let fd = self.connection.raw_fd()?;
        Ok(ManuallyDrop::new(SocketWrapper { fd }))
    }
}

// ── free helpers ──────────────────────────────────────────────────────────────

async fn send_and_receive_on(sock: &SocketWrapper, frame: &[u8]) -> Result<Vec<u8>, IggyError> {
    send_all(sock, frame).await?;

    let mut header = [0u8; 8];
    recv_exact(sock, &mut header).await?;
    let (status, length) = decode_response_header(&header);

    let mut payload = vec![0u8; length as usize];
    if length > 0 {
        recv_exact(sock, &mut payload).await?;
    }

    if status != 0 {
        return Err(IggyError::Server(status));
    }
    Ok(payload)
}

fn parse_poll_response(resp: &[u8]) -> Result<Vec<ReceivedMessage>, IggyError> {
    if resp.is_empty() {
        return Ok(vec![]);
    }
    if resp.len() < 16 {
        return Err(IggyError::Protocol("poll response too short"));
    }

    // [4B] partition_id | [8B] current_offset | [4B] messages_count
    let messages_count = u32::from_le_bytes(resp[12..16].try_into().unwrap());

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut pos = 16;

    for _ in 0..messages_count {
        if pos + IGGY_MESSAGE_HEADER_SIZE > resp.len() {
            return Err(IggyError::Protocol(
                "truncated message header in poll response",
            ));
        }

        let hdr = &resp[pos..pos + IGGY_MESSAGE_HEADER_SIZE];
        let id = u128::from_le_bytes(hdr[8..24].try_into().unwrap());
        let offset = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        let timestamp = u64::from_le_bytes(hdr[32..40].try_into().unwrap());
        let user_headers_len = u32::from_le_bytes(hdr[48..52].try_into().unwrap()) as usize;
        let payload_len = u32::from_le_bytes(hdr[52..56].try_into().unwrap()) as usize;
        pos += IGGY_MESSAGE_HEADER_SIZE;

        if pos + payload_len + user_headers_len > resp.len() {
            return Err(IggyError::Protocol("truncated payload in poll response"));
        }

        let payload = resp[pos..pos + payload_len].to_vec();
        pos += payload_len + user_headers_len;

        messages.push(ReceivedMessage {
            id,
            offset,
            timestamp,
            payload,
        });
    }

    Ok(messages)
}

fn current_micros() -> u64 {
    now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
