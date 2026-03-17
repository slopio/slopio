use http::{HeaderMap, HeaderValue, Method};

use crate::types::FrameError;

use super::{Body, H2Connection};

pub struct GrpcResponse {
    pub data: Vec<u8>,
    pub grpc_status: u32,
    pub grpc_message: Option<String>,
}

#[derive(Debug)]
pub enum GrpcError {
    Http(FrameError),
    InvalidFrame,
    ServerError {
        status: u32,
        message: Option<String>,
    },
}

fn decode_grpc_frame(body: &[u8]) -> Result<Vec<u8>, GrpcError> {
    if body.len() < 5 {
        return Err(GrpcError::InvalidFrame);
    }
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    if body.len() < 5 + len {
        return Err(GrpcError::InvalidFrame);
    }
    Ok(body[5..5 + len].to_vec())
}

impl H2Connection {
    /// Send a unary gRPC call. `path` is the gRPC path (e.g. `/package.Service/Method`).
    /// `body` must be a gRPC-framed message: `[0x00][len: u32 BE][protobuf...]`.
    pub async fn grpc_unary(
        &mut self,
        path: &str,
        body: Vec<u8>,
    ) -> Result<GrpcResponse, GrpcError> {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/grpc"));
        headers.insert("te", HeaderValue::from_static("trailers"));

        let stream = self
            .request(Method::POST, path, headers, Some(Body::Preframed(body)))
            .map_err(GrpcError::Http)?;

        let response = stream.recv_response().await.map_err(GrpcError::Http)?;

        if response.status != 200 {
            return Err(GrpcError::Http(FrameError::ProtocolError(
                "non-200 HTTP status for gRPC",
            )));
        }

        let grpc_status = response
            .headers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        let grpc_message = response
            .headers
            .get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        if grpc_status != 0 {
            return Err(GrpcError::ServerError {
                status: grpc_status,
                message: grpc_message,
            });
        }

        let data = decode_grpc_frame(&response.body)?;

        Ok(GrpcResponse {
            data,
            grpc_status,
            grpc_message,
        })
    }
}
