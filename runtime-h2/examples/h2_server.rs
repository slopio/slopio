use http::{HeaderMap, StatusCode};
use runtime_core::runtime::main_thread;
use runtime_h2::codec::frames::headers::Headers;
use runtime_h2::server::stream::{Handler, Response};
use runtime_h2::server::{H2Server, Stream, StreamError};

#[derive(Clone)]
struct HttpHandler;

impl Handler for HttpHandler {
    async fn process_headers_only(
        &self,
        _headers: &Headers,
    ) -> Result<Option<Response>, StreamError> {
        Ok(Some(Response {
            status: StatusCode::from_u16(200).unwrap(),
            headers: HeaderMap::new(),
            body: Some(b"salut".to_vec()),
        }))
    }

    async fn process_accumulated_body(
        &self,
        _headers: &Headers,
        _body: Vec<u8>,
    ) -> Result<Option<Response>, StreamError> {
        Ok(Some(Response {
            status: StatusCode::from_u16(200).unwrap(),
            headers: HeaderMap::new(),
            body: None,
        }))
    }

    async fn process_stream(&self, stream: &mut Stream) -> Result<Option<Response>, StreamError> {
        let body = b"salut".to_vec();
        while let Some(chunk) = stream.recv_data().await? {
            println!("{}", chunk.len());
            stream
                .output
                .send_window_update(stream.headers.stream_id(), chunk.len() as u32);
        }
        Ok(Some(Response {
            status: StatusCode::from_u16(200).unwrap(),
            headers: HeaderMap::new(),
            body: { if !body.is_empty() { Some(body) } else { None } },
        }))
    }
}

fn main() {
    main_thread(async {
        let server = H2Server::new(8443, HttpHandler);
        println!("H2 server listening on :8443");
        server.run().await;
    });
}
