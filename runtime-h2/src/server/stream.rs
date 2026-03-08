use std::rc::Rc;

use http::{HeaderMap, StatusCode, header};
use runtime_core::{channels, channels::Sender, runtime::spawn};

use crate::{
    codec::frames::headers::{Headers, Pseudo},
    server::{H2Stream, StreamSendingEnd},
    types::Frame,
};

pub fn new_stream(headers: Headers, output: Rc<Sender<Frame>>) -> StreamSendingEnd {
    let (sender, receiver) = channels::unbounded_channel::<Frame>();
    let stream_id = headers.stream_id();
    let stream = H2Stream {
        headers,
        receiver,
        output,
    };
    spawn(process_stream(stream));
    StreamSendingEnd {
        sender,
        window: 65000,
    }
}

async fn process_stream(mut stream: H2Stream) {
    let stream_id = stream.headers.stream_id();

    // Send response headers
    let pseudo = Pseudo::response(StatusCode::OK);
    let mut fields = HeaderMap::new();
    fields.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());

    let response_headers = Headers::new(stream_id, pseudo, fields);
    stream.output.send(Frame::Headers(response_headers));

    // Send response body
    let body = b"hello from h2 stream!".to_vec();
    stream.output.send(Frame::Data {
        stream_id,
        end_stream: true,
        data: body,
    });

    // Drain remaining incoming frames
    while let Some(e) = stream.receiver.recv().await {
        match e {
            Frame::RstStream { .. } => break,
            _ => {}
        }
    }
}
