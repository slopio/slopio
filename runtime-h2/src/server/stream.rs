use runtime_core::{channels, runtime::spawn};

use crate::{
    codec::frames::headers::Headers,
    server::{H2Stream, StreamSendingEnd},
    types::Frame,
};

pub fn new_stream(headers: Headers) -> StreamSendingEnd {
    let (sender, receiver) = channels::unbounded_channel::<Frame>();
    let stream = H2Stream {
        headers,
        receiver,
        window: 4096,
    };
    spawn(process_stream(stream));
    StreamSendingEnd { sender }
}

async fn process_stream(mut stream: H2Stream) {
    while let Some(e) = stream.receiver.recv().await {
        match e {
            Frame::Data {
                end_stream, data, ..
            } => {
                if end_stream {
                    println!("processing and ending, data : {data:?}");
                }
            }
            Frame::Headers(_) => unimplemented!(),
            Frame::RstStream { .. } => break,
            Frame::WindowUpdate { increment, .. } => {
                stream.window += increment;
            }
            _ => {}
        }
    }
}
