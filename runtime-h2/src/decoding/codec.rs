use std::collections::HashMap;

use runtime_core::{channels::Sender, io_driver::operations::sockets::SocketWrapper};

type StreamId = u32;

struct ConnectionFrameReader {
    buffer: [u8; 1024],
    start: usize,
    end: usize,
    socket: SocketWrapper,
    map: HashMap<StreamId, Sender<Frame>>,
}

enum Frame {}

impl Frame {
    fn stream_id(&self) -> Option<StreamId> {
        Some(1)
    }
}

#[derive(Debug)]
struct Err {}

impl ConnectionFrameReader {
    fn try_get_frame(&mut self) -> Result<Option<Frame>, Err> {
        todo!()
    }

    ///copies the remaining bytes at the beginning and fill the buffer
    async fn fill(&mut self) -> Result<(), Err> {
        todo!()
    }

    async fn process(&mut self) {
        loop {
            match self.try_get_frame() {
                Ok(Some(frame)) => {
                    if let Some(id) = frame.stream_id() {
                        self.map.get(&id).unwrap().send(frame)
                    }
                }
                Ok(None) => {
                    self.fill().await.unwrap();
                }
                Result::Err(_) => break,
            }
        }
    }
}
