use std::collections::HashMap;

use runtime_core::{
    channels::Sender,
    io_driver::operations::sockets::{SocketWrapper, recv::socket_recv_init},
};

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
        let remaining = self.end - self.start;
        self.buffer.copy_within(self.start..self.end, 0);
        self.start = 0;
        self.end = remaining;

        let capacity = self.buffer.len() - self.end;
        let n = socket_recv_init(&self.socket, &mut self.buffer[self.end..], capacity)
            .await
            .map_err(|_| Err {})?;
        self.end += n;
        Ok(())
    }

    async fn process(&mut self) {
        loop {
            match self.try_get_frame() {
                Ok(Some(frame)) => {
                    //ignoring unknown streams
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
