// use std::fmt::Debug;

// use runtime_core::channels::Receiver;

// use crate::server::stream::StreamError;
// use crate::server::stream::events_in::{InputStreamEvent, WindowModification};
// use crate::server::stream::window::SendWindow;

// pub enum Chunk {
//     Data(Vec<u8>),
//     Last(Vec<u8>),
//     Done,
// }

// pub enum ReceivedBody {
//     None,
//     Complete(Vec<u8>),
//     Stream(UnsizedBody),
// }

// pub enum Body {
//     None,
//     Sized(SizedBody),
//     Unsized(UnsizedBody),
// }

// impl Body {
//     pub async fn receive(
//         self,
//         receiver: &Receiver<InputStreamEvent>,
//         send_window: &mut SendWindow,
//     ) -> Result<ReceivedBody, StreamError> {
//         match self {
//             Body::None => Ok(ReceivedBody::None),
//             Body::Sized(mut body) => {
//                 let data = body.collect(receiver, send_window).await?;
//                 Ok(ReceivedBody::Complete(data))
//             }
//             Body::Unsized(body) => Ok(ReceivedBody::Stream(body)),
//         }
//     }
// }

// pub struct SizedBody {
//     pub expected: u64,
//     pub received: u64,
// }

// pub struct UnsizedBody {
//     received: u64,
//     done: bool,
// }

// impl SizedBody {
//     pub fn new(expected: u64) -> Self {
//         Self {
//             expected,
//             received: 0,
//         }
//     }

//     async fn collect(
//         &mut self,
//         receiver: &Receiver<InputStreamEvent>,
//         send_window: &mut SendWindow,
//     ) -> Result<Vec<u8>, StreamError> {
//         let mut buf = Vec::with_capacity(self.expected as usize);
//         loop {
//             let Some(event) = receiver.recv().await else {
//                 if self.received != self.expected {
//                     return Err(StreamError::ContentLengthMismatch);
//                 }
//                 return Ok(buf);
//             };
//             match event {
//                 InputStreamEvent::Data { end_stream, data } => {
//                     self.received += data.len() as u64;
//                     buf.extend_from_slice(&data);
//                     if end_stream {
//                         if self.received != self.expected {
//                             return Err(StreamError::ContentLengthMismatch);
//                         }
//                         return Ok(buf);
//                     }
//                 }
//                 InputStreamEvent::Window(modification) => {
//                     apply_window(send_window, modification)?;
//                 }
//                 InputStreamEvent::Header { headers } => {
//                     if headers.is_end_headers() {
//                         if self.received != self.expected {
//                             return Err(StreamError::ContentLengthMismatch);
//                         }
//                         return Ok(buf);
//                     }
//                     unimplemented!()
//                 }
//                 InputStreamEvent::ResetFrame => return Err(StreamError::Reset),
//             }
//         }
//     }
// }

// impl UnsizedBody {
//     pub fn new() -> Self {
//         Self {
//             received: 0,
//             done: false,
//         }
//     }

//     pub async fn next(
//         &mut self,
//         receiver: &Receiver<InputStreamEvent>,
//         send_window: &mut SendWindow,
//     ) -> Result<Chunk, StreamError> {
//         if self.done {
//             return Ok(Chunk::Done);
//         }
//         loop {
//             let Some(event) = receiver.recv().await else {
//                 self.done = true;
//                 return Ok(Chunk::Done);
//             };
//             match event {
//                 InputStreamEvent::Data { end_stream, data } => {
//                     self.received += data.len() as u64;
//                     if end_stream {
//                         self.done = true;
//                         return Ok(Chunk::Last(data));
//                     }
//                     return Ok(Chunk::Data(data));
//                 }
//                 InputStreamEvent::Window(modification) => {
//                     apply_window(send_window, modification)?;
//                 }
//                 InputStreamEvent::Header { headers } => {
//                     if headers.is_end_headers() {
//                         self.done = true;
//                         return Ok(Chunk::Done);
//                     }
//                     unimplemented!()
//                 }
//                 InputStreamEvent::ResetFrame => return Err(StreamError::Reset),
//             }
//         }
//     }
// }
