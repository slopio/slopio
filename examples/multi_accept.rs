use runtime::io_driver::operations::StreamExt;
use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::io_driver::operations::sockets::{AsyncRead, AsyncWrite};
use runtime::runtime::spawn;

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    rt.block_on(async {
        let ma = MultiAccept::new(8080).unwrap();
        let mut ma = std::pin::pin!(ma);
        while let Some(socket) = ma.next().await {
            spawn(async move {
                loop {
                    let mut buffer = Box::new([0u8; 50]);
                    let reading = socket.recv(buffer.as_mut(), 50);
                    let n_bytes = reading.await.unwrap();
                    let sending = socket.send(&buffer[..n_bytes]);
                    let _ = sending.await.unwrap();
                }
            });
        }
    });
}
