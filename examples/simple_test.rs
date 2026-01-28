use runtime::io_driver::operations::sockets::AsyncWrite;
use runtime::runtime::spawn;
use runtime::{
    io_driver::operations::sockets::{multi_accept::MultiAccept, multi_recv_bundle::MultiRecv},
    main_thread,
};
use runtime_streams::StreamExt;

fn main() {
    main_thread(async {
        let mut multiaccept = MultiAccept::new(9091).unwrap();
        while let Some(socket) = multiaccept.next().await {
            spawn(async move {
                let _socket = socket;
                let mut multi_recv = MultiRecv::new(_socket.fd);
                while let Some(d) = multi_recv.next().await {
                    for chunk in d.as_parts().iter().flatten() {
                        let k = _socket.send(chunk).await;
                    }
                }
            });
        }
    });
}
