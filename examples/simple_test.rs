use runtime::io_driver::operations::sockets::multi_recv::MultiRecv;
use runtime::io_driver::operations::sockets::send::socket_send;
use runtime::runtime::spawn;
use runtime::{io_driver::operations::sockets::multi_accept::MultiAccept, main_thread};
use runtime_streams::StreamExt;

fn main() {
    main_thread(async {
        let mut multiaccept = MultiAccept::new(9091).unwrap();
        while let Some(socket) = multiaccept.next().await {
            spawn(async move {
                let s = socket;
                let mut multi_recv = MultiRecv::new(s.fd);
                while let Some(d) = multi_recv.next().await {
                    socket_send(&s, d.as_ref()).await.unwrap();
                }
            });
        }
    });
}
