use runtime::io_driver::operations::sockets::AsyncWrite;
use runtime::{
    io_driver::operations::sockets::{multi_accept::MultiAccept, multi_recv_bundle::MultiRecv},
    main_thread,
};
use runtime_streams::StreamExt;

fn main() {
    main_thread(async {
        let mut multiaccept = MultiAccept::new(9091).unwrap();
        if let Some(socket) = multiaccept.next().await {
            let mut multi_recv = MultiRecv::new(socket.fd);
            while let Some(d) = multi_recv.next().await {
                let d = d.as_parts();
                let i: Vec<_> = d.iter().flatten().collect();
                dbg!(i);
            }
        };
    });
}
