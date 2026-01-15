use runtime::io_driver::operations::{
    StreamExt,
    sockets::{multi_accept::MultiAccept, multi_recv::MultiRecv},
};

fn main() {
    runtime::thread_per_core(|| async {
        let mut multi_accept = MultiAccept::new(9090).unwrap();
        while let Some(socket) = multi_accept.next().await {
            let mut multi_recv = MultiRecv::new(socket.fd);
            while let Some(_chunk) = multi_recv.next().await {
                println!("a");
            }
        }
    });
}
