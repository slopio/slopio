use runtime::{io_driver::operations::sockets::udp::UdpSocket, main_thread};

fn main() {
    main_thread(async {
        let udp = UdpSocket::bind(9091).unwrap();
        // Pre-allocated 1500-byte recv buffer, reused across iterations.
        let mut buffer: Vec<u8> = Vec::with_capacity(1500);
        loop {
            let (recv_result, buf) = udp.recv_from(buffer).await;
            let packet = recv_result.unwrap();
            // `buf.len()` is now `packet.len()` (set via `set_init`); the
            // capacity (1500) is preserved for the next recv.
            let payload = buf[..packet.len()].to_vec();
            buffer = buf;
            udp.send_to(payload, packet.addr()).await.0.unwrap();
        }
    });
}
