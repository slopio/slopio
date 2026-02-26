use std::mem::MaybeUninit;

use runtime::{io_driver::operations::sockets::udp::UdpSocket, main_thread};

fn main() {
    main_thread(async {
        let udp = UdpSocket::bind(9091).unwrap();
        let mut buffer = [MaybeUninit::uninit(); 1500];
        loop {
            let packet = udp.recv_from(&mut buffer).await.unwrap();
            let data =
                unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), packet.len()) };
            udp.send_to(data, packet.addr()).await.unwrap();
        }
    });
}
