use std::{
    mem::MaybeUninit, net::SocketAddr, os::fd::RawFd, rc::Rc, str::FromStr, sync::Arc,
    time::Instant,
};

use runtime::{io_driver::operations::sockets::udp::UdpSocket, main_thread};
use tquic::{
    Config, Endpoint, PacketInfo, PacketSendHandler, TlsConfig, TlsConfigSelector, TransportHandler,
};

struct QuicServer {
    endpoint: Endpoint,
    socket: Rc<UdpSocket>,
}

impl QuicServer {
    async fn recv(&mut self) {
        let mut buffer = [MaybeUninit::uninit(); 1500];
        let datagram = self.socket.recv_from(&mut buffer).await.unwrap();
        let data = unsafe {
            std::slice::from_raw_parts_mut(buffer.as_mut_ptr().cast::<u8>(), datagram.len())
        };
        let info = PacketInfo {
            src: datagram.addr(),
            time: Instant::now(),
            dst: SocketAddr::from_str("0.0.0.0:4433").unwrap(),
        };

        self.endpoint.recv(data, &info).unwrap();
        self.endpoint.process_connections().unwrap();
    }

    fn build() -> Self {
        let udp = Rc::new(UdpSocket::bind(4433).unwrap());
        let mut config = Box::new(Config::new().unwrap());

        config.set_tls_config_selector(Arc::new(Tls {}));
        let handler = Box::new(Handler {});
        let sender = Rc::new(Sender { fd: udp.fd() });
        let endpoint = tquic::Endpoint::new(config, true, handler, sender);
        Self {
            endpoint,
            socket: udp,
        }
    }
}

fn main() {
    main_thread(async {
        let mut server = QuicServer::build();
        loop {
            server.recv().await;
        }
    });
}

struct Handler {}

struct Sender {
    fd: RawFd,
}

struct Tls {}

impl TlsConfigSelector for Tls {
    fn get_default(&self) -> Option<Arc<TlsConfig>> {
        let mut tls_config = TlsConfig::new().unwrap();
        tls_config.set_certificate_file("certs/cert.pem").unwrap();
        tls_config.set_private_key_file("certs/key.pem").unwrap();
        tls_config
            .set_application_protos(vec![b"h3".to_vec()])
            .unwrap();
        Some(Arc::new(tls_config))
    }

    fn select(&self, server_name: &str) -> Option<Arc<TlsConfig>> {
        self.get_default()
    }
}

impl TransportHandler for Handler {
    fn on_conn_created(&mut self, conn: &mut tquic::Connection) {}

    fn on_conn_established(&mut self, conn: &mut tquic::Connection) {
        println!("connection {} established", conn.dcid().unwrap());
        // todo!()
    }

    fn on_conn_closed(&mut self, conn: &mut tquic::Connection) {
        todo!()
    }

    fn on_stream_created(&mut self, conn: &mut tquic::Connection, stream_id: u64) {
        println!("stream {stream_id} created");
    }

    fn on_stream_readable(&mut self, conn: &mut tquic::Connection, stream_id: u64) {
        println!("stream {stream_id} is readable!");
    }

    fn on_stream_writable(&mut self, conn: &mut tquic::Connection, stream_id: u64) {
        println!("stream writable");
    }

    fn on_stream_closed(&mut self, conn: &mut tquic::Connection, stream_id: u64) {
        todo!()
    }

    fn on_new_token(&mut self, conn: &mut tquic::Connection, token: Vec<u8>) {
        todo!()
    }
}

impl PacketSendHandler for Sender {
    fn on_packets_send(&self, pkts: &[(Vec<u8>, tquic::PacketInfo)]) -> tquic::Result<usize> {
        if pkts.is_empty() {
            return Ok(0);
        }
        println!("writing");

        // Build mmsghdr array for sendmmsg
        let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(pkts.len());
        let mut addrs: Vec<libc::sockaddr_storage> = Vec::with_capacity(pkts.len());
        let mut msgs: Vec<libc::mmsghdr> = Vec::with_capacity(pkts.len());

        for (data, info) in pkts {
            iovecs.push(libc::iovec {
                iov_base: data.as_ptr() as *mut _,
                iov_len: data.len(),
            });

            let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
            match info.dst {
                SocketAddr::V4(v4) => {
                    let sin = &mut addr as *mut _ as *mut libc::sockaddr_in;
                    unsafe {
                        (*sin).sin_family = libc::AF_INET as u16;
                        (*sin).sin_port = v4.port().to_be();
                        (*sin).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
                    }
                }
                SocketAddr::V6(v6) => {
                    let sin6 = &mut addr as *mut _ as *mut libc::sockaddr_in6;
                    unsafe {
                        (*sin6).sin6_family = libc::AF_INET6 as u16;
                        (*sin6).sin6_port = v6.port().to_be();
                        (*sin6).sin6_addr.s6_addr = v6.ip().octets();
                    }
                }
            }
            addrs.push(addr);
        }

        // Build mmsghdr referencing iovecs and addrs
        for i in 0..pkts.len() {
            let addr_len = match pkts[i].1.dst {
                SocketAddr::V4(_) => std::mem::size_of::<libc::sockaddr_in>() as u32,
                SocketAddr::V6(_) => std::mem::size_of::<libc::sockaddr_in6>() as u32,
            };

            msgs.push(libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: &mut addrs[i] as *mut _ as *mut _,
                    msg_namelen: addr_len,
                    msg_iov: &mut iovecs[i],
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            });
        }

        let ret = unsafe { libc::sendmmsg(self.fd, msgs.as_mut_ptr(), msgs.len() as u32, 0) };

        if ret < 0 {
            return Err(tquic::Error::InvalidOperation(
                std::io::Error::last_os_error().to_string(),
            ));
        }

        Ok(ret as usize)
    }
}
