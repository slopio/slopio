// use std::{
//     collections::{HashMap, hash_map::Entry},
//     mem::MaybeUninit,
//     net::SocketAddr,
// };

// use quiche::{Config, Connection, ConnectionId, Header, RecvInfo};
// use runtime::{
//     io_driver::operations::sockets::udp::{UdpPacket, UdpSocket},
//     main_thread,
// };

// const MAX_DATAGRAM_SIZE: usize = 1350;

// struct Network {
//     socket: UdpSocket,
//     local_addr: SocketAddr,
// }

// struct Quic {
//     configuration: Config,
//     network: Network,
//     connections: HashMap<ConnectionId<'static>, Connection>,
// }

// impl Quic {
//     async fn process_datagram(&mut self) -> quiche::Result<()> {
//         let mut buf = [MaybeUninit::uninit(); 65535];
//         let packet = self.network.socket.recv_from(&mut buf).await.unwrap();
//         let mut data =
//             unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, packet.len) };
//         let hdr = quiche::Header::from_slice(data, quiche::MAX_CONN_ID_LEN)?;
//         let connection = self.get_connection(hdr, &packet).unwrap();

//         let recv_info = quiche::RecvInfo {
//             from: packet.addr,
//             to: self.network.local_addr,
//         };
//         connection_recv(connection, recv_info)?;

//         Ok(())
//     }

//     fn get_connection(
//         &mut self,
//         hdr: Header<'static>,
//         packet: &UdpPacket,
//     ) -> Option<&mut Connection> {
//         let conn_id = hdr.dcid;
//         match self.connections.entry(conn_id) {
//             Entry::Occupied(e) => {
//                 println!("existing connection");
//                 Some(e.into_mut())
//             }
//             Entry::Vacant(e) if hdr.ty == quiche::Type::Initial => {
//                 println!("new connection, id={:?}", e.key());
//                 let conn = quiche::accept(
//                     e.key(),
//                     None,
//                     self.network.local_addr,
//                     packet.addr,
//                     &mut self.configuration,
//                 )
//                 .unwrap();
//                 Some(e.insert(conn))
//             }
//             _ => None,
//         }
//     }
// }

// //feed -> Header

// fn main() {
//     main_thread(async {
//         let udp = UdpSocket::bind(4433).unwrap();
//         let mut buf = [MaybeUninit::uninit(); 65535];
//         let mut out = [0u8; MAX_DATAGRAM_SIZE];
//         let local_addr: SocketAddr = "0.0.0.0:4433".parse().unwrap();
//         let mut config = setup_quic();

//         loop {
//             let packet = udp.recv_from(&mut buf).await.unwrap();
//             let mut data =
//                 unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, packet.len) };

//             let hdr = match quiche::Header::from_slice(data, quiche::MAX_CONN_ID_LEN) {
//                 Ok(v) => v,
//                 Err(_) => continue,
//             };
//             //on sait qui c'est et d'ou il vient
//             let recv_info = quiche::RecvInfo {
//                 from: packet.addr,
//                 to: local_addr,
//             };

//             // Use hdr.dcid as scid so subsequent lookups work
//             let conn_id = hdr.dcid;
//             let connection = match connections.entry(conn_id) {
//                 Entry::Occupied(e) => {
//                     println!("existing connection");
//                     e.into_mut()
//                 }
//                 Entry::Vacant(e) if hdr.ty == quiche::Type::Initial => {
//                     println!("new connection, id={:?}", e.key());
//                     let conn = quiche::accept(e.key(), None, local_addr, packet.addr, &mut config)
//                         .unwrap();
//                     e.insert(conn)
//                 }
//                 _ => continue,
//             };
//             // Process the received packet
//             match connection.recv(&mut data, recv_info) {
//                 Ok(len) => println!("recv {} bytes", len),
//                 Err(quiche::Error::Done) => {}
//                 Err(e) => {
//                     eprintln!("recv error: {:?}", e);
//                     continue;
//                 }
//             }

//             if connection.is_established() {
//                 println!("established");
//                 for stream_id in connection.readable() {
//                     let mut stream_buf = [0u8; 1024];
//                     while let Ok((read, fin)) = connection.stream_recv(stream_id, &mut stream_buf) {
//                         println!("stream {}: {} bytes, fin={}", stream_id, read, fin);
//                         connection
//                             .stream_send(stream_id, &stream_buf[..read], fin)
//                             .ok();
//                     }
//                 }
//             }

//             loop {
//                 let (write, send_info) = match connection.send(&mut out) {
//                     Ok(v) => v,
//                     Err(quiche::Error::Done) => break,
//                     Err(e) => {
//                         eprintln!("send error {:?}", e);
//                         break;
//                     }
//                 };
//                 udp.send_to(&out[..write], send_info.to).await.ok();
//             }

//             // Check if connection is closed
//             if connection.is_closed() {
//                 println!(
//                     "connection closed, peer_error={:?}",
//                     connection.peer_error()
//                 );
//                 // connections.remove(&conn_id);
//                 // -> when it returns
//             }
//         }
//     });
// }

// fn setup_quic() -> Config {
//     let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
//     config
//         .load_cert_chain_from_pem_file("certs/cert.pem")
//         .unwrap();
//     config.load_priv_key_from_pem_file("certs/key.pem").unwrap();
//     //quic settings
//     config.set_application_protos(&[b"h3"]).unwrap();
//     config.set_max_idle_timeout(5000);
//     config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
//     config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
//     config.set_initial_max_data(10_000_000);
//     config.set_initial_max_stream_data_bidi_local(1_000_000);
//     config.set_initial_max_stream_data_bidi_remote(1_000_000);
//     config.set_initial_max_streams_bidi(100);
//     config
// }

// fn connection_recv(
//     connection: &mut Connection,
//     recv_info: RecvInfo,
// ) -> Result<Option<NonZero<usize>>> {
//     match connection.recv(&mut data, recv_info) {
//         Ok(len) => Ok(Some(len)),
//         Err(quiche::Error::Done) => Ok(None),
//         Err(e) => Err(e),
//     }
// }

fn main() {
    println!("uwu");
}
