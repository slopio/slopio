use std::{collections::HashMap, mem::MaybeUninit, net::SocketAddr};

use quiche::{
    Config, Connection, ConnectionId,
    h3::{self, NameValue},
};
use ring::rand::{SecureRandom, SystemRandom};
use runtime::{io_driver::operations::sockets::udp::UdpSocket, main_thread};

const MAX_DATAGRAM_SIZE: usize = 1350;

struct Client {
    conn: Connection,
    h3: Option<h3::Connection>,
}

fn main() {
    main_thread(async {
        // Create UDP socket for QUIC
        let socket = UdpSocket::bind(4433).expect("failed to bind UDP socket");
        let local_addr: SocketAddr = "0.0.0.0:4433".parse().unwrap();
        println!("HTTP/3 server listening on :4433");

        // Configure QUIC
        let mut config = Config::new(quiche::PROTOCOL_VERSION).unwrap();
        config
            .load_cert_chain_from_pem_file("certs/cert.pem")
            .expect("failed to load cert");
        config
            .load_priv_key_from_pem_file("certs/key.pem")
            .expect("failed to load key");
        // Use HTTP/3 ALPN
        config
            .set_application_protos(h3::APPLICATION_PROTOCOL)
            .unwrap();
        config.set_max_idle_timeout(5000);
        config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
        config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
        config.set_initial_max_data(10_000_000);
        config.set_initial_max_stream_data_bidi_local(1_000_000);
        config.set_initial_max_stream_data_bidi_remote(1_000_000);
        config.set_initial_max_streams_bidi(100);
        config.set_initial_max_streams_uni(100);

        let h3_config = h3::Config::new().unwrap();

        let rng = SystemRandom::new();
        let mut clients: HashMap<ConnectionId<'static>, Client> = HashMap::new();
        let mut recv_buf = [MaybeUninit::uninit(); 65535];
        let mut send_buf = [0u8; MAX_DATAGRAM_SIZE];

        loop {
            // Receive UDP packet
            let packet = socket.recv_from(&mut recv_buf).await.unwrap();
            let mut data = unsafe {
                std::slice::from_raw_parts_mut(recv_buf.as_mut_ptr().cast::<u8>(), packet.len)
            };
            let from = packet.addr;

            // Parse QUIC header
            let hdr = match quiche::Header::from_slice(&mut data, quiche::MAX_CONN_ID_LEN) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Find or create connection
            let client = if let Some(client) = clients.get_mut(&hdr.dcid) {
                client
            } else if hdr.ty == quiche::Type::Initial {
                let scid = generate_conn_id(&rng);
                let conn = quiche::accept(&scid, None, local_addr, from, &mut config).unwrap();
                clients.insert(scid.clone(), Client { conn, h3: None });
                clients.get_mut(&scid).unwrap()
            } else {
                continue;
            };

            // Process received data
            let recv_info = quiche::RecvInfo {
                from,
                to: local_addr,
            };
            if let Err(e) = client.conn.recv(&mut data, recv_info) {
                eprintln!("recv failed: {:?}", e);
                continue;
            }

            // Initialize HTTP/3 once QUIC handshake is complete
            if client.conn.is_established() && client.h3.is_none() {
                client.h3 = Some(
                    h3::Connection::with_transport(&mut client.conn, &h3_config)
                        .expect("failed to create h3 connection"),
                );
                println!("HTTP/3 connection established with {from}");
            }

            // Handle HTTP/3 events
            if let Some(h3_conn) = &mut client.h3 {
                loop {
                    match h3_conn.poll(&mut client.conn) {
                        Ok((stream_id, h3::Event::Headers { list, .. })) => {
                            let mut path = "/";
                            let mut method = "GET";
                            for hdr in &list {
                                match hdr.name() {
                                    b":path" => {
                                        path = std::str::from_utf8(hdr.value()).unwrap_or("/")
                                    }
                                    b":method" => {
                                        method = std::str::from_utf8(hdr.value()).unwrap_or("GET")
                                    }
                                    _ => {}
                                }
                            }

                            let req = Request {
                                method,
                                path,
                                headers: &list,
                            };
                            let resp = handle_request(req);
                            send_response(h3_conn, &mut client.conn, stream_id, resp);
                        }
                        Ok((_, h3::Event::Data)) => {}
                        Ok((_, h3::Event::Finished)) => {}
                        Ok((_, h3::Event::Reset(_))) => {}
                        Ok((_, h3::Event::GoAway)) => {}
                        Ok((_, h3::Event::PriorityUpdate)) => {}
                        Err(h3::Error::Done) => break,
                        Err(e) => {
                            eprintln!("h3 error: {:?}", e);
                            break;
                        }
                    }
                }
            }

            // Send pending data
            send_pending(&mut client.conn, &socket, from, &mut send_buf).await;

            // Clean up closed connections
            clients.retain(|_, c| !c.conn.is_closed());
        }
    });
}

struct Request<'a> {
    method: &'a str,
    path: &'a str,
    headers: &'a [h3::Header],
}

struct Response {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

fn handle_request(req: Request) -> Response {
    println!("{} {}", req.method, req.path);

    match (req.method, req.path) {
        ("GET", "/") => Response {
            status: 200,
            content_type: "text/plain",
            body: b"Hello from HTTP/3!\n".to_vec(),
        },
        ("GET", "/json") => Response {
            status: 200,
            content_type: "application/json",
            body: br#"{"message": "Hello from HTTP/3!"}"#.to_vec(),
        },
        ("GET", path) => Response {
            status: 200,
            content_type: "text/plain",
            body: format!("You requested: {path}\n").into_bytes(),
        },
        _ => Response {
            status: 405,
            content_type: "text/plain",
            body: b"Method not allowed\n".to_vec(),
        },
    }
}

fn send_response(
    h3_conn: &mut h3::Connection,
    conn: &mut Connection,
    stream_id: u64,
    resp: Response,
) {
    let headers = vec![
        h3::Header::new(b":status", resp.status.to_string().as_bytes()),
        h3::Header::new(b"content-type", resp.content_type.as_bytes()),
        h3::Header::new(b"content-length", resp.body.len().to_string().as_bytes()),
    ];
    h3_conn.send_response(conn, stream_id, &headers, false).ok();
    h3_conn.send_body(conn, stream_id, &resp.body, true).ok();
}

fn generate_conn_id(rng: &SystemRandom) -> ConnectionId<'static> {
    let mut id = [0u8; quiche::MAX_CONN_ID_LEN];
    rng.fill(&mut id).unwrap();
    ConnectionId::from_vec(id.to_vec())
}

async fn send_pending(conn: &mut Connection, socket: &UdpSocket, to: SocketAddr, buf: &mut [u8]) {
    loop {
        let (len, _info) = match conn.send(buf) {
            Ok(v) => v,
            Err(quiche::Error::Done) => break,
            Err(e) => {
                eprintln!("send failed: {:?}", e);
                break;
            }
        };
        socket.send_to(&buf[..len], to).await.ok();
    }
}
