use slopio_ebpf_balancer::id_generation::ThreadAwareCidGenerator;
use slopio_ebpf_balancer::loader::QuicBalancer;
use std::net::UdpSocket;
use std::os::fd::AsRawFd;

// ---------------------------------------------------------------------------
// ThreadAwareCidGenerator tests (no privileges required)
// ---------------------------------------------------------------------------

#[test]
fn test_cid_embeds_thread_index() {
    let mut cid_gen = ThreadAwareCidGenerator::new(42, 8);
    let mut buf = [0u8; 8];
    let random = [0xAA; 8];
    cid_gen.generate(&mut buf, &random);
    assert_eq!(buf[0], 42, "byte 0 should carry the thread index");
}

#[test]
fn test_cid_counter_increments() {
    let mut cid_gen = ThreadAwareCidGenerator::new(0, 8);
    let random = [0u8; 8];

    let mut buf1 = [0u8; 8];
    cid_gen.generate(&mut buf1, &random);
    let counter1 = u32::from_le_bytes([buf1[1], buf1[2], buf1[3], 0]);

    let mut buf2 = [0u8; 8];
    cid_gen.generate(&mut buf2, &random);
    let counter2 = u32::from_le_bytes([buf2[1], buf2[2], buf2[3], 0]);

    assert_eq!(counter2, counter1 + 1, "counter should increment by 1");
}

#[test]
fn test_cid_random_tail() {
    let mut cid_gen = ThreadAwareCidGenerator::new(0, 8);
    let random = [0xBB; 8];
    let mut buf = [0u8; 8];
    cid_gen.generate(&mut buf, &random);
    assert_eq!(
        &buf[4..8],
        &[0xBB; 4],
        "tail bytes should come from random input"
    );
}

#[test]
fn test_cid_length() {
    let cid_gen = ThreadAwareCidGenerator::new(0, 16);
    assert_eq!(cid_gen.cid_len(), 16);
}

// ---------------------------------------------------------------------------
// eBPF loader tests (require root privileges — marked #[ignore])
// ---------------------------------------------------------------------------

/// Helper: create a UDP socket with SO_REUSEPORT set *before* bind,
/// bound to 127.0.0.1:<port>. When `port` is 0 the OS picks a free port.
fn reuseport_udp_socket(port: u16) -> UdpSocket {
    use std::os::fd::FromRawFd;
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        assert!(fd >= 0, "socket() failed");

        let optval: libc::c_int = 1;
        let ret = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        assert!(ret == 0, "setsockopt SO_REUSEPORT failed");

        let sa = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
            },
            sin_zero: [0; 8],
        };
        let ret = libc::bind(
            fd,
            &sa as *const libc::sockaddr_in as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
        assert!(
            ret == 0,
            "bind() failed: {}",
            std::io::Error::last_os_error()
        );

        UdpSocket::from_raw_fd(fd)
    }
}

/// Bind a fresh SO_REUSEPORT socket to the same address as `existing`.
fn reuseport_udp_socket_same_addr(existing: &UdpSocket) -> UdpSocket {
    let port = existing.local_addr().unwrap().port();
    reuseport_udp_socket(port)
}

#[test]
#[ignore] // requires root / CAP_BPF + CAP_NET_ADMIN
fn test_load_and_attach() {
    let sock = reuseport_udp_socket(0);
    let balancer = QuicBalancer::new(1, sock.as_raw_fd());
    assert!(
        balancer.is_ok(),
        "QuicBalancer::new failed: {:?}",
        balancer.err()
    );
}

#[test]
#[ignore] // requires root / CAP_BPF + CAP_NET_ADMIN
fn test_short_header_routes_to_index_0() {
    // Validates eBPF routing for a short-header QUIC packet targeting index 0.
    let sock0 = reuseport_udp_socket(0);
    let port = sock0.local_addr().unwrap().port();
    let _balancer = QuicBalancer::new(1, sock0.as_raw_fd()).expect("failed to load eBPF balancer");

    sock0
        .set_read_timeout(Some(std::time::Duration::from_millis(500)))
        .unwrap();

    // Short header (bit 7 clear), thread index 0 in byte 1
    let packet: [u8; 32] = {
        let mut pkt = [0u8; 32];
        pkt[0] = 0x40;
        pkt[1] = 0; // thread index 0
        pkt
    };

    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender
        .send_to(&packet, format!("127.0.0.1:{port}"))
        .expect("send_to failed");

    let mut recv_buf = [0u8; 64];
    let result = sock0.recv(&mut recv_buf);
    assert!(
        result.is_ok(),
        "socket at index 0 should have received the packet: {:?}",
        result.err()
    );
    assert_eq!(&recv_buf[..32], &packet);
}

#[test]
#[ignore] // requires root / CAP_BPF + CAP_NET_ADMIN
fn test_multi_socket_routing() {
    // 1. Create both sockets FIRST, then load eBPF.
    //    Both sockets must be in the reuseport group before the eBPF
    //    program is attached so that bpf_sk_select_reuseport can find them.
    let sock0 = reuseport_udp_socket(0);
    let port = sock0.local_addr().unwrap().port();
    let sock1 = reuseport_udp_socket_same_addr(&sock0);

    // 2. Load eBPF and register both sockets in the sockarray map
    let balancer = QuicBalancer::new(2, sock0.as_raw_fd()).expect("failed to load eBPF balancer");
    balancer
        .add_socket(1, sock1.as_raw_fd())
        .expect("add_socket failed");

    // 3. Set a short read timeout
    let timeout = std::time::Duration::from_millis(500);
    sock0.set_read_timeout(Some(timeout)).unwrap();
    sock1.set_read_timeout(Some(timeout)).unwrap();

    // 4. Build a QUIC short-header packet targeting thread index 1.
    //    Byte 0: 0x40 (short header — bit 7 clear, fixed bit set)
    //    Byte 1: 0x01 (thread index = 1)
    let packet: [u8; 32] = {
        let mut pkt = [0u8; 32];
        pkt[0] = 0x40; // short header
        pkt[1] = 1; // thread index 1
        pkt
    };

    // 5. Send the packet to the shared port
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender
        .send_to(&packet, format!("127.0.0.1:{port}"))
        .expect("send_to failed");

    // 6. Check which socket received the packet
    let mut recv_buf = [0u8; 64];
    let got_on_0 = sock0.recv(&mut recv_buf);
    let got_on_1 = sock1.recv(&mut recv_buf);

    // The eBPF program should route short-header packets with thread_id=1 to index 1
    assert!(
        got_on_1.is_ok(),
        "socket at index 1 should have received the packet (sock0={:?}, sock1={:?})",
        got_on_0,
        got_on_1
    );
    assert_eq!(
        &recv_buf[..32],
        &packet,
        "received data should match sent packet"
    );
}
