use std::io::Read;
use std::net::TcpListener;

use slopio::net::{connect_tcp, socket_send_control_record};
use slopio::runtime::main_thread;

const SOL_TLS: libc::c_int = 282;
const TCP_ULP: libc::c_int = 31;
const TLS_TX: libc::c_int = 1;
const TLS_1_2_VERSION: u16 = 0x0303;
const TLS_CIPHER_AES_GCM_128: u16 = 51;

#[repr(C)]
struct TlsCryptoInfoAesGcm128 {
    version: u16,
    cipher_type: u16,
    iv: [u8; 8],
    key: [u8; 16],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

fn enable_ktls_tx(fd: i32) -> std::io::Result<()> {
    unsafe {
        let ulp = b"tls";
        let r = libc::setsockopt(
            fd,
            libc::SOL_TCP,
            TCP_ULP,
            ulp.as_ptr() as *const _,
            ulp.len() as libc::socklen_t,
        );
        if r < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let info = TlsCryptoInfoAesGcm128 {
            version: TLS_1_2_VERSION,
            cipher_type: TLS_CIPHER_AES_GCM_128,
            iv: [0; 8],
            key: [0; 16],
            salt: [0; 4],
            rec_seq: [0; 8],
        };
        let r = libc::setsockopt(
            fd,
            SOL_TLS,
            TLS_TX,
            &info as *const _ as *const _,
            std::mem::size_of::<TlsCryptoInfoAesGcm128>() as libc::socklen_t,
        );
        if r < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[test]
fn control_record_emits_correct_content_type() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let rx = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).unwrap();
        hdr
    });

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        if let Err(e) = enable_ktls_tx(socket.fd()) {
            panic!("kTLS setup failed ({e}); load the `tls` kernel module: `sudo modprobe tls`");
        }
        // record_type 21 = TLS Alert; payload is a close_notify (level=warning, desc=0).
        let payload = vec![1u8, 0];
        let len = payload.len();
        let n = socket_send_control_record(&socket, 21, payload)
            .await
            .expect("sendmsg failed");
        assert_eq!(n, len);
    });

    let hdr = rx.join().unwrap();
    // TLS record header on the wire: [content_type][version_major][version_minor][len_hi][len_lo]
    assert_eq!(hdr[0], 21, "expected Alert (21), got {}", hdr[0]);
    assert_eq!(hdr[1], 0x03);
    assert_eq!(hdr[2], 0x03);
}
