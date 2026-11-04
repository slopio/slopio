use std::{io::Read, net::TcpStream, os::fd::FromRawFd, time::Duration};

use slopio::streams::StreamExt;
use slopio::{
    main_thread_with,
    net::{MultiAccept, SocketWrapper, socket_recv, socket_send},
    runtime::{select::select, time::Sleep},
};

use crate::common::Cv;

mod common;

slopio::define_buf_ring!(pub Small, bgid = 0, buffer_size = 4096, ring_size = 8192);

const GREETING: &[u8] = b"sent after the wrapper dropped";

/// A send prepared before the wrapper drops must keep the fd alive.
#[test]
fn test_op_outlives_wrapper() {
    let (cv1, cv2) = Cv::new();

    let server = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9104).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();

            // Drop the wrapper before the lazy op's first poll.
            let write = socket_send(&socket, GREETING);
            drop(socket);
            let (res, _buf) = write.await;
            res.unwrap();
            // The future's inner ref drops here, which closes the fd.
        })
    });

    cv2.wait();

    let mut client = TcpStream::connect("localhost:9104").unwrap();
    let mut greeting = vec![0u8; GREETING.len()];
    client.read_exact(&mut greeting).unwrap();
    assert_eq!(greeting, GREETING);

    // The fd closed after the send completed, so the client sees EOF next.
    let n = client.read(&mut [0u8; 1]).unwrap();
    assert_eq!(n, 0, "socket should close once the last op ref drops");

    server.join().unwrap();
}

/// Dropping an in-flight op future that is also the last socket handle:
/// the drop pushes the cancel, then the close, and the peer sees EOF.
#[test]
fn test_last_handle_dropped_mid_op() {
    let (cv1, cv2) = Cv::new();

    let server = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9105).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();
            let recv = socket_recv(&socket, vec![0u8; 32]);
            drop(socket);
            // First poll submits the recv; the client never writes, so the
            // sleep wins and `recv` drops while its op is in flight.
            select(recv, Sleep::during(Duration::from_millis(60))).await;
            Sleep::during(Duration::from_millis(60)).await;
        })
    });

    cv2.wait();
    let mut client = TcpStream::connect("localhost:9105").unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let n = client.read(&mut [0u8; 8]).unwrap();
    assert_eq!(n, 0, "peer should see EOF once the close lands");
    server.join().unwrap();
}

/// -EBADF is an fd-ownership violation: the driver must panic, not return an error.
#[test]
fn test_ebadf_completion_panics() {
    let result = std::thread::spawn(|| {
        main_thread_with::<Small, _>(async {
            // Never-open fd: the send completes with -EBADF.
            let socket = unsafe { SocketWrapper::from_raw_fd(1_000_000) };
            let (res, _buf) = socket_send(&socket, GREETING).await;
            drop(res);
        })
    })
    .join();
    assert!(result.is_err(), "EBADF completion must panic the runtime");
}
