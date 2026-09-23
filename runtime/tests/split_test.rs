use std::{
    io::{Read, Write},
    net::TcpStream,
};

use runtime::{
    main_thread_with,
    net::{MultiAccept, MultiRecv, OwnedReadHalf, ReuniteError, send_all},
    spawn,
};
use runtime_streams::StreamExt;

use crate::common::Cv;

mod common;

runtime::define_buf_ring!(pub Small, bgid = 0, buffer_size = 4096, ring_size = 8192);

const GREETING: &[u8] = b"hello from writer task";

/// The write half moves into a spawned task while the read half feeds a
/// MultiRecv in the accepting task. Dropping the write half in the other
/// task must not close the fd under the armed multishot.
#[test]
fn test_split_write_from_spawned_task() {
    let (cv1, cv2) = Cv::new();

    let server = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9101).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();
            let (read, write) = socket.into_split();

            spawn(async move {
                let (res, _buf) = send_all(write.socket(), GREETING).await;
                res.unwrap();
                // `write` drops here; `read` keeps the fd open.
            });

            let mut recv = MultiRecv::<Small, OwnedReadHalf>::new(read);
            let mut got = Vec::new();
            while let Some(item) = recv.next().await {
                got.extend_from_slice(item.unwrap().as_ref());
            }
            got
        })
    });

    cv2.wait();

    let mut client = TcpStream::connect("localhost:9101").unwrap();
    let mut greeting = vec![0u8; GREETING.len()];
    client.read_exact(&mut greeting).unwrap();
    assert_eq!(greeting, GREETING);
    client.write_all(b"done").unwrap();
    drop(client);

    let got = server.join().unwrap();
    assert_eq!(got, b"done");
}

/// Reuniting halves of different sockets fails and returns both halves;
/// reuniting matching halves restores working sockets.
#[test]
fn test_split_reunite() {
    let (cv1, cv2) = Cv::new();

    let server = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9102).unwrap();
            cv1.notify();
            let first = multiaccept.next().await.unwrap().unwrap();
            let second = multiaccept.next().await.unwrap().unwrap();

            let (read1, write1) = first.into_split();
            let (read2, write2) = second.into_split();

            let ReuniteError(read1, write2) = read1
                .reunite(write2)
                .expect_err("halves of different sockets must not reunite");

            let first = read1.reunite(write1).unwrap();
            let second = write2.reunite(read2).unwrap();

            let (res, _buf) = send_all(&first, GREETING).await;
            res.unwrap();
            let (res, _buf) = send_all(&second, GREETING).await;
            res.unwrap();
        })
    });

    cv2.wait();

    let mut clients = [
        TcpStream::connect("localhost:9102").unwrap(),
        TcpStream::connect("localhost:9102").unwrap(),
    ];
    for client in &mut clients {
        let mut greeting = vec![0u8; GREETING.len()];
        client.read_exact(&mut greeting).unwrap();
        assert_eq!(greeting, GREETING);
    }

    server.join().unwrap();
}

/// `sync_shutdown` on the write half sends FIN (the peer reads EOF) while
/// the read half keeps receiving what the peer sends afterwards.
#[test]
fn test_split_shutdown_write_keeps_reading() {
    let (cv1, cv2) = Cv::new();

    let server = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9103).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();
            let (read, write) = socket.into_split();

            write.sync_shutdown();

            let mut recv = MultiRecv::<Small, OwnedReadHalf>::new(read);
            let mut got = Vec::new();
            while let Some(item) = recv.next().await {
                got.extend_from_slice(item.unwrap().as_ref());
            }
            got
        })
    });

    cv2.wait();

    let mut client = TcpStream::connect("localhost:9103").unwrap();
    let mut buf = [0u8; 16];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(n, 0, "client should see EOF from the server's SHUT_WR");
    client.write_all(b"after-fin").unwrap();
    drop(client);

    let got = server.join().unwrap();
    assert_eq!(got, b"after-fin");
}
