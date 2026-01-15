use std::{
    io::Write, net::TcpStream, os::unix::io::AsRawFd, process::Command, thread::sleep,
    time::Duration,
};

use runtime::{
    io_driver::operations::{
        StreamExt,
        sockets::{multi_accept::MultiAccept, multi_recv::MultiRecv},
    },
    main_thread,
};

#[test]
fn test_stream_stops_on_close() {
    let reading_thread = std::thread::spawn(|| {
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9091).unwrap();
            if let Some(socket) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::new(socket.fd);
                while let Some(_data) = multi_recv.next().await {
                    recv_count += 1;
                }
                return Some(recv_count);
            }
            None
        })
    });

    sleep(Duration::from_millis(100));

    let client = TcpStream::connect("localhost:9091").unwrap();
    drop(client);

    let recv_count = reading_thread.join().unwrap().unwrap();
    assert_eq!(recv_count, 0, "Stream should receive no data, only EOF");
}

#[test]
fn test_stream_stops_on_abrupt_close_rst() {
    let reading_thread = std::thread::spawn(|| {
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9092).unwrap();
            if let Some(socket) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::new(socket.fd);
                while let Some(_data) = multi_recv.next().await {
                    recv_count += 1;
                }
                return Some(recv_count);
            }
            None
        })
    });

    sleep(Duration::from_millis(100));

    let client = TcpStream::connect("localhost:9092").unwrap();

    unsafe {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        libc::setsockopt(
            client.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            &linger as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::linger>() as u32,
        );
    }
    drop(client);

    let recv_count = reading_thread.join().unwrap().unwrap();
    assert_eq!(recv_count, 0, "Stream should stop on RST");
}

#[test]
fn test_stream_stops_on_process_kill() {
    let reading_thread = std::thread::spawn(|| {
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9093).unwrap();
            if let Some(socket) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::new(socket.fd);
                while let Some(_data) = multi_recv.next().await {
                    recv_count += 1;
                }
                return Some(recv_count);
            }
            None
        })
    });

    sleep(Duration::from_millis(100));

    let mut child = Command::new("bash")
        .args(["-c", "exec 3<>/dev/tcp/localhost/9093; sleep 10"])
        .spawn()
        .unwrap();

    sleep(Duration::from_millis(200));
    child.kill().unwrap();
    child.wait().unwrap();

    let recv_count = reading_thread.join().unwrap().unwrap();
    assert_eq!(
        recv_count, 0,
        "Stream should stop when client process is killed"
    );
}

#[test]
fn byte_per_byte() {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut random_bytes = vec![0u8; 5_000_000];
    rng.fill_bytes(&mut random_bytes);

    let random_bytes_clone = random_bytes.clone();

    let reading_thread = std::thread::spawn(move || {
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9090).unwrap();
            if let Some(socket) = multiaccept.next().await {
                let mut received = Vec::new();
                let mut multirecv = MultiRecv::new(socket.fd);
                while let Some(data) = multirecv.next().await {
                    received.extend_from_slice(data.as_ref());
                }
                Some(received)
            } else {
                None
            }
        })
    });

    sleep(Duration::from_millis(100));
    let mut client = TcpStream::connect("localhost:9090").unwrap();
    for b in random_bytes_clone.iter() {
        let k = client.write(&[*b]).unwrap();
        assert_eq!(k, 1);
    }
    drop(client);

    let received = reading_thread.join().unwrap().unwrap();
    assert_eq!(
        received.len(),
        random_bytes.len(),
        "Received {} bytes but expected {}",
        received.len(),
        random_bytes.len()
    );
    assert_eq!(
        received, random_bytes,
        "Received data doesn't match sent data"
    );
}
