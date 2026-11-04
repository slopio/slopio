use std::net::TcpListener;

use slopio_core::net::connect_tcp;
use slopio_core::runtime::main_thread;

fn is_nonblocking(fd: i32) -> bool {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(flags >= 0, "fcntl failed");
    flags & libc::O_NONBLOCK != 0
}

fn is_nodelay(fd: i32) -> bool {
    let mut v: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let r = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &mut v as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    assert_eq!(r, 0, "getsockopt failed");
    v != 0
}

fn is_cloexec(fd: i32) -> bool {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0, "fcntl failed");
    flags & libc::FD_CLOEXEC != 0
}

#[test]
fn connect_defaults_nonblocking_and_nodelay() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = std::thread::spawn(move || listener.accept().unwrap());

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        assert!(is_nonblocking(socket.fd()));
        assert!(is_nodelay(socket.fd()));
        assert!(is_cloexec(socket.fd()));
    });
    acceptor.join().unwrap();
}

#[test]
fn connect_opt_out_of_defaults() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = std::thread::spawn(move || listener.accept().unwrap());

    main_thread(async move {
        let socket = connect_tcp(addr)
            .nonblocking(false)
            .nodelay(false)
            .await
            .unwrap();
        assert!(!is_nonblocking(socket.fd()));
        assert!(!is_nodelay(socket.fd()));
        assert!(is_cloexec(socket.fd()));
    });
    acceptor.join().unwrap();
}
