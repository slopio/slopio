use runtime::{channels::unbounded_channel, main_thread, runtime::spawn};

#[test]
fn unbounding() {
    main_thread(async {
        let (send, recv) = unbounded_channel::<usize>();
        let k = spawn(async move {
            send.send(3);
            println!("sent");
        });
        let k2 = spawn(async move {
            let v = recv.recv().await;
            v.unwrap();
        });
        let _ = k.await;
        let _ = k2.await;
    });
}

#[test]
fn unbounded_drop_parts() {
    main_thread(async {
        let (send, recv) = unbounded_channel::<usize>();
        let k = spawn(async move { drop(send) });
        let k2 = spawn(recv.recv());
        let _ = k.await;
        let k2 = k2.await;
        assert_eq!(k2, None);
    });
}
