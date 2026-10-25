//! Simulated peers written as code.
//!
//! An actor is asked what each read gets and what each write does as the
//! completion is posted, and is called back when its timers fire. These pin
//! that routing on the bare ring, then run real programs against actors: a
//! ping-pong client that only speaks once its last message came back, which
//! is what a script, fixed in advance, cannot be.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use io_uring::{opcode, types::Fd};
use runtime_core::deterministic_testing::{
    AcceptEvent, Actor, ActorCx, FakeRing, FdKind, ReadEvent, ReadReply, WriteReply, mint,
    simulated_socket,
};
use runtime_core::io_driver::operations::sockets::SocketWrapper;
use runtime_core::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::io_driver::operations::sockets::send::send_all;
use runtime_core::runtime::RuntimeInner;
use runtime_streams::StreamExt;

runtime_core::define_buf_ring!(PeerRing, bgid = 0, buffer_size = 16, ring_size = 8);
// One tiny buffer, so holding it leaves the next read nowhere to land.
runtime_core::define_buf_ring!(OneBuf, bgid = 1, buffer_size = 4, ring_size = 1);

/// Every seed natively; a handful under Miri, which interprets. The paths are
/// the same either way, only how many times they are walked changes.
fn seeds(n: u64) -> std::ops::Range<u64> {
    0..if cfg!(miri) { n.min(4) } else { n }
}

/// Whether to judge how widely the seeds explored: too few run under Miri.
const EXPLORING: bool = !cfg!(miri);

const fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn ring() -> (
    FakeRing,
    runtime_core::deterministic_testing::FakeRingHandle,
) {
    FakeRing::builder().virtual_time().build()
}

fn recv(fd: i32, buf: &mut [u8], user_data: u64) -> io_uring::squeue::Entry {
    opcode::Recv::new(Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data)
}

fn send(fd: i32, buf: &[u8], user_data: u64) -> io_uring::squeue::Entry {
    opcode::Send::new(Fd(fd), buf.as_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data)
}

/// Wait on the ring until `n` completions came back, oldest first.
fn collect(ring: &mut FakeRing, n: usize) -> Vec<(u64, i32)> {
    let mut got = Vec::new();
    while got.len() < n {
        ring.submit_and_wait();
        while let Some(c) = ring.pop_completion() {
            got.push((c.user_data, c.result));
        }
    }
    got
}

/// Answers reads from a fixed queue and writes with fixed replies.
#[derive(Default)]
struct Canned {
    reads: VecDeque<ReadReply>,
    writes: VecDeque<WriteReply>,
}

impl Actor for Canned {
    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        self.reads.pop_front().unwrap_or(ReadReply::Pending)
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        self.writes
            .pop_front()
            .unwrap_or(WriteReply::Accepted(bytes.len()))
    }
}

// --------------------------------------------------------------- the ring

#[test]
fn an_actor_answers_each_read_as_it_completes() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(
        fd,
        Canned {
            reads: [ReadReply::Data(b"hi".to_vec()), ReadReply::Eof].into(),
            ..Canned::default()
        },
    );
    let mut buf = [0u8; 8];

    ring.add(&recv(fd, &mut buf, 1));
    assert_eq!(collect(&mut ring, 1), vec![(1, 2)]);
    assert_eq!(&buf[..2], b"hi");
    ring.add(&recv(fd, &mut buf, 2));
    assert_eq!(collect(&mut ring, 1), vec![(2, 0)]);
    assert_eq!(handle.reads(fd), vec![b"hi".to_vec()]);
}

/// Has nothing to say until its timer fires five milliseconds in.
struct Late {
    ready: bool,
}

impl Actor for Late {
    fn on_open(&mut self, cx: &mut ActorCx) {
        cx.timer(ms(5), 0);
    }

    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        if self.ready {
            ReadReply::Data(b"late".to_vec())
        } else {
            ReadReply::Pending
        }
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        if self.ready {
            WriteReply::Accepted(bytes.len())
        } else {
            WriteReply::Pending
        }
    }

    fn on_timer(&mut self, _tag: u64, _cx: &mut ActorCx) {
        self.ready = true;
    }
}

#[test]
fn a_read_waits_on_its_actor_until_a_timer_gives_it_something() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(fd, Late { ready: false });
    let mut buf = [0u8; 8];

    ring.add(&recv(fd, &mut buf, 1));
    ring.submit();
    assert_eq!(collect(&mut ring, 1), vec![(1, 4)]);
    assert_eq!(handle.clock().unwrap().elapsed(), ms(5));
}

#[test]
fn a_write_the_actor_holds_waits_like_a_full_send_buffer() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(fd, Late { ready: false });

    ring.add(&send(fd, b"abc", 1));
    assert_eq!(collect(&mut ring, 1), vec![(1, 3)]);
    assert_eq!(handle.clock().unwrap().elapsed(), ms(5));
    assert_eq!(handle.writes(fd), vec![b"abc".to_vec()]);
}

#[test]
fn writes_reach_the_actor_and_take_what_it_accepts() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(
        fd,
        Canned {
            writes: [WriteReply::Accepted(2), WriteReply::Error(libc::EPIPE)].into(),
            ..Canned::default()
        },
    );

    ring.add(&send(fd, b"abcd", 1));
    ring.add(&send(fd, b"ef", 2));
    assert_eq!(collect(&mut ring, 2), vec![(1, 2), (2, -libc::EPIPE)]);
    assert_eq!(handle.writes(fd), vec![b"ab".to_vec()]);
}

/// Arms, re-arms and cancels, and notes what fired when.
struct Timers {
    fired: Rc<RefCell<Vec<(u64, Duration)>>>,
}

impl Actor for Timers {
    fn on_open(&mut self, cx: &mut ActorCx) {
        cx.timer(ms(5), 1);
        cx.timer(ms(10), 1);
        cx.timer(ms(3), 2);
        cx.cancel(2);
    }

    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        if self.fired.borrow().is_empty() {
            ReadReply::Pending
        } else {
            ReadReply::Eof
        }
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        WriteReply::Accepted(bytes.len())
    }

    fn on_timer(&mut self, tag: u64, cx: &mut ActorCx) {
        self.fired.borrow_mut().push((tag, cx.now()));
    }
}

#[test]
fn a_rearmed_timer_fires_once_and_a_cancelled_one_never() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    let fired = Rc::new(RefCell::new(Vec::new()));
    handle.attach(
        fd,
        Timers {
            fired: fired.clone(),
        },
    );
    let mut buf = [0u8; 8];

    ring.add(&recv(fd, &mut buf, 1));
    assert_eq!(collect(&mut ring, 1), vec![(1, 0)]);
    assert_eq!(*fired.borrow(), vec![(1, ms(10))]);
}

/// Notes its close, and keeps a timer armed past it.
struct Closing {
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Actor for Closing {
    fn on_open(&mut self, cx: &mut ActorCx) {
        cx.timer(ms(5), 0);
    }

    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        ReadReply::Pending
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        WriteReply::Accepted(bytes.len())
    }

    fn on_closed(&mut self, _cx: &mut ActorCx) {
        self.log.borrow_mut().push("closed");
    }

    fn on_timer(&mut self, _tag: u64, _cx: &mut ActorCx) {
        self.log.borrow_mut().push("timer");
    }
}

#[test]
fn closing_the_fd_tells_the_actor_and_silences_its_timers() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    let log = Rc::new(RefCell::new(Vec::new()));
    handle.attach(fd, Closing { log: log.clone() });

    // Opens the actor, which arms its timer.
    ring.add(&send(fd, b"x", 1));
    collect(&mut ring, 1);
    ring.add(&opcode::Close::new(Fd(fd)).build().user_data(2));
    collect(&mut ring, 1);

    // A later op of another fd outlasts the timer the closed actor armed.
    let other = mint(FdKind::Socket);
    handle.on_write(
        other,
        runtime_core::deterministic_testing::WriteEvent::All.after(ms(20)),
    );
    ring.add(&send(other, b"y", 3));
    collect(&mut ring, 1);

    assert_eq!(*log.borrow(), vec!["closed"]);
}

#[test]
#[should_panic(expected = "has an actor")]
fn scripting_a_descriptor_with_an_actor_is_refused() {
    let (_ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(fd, Canned::default());
    handle.on_read(fd, ReadEvent::Eof);
}

#[test]
#[should_panic(expected = "already has reads or writes scripted")]
fn an_actor_on_a_scripted_descriptor_is_refused() {
    let (_ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.on_read(fd, ReadEvent::Eof);
    handle.attach(fd, Canned::default());
}

// -------------------------------------------------------- with the runtime

/// A client that sends one message, waits for all of it to come back, and
/// only then sends the next; once every message is back, it closes.
struct PingPong {
    messages: VecDeque<Vec<u8>>,
    /// What the server still owes back of the message in flight.
    awaited: VecDeque<u8>,
    outbox: VecDeque<u8>,
    done: bool,
    report: Rc<RefCell<PingPongReport>>,
}

#[derive(Default, Debug)]
struct PingPongReport {
    echoed: Vec<Vec<u8>>,
    wrong: Option<String>,
}

const NEXT: u64 = 1;

impl PingPong {
    fn new(messages: Vec<Vec<u8>>, report: Rc<RefCell<PingPongReport>>) -> Self {
        Self {
            messages: messages.into(),
            awaited: VecDeque::new(),
            outbox: VecDeque::new(),
            done: false,
            report,
        }
    }

    fn send_next(&mut self) {
        match self.messages.pop_front() {
            Some(message) => {
                self.awaited = message.iter().copied().collect();
                self.outbox = message.into();
            }
            None => self.done = true,
        }
    }
}

impl Actor for PingPong {
    fn on_open(&mut self, _cx: &mut ActorCx) {
        self.send_next();
    }

    fn read(&mut self, max: usize, cx: &mut ActorCx) -> ReadReply {
        if !self.outbox.is_empty() {
            // As TCP would, in pieces of whatever size.
            let n = (cx.between(1, max as u64) as usize).min(self.outbox.len());
            return ReadReply::Data(self.outbox.drain(..n).collect());
        }
        if self.done {
            ReadReply::Eof
        } else {
            ReadReply::Pending
        }
    }

    fn write(&mut self, bytes: &[u8], cx: &mut ActorCx) -> WriteReply {
        let expected: Vec<u8> = self.awaited.iter().take(bytes.len()).copied().collect();
        if expected != bytes {
            self.report.borrow_mut().wrong =
                Some(format!("expected {expected:?}, the server sent {bytes:?}"));
        }
        self.awaited.drain(..bytes.len().min(self.awaited.len()));
        self.report.borrow_mut().echoed.push(bytes.to_vec());
        if self.awaited.is_empty() && self.outbox.is_empty() {
            // Thinks a little before the next message.
            let think = ms(cx.between(0, 3));
            cx.timer(think, NEXT);
        }
        WriteReply::Accepted(bytes.len())
    }

    fn on_timer(&mut self, _tag: u64, _cx: &mut ActorCx) {
        self.send_next();
    }
}

async fn echo(socket: SocketWrapper) {
    let mut recv = MultiRecv::<PeerRing>::new(socket);
    while let Some(Ok(buffer)) = recv.next().await {
        let (sent, _) = send_all(recv.socket(), buffer).await;
        sent.expect("the actor takes every write");
    }
}

fn messages() -> Vec<Vec<u8>> {
    vec![b"ping".to_vec(), (0..40).collect(), b"last one".to_vec()]
}

#[test]
fn a_ping_pong_client_actor_is_answered_by_an_echo_server() {
    for seed in seeds(16) {
        let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
        let socket = simulated_socket();
        let fd = socket.fd();
        let report = Rc::new(RefCell::new(PingPongReport::default()));
        handle.attach(fd, PingPong::new(messages(), report.clone()));

        RuntimeInner::simulated(fake).block_on::<PeerRing, _>(echo(socket), None, |run| run());

        let report = report.borrow();
        assert_eq!(report.wrong, None, "seed {seed}");
        assert_eq!(report.echoed.concat(), messages().concat(), "seed {seed}");
        assert_eq!(
            handle.writes(fd).concat(),
            messages().concat(),
            "seed {seed}"
        );
    }
}

#[test]
#[should_panic(expected = "the runtime would never wake up")]
fn a_server_that_holds_its_answer_blocks_a_ping_pong_client() {
    let (fake, handle) = ring();
    let socket = simulated_socket();
    let report = Rc::new(RefCell::new(PingPongReport::default()));
    handle.attach(socket.fd(), PingPong::new(messages(), report));

    // Answers only once the client is done — which it never is.
    RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
        async move {
            let mut recv = MultiRecv::<PeerRing>::new(socket);
            let mut held = Vec::new();
            while let Some(Ok(buffer)) = recv.next().await {
                held.extend_from_slice(buffer.as_ref());
            }
            let (sent, _) = send_all(recv.socket(), held).await;
            sent.expect("the actor takes every write");
        },
        None,
        |run| run(),
    );
}

/// Sends `payload` in pieces as large as each read allows, then closes.
struct Sender {
    payload: VecDeque<u8>,
}

impl Actor for Sender {
    fn read(&mut self, max: usize, _cx: &mut ActorCx) -> ReadReply {
        if self.payload.is_empty() {
            return ReadReply::Eof;
        }
        let n = max.min(self.payload.len());
        ReadReply::Data(self.payload.drain(..n).collect())
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        WriteReply::Accepted(bytes.len())
    }
}

#[test]
fn bytes_no_buffer_could_take_wait_in_the_socket() {
    let (fake, handle) = ring();
    let socket = simulated_socket();
    let payload: Vec<u8> = (0..12).collect();
    handle.attach(
        socket.fd(),
        Sender {
            payload: payload.iter().copied().collect(),
        },
    );

    let got = RuntimeInner::simulated(fake).block_on::<OneBuf, _>(
        async move {
            let mut recv = MultiRecv::<OneBuf>::new(socket);
            let mut got = Vec::new();
            // Held, so the next read has nowhere to land.
            let first = recv.next().await.expect("a chunk").expect("no error");
            got.extend_from_slice(first.as_ref());
            match recv.next().await {
                Some(Err(CompletionError::Other(code))) if code == -libc::ENOBUFS => {}
                Some(Ok(buffer)) => {
                    panic!("expected the ring to run dry, got {} bytes", buffer.len())
                }
                other => panic!(
                    "expected the ring to run dry, got {:?}",
                    other.map(|r| r.err())
                ),
            }
            drop(first);
            while let Some(chunk) = recv.next().await {
                match chunk {
                    Ok(buffer) => got.extend_from_slice(buffer.as_ref()),
                    Err(CompletionError::Other(code)) if code == -libc::ENOBUFS => {}
                    Err(other) => panic!("{other:?}"),
                }
            }
            got
        },
        None,
        |run| run(),
    );

    assert_eq!(got, payload, "bytes were lost when the ring ran dry");
}

/// Notes when its connection opened.
struct Opened {
    at: Rc<RefCell<Option<Duration>>>,
}

impl Actor for Opened {
    fn on_open(&mut self, cx: &mut ActorCx) {
        *self.at.borrow_mut() = Some(cx.now());
    }

    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        ReadReply::Eof
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        WriteReply::Accepted(bytes.len())
    }
}

#[test]
fn an_actor_opens_when_the_accept_hands_its_descriptor_out() {
    let (fake, handle) = ring();
    let client = mint(FdKind::Socket);
    let at = Rc::new(RefCell::new(None));
    handle.attach(client, Opened { at: at.clone() });

    let script = handle.clone();
    RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
        async move {
            let mut listener = MultiAccept::new(0).expect("bind");
            script.on_multishot_accept(
                listener.socket().fd(),
                [AcceptEvent::On(client).after(ms(7))],
            );
            let conn = listener.next().await.expect("a client").expect("accepted");
            drop(listener);
            echo(conn).await;
        },
        None,
        |run| run(),
    );

    assert_eq!(*at.borrow(), Some(ms(7)));
}

/// Says nothing until written to, then answers on the spot — no timer.
#[derive(Default)]
struct Responder {
    answer: Vec<u8>,
}

impl Actor for Responder {
    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        if self.answer.is_empty() {
            ReadReply::Pending
        } else {
            ReadReply::Data(std::mem::take(&mut self.answer))
        }
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        self.answer = bytes.iter().rev().copied().collect();
        WriteReply::Accepted(bytes.len())
    }
}

#[test]
fn a_write_wakes_a_read_already_waiting_on_the_actor() {
    let (fake, handle) = ring();
    let socket = simulated_socket();
    handle.attach(socket.fd(), Responder::default());

    let got = RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
        async move {
            let (read, write) = socket.into_split();
            let mut recv = MultiRecv::<PeerRing, _>::new(read);
            // The read is armed, and waiting on the actor, before the write goes.
            let (chunk, sent) = runtime_core::runtime::join::join(recv.next(), async {
                send_all(write.socket(), b"abc".to_vec()).await.0
            })
            .await;
            sent.expect("the actor takes the write");
            chunk.expect("a chunk").expect("no error").as_ref().to_vec()
        },
        None,
        |run| run(),
    );

    assert_eq!(got, b"cba");
}

// ------------------------------------------------------------- dialogues

use runtime_core::deterministic_testing::{Check, Dialogue};

#[test]
fn a_dialogue_plays_out_against_an_echo_server() {
    let numbers: Vec<u8> = (0..40).collect();
    for seed in seeds(8) {
        let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
        let socket = simulated_socket();
        let want = numbers.clone();
        let (peer, report) = Dialogue::new()
            .send(b"hello".to_vec())
            .expect_bytes(b"hello".to_vec())
            .wait(ms(2))
            .send(numbers.clone())
            .expect("the numbers back", move |got| {
                if got.len() < want.len() {
                    Check::More
                } else if got[..want.len()] == want[..] {
                    Check::Pass(want.len())
                } else {
                    Check::Fail(format!("got {:?}", &got[..want.len()]))
                }
            })
            .close()
            .build();
        handle.attach(socket.fd(), peer);

        RuntimeInner::simulated(fake).block_on::<PeerRing, _>(echo(socket), None, |run| run());

        report.borrow().assert_finished();
    }
}

/// Answers each chunk it reads as a frame: one length byte, then the chunk.
async fn framed_echo(socket: SocketWrapper) {
    let mut recv = MultiRecv::<PeerRing>::new(socket);
    while let Some(Ok(buffer)) = recv.next().await {
        let mut frame = vec![buffer.len() as u8];
        frame.extend_from_slice(buffer.as_ref());
        let (sent, _) = send_all(recv.socket(), frame).await;
        sent.expect("the actor takes every write");
    }
}

/// Parses length-prefixed frames off the front of `got` until their payloads
/// add up to `want`, however the frames and the writes carrying them split.
fn frames_carrying(want: &'static [u8]) -> impl FnMut(&[u8]) -> Check {
    move |got| {
        let (mut at, mut payload) = (0, Vec::new());
        while payload.len() < want.len() {
            let Some(&len) = got.get(at) else {
                return Check::More;
            };
            let Some(body) = got.get(at + 1..at + 1 + len as usize) else {
                return Check::More;
            };
            payload.extend_from_slice(body);
            at += 1 + len as usize;
        }
        if payload == want {
            Check::Pass(at)
        } else {
            Check::Fail(format!("the frames carried {payload:?}"))
        }
    }
}

#[test]
fn a_closure_checks_a_framed_reply_however_it_splits() {
    for seed in seeds(16) {
        let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
        let socket = simulated_socket();
        let (peer, report) = Dialogue::new()
            .send(b"framed".to_vec())
            .expect("frames carrying \"framed\"", frames_carrying(b"framed"))
            .close()
            .build();
        handle.attach(socket.fd(), peer);

        RuntimeInner::simulated(fake)
            .block_on::<PeerRing, _>(framed_echo(socket), None, |run| run());

        report.borrow().assert_finished();
    }
}

#[test]
fn a_wrong_reply_resets_the_connection_and_is_reported() {
    let (fake, handle) = ring();
    let socket = simulated_socket();
    let (peer, report) = Dialogue::new()
        .expect_bytes(b"hello".to_vec())
        .send(b"never sent".to_vec())
        .build();
    handle.attach(socket.fd(), peer);

    let ended = RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
        async move {
            let (sent, _) = send_all(&socket, b"help!".to_vec()).await;
            sent.expect("the actor takes the write before judging it");
            let mut recv = MultiRecv::<PeerRing>::new(socket);
            match recv.next().await {
                Some(Err(error)) => format!("{error:?}"),
                Some(Ok(buffer)) => panic!("read {} bytes from a reset peer", buffer.len()),
                None => "eof".into(),
            }
        },
        None,
        |run| run(),
    );

    assert_eq!(ended, "ConnectionReset");
    let report = report.borrow();
    assert_eq!(report.done, 0);
    let failure = report.failure.as_deref().expect("the mismatch is reported");
    assert!(
        failure.starts_with("step 1,") && failure.contains("byte 3"),
        "{failure}"
    );
}

#[test]
#[should_panic(expected = "dialogue played 3 of 4 steps: the program stopped answering")]
fn a_program_that_stops_early_leaves_the_dialogue_unfinished() {
    let (fake, handle) = ring();
    let socket = simulated_socket();
    let (peer, report) = Dialogue::new()
        .send(b"one".to_vec())
        .expect_bytes(b"one".to_vec())
        .send(b"two".to_vec())
        .expect_bytes(b"two".to_vec())
        .build();
    handle.attach(socket.fd(), peer);

    // Echoes the first message only, then hangs up.
    RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
        async move {
            let mut recv = MultiRecv::<PeerRing>::new(socket);
            let mut got = Vec::new();
            while got.len() < 3 {
                let buffer = recv.next().await.expect("a chunk").expect("no error");
                got.extend_from_slice(buffer.as_ref());
            }
            let (sent, _) = send_all(recv.socket(), got).await;
            sent.expect("the actor takes every write");
        },
        None,
        |run| run(),
    );

    report.borrow().assert_finished();
}

// ---------------------------------------------------------- held sends

/// Takes every write at once, but reports each after the delay it is given.
struct Slow {
    delays: VecDeque<Duration>,
}

impl Actor for Slow {
    fn read(&mut self, _max: usize, _cx: &mut ActorCx) -> ReadReply {
        ReadReply::Pending
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        match self.delays.pop_front() {
            Some(after) if !after.is_zero() => WriteReply::AcceptedAfter(bytes.len(), after),
            _ => WriteReply::Accepted(bytes.len()),
        }
    }
}

#[test]
fn a_send_taken_now_completes_after_its_delay() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(
        fd,
        Slow {
            delays: [ms(5)].into(),
        },
    );

    ring.add(&send(fd, b"abc", 1));
    assert_eq!(collect(&mut ring, 1), vec![(1, 3)]);
    assert_eq!(handle.clock().unwrap().elapsed(), ms(5));
    assert_eq!(handle.writes(fd), vec![b"abc".to_vec()]);
}

#[test]
fn a_send_held_back_is_not_overtaken_by_the_next() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(
        fd,
        Slow {
            delays: [ms(10), Duration::ZERO].into(),
        },
    );

    ring.add(&send(fd, b"first", 1));
    ring.add(&send(fd, b"next", 2));
    assert_eq!(collect(&mut ring, 2), vec![(1, 5), (2, 4)]);
    assert_eq!(handle.clock().unwrap().elapsed(), ms(10));
}

#[test]
fn a_cancel_cannot_take_back_a_send_already_taken() {
    let (mut ring, handle) = ring();
    let fd = mint(FdKind::Socket);
    handle.attach(
        fd,
        Slow {
            delays: [ms(10)].into(),
        },
    );

    ring.add(&send(fd, b"abc", 1));
    ring.add(&opcode::AsyncCancel::new(1).build().user_data(2));
    // The peer has the bytes; the send can only finish as it was going to.
    assert_eq!(collect(&mut ring, 2), vec![(2, -libc::EALREADY), (1, 3)]);
    assert_eq!(handle.writes(fd), vec![b"abc".to_vec()]);
}

#[test]
fn an_echo_server_finishes_a_dialogue_whose_sends_drain_slowly() {
    for seed in seeds(8) {
        let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
        let socket = simulated_socket();
        let (peer, report) = Dialogue::new()
            .send(b"slow".to_vec())
            .expect_bytes(b"slow".to_vec())
            .send((0..40).collect::<Vec<u8>>())
            .expect_len(40)
            .close()
            .write_latency(ms(1), ms(4))
            .build();
        handle.attach(socket.fd(), peer);

        RuntimeInner::simulated(fake).block_on::<PeerRing, _>(echo(socket), None, |run| run());

        report.borrow().assert_finished();
        assert!(
            handle.clock().unwrap().elapsed() >= ms(1),
            "seed {seed}: no send was held",
        );
    }
}

// ------------------------------------------------------------ determinism

/// Everything a run of two actor peers lets a test observe.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Fingerprint {
    reads: Vec<Vec<Vec<u8>>>,
    writes: Vec<Vec<Vec<u8>>>,
    elapsed: Duration,
    finished: bool,
}

/// A ping-pong client and a slow dialogue, on two connections the echo
/// server takes through one accept.
fn two_peers(seed: u64) -> Fingerprint {
    let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
    let (ping, talk) = (mint(FdKind::Socket), mint(FdKind::Socket));
    let pongs = Rc::new(RefCell::new(PingPongReport::default()));
    handle.attach(ping, PingPong::new(messages(), pongs.clone()));
    let (peer, report) = Dialogue::new()
        .send(b"slow".to_vec())
        .expect_bytes(b"slow".to_vec())
        .wait(ms(3))
        .send((0..40).collect::<Vec<u8>>())
        .expect_len(40)
        .close()
        .write_latency(ms(0), ms(4))
        .build();
    handle.attach(talk, peer);

    let script = handle.clone();
    RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
        async move {
            let mut listener = MultiAccept::new(0).expect("bind");
            script.on_multishot_accept(
                listener.socket().fd(),
                [ping, talk].map(|fd| AcceptEvent::On(fd).after_between(ms(0), ms(5))),
            );
            let mut servers = Vec::new();
            for _ in 0..2 {
                let conn = listener.next().await.expect("a client").expect("accepted");
                servers.push(runtime_core::runtime::spawn(echo(conn)));
            }
            drop(listener);
            for server in servers {
                server.await;
            }
        },
        None,
        |run| run(),
    );

    Fingerprint {
        reads: [ping, talk].map(|fd| handle.reads(fd)).to_vec(),
        writes: [ping, talk].map(|fd| handle.writes(fd)).to_vec(),
        elapsed: handle.clock().unwrap().elapsed(),
        finished: report.borrow().finished() && pongs.borrow().wrong.is_none(),
    }
}

#[test]
fn actors_replay_the_same_run_for_the_same_seed() {
    let mut distinct = std::collections::HashSet::new();
    for seed in seeds(8) {
        let first = two_peers(seed);
        assert!(first.finished, "seed {seed}: a peer did not finish");
        assert_eq!(first, two_peers(seed), "seed {seed} replayed differently");
        distinct.insert(first);
    }
    if EXPLORING {
        assert!(distinct.len() > 1, "every seed drew the same run");
    }
}

// ---------------------------------------------------------------- faults

use runtime_core::deterministic_testing::{Fault, Faults};
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::time::Sleep;

/// How a robust server's side of a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    Eof,
    ReadFailed,
    SendFailed,
    Idle,
}

/// How long the robust server gives a quiet peer.
const IDLE: Duration = ms(50);

/// An echo server that ends a connection on any error, and on a peer silent
/// for `IDLE` — whether it is waiting to read or to finish a send.
async fn guarded_echo(socket: SocketWrapper) -> Ended {
    let mut recv = MultiRecv::<PeerRing>::new(socket);
    loop {
        match select(recv.next(), Sleep::during(IDLE)).await {
            SelectResult::First(Some(Ok(buffer))) => {
                match select(send_all(recv.socket(), buffer), Sleep::during(IDLE)).await {
                    SelectResult::First((Ok(()), _)) => {}
                    SelectResult::First((Err(_), _)) => return Ended::SendFailed,
                    SelectResult::Second(_) => return Ended::Idle,
                }
            }
            SelectResult::First(Some(Err(CompletionError::Other(code))))
                if code == -libc::ENOBUFS =>
            {
                Sleep::during(ms(1)).await;
            }
            SelectResult::First(Some(Err(_))) => return Ended::ReadFailed,
            SelectResult::First(None) => return Ended::Eof,
            SelectResult::Second(_) => return Ended::Idle,
        }
    }
}

/// Run `guarded_echo` against `peer` on one connection.
fn guarded(
    seed: u64,
    peer: Dialogue,
) -> (
    Ended,
    Rc<RefCell<runtime_core::deterministic_testing::DialogueReport>>,
    Vec<u8>,
    Duration,
) {
    let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
    let socket = simulated_socket();
    let fd = socket.fd();
    let (peer, report) = peer.build();
    handle.attach(fd, peer);
    let ended =
        RuntimeInner::simulated(fake)
            .block_on::<PeerRing, _>(guarded_echo(socket), None, |run| run());
    let elapsed = handle.clock().unwrap().elapsed();
    (ended, report, handle.reads(fd).concat(), elapsed)
}

#[test]
fn a_send_cut_short_reaches_the_server_as_a_prefix_then_a_reset() {
    let message: Vec<u8> = (0..100).collect();
    for seed in seeds(16) {
        let (ended, report, read, _) = guarded(
            seed,
            Dialogue::new()
                .send(b"hello".to_vec())
                .expect_bytes(b"hello".to_vec())
                .cut_send(message.clone())
                .expect_len(100),
        );
        let report = report.borrow();
        let Some(Fault::CutInSend { delivered, of }) = report.fault else {
            panic!("seed {seed}: no cut, {:?}", report.fault);
        };
        assert_eq!(of, 100);
        assert!(delivered < 100, "seed {seed}: the cut came after the end");
        // Nothing past the cut, nothing out of order; and a read only sees the
        // reset once everything sent before it is read.
        let sent = [b"hello".as_slice(), &message[..delivered]].concat();
        assert!(sent.starts_with(&read), "seed {seed}: read past the cut");
        match ended {
            Ended::ReadFailed => assert_eq!(read, sent, "seed {seed}: the reset overtook data"),
            Ended::SendFailed => {}
            other => panic!("seed {seed}: the cut went unnoticed, ended {other:?}"),
        }
        report.assert_ok();
    }
}

#[test]
#[should_panic(expected = "the runtime would never wake up")]
fn a_peer_that_vanishes_hangs_a_server_without_a_timeout() {
    let (fake, handle) = ring();
    let socket = simulated_socket();
    let (peer, _report) = Dialogue::new()
        .send(b"hello".to_vec())
        .expect_bytes(b"hello".to_vec())
        .vanish()
        .send(b"never".to_vec())
        .build();
    handle.attach(socket.fd(), peer);

    RuntimeInner::simulated(fake).block_on::<PeerRing, _>(echo(socket), None, |run| run());
}

#[test]
fn a_server_with_an_idle_timeout_drops_a_vanished_peer() {
    let (ended, report, _, elapsed) = guarded(
        0,
        Dialogue::new()
            .send(b"hello".to_vec())
            .expect_bytes(b"hello".to_vec())
            .vanish()
            .send(b"never".to_vec()),
    );
    assert_eq!(ended, Ended::Idle);
    assert_eq!(report.borrow().fault, Some(Fault::Vanished));
    report.borrow().assert_ok();
    assert!(elapsed >= IDLE);
}

#[test]
fn a_reset_at_a_set_time_interrupts_whatever_the_dialogue_was_doing() {
    let (ended, report, _, elapsed) = guarded(
        0,
        Dialogue::new()
            .send(b"a".to_vec())
            .expect_bytes(b"a".to_vec())
            .reset_after(ms(5))
            .wait(ms(20))
            .send(b"b".to_vec())
            .expect_bytes(b"b".to_vec())
            .close(),
    );
    assert_eq!(ended, Ended::ReadFailed);
    assert_eq!(report.borrow().fault, Some(Fault::Reset));
    assert_eq!(
        elapsed,
        ms(5),
        "the reset came at its time, not at the end of the wait"
    );
}

#[test]
fn a_failed_send_ends_the_connection() {
    let (ended, report, _, _) = guarded(
        0,
        Dialogue::new()
            .fail_write(libc::EPIPE)
            .send(b"hello".to_vec())
            .expect_bytes(b"hello".to_vec()),
    );
    assert_eq!(ended, Ended::SendFailed);
    assert_eq!(report.borrow().fault, Some(Fault::SendError(libc::EPIPE)));
    report.borrow().assert_ok();
}

#[test]
fn drawn_faults_strike_and_the_server_survives_every_one() {
    let mut kinds = std::collections::HashSet::new();
    let mut healthy = 0;
    for seed in seeds(64) {
        let mut peer = Dialogue::new().faults(Faults {
            send_error: 0.1,
            cut_in_send: 0.1,
            vanish: 0.05,
        });
        for round in 0..4u8 {
            let message: Vec<u8> = (0..20).map(|i| i + 20 * round).collect();
            peer = peer.send(message.clone()).expect_bytes(message);
        }
        let (ended, report, _, _) = guarded(seed, peer.close());
        let report = report.borrow();
        assert!(report.ok(), "seed {seed}: {report:?}");
        match report.fault {
            None => {
                assert!(report.finished(), "seed {seed}: healthy, yet {report:?}");
                assert_eq!(ended, Ended::Eof, "seed {seed}");
                healthy += 1;
            }
            Some(fault) => {
                assert_ne!(ended, Ended::Eof, "seed {seed}: {fault:?} went unnoticed");
                kinds.insert(std::mem::discriminant(&fault));
            }
        }
    }
    if EXPLORING {
        assert!(healthy > 0, "no seed was spared");
    }
    if EXPLORING {
        assert_eq!(kinds.len(), 3, "not every kind of fault was drawn");
    }
}

#[test]
fn a_reader_gets_every_byte_sent_before_a_cut_and_then_the_reset() {
    let message: Vec<u8> = (0..100).collect();
    let mut cuts = std::collections::BTreeSet::new();
    for seed in seeds(16) {
        let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
        let socket = simulated_socket();
        let (peer, report) = Dialogue::new().cut_send(message.clone()).build();
        handle.attach(socket.fd(), peer);

        // Only reads, so it is a read that meets the reset.
        let (got, end) = RuntimeInner::simulated(fake).block_on::<PeerRing, _>(
            async move {
                let mut recv = MultiRecv::<PeerRing>::new(socket);
                let mut got = Vec::new();
                loop {
                    match recv.next().await {
                        Some(Ok(buffer)) => got.extend_from_slice(buffer.as_ref()),
                        Some(Err(error)) => break (got, Some(error)),
                        None => break (got, None),
                    }
                }
            },
            None,
            |run| run(),
        );

        let Some(Fault::CutInSend { delivered, .. }) = report.borrow().fault else {
            panic!("seed {seed}: no cut");
        };
        assert_eq!(
            got,
            message[..delivered],
            "seed {seed}: the reset overtook data"
        );
        assert!(
            matches!(end, Some(CompletionError::ConnectionReset)),
            "seed {seed}: ended on {end:?}",
        );
        cuts.insert(delivered);
    }
    if EXPLORING {
        assert!(cuts.len() > 1, "every seed cut at the same byte");
    }
}

// ---------------------------------------------------------------- journal

#[test]
fn the_journal_is_off_unless_asked_and_then_records_what_the_ring_saw() {
    let (_ring, handle) = ring();
    assert!(handle.journal().is_none(), "on without being asked");

    let (mut ring, handle) = FakeRing::builder().virtual_time().journal().build();
    let fd = mint(FdKind::Socket);
    handle.attach(
        fd,
        Canned {
            reads: [ReadReply::Data(b"hi".to_vec())].into(),
            ..Canned::default()
        },
    );
    let mut buf = [0u8; 8];
    ring.add(&recv(fd, &mut buf, 1));
    collect(&mut ring, 1);

    let journal = handle.journal().expect("journal on");
    let lines: Vec<String> = journal
        .iter()
        .map(|entry| entry.event.to_string())
        .collect();
    assert_eq!(
        lines,
        [
            "submit   RECV ud=0x1",
            "actor    opened",
            "actor    read(8) -> Data(2)",
            "complete ud=0x1 res=2",
        ],
    );
    assert!(journal.iter().all(|entry| entry.fd() == Some(fd)));
}

#[test]
fn a_dialogue_notes_its_steps_and_faults_in_the_journal() {
    let (fake, handle) = FakeRing::builder().virtual_time().journal().build();
    let socket = simulated_socket();
    let fd = socket.fd();
    let (peer, _report) = Dialogue::new()
        .send(b"hi".to_vec())
        .expect_bytes(b"hi".to_vec())
        .reset()
        .build();
    handle.attach(fd, peer);

    RuntimeInner::simulated(fake).block_on::<PeerRing, _>(guarded_echo(socket), None, |run| run());

    let text = handle.journal_text(&[fd], None).expect("journal on");
    for note in [
        "step 1 done: send \"hi\"",
        "step 2 done: expect \"hi\"",
        "fault: Reset",
    ] {
        assert!(text.contains(note), "no {note:?} in:\n{text}");
    }
}
