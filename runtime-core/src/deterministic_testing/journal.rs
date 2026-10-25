//! What a simulated run did, recorded only when asked for.
//!
//! Off by default: a run is a function of its seed, so the way to see one is
//! to replay it with [`FakeRingBuilder::journal`] on. Recording draws nothing
//! and orders nothing, so the replay is the same run, now visible.
//!
//! Everything here is seen from the ring — submissions, completions as the
//! driver reads them, what actors were asked and answered, timers, faults —
//! never from the runtime's own tracing, so a journal still holds when the
//! runtime under test is what broke. No addresses are kept: two runs of one
//! seed give equal journals.
//!
//! [`FakeRingBuilder::journal`]: super::fake_ring::FakeRingBuilder::journal

use std::collections::HashMap;
use std::fmt;
use std::os::fd::RawFd;
use std::time::Duration;

use io_uring::opcode;

use super::fake_ring::Peer;

/// One thing the ring saw happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The program submitted an op.
    Submit {
        user_data: u64,
        opcode: u8,
        fd: RawFd,
        multishot: bool,
    },
    /// The driver read a completion.
    Complete {
        user_data: u64,
        fd: Option<RawFd>,
        result: i32,
        more: bool,
    },
    /// A cancel of `target` resolved with `result`.
    Cancel { target: u64, result: i32 },
    /// An actor was asked what a read of at most `max` bytes gets.
    Read {
        fd: RawFd,
        max: usize,
        answer: Answer,
    },
    /// An actor was offered a write of `offered` bytes.
    Write {
        fd: RawFd,
        offered: usize,
        answer: Answer,
    },
    /// An actor's connection opened.
    Opened { fd: RawFd },
    /// The program closed the descriptor.
    Closed { fd: RawFd },
    /// An actor's timer fired.
    Timer { fd: RawFd, tag: u64 },
    /// The kernel stumbled on a read: an `EINTR`, or a multishot it ended.
    Kernel { fd: RawFd, fault: &'static str },
    /// A scripted connection closed or reset.
    Peer { fd: RawFd, peer: Peer },
    /// An actor's own words.
    Note { fd: RawFd, text: String },
}

/// What an actor answered, by size rather than content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Data(usize),
    Eof,
    Error(i32),
    Pending,
    Accepted(usize),
    AcceptedAfter(usize, Duration),
    /// Data no buffer could take: the ring was dry.
    Dry(usize),
}

/// An event, and when in virtual time since the ring was built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub at: Duration,
    pub event: Event,
}

impl Entry {
    /// The descriptor the event is about, if any.
    pub fn fd(&self) -> Option<RawFd> {
        let fd = match &self.event {
            Event::Submit { fd, .. }
            | Event::Read { fd, .. }
            | Event::Write { fd, .. }
            | Event::Opened { fd }
            | Event::Closed { fd }
            | Event::Timer { fd, .. }
            | Event::Kernel { fd, .. }
            | Event::Peer { fd, .. }
            | Event::Note { fd, .. } => Some(*fd),
            Event::Complete { fd, .. } => *fd,
            Event::Cancel { .. } => None,
        };
        // A cancel names no descriptor: -1.
        fd.filter(|fd| *fd >= 0)
    }
}

/// The record of one run.
#[derive(Default)]
pub struct Journal {
    entries: Vec<Entry>,
    /// The descriptor each submitted op names, to tag its completions.
    ops: HashMap<u64, RawFd>,
}

impl Journal {
    pub(crate) fn record(&mut self, at: Duration, mut event: Event) {
        match &mut event {
            Event::Submit { user_data, fd, .. } => {
                self.ops.insert(*user_data, *fd);
            }
            // An op's last completion retires its user_data, which the runtime
            // may hand an unrelated op — or its own wakeups — next.
            Event::Complete {
                user_data,
                fd,
                more,
                ..
            } => {
                *fd = if *more {
                    self.ops.get(user_data).copied()
                } else {
                    self.ops.remove(user_data)
                };
            }
            _ => {}
        }
        self.entries.push(Entry { at, event });
    }

    /// Whether `user_data` names an op the journal saw submitted.
    pub(crate) fn knows(&self, user_data: u64) -> bool {
        self.ops.contains_key(&user_data)
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

/// The entries about any of `fds` — all of them if `fds` is empty — one per
/// line, and only the last `last` if given.
pub fn render(entries: &[Entry], fds: &[RawFd], last: Option<usize>) -> String {
    let picked: Vec<&Entry> = entries
        .iter()
        .filter(|e| fds.is_empty() || e.fd().is_some_and(|fd| fds.contains(&fd)))
        .collect();
    let skip = last.map_or(0, |n| picked.len().saturating_sub(n));
    let mut text = String::new();
    if skip > 0 {
        text.push_str(&format!("… {skip} earlier event(s)\n"));
    }
    for entry in &picked[skip..] {
        text.push_str(&entry.to_string());
        text.push('\n');
    }
    text
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millis = self.at.as_secs_f64() * 1e3;
        let fd = self
            .fd()
            .map_or("      -".to_string(), |fd| format!("{fd:>7}"));
        write!(f, "{millis:>11.3}ms fd {fd}  {}", self.event)
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Submit {
                user_data,
                opcode,
                multishot,
                ..
            } => {
                let multishot = if *multishot { " multishot" } else { "" };
                write!(f, "submit   {}{multishot} ud={user_data:#x}", name(*opcode))
            }
            Event::Complete {
                user_data,
                result,
                more,
                ..
            } => {
                let more = if *more { " F_MORE" } else { "" };
                write!(
                    f,
                    "complete ud={user_data:#x} res={}{more}",
                    outcome(*result)
                )
            }
            Event::Cancel { target, result } => {
                write!(f, "cancel   ud={target:#x} -> {}", outcome(*result))
            }
            Event::Read { max, answer, .. } => write!(f, "actor    read({max}) -> {answer}"),
            Event::Write {
                offered, answer, ..
            } => write!(f, "actor    write({offered}) -> {answer}"),
            Event::Opened { .. } => write!(f, "actor    opened"),
            Event::Closed { .. } => write!(f, "closed   by the program"),
            Event::Timer { tag, .. } => write!(f, "actor    timer {tag} fired"),
            Event::Kernel { fault, .. } => write!(f, "kernel   {fault}"),
            Event::Peer { peer, .. } => write!(f, "peer     {peer:?}"),
            Event::Note { text, .. } => write!(f, "note     {text}"),
        }
    }
}

impl fmt::Display for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Answer::Data(n) => write!(f, "Data({n})"),
            Answer::Eof => write!(f, "Eof"),
            Answer::Error(errno) => write!(f, "Error({})", &outcome(-errno.abs())[1..]),
            Answer::Pending => write!(f, "Pending"),
            Answer::Accepted(n) => write!(f, "Accepted({n})"),
            Answer::AcceptedAfter(n, after) => write!(f, "Accepted({n}) after {after:?}"),
            Answer::Dry(n) => write!(f, "Data({n}), but the ring was dry"),
        }
    }
}

/// A result as a count or an errno.
fn outcome(result: i32) -> String {
    if result >= 0 {
        result.to_string()
    } else {
        let errno = -result;
        let name = match errno {
            libc::EINTR => "EINTR",
            libc::EAGAIN => "EAGAIN",
            libc::ENOBUFS => "ENOBUFS",
            libc::ECANCELED => "ECANCELED",
            libc::ENOENT => "ENOENT",
            libc::EALREADY => "EALREADY",
            libc::ECONNRESET => "ECONNRESET",
            libc::EPIPE => "EPIPE",
            libc::ETIMEDOUT => "ETIMEDOUT",
            libc::ETIME => "ETIME",
            _ => return format!("-{errno}"),
        };
        format!("-{name}")
    }
}

fn name(code: u8) -> String {
    let known = [
        (opcode::Recv::CODE, "RECV"),
        (opcode::RecvMsg::CODE, "RECVMSG"),
        (opcode::Read::CODE, "READ"),
        (opcode::Send::CODE, "SEND"),
        (opcode::SendZc::CODE, "SEND_ZC"),
        (opcode::SendMsg::CODE, "SENDMSG"),
        (opcode::SendMsgZc::CODE, "SENDMSG_ZC"),
        (opcode::Write::CODE, "WRITE"),
        (opcode::Writev::CODE, "WRITEV"),
        (opcode::WriteFixed::CODE, "WRITE_FIXED"),
        (opcode::Accept::CODE, "ACCEPT"),
        (opcode::Connect::CODE, "CONNECT"),
        (opcode::Close::CODE, "CLOSE"),
        (opcode::OpenAt::CODE, "OPENAT"),
        (opcode::AsyncCancel::CODE, "ASYNC_CANCEL"),
        (opcode::Timeout::CODE, "TIMEOUT"),
        (opcode::MsgRingData::CODE, "MSG_RING"),
    ];
    known
        .iter()
        .find(|(c, _)| *c == code)
        .map_or_else(|| format!("op {code}"), |(_, n)| n.to_string())
}
