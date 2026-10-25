//! A peer that plays a dialogue: send this, expect that, wait, close — and,
//! because the interesting runs are the ones that go wrong, misbehave.
//!
//! [`Dialogue`] is an [`Actor`] built from steps. Each `expect` step is a
//! closure handed everything the program has written and the dialogue has not
//! consumed yet, called again each time more arrives — so it sees a reply that
//! TCP split across writes, or glued to the next one, without reassembling it
//! itself. It says whether it needs more, how many bytes it consumed, or what
//! was wrong. On a wrong reply the peer resets the connection and the
//! [`DialogueReport`] says why, so the run still ends cleanly.
//!
//! Faults are steps too — a send cut short by a reset, a peer that vanishes, a
//! reset at a set time, a send of the program's that fails — or drawn from the
//! seed with [`Dialogue::faults`]. The report names the one that struck, so a
//! test can hold a healthy dialogue to finishing and a faulted one only to
//! ending cleanly.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use super::actor::{Actor, ActorCx, ReadReply, WriteReply};

/// What an `expect` step makes of the bytes received so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// Not enough yet: ask again once more arrives.
    More,
    /// The step is satisfied by the first `n` bytes, which it consumes; the
    /// rest is left for the steps after it.
    Pass(usize),
    /// The bytes are wrong, for this reason.
    Fail(String),
}

/// A fault that struck a dialogue, ending the connection or leaving it hanging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The peer sent RST.
    Reset,
    /// The peer sent `delivered` of a message's `of` bytes, then RST.
    CutInSend { delivered: usize, of: usize },
    /// One of the program's sends failed with this errno, and the connection
    /// with it.
    SendError(i32),
    /// The peer went silent: no FIN, no RST. Reads wait for ever, and sends
    /// fill a buffer nobody drains, then wait too.
    Vanished,
}

/// Odds, per step or per send, of a fault a [`Dialogue`] draws from the seed.
/// At most one strikes: the first ends the dialogue.
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    /// Each send of the program's fails, with `EPIPE` or `ECONNRESET`.
    pub send_error: f64,
    /// Each `send` step is cut at a drawn byte and followed by RST.
    pub cut_in_send: f64,
    /// Before each step, the peer vanishes.
    pub vanish: f64,
}

type Expect = Box<dyn FnMut(&[u8]) -> Check>;

enum Step {
    Send(Vec<u8>),
    CutSend(Vec<u8>),
    Expect { what: String, check: Expect },
    Wait(Duration),
    Close,
    Reset,
    ResetAfter(Duration),
    FailWrite(i32),
    Vanish,
}

/// How far a [`Dialogue`] got.
#[derive(Debug, Default, Clone)]
pub struct DialogueReport {
    /// Steps played to the end.
    pub done: usize,
    /// Steps in the dialogue.
    pub total: usize,
    /// The first wrong reply, if any.
    pub failure: Option<String>,
    /// Bytes the program wrote that no step consumed.
    pub unconsumed: usize,
    /// The fault that struck, scripted or drawn.
    pub fault: Option<Fault>,
}

impl DialogueReport {
    /// Every step played, nothing wrong, nothing left over.
    pub fn finished(&self) -> bool {
        self.done == self.total && self.failure.is_none() && self.unconsumed == 0
    }

    /// No wrong reply, and the dialogue either finished or was cut short by a
    /// fault.
    pub fn ok(&self) -> bool {
        self.failure.is_none() && (self.finished() || self.fault.is_some())
    }

    /// Panic with what went wrong unless [`finished`](Self::finished).
    pub fn assert_finished(&self) {
        assert!(self.finished(), "{}", self.why());
    }

    /// Panic with what went wrong unless [`ok`](Self::ok).
    pub fn assert_ok(&self) {
        assert!(self.ok(), "{}", self.why());
    }

    fn why(&self) -> String {
        format!(
            "dialogue played {} of {} steps{}: {}",
            self.done,
            self.total,
            self.fault
                .map_or(String::new(), |f| format!(" before {f:?}")),
            match (&self.failure, self.unconsumed) {
                (Some(failure), _) => failure.clone(),
                (None, 0) => "the program stopped answering".into(),
                (None, n) => format!("{n} byte(s) written that no step expected"),
            },
        )
    }
}

/// A peer that plays its steps in order. Build with [`Dialogue::new`], then
/// [`build`](Dialogue::build) for the actor and its report.
pub struct Dialogue {
    steps: VecDeque<Step>,
    received: Vec<u8>,
    outbox: VecDeque<u8>,
    waiting: bool,
    closed: bool,
    reset: bool,
    reset_reported: bool,
    /// Room left in the send buffer of a peer that vanished.
    vanished: Option<usize>,
    fail_next_write: Option<i32>,
    faults: Faults,
    /// How long the program waits to learn each of its sends went through.
    write_latency: Option<(Duration, Duration)>,
    report: Rc<RefCell<DialogueReport>>,
}

const WAIT: u64 = 0;
const RESET: u64 = 1;

/// The send buffer a vanished peer leaves to fill, at most.
const VANISHED_ROOM: u64 = 4096;

impl Dialogue {
    pub fn new() -> Self {
        Self {
            steps: VecDeque::new(),
            received: Vec::new(),
            outbox: VecDeque::new(),
            waiting: false,
            closed: false,
            reset: false,
            reset_reported: false,
            vanished: None,
            fail_next_write: None,
            faults: Faults::default(),
            write_latency: None,
            report: Rc::default(),
        }
    }

    fn step(mut self, step: Step) -> Self {
        self.steps.push_back(step);
        self
    }

    /// Make these bytes readable by the program.
    pub fn send(self, bytes: impl Into<Vec<u8>>) -> Self {
        self.step(Step::Send(bytes.into()))
    }

    /// Send these bytes up to a drawn byte, then RST: what got out is still
    /// read before the reset is.
    pub fn cut_send(self, bytes: impl Into<Vec<u8>>) -> Self {
        self.step(Step::CutSend(bytes.into()))
    }

    /// Wait until `check` passes on what the program wrote. `what` names the
    /// step in a failure.
    pub fn expect(
        self,
        what: impl Into<String>,
        check: impl FnMut(&[u8]) -> Check + 'static,
    ) -> Self {
        self.step(Step::Expect {
            what: what.into(),
            check: Box::new(check),
        })
    }

    /// Expect exactly these bytes next.
    pub fn expect_bytes(self, want: impl Into<Vec<u8>>) -> Self {
        let want = want.into();
        let what = format!("{:?}", String::from_utf8_lossy(&want));
        self.expect(what, move |got| {
            let n = got.len().min(want.len());
            match got[..n].iter().zip(&want).position(|(a, b)| a != b) {
                Some(at) => Check::Fail(format!(
                    "byte {at} is {:#04x}, expected {:#04x}",
                    got[at], want[at]
                )),
                None if n < want.len() => Check::More,
                None => Check::Pass(want.len()),
            }
        })
    }

    /// Expect any `len` bytes next.
    pub fn expect_len(self, len: usize) -> Self {
        self.expect(format!("{len} bytes"), move |got| {
            if got.len() < len {
                Check::More
            } else {
                Check::Pass(len)
            }
        })
    }

    /// Expect anything up to and including `delimiter`.
    pub fn expect_until(self, delimiter: impl Into<Vec<u8>>) -> Self {
        let delimiter = delimiter.into();
        let what = format!("up to {:?}", String::from_utf8_lossy(&delimiter));
        self.expect(what, move |got| {
            match got.windows(delimiter.len()).position(|w| w == delimiter) {
                Some(at) => Check::Pass(at + delimiter.len()),
                None => Check::More,
            }
        })
    }

    /// Pause before the next step.
    pub fn wait(self, delay: Duration) -> Self {
        self.step(Step::Wait(delay))
    }

    /// Send FIN once everything sent so far is read: reads see EOF after it.
    pub fn close(self) -> Self {
        self.step(Step::Close)
    }

    /// Send RST: the connection is gone.
    pub fn reset(self) -> Self {
        self.step(Step::Reset)
    }

    /// Send RST `delay` from here, whatever step the dialogue is on by then.
    pub fn reset_after(self, delay: Duration) -> Self {
        self.step(Step::ResetAfter(delay))
    }

    /// Fail the program's next send with `errno`, which ends the connection.
    pub fn fail_write(self, errno: i32) -> Self {
        self.step(Step::FailWrite(errno))
    }

    /// Go silent: no FIN, no RST, nothing read or answered any more.
    pub fn vanish(self) -> Self {
        self.step(Step::Vanish)
    }

    /// Draw faults from the seed at these odds.
    pub fn faults(mut self, faults: Faults) -> Self {
        self.faults = faults;
        self
    }

    /// Hold each of the program's sends for a delay drawn from `min..=max`
    /// before it completes, as a send buffer slow to drain would.
    pub fn write_latency(mut self, min: Duration, max: Duration) -> Self {
        assert!(
            min <= max,
            "write_latency({min:?}, {max:?}) is an empty range"
        );
        self.write_latency = Some((min, max));
        self
    }

    /// The actor, and the report it fills in as it plays.
    pub fn build(self) -> (Self, Rc<RefCell<DialogueReport>>) {
        self.report.borrow_mut().total = self.steps.len();
        let report = self.report.clone();
        (self, report)
    }

    fn strike(&mut self, fault: Fault) {
        let mut report = self.report.borrow_mut();
        report.fault.get_or_insert(fault);
    }

    fn over(&self) -> bool {
        self.reset || self.vanished.is_some()
    }

    /// Cut `bytes` at a drawn byte and reset after what got out.
    fn cut(&mut self, bytes: &[u8], cx: &mut ActorCx) {
        let delivered = cx.between(0, bytes.len().saturating_sub(1) as u64) as usize;
        self.outbox.extend(&bytes[..delivered]);
        self.reset = true;
        self.strike(Fault::CutInSend {
            delivered,
            of: bytes.len(),
        });
    }

    fn vanish_now(&mut self, cx: &mut ActorCx) {
        self.vanished = Some(cx.between(0, VANISHED_ROOM) as usize);
        self.strike(Fault::Vanished);
    }

    /// Play every step that can be played now.
    fn play(&mut self, cx: &mut ActorCx) {
        while !self.waiting && !self.over() {
            if self.steps.is_empty() {
                break;
            }
            if chance(cx, self.faults.vanish) {
                self.vanish_now(cx);
                break;
            }
            let Some(step) = self.steps.front_mut() else {
                break;
            };
            match step {
                Step::Send(bytes) => {
                    let bytes = std::mem::take(bytes);
                    if chance(cx, self.faults.cut_in_send) {
                        self.cut(&bytes, cx);
                        break;
                    }
                    self.outbox.extend(bytes);
                }
                Step::CutSend(bytes) => {
                    let bytes = std::mem::take(bytes);
                    self.cut(&bytes, cx);
                    break;
                }
                Step::Expect { what, check } => match check(&self.received) {
                    Check::More => break,
                    Check::Pass(n) => {
                        assert!(
                            n <= self.received.len(),
                            "the check for {what} consumed {n} of {} bytes received",
                            self.received.len(),
                        );
                        self.received.drain(..n);
                    }
                    Check::Fail(why) => {
                        let step = self.report.borrow().done + 1;
                        self.report.borrow_mut().failure =
                            Some(format!("step {step}, {what}: {why}"));
                        self.reset = true;
                        break;
                    }
                },
                Step::Wait(delay) => {
                    cx.timer(*delay, WAIT);
                    self.waiting = true;
                }
                Step::Close => self.closed = true,
                Step::Reset => {
                    self.reset = true;
                    self.strike(Fault::Reset);
                }
                Step::ResetAfter(delay) => cx.timer(*delay, RESET),
                Step::FailWrite(errno) => self.fail_next_write = Some(*errno),
                Step::Vanish => {
                    self.steps.pop_front();
                    self.report.borrow_mut().done += 1;
                    self.vanish_now(cx);
                    break;
                }
            }
            self.steps.pop_front();
            self.report.borrow_mut().done += 1;
        }
        let unconsumed = if self.steps.is_empty() && !self.over() {
            self.received.len()
        } else {
            0
        };
        self.report.borrow_mut().unconsumed = unconsumed;
    }

    /// The error a reset reports to the first op that sees it.
    fn reset_error(&mut self) -> Option<i32> {
        (!std::mem::replace(&mut self.reset_reported, true)).then_some(libc::ECONNRESET)
    }

    /// Fail this send and the connection with it.
    fn fail(&mut self, errno: i32) -> WriteReply {
        self.reset = true;
        self.reset_reported = true;
        self.strike(Fault::SendError(errno));
        WriteReply::Error(errno)
    }
}

/// True with probability `odds`, drawing nothing when it is zero.
fn chance(cx: &mut ActorCx, odds: f64) -> bool {
    let unit = (cx.random() >> 11) as f64 / (1u64 << 53) as f64;
    odds > 0.0 && unit < odds
}

impl Default for Dialogue {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for Dialogue {
    fn on_open(&mut self, cx: &mut ActorCx) {
        self.play(cx);
    }

    fn read(&mut self, max: usize, cx: &mut ActorCx) -> ReadReply {
        if self.vanished.is_some() {
            return ReadReply::Pending;
        }
        // What got out before a reset is still read before it, as on Linux.
        if !self.outbox.is_empty() {
            let n = (cx.between(1, max as u64) as usize).min(self.outbox.len());
            return ReadReply::Data(self.outbox.drain(..n).collect());
        }
        if self.reset {
            return self.reset_error().map_or(ReadReply::Eof, ReadReply::Error);
        }
        if self.closed {
            ReadReply::Eof
        } else {
            ReadReply::Pending
        }
    }

    fn write(&mut self, bytes: &[u8], cx: &mut ActorCx) -> WriteReply {
        if self.reset {
            return WriteReply::Error(self.reset_error().unwrap_or(libc::EPIPE));
        }
        if let Some(room) = &mut self.vanished {
            // Into a buffer nobody drains, until it is full.
            let taken = bytes.len().min(*room);
            *room -= taken;
            return if taken == 0 {
                WriteReply::Pending
            } else {
                WriteReply::Accepted(taken)
            };
        }
        if let Some(errno) = self.fail_next_write.take() {
            return self.fail(errno);
        }
        if chance(cx, self.faults.send_error) {
            let errno = if cx.between(0, 1) == 0 {
                libc::EPIPE
            } else {
                libc::ECONNRESET
            };
            return self.fail(errno);
        }
        self.received.extend_from_slice(bytes);
        self.play(cx);
        match self.write_latency {
            None => WriteReply::Accepted(bytes.len()),
            Some((min, max)) => {
                let span = (max - min).as_nanos() as u64;
                let after = min + Duration::from_nanos(cx.between(0, span));
                WriteReply::AcceptedAfter(bytes.len(), after)
            }
        }
    }

    fn on_timer(&mut self, tag: u64, cx: &mut ActorCx) {
        match tag {
            RESET if !self.over() => {
                self.reset = true;
                self.strike(Fault::Reset);
            }
            RESET => {}
            _ => {
                self.waiting = false;
                self.play(cx);
            }
        }
    }
}
