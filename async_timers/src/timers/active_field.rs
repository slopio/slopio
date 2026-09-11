//! The bit field tracking which armed timers are still active.

use std::collections::VecDeque;

/// Index of the first set bit, or None if every word is zero.
///
/// Bits are numbered from the low end of `words[0]`, so bit `b` is
/// `words[b >> 6] & (1 << (b & 63))`.
fn find_one(words: &[u64]) -> Option<usize> {
    let widx = words.iter().position(|word| *word != 0)?;
    Some(widx * 64 + words[widx].trailing_zeros() as usize)
}

/// `word` with everything below `from` cleared. Past the word, nothing is left.
fn keep_from(word: u64, from: usize) -> u64 {
    word & u64::MAX.checked_shl(from as u32).unwrap_or(0)
}

/// `word` with everything from `end` up cleared. At or past the word, all of it
/// is left.
fn keep_until(word: u64, end: usize) -> u64 {
    word & 1u64.checked_shl(end as u32).map_or(u64::MAX, |top| top - 1)
}

/// The set bits of `word`, lowest first.
fn ones_of(mut word: u64) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        (word != 0).then(|| {
            let bit = word.trailing_zeros() as usize;
            word &= word - 1;
            bit
        })
    })
}

/// Index of the first set bit at or after `from`, or None if there is none.
///
/// `from` is inclusive, and past the word it keeps nothing.
fn find_one_from(word: u64, from: usize) -> Option<usize> {
    let kept = keep_from(word, from);
    (kept != 0).then(|| kept.trailing_zeros() as usize)
}

/// Index of the first set bit below `end`, or None if there is none.
///
/// `end` is exclusive, and at or past the word it keeps everything.
fn find_one_until(word: u64, end: usize) -> Option<usize> {
    let kept = keep_until(word, end);
    (kept != 0).then(|| kept.trailing_zeros() as usize)
}

/// Index of the first set bit in `words`, ignoring the `from` low bits of the
/// first word and everything from `until` up in the last one.
///
/// Both cuts are offsets inside their own word, so `(0, 64)` searches the whole
/// slice. A one-word slice takes both at once. Answers in bits from the low end
/// of `words[0]`.
fn find_one_in(words: &[u64], from: usize, until: usize) -> Option<usize> {
    let last = words.len().checked_sub(1)?;
    if last == 0 {
        return find_one_from(words[0], from).filter(|bit| *bit < until);
    }
    find_one_from(words[0], from)
        .or_else(|| find_one(&words[1..last]).map(|bit| 64 + bit))
        .or_else(|| find_one_until(words[last], until).map(|bit| last * 64 + bit))
}

/// One bit per entry, set while the entry is still active.
///
/// The window is bounded by two sub-word offsets rather than by a count:
///
/// ```text
///   words:  [ ........ ][ ........ ] ... [ ........ ]
///            ^head_bit                        trail_bit^
///                    <----- the entries ----->
/// ```
///
/// `head_bit` is the dead bits before the first entry, inside `words[0]`;
/// `trail_bit` the unused ones after the last, inside the last word. Both stay
/// under 64 — a word fully consumed at the front is released, and one filled at
/// the back opens the next. So the window slides by moving the offsets, never
/// by shifting the bitmap, and `pop_n` leaves the bits it passes behind for the
/// readers to mask off rather than paying to erase them.
pub(super) struct ActiveField {
    words: VecDeque<u64>,
    head_bit: usize,
    trail_bit: usize,
}

impl ActiveField {
    pub(super) fn new() -> Self {
        Self {
            words: VecDeque::new(),
            head_bit: 0,
            trail_bit: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.words.len() * 64 - self.head_bit - self.trail_bit
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Writes its own bit rather than trusting the word to be zero, so a slot
    // can never inherit whatever stood there before.
    pub(super) fn push(&mut self) {
        if self.trail_bit == 0 {
            self.words.push_back(0);
            self.trail_bit = 64;
        }
        let last = self.words.back_mut().expect("trail_bit > 0 leaves a word");
        *last |= 1 << (64 - self.trail_bit);
        self.trail_bit -= 1;
    }

    pub(super) fn cancel(&mut self, i: usize) {
        assert!(i < self.len(), "index {i} past the end of {}", self.len());
        let p = self.head_bit + i;
        self.words[p >> 6] &= !(1 << (p & 63));
    }

    pub(super) fn is_active(&self, i: usize) -> bool {
        assert!(i < self.len(), "index {i} past the end of {}", self.len());
        let p = self.head_bit + i;
        self.words[p >> 6] & (1 << (p & 63)) != 0
    }

    pub(super) fn pop_n(&mut self, n: usize) {
        let n = n.min(self.len());
        self.head_bit += n;
        while self.head_bit >= 64 {
            self.words.pop_front();
            self.head_bit -= 64;
        }
        // Emptying the last word drains it to the brim, so dropping it leaves
        // both offsets at zero and the field ready to open a fresh one.
        debug_assert!(!self.words.is_empty() || self.trail_bit == 0);
    }

    // Indices of the entries still active among the first `n`.
    pub(super) fn iter(&self, n: usize) -> impl Iterator<Item = usize> + '_ {
        let n = n.min(self.len());
        let start = self.head_bit;
        let end = start + n;
        let touched = if n == 0 { 0 } else { ((end - 1) >> 6) + 1 };
        let (front, back) = self.words.as_slices();
        let split = front.len();

        (0..touched)
            .flat_map(move |widx| {
                let mut word = if widx < split {
                    front[widx]
                } else {
                    back[widx - split]
                };
                if widx == 0 {
                    word = keep_from(word, start);
                }
                if widx == touched - 1 {
                    word = keep_until(word, end - widx * 64);
                }
                ones_of(word).map(move |bit| widx * 64 + bit)
            })
            .map(move |bit| bit - start)
    }

    // First entry still active, or None once the whole window is cancelled.
    pub(super) fn first(&self) -> Option<usize> {
        let (front, back) = self.words.as_slices();
        // `trail_bit` cuts the last word the deque holds, whichever run it is in.
        let end = 64 - self.trail_bit;
        let bit = if back.is_empty() {
            find_one_in(front, self.head_bit, end)
        } else {
            find_one_in(front, self.head_bit, 64)
                .or_else(|| find_one_in(back, 0, end).map(|bit| front.len() * 64 + bit))
        }?;
        Some(bit - self.head_bit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_one_on_nothing() {
        assert_eq!(find_one(&[]), None);
        assert_eq!(find_one(&[0, 0, 0]), None);
    }

    #[test]
    fn find_one_takes_the_lowest_bit_of_the_first_word() {
        assert_eq!(find_one(&[0b1001]), Some(0));
        assert_eq!(find_one(&[0b1000]), Some(3));
        assert_eq!(find_one(&[1 << 63]), Some(63));
        assert_eq!(find_one(&[u64::MAX, u64::MAX]), Some(0));
    }

    #[test]
    fn find_one_skips_empty_words() {
        assert_eq!(find_one(&[0, u64::MAX]), Some(64));
        assert_eq!(find_one(&[0, 0, 0b100]), Some(130));
        assert_eq!(find_one(&[0, 0, 0, 1 << 63]), Some(255));
    }

    #[test]
    fn find_one_from_is_inclusive() {
        let word = 0b1010_1010u64;
        assert_eq!(find_one_from(word, 0), Some(1));
        assert_eq!(find_one_from(word, 1), Some(1), "`from` is inclusive");
        assert_eq!(find_one_from(word, 2), Some(3));
        assert_eq!(find_one_from(word, 4), Some(5));
        assert_eq!(find_one_from(word, 8), None);
    }

    #[test]
    fn find_one_from_at_the_edges() {
        assert_eq!(find_one_from(0, 0), None);
        assert_eq!(find_one_from(1 << 63, 63), Some(63));
        assert_eq!(find_one_from(u64::MAX, 63), Some(63));
        assert_eq!(find_one_from(u64::MAX, 64), None, "past the word");
        assert_eq!(find_one_from(u64::MAX, 999), None);
    }

    #[test]
    fn find_one_until_is_exclusive() {
        let word = 0b1010_1010u64;
        assert_eq!(find_one_until(word, 8), Some(1));
        assert_eq!(find_one_until(word, 2), Some(1), "bit 1 is below 2");
        assert_eq!(find_one_until(word, 1), None, "`end` is exclusive");
        assert_eq!(find_one_until(word, 0), None);
    }

    #[test]
    fn find_one_until_at_the_edges() {
        assert_eq!(find_one_until(0, 64), None);
        assert_eq!(find_one_until(1 << 63, 63), None);
        assert_eq!(find_one_until(1 << 63, 64), Some(63));
        assert_eq!(find_one_until(1 << 63, 999), Some(63), "past the word");
        assert_eq!(find_one_until(u64::MAX, 64), Some(0));
    }

    #[test]
    fn the_two_bounds_agree_with_a_bit_by_bit_search() {
        let mut rng = 0x2f6a_c1d4_58b3_9e07u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        for case in 0..2_000 {
            let word = match next() % 4 {
                0 => 0,
                1 => 1 << (next() % 64),
                2 => u64::MAX,
                _ => next(),
            };
            let set = |b: usize| word & (1 << b) != 0;

            for bound in 0..=70 {
                assert_eq!(
                    find_one_from(word, bound),
                    (bound..64).find(|b| set(*b)),
                    "case {case}, from {bound}",
                );
                assert_eq!(
                    find_one_until(word, bound),
                    (0..bound.min(64)).find(|b| set(*b)),
                    "case {case}, until {bound}",
                );
            }
        }
    }

    #[test]
    fn find_one_agrees_with_a_bit_by_bit_search() {
        let mut rng = 0x51ed_270b_9f1e_2a3du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        for case in 0..2_000 {
            // Sparse words as well as dense ones, so the scan has to walk
            // several zero words before it lands.
            let words: Vec<u64> = (0..6)
                .map(|_| match next() % 4 {
                    0 => 0,
                    1 => 1 << (next() % 64),
                    2 => u64::MAX,
                    _ => next(),
                })
                .collect();

            let expected = (0..words.len() * 64).find(|b| words[b >> 6] & (1 << (b & 63)) != 0);
            assert_eq!(find_one(&words), expected, "case {case}");
        }
    }

    fn model_of(f: &ActiveField) -> Vec<bool> {
        (0..f.len()).map(|i| f.is_active(i)).collect()
    }

    fn first_active_of(model: &[bool]) -> Option<usize> {
        model.iter().position(|active| *active)
    }

    #[test]
    fn active_field_starts_empty() {
        let f = ActiveField::new();
        assert_eq!(f.len(), 0);
        assert!(f.is_empty());
        assert_eq!(f.first(), None);
    }

    #[test]
    fn pushed_slots_are_active() {
        let mut f = ActiveField::new();
        f.push();
        f.push();
        assert_eq!(model_of(&f), [true, true]);
        assert_eq!(f.first(), Some(0));
    }

    #[test]
    fn cancel_clears_only_its_own_slot() {
        let mut f = ActiveField::new();
        for _ in 0..5 {
            f.push();
        }
        f.cancel(2);
        assert_eq!(model_of(&f), [true, true, false, true, true]);
        assert_eq!(f.first(), Some(0));
    }

    #[test]
    fn first_skips_a_cancelled_prefix() {
        let mut f = ActiveField::new();
        for _ in 0..5 {
            f.push();
        }
        f.cancel(0);
        f.cancel(1);
        f.cancel(2);
        assert_eq!(f.first(), Some(3));
    }

    #[test]
    fn first_is_none_when_everything_is_cancelled() {
        let mut f = ActiveField::new();
        for _ in 0..70 {
            f.push();
        }
        for i in 0..70 {
            f.cancel(i);
        }
        assert_eq!(f.first(), None);
    }

    fn active_in(f: &ActiveField, n: usize) -> Vec<usize> {
        f.iter(n).collect()
    }

    #[test]
    fn iter_lists_the_active_in_order() {
        let mut f = ActiveField::new();
        for _ in 0..6 {
            f.push();
        }
        f.cancel(1);
        f.cancel(4);
        assert_eq!(active_in(&f, 6), [0, 2, 3, 5]);
    }

    #[test]
    fn iter_stops_at_n() {
        let mut f = ActiveField::new();
        for _ in 0..6 {
            f.push();
        }
        f.cancel(1);
        assert_eq!(active_in(&f, 3), [0, 2]);
        assert_eq!(active_in(&f, 0), []);
    }

    #[test]
    fn iter_clamps_n_to_the_length() {
        let mut f = ActiveField::new();
        f.push();
        f.push();
        assert_eq!(active_in(&f, 100), [0, 1]);
    }

    #[test]
    fn iter_yields_nothing_when_all_are_cancelled() {
        let mut f = ActiveField::new();
        for _ in 0..130 {
            f.push();
        }
        for i in 0..130 {
            f.cancel(i);
        }
        assert_eq!(active_in(&f, 130), []);
    }

    #[test]
    fn iter_crosses_words_and_a_moving_head() {
        let mut f = ActiveField::new();
        let mut model: Vec<bool> = Vec::new();
        for round in 0..30 {
            for k in 0..37 {
                f.push();
                model.push(true);
                if (round + k) % 3 == 0 {
                    let last = model.len() - 1;
                    f.cancel(last);
                    model[last] = false;
                }
            }
            let expected: Vec<usize> = model
                .iter()
                .enumerate()
                .filter(|(_, active)| **active)
                .map(|(i, _)| i)
                .collect();
            assert_eq!(active_in(&f, model.len()), expected, "round {round}");

            let half = model.len() / 2;
            assert_eq!(
                active_in(&f, half),
                expected_upto(&expected, half),
                "round {round}"
            );

            f.pop_n(19);
            model.drain(..19);
        }
    }

    fn expected_upto(all: &[usize], n: usize) -> Vec<usize> {
        all.iter().copied().take_while(|i| *i < n).collect()
    }

    #[test]
    fn iter_agrees_with_the_model() {
        let mut rng = 0xdead_beef_cafe_f00du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut f = ActiveField::new();
        let mut model: Vec<bool> = Vec::new();

        for step in 0..2_000 {
            match next() % 6 {
                0..=3 => {
                    for _ in 0..1 + next() % 30 {
                        f.push();
                        model.push(true);
                    }
                }
                4 if !model.is_empty() => {
                    let i = (next() as usize) % model.len();
                    f.cancel(i);
                    model[i] = false;
                }
                _ => {
                    let n = (next() as usize) % (model.len() + 1);
                    f.pop_n(n);
                    model.drain(..n);
                }
            }

            let n = if model.is_empty() {
                0
            } else {
                (next() as usize) % (model.len() + 1)
            };
            let expected: Vec<usize> = model[..n]
                .iter()
                .enumerate()
                .filter(|(_, active)| **active)
                .map(|(i, _)| i)
                .collect();

            let live: Vec<usize> = f.iter(n).collect();
            assert_eq!(live, expected, "step {step}");
        }
    }

    #[test]
    fn pop_n_shifts_the_indices_down() {
        let mut f = ActiveField::new();
        for _ in 0..5 {
            f.push();
        }
        f.cancel(3);
        f.pop_n(2);
        assert_eq!(model_of(&f), [true, false, true]);
        assert_eq!(f.first(), Some(0));
    }

    #[test]
    fn pop_n_past_the_end_empties_it() {
        let mut f = ActiveField::new();
        f.push();
        f.push();
        f.pop_n(10);
        assert!(f.is_empty());
        assert_eq!(f.first(), None);
    }

    #[test]
    fn a_word_is_released_once_fully_consumed() {
        let mut f = ActiveField::new();
        for _ in 0..200 {
            f.push();
        }
        assert_eq!(f.words.len(), 4);
        f.pop_n(128);
        assert_eq!(f.words.len(), 2);
        assert_eq!(f.len(), 72);
    }

    #[test]
    fn a_slot_reached_after_a_word_boundary_is_active() {
        let mut f = ActiveField::new();
        for _ in 0..64 {
            f.push();
        }
        for i in 0..64 {
            f.cancel(i);
        }
        f.pop_n(64);
        for _ in 0..64 {
            f.push();
        }
        assert_eq!(
            model_of(&f),
            [true; 64],
            "a fresh slot must never inherit a bit"
        );
        assert_eq!(f.first(), Some(0));
    }

    #[test]
    fn crossing_word_boundaries_keeps_indices_aligned() {
        let mut f = ActiveField::new();
        let mut model: Vec<bool> = Vec::new();

        for round in 0..40 {
            for _ in 0..37 {
                f.push();
                model.push(true);
            }
            let target = round % 37;
            f.cancel(target);
            model[target] = false;

            assert_eq!(model_of(&f), model, "round {round} after cancel");
            assert_eq!(f.first(), first_active_of(&model), "round {round}");

            f.pop_n(19);
            model.drain(..19);
            assert_eq!(model_of(&f), model, "round {round} after pop");
            assert_eq!(f.first(), first_active_of(&model), "round {round}");
        }
    }

    #[test]
    fn stays_consistent_with_a_reference_model() {
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut f = ActiveField::new();
        let mut model: Vec<bool> = Vec::new();
        let mut max_len = 0;

        for step in 0..20_000 {
            match next() % 10 {
                0..=5 => {
                    let n = 1 + next() % 40;
                    for _ in 0..n {
                        f.push();
                        model.push(true);
                    }
                }
                6..=7 if !model.is_empty() => {
                    let i = (next() as usize) % model.len();
                    f.cancel(i);
                    model[i] = false;
                }
                _ => {
                    let n = (next() as usize) % (model.len() + 1);
                    f.pop_n(n);
                    model.drain(..n);
                }
            }

            assert_eq!(f.len(), model.len(), "step {step}");
            assert_eq!(f.is_empty(), model.is_empty(), "step {step}");
            assert_eq!(model_of(&f), model, "step {step}");
            assert_eq!(f.first(), first_active_of(&model), "step {step}");
            assert!(
                f.words.len() <= (f.head_bit + f.len()) / 64 + 1,
                "step {step}: {} words held for {} bits",
                f.words.len(),
                f.len(),
            );
            max_len = max_len.max(model.len());
        }

        assert!(
            max_len > 500,
            "the run stayed too small to cross word boundaries: {max_len}",
        );
    }

    #[test]
    #[ignore]
    fn active_field_cost() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 2_000_000;

        let mut f = ActiveField::new();
        let t = Instant::now();
        for _ in 0..N {
            f.push();
        }
        let push = t.elapsed().as_nanos() as f64 / N as f64;

        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let idx: Vec<usize> = (0..N).map(|_| next() as usize % N).collect();

        let t = Instant::now();
        for &i in &idx {
            f.cancel(i);
        }
        let cancel = t.elapsed().as_nanos() as f64 / N as f64;

        let t = Instant::now();
        let mut acc = 0usize;
        for &i in &idx {
            acc += black_box(f.is_active(i)) as usize;
        }
        let test = t.elapsed().as_nanos() as f64 / N as f64;
        black_box(acc);

        println!(
            "push {push:.2} ns   cancel {cancel:.2} ns   is_active {test:.2} ns  ({} of {N} still active)",
            acc,
        );

        for prefix in [64usize, 1_000, 10_000, 100_000] {
            let mut f = ActiveField::new();
            for _ in 0..prefix + 1 {
                f.push();
            }
            for i in 0..prefix {
                f.cancel(i);
            }
            const REPS: usize = 20_000;
            let t = Instant::now();
            for _ in 0..REPS {
                black_box(f.first());
            }
            let per = t.elapsed().as_nanos() as f64 / REPS as f64;
            println!(
                "first over {prefix:>7} cancelled: {per:>9.1} ns  ({:.3} ns/entry)",
                per / prefix as f64,
            );
        }

        let mut f = ActiveField::new();
        for _ in 0..N {
            f.push();
        }
        let t = Instant::now();
        let mut popped = 0;
        while !f.is_empty() {
            f.pop_n(37);
            popped += 1;
        }
        println!(
            "pop_n(37) {:.2} ns/call over {popped} calls",
            t.elapsed().as_nanos() as f64 / popped as f64,
        );
    }

    #[test]
    #[ignore]
    fn fire_batch_cost() {
        use std::hint::black_box;
        use std::time::Instant;

        const BATCH: usize = 100_000;
        const REPS: usize = 200;

        println!("{:>4}  {:>12} {:>12}", "%", "live()", "bit by bit");

        for cancelled_pct in [0usize, 50, 90, 95, 99, 100] {
            let mut f = ActiveField::new();
            for _ in 0..BATCH {
                f.push();
            }
            let mut rng = 0x2545_f491_4f6c_dd1du64;
            for i in 0..BATCH {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                if (rng % 100) < cancelled_pct as u64 {
                    f.cancel(i);
                }
            }

            let mut acc = 0usize;

            let t = Instant::now();
            for _ in 0..REPS {
                acc += f.iter(BATCH).sum::<usize>();
            }
            let batched = t.elapsed().as_nanos() as f64 / REPS as f64;

            let t = Instant::now();
            for _ in 0..REPS {
                for i in 0..BATCH {
                    if f.is_active(i) {
                        acc += i;
                    }
                }
            }
            let one_by_one = t.elapsed().as_nanos() as f64 / REPS as f64;

            black_box(acc);
            println!("{cancelled_pct:>3}%  {batched:>9.0} ns {one_by_one:>9.0} ns");
        }
    }
}
