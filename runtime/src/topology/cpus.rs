use std::fmt;

use libc::cpu_set_t;

use super::Cpu;

/// The CPUs the calling thread may run on, in ascending order. Not `0..n`: a
/// cpuset such as `0-2,4` leaves holes.
pub(super) fn allowed_cpus() -> Vec<usize> {
    unsafe {
        let mut cpu_set: cpu_set_t = std::mem::zeroed();
        let result = libc::sched_getaffinity(0, std::mem::size_of::<cpu_set_t>(), &mut cpu_set);
        if result != 0 {
            panic!(
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            );
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| libc::CPU_ISSET(cpu, &cpu_set))
            .collect()
    }
}

pub(super) fn pin_core(core_id: usize) {
    unsafe {
        let mut cpu_set: cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core_id, &mut cpu_set);
        let result = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpu_set);
        if result != 0 {
            panic!("sched_setaffinity failed for core {core_id}.");
        }
    }
}

/// Reserved CPUs in request order, and the rest in ascending order.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Placed {
    pub reserved: Vec<usize>,
    pub free: Vec<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum PlaceError {
    NotAllowed(usize),
    Taken(usize),
    Exhausted,
}

impl fmt::Display for PlaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAllowed(cpu) => write!(f, "CPU {cpu} is not one this process may run on"),
            Self::Taken(cpu) => write!(f, "CPU {cpu} is reserved twice"),
            Self::Exhausted => write!(f, "more CPUs reserved than this process may run on"),
        }
    }
}

/// Fixed CPUs first, so an `Auto` never takes one asked for by name; `Auto`
/// takes from the top, leaving the low CPUs to the workers.
pub(super) fn place(allowed: &[usize], requests: &[Cpu]) -> Result<Placed, PlaceError> {
    let mut free = allowed.to_vec();
    let mut reserved = vec![None; requests.len()];
    for (slot, request) in reserved.iter_mut().zip(requests) {
        if let Cpu::Fixed(cpu) = *request {
            let Some(at) = free.iter().position(|&c| c == cpu) else {
                return Err(if allowed.contains(&cpu) {
                    PlaceError::Taken(cpu)
                } else {
                    PlaceError::NotAllowed(cpu)
                });
            };
            *slot = Some(free.remove(at));
        }
    }
    for (slot, request) in reserved.iter_mut().zip(requests) {
        if *request == Cpu::Auto {
            *slot = Some(free.pop().ok_or(PlaceError::Exhausted)?);
        }
    }
    Ok(Placed {
        reserved: reserved.into_iter().map(Option::unwrap).collect(),
        free,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_reserved_leaves_every_cpu_to_the_workers() {
        let placed = place(&[0, 1, 2, 4], &[]).unwrap();
        assert_eq!(placed.reserved, Vec::<usize>::new());
        assert_eq!(placed.free, vec![0, 1, 2, 4]);
    }

    #[test]
    fn auto_takes_from_the_top_in_request_order() {
        let placed = place(&[0, 1, 2, 4], &[Cpu::Auto, Cpu::Auto]).unwrap();
        assert_eq!(placed.reserved, vec![4, 2]);
        assert_eq!(placed.free, vec![0, 1]);
    }

    #[test]
    fn fixed_wins_over_an_earlier_auto() {
        let placed = place(&[0, 1, 2, 3], &[Cpu::Auto, Cpu::Fixed(3)]).unwrap();
        assert_eq!(placed.reserved, vec![2, 3]);
        assert_eq!(placed.free, vec![0, 1]);
    }

    #[test]
    fn a_cpu_outside_the_mask_is_refused() {
        assert_eq!(
            place(&[0, 1, 2, 4], &[Cpu::Fixed(3)]),
            Err(PlaceError::NotAllowed(3))
        );
    }

    #[test]
    fn a_cpu_reserved_twice_is_refused() {
        assert_eq!(
            place(&[0, 1], &[Cpu::Fixed(1), Cpu::Fixed(1)]),
            Err(PlaceError::Taken(1))
        );
    }

    #[test]
    fn more_reservations_than_cpus_is_refused() {
        assert_eq!(
            place(&[0], &[Cpu::Auto, Cpu::Auto]),
            Err(PlaceError::Exhausted)
        );
    }
}
