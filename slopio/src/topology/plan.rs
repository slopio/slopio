//! What a topology resolved before starting its threads, printed at startup.

use std::fmt;
use std::ops::Range;
use std::time::Duration;

use slopio_core::runtime::RuntimeOptions;

use slopio_machine::cpu_list;

/// A group, or the per-core workers, once placed.
pub(super) struct Placed {
    pub label: String,
    pub ids: Range<usize>,
    /// Thread `ids.start + i` runs on `cpus[i]`.
    pub cpus: Vec<usize>,
    /// Its SQ thread on `sq_cpus[i]`, under `sqpoll`.
    pub sq_cpus: Vec<usize>,
    pub options: RuntimeOptions,
}

pub(super) struct Plan {
    pub allowed: Vec<usize>,
    /// `available_parallelism`: below `allowed` under a cgroup CPU quota.
    pub parallelism: usize,
    pub groups: Vec<Placed>,
}

impl Plan {
    /// The CPUs of the threads that steer connections.
    pub fn steering(&self) -> Vec<usize> {
        self.groups
            .iter()
            .filter(|g| g.options.net.steer_connections)
            .flat_map(|g| g.cpus.iter().copied())
            .collect()
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let threads: usize = self.groups.iter().map(|g| g.ids.len()).sum();
        write!(
            f,
            "slopio: topology of {threads} threads, allowed CPUs {}, parallelism {}",
            cpu_list(&self.allowed),
            self.parallelism
        )?;
        for g in &self.groups {
            write!(
                f,
                "\nslopio:   {:<9} threads {:<7} {}",
                g.label,
                ids(&g.ids),
                cpus(&g.cpus)
            )?;
            if !g.sq_cpus.is_empty() {
                write!(f, ", SQ threads on {}", cpus(&g.sq_cpus))?;
            }
            write!(f, "; ring {}; net {}", ring(&g.options), net(&g.options))?;
        }
        Ok(())
    }
}

fn ids(ids: &Range<usize>) -> String {
    match ids.len() {
        1 => ids.start.to_string(),
        _ => format!("{}-{}", ids.start, ids.end - 1),
    }
}

/// `CPU 4` or `CPUs 9,8`, in thread order.
pub(super) fn cpus(cpus: &[usize]) -> String {
    match cpus {
        [cpu] => format!("CPU {cpu}"),
        _ => format!("CPUs {}", in_order(cpus)),
    }
}

/// Thread order kept: folded into ranges only when it is ascending.
fn in_order(cpus: &[usize]) -> String {
    if cpus.windows(2).all(|w| w[0] < w[1]) {
        cpu_list(cpus)
    } else {
        cpus.iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn ring(options: &RuntimeOptions) -> String {
    let ring = &options.ring;
    let mut flags = Vec::new();
    if ring.defer_taskrun {
        flags.push("defer_taskrun".to_owned());
    }
    if ring.coop_taskrun {
        flags.push("coop_taskrun".to_owned());
    }
    if ring.sqpoll {
        flags.push(format!("sqpoll idle {}", duration(ring.sqpoll_idle)));
    }
    if let Some(budget) = ring.napi_busy_poll {
        let prefer = if ring.napi_prefer_busy_poll {
            " prefer"
        } else {
            ""
        };
        flags.push(format!("napi_busy_poll {}{prefer}", duration(budget)));
    }
    if flags.is_empty() {
        "plain".to_owned()
    } else {
        flags.join(" ")
    }
}

fn net(options: &RuntimeOptions) -> &'static str {
    if options.net.steer_connections {
        "steer_connections"
    } else {
        "plain"
    }
}

fn duration(d: Duration) -> String {
    if d.subsec_nanos() == 0 {
        format!("{}s", d.as_secs())
    } else if d.as_nanos() % 1_000_000 == 0 {
        format!("{}ms", d.as_millis())
    } else {
        format!("{}µs", d.as_micros())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slopio_core::io_driver::RingOptions;
    use slopio_core::runtime::NetOptions;

    #[test]
    fn a_plan_prints_one_line_per_group() {
        let db = RuntimeOptions {
            ring: RingOptions {
                sqpoll: true,
                defer_taskrun: false,
                coop_taskrun: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let http = RuntimeOptions {
            ring: RingOptions {
                napi_busy_poll: Some(Duration::from_micros(50)),
                ..Default::default()
            },
            net: NetOptions {
                steer_connections: true,
            },
        };
        let plan = Plan {
            allowed: (0..10).collect(),
            parallelism: 10,
            groups: vec![
                Placed {
                    label: "group 0".into(),
                    ids: 0..2,
                    cpus: vec![9, 8],
                    sq_cpus: vec![7, 6],
                    options: db,
                },
                Placed {
                    label: "per_core".into(),
                    ids: 2..8,
                    cpus: (0..6).collect(),
                    sq_cpus: Vec::new(),
                    options: http,
                },
            ],
        };
        assert_eq!(
            plan.to_string(),
            "slopio: topology of 8 threads, allowed CPUs 0-9, parallelism 10\n\
             slopio:   group 0   threads 0-1     CPUs 9,8, SQ threads on CPUs 7,6; ring sqpoll idle 1s; net plain\n\
             slopio:   per_core  threads 2-7     CPUs 0-5; ring defer_taskrun coop_taskrun napi_busy_poll 50µs; net steer_connections"
        );
        assert_eq!(plan.steering(), [0, 1, 2, 3, 4, 5]);
    }
}
