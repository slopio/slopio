//! The line a topology prints when some of its threads steer connections:
//! where the machine processes the NICs' packets, against those threads' CPUs.

use std::collections::BTreeSet;

use runtime_machine::{Machine, cpu_list};

/// What keeps `steering`, the CPUs of the threads that steer, from each
/// serving the connections it processes the packets of; empty when nothing does.
pub(super) fn check(machine: &Machine, steering: &[usize]) -> Vec<String> {
    let mut problems = Vec::new();
    match machine.packet_cpus() {
        Err(e) => problems.push(e.to_string()),
        Ok(packets) => {
            let packets: BTreeSet<usize> = packets.into_iter().collect();
            let steering: BTreeSet<usize> = steering.iter().copied().collect();
            let idle: Vec<usize> = steering.difference(&packets).copied().collect();
            if !idle.is_empty() {
                problems.push(format!(
                    "no packets processed on {}, whose threads get almost no connections",
                    named(&idle)
                ));
            }
            let away: Vec<usize> = packets.difference(&steering).copied().collect();
            if !away.is_empty() {
                problems.push(format!(
                    "packets processed on {} reach no steering thread, their connections go to the kernel's hash",
                    named(&away)
                ));
            }
        }
    }
    if machine.irqbalance {
        problems.push("irqbalance is running and moves interrupts".to_owned());
    }
    problems
}

pub(super) fn report(machine: &Machine, steering: &[usize]) -> String {
    let problems = check(machine, steering);
    let cpus = named(steering);
    if problems.is_empty() {
        format!("connections steered on {cpus}, where the NICs' packets are processed")
    } else {
        format!("connections steered on {cpus}, but {}", problems.join("; "))
    }
}

/// `CPU 4` or `CPUs 0-3,8`.
fn named(cpus: &[usize]) -> String {
    match cpus {
        [cpu] => format!("CPU {cpu}"),
        _ => format!("CPUs {}", cpu_list(cpus)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_machine::{Nic, RxQueue};

    /// One NIC, queue n's packets processed on `cpus[n]`.
    fn machine(cpus: &[usize]) -> Machine {
        let queues = cpus
            .iter()
            .enumerate()
            .map(|(n, &cpu)| RxQueue {
                irq: 31 + n as u32,
                irq_name: format!("ens5-Tx-Rx-{n}"),
                irq_cpus: vec![cpu],
                rps_cpus: Vec::new(),
            })
            .collect();
        Machine {
            nics: vec![Nic {
                name: "ens5".into(),
                numa_node: Some(0),
                threaded_napi: false,
                queues: Ok(queues),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn one_queue_on_each_steering_cpu_is_aligned() {
        assert_eq!(
            check(&machine(&[0, 1, 2, 3]), &[0, 1, 2, 3]),
            Vec::<String>::new()
        );
    }

    #[test]
    fn fewer_queues_than_steering_cpus_starve_the_others() {
        assert_eq!(
            check(&machine(&[0, 1]), &[0, 1, 2, 3]),
            ["no packets processed on CPUs 2-3, whose threads get almost no connections"]
        );
    }

    #[test]
    fn queues_away_from_the_steering_cpus_reach_none() {
        assert_eq!(
            check(&machine(&[8]), &[0, 1]),
            [
                "no packets processed on CPUs 0-1, whose threads get almost no connections",
                "packets processed on CPU 8 reach no steering thread, their connections go to the kernel's hash",
            ]
        );
    }

    #[test]
    fn rps_onto_the_steering_cpus_is_aligned() {
        let mut machine = machine(&[0]);
        if let Ok(queues) = &mut machine.nics[0].queues {
            queues[0].rps_cpus = vec![0, 1, 2, 3];
        }
        assert_eq!(check(&machine, &[0, 1, 2, 3]), Vec::<String>::new());
    }

    #[test]
    fn what_hides_the_packet_cpus_and_irqbalance_are_reported() {
        let machine = Machine {
            irqbalance: true,
            ..Default::default()
        };
        assert_eq!(
            check(&machine, &[0]),
            [
                "no NIC in sight, as in a container without the host's network",
                "irqbalance is running and moves interrupts",
            ]
        );
    }
}
