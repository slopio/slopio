//! What the machine shows of its CPUs and of its NICs' receive path, read from
//! `/sys` and `/proc` without privilege. It only reads: nothing here places a
//! thread or changes a setting. The runtime reads the machine with it, and so
//! does whatever tunes the machine, so that both agree on what they see.

use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// The CPUs and NICs as the process finds them when it reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Machine {
    /// The CPUs this process may run on.
    pub allowed: Vec<usize>,
    /// `isolcpus`: left to threads pinned there, absent from the default affinity.
    pub isolated: Vec<usize>,
    /// `nohz_full`: no timer tick while one task runs.
    pub nohz_full: Vec<usize>,
    /// The CPUs of each core, SMT siblings together, lowest first.
    pub cores: Vec<Vec<usize>>,
    /// The CPUs of each NUMA node, by node id.
    pub nodes: Vec<(usize, Vec<usize>)>,
    /// Every NIC that is up.
    pub nics: Vec<Nic>,
    /// RFS moves packet processing to where each socket is read.
    pub rfs: bool,
    /// An `irqbalance` process is running and may move interrupts.
    pub irqbalance: bool,
}

/// A NIC that is up: loopback, veth and bridges have no device and are left out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nic {
    pub name: String,
    /// The NUMA node its device hangs off, `None` when the machine does not say.
    pub numa_node: Option<usize>,
    /// NAPI runs in kernel threads the scheduler places.
    pub threaded_napi: bool,
    /// Its receive queues in the order the driver numbered their interrupts,
    /// or why they cannot be told apart.
    pub queues: Result<Vec<RxQueue>, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RxQueue {
    pub irq: u32,
    /// As in `/proc/interrupts`: `ens5-Tx-Rx-0`, `virtio0-input.0`.
    pub irq_name: String,
    /// The CPUs its interrupt runs on, its effective affinity.
    pub irq_cpus: Vec<usize>,
    /// The CPUs RPS hands its packets to, empty when RPS is off.
    pub rps_cpus: Vec<usize>,
}

/// Why [`Machine::packet_cpus`] cannot tell where packets are processed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PacketCpusError {
    NoNic,
    Unreadable {
        nic: String,
        why: String,
    },
    Ambiguous {
        nic: String,
        irq: u32,
        cpus: Vec<usize>,
    },
    ThreadedNapi {
        nic: String,
    },
    Rfs,
}

impl fmt::Display for PacketCpusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoNic => write!(
                f,
                "no NIC in sight, as in a container without the host's network"
            ),
            Self::Unreadable { nic, why } => {
                write!(f, "cannot tell {nic}'s receive queues apart: {why}")
            }
            Self::Ambiguous { nic, irq, cpus } => {
                write!(
                    f,
                    "interrupt {irq} of {nic} may run on CPUs {}",
                    cpu_list(cpus)
                )
            }
            Self::ThreadedNapi { nic } => {
                write!(f, "{nic} runs NAPI in threads the scheduler moves")
            }
            Self::Rfs => write!(
                f,
                "RFS moves packet processing to where each socket is read"
            ),
        }
    }
}

impl std::error::Error for PacketCpusError {}

impl Machine {
    /// Reads `/sys` and `/proc` now; microseconds, a few dozen small files.
    pub fn read() -> Self {
        Self::read_from(Path::new("/sys"), Path::new("/proc"), allowed_cpus())
    }

    /// Reads `sys` and `proc` in place of `/sys` and `/proc`, with `allowed`
    /// as the CPUs the process may run on: a recorded tree, in tests.
    pub fn read_from(sys: &Path, proc: &Path, allowed: Vec<usize>) -> Self {
        let cpu = sys.join("devices/system/cpu");
        let cores: BTreeSet<Vec<usize>> = parse_cpu_list(&read(&cpu.join("online")))
            .into_iter()
            .map(|c| {
                let siblings = read(&cpu.join(format!("cpu{c}/topology/thread_siblings_list")));
                Some(parse_cpu_list(&siblings))
                    .filter(|s| !s.is_empty())
                    .unwrap_or(vec![c])
            })
            .collect();
        let mut nodes: Vec<(usize, Vec<usize>)> = entries(&sys.join("devices/system/node"))
            .filter_map(|p| {
                let id = file_name(&p).strip_prefix("node")?.parse().ok()?;
                Some((id, parse_cpu_list(&read(&p.join("cpulist")))))
            })
            .collect();
        nodes.sort();
        let interrupts = read(&proc.join("interrupts"));
        let nics = entries(&sys.join("class/net"))
            .filter(|p| p.join("device").exists() && read(&p.join("operstate")) == "up")
            .map(|p| nic(&p, &interrupts, proc))
            .collect();
        let irqbalance = entries(proc)
            .filter(|p| file_name(p).bytes().all(|b| b.is_ascii_digit()))
            .any(|p| read(&p.join("comm")) == "irqbalance");
        Self {
            allowed,
            isolated: parse_cpu_list(&read(&cpu.join("isolated"))),
            nohz_full: parse_cpu_list(&read(&cpu.join("nohz_full"))),
            cores: cores.into_iter().collect(),
            nodes,
            nics,
            rfs: read(&proc.join("sys/net/core/rps_sock_flow_entries"))
                .parse()
                .unwrap_or(0)
                > 0,
            irqbalance,
        }
    }

    /// The CPUs that process the packets the NICs receive, each once, lowest
    /// first: a queue's RPS CPUs when RPS is on, its interrupt's CPU when not.
    /// A thread steering connections on one of them serves the connections
    /// whose packets it processes.
    pub fn packet_cpus(&self) -> Result<Vec<usize>, PacketCpusError> {
        if self.rfs {
            return Err(PacketCpusError::Rfs);
        }
        if self.nics.is_empty() {
            return Err(PacketCpusError::NoNic);
        }
        let mut cpus = BTreeSet::new();
        for nic in &self.nics {
            if nic.threaded_napi {
                return Err(PacketCpusError::ThreadedNapi {
                    nic: nic.name.clone(),
                });
            }
            let queues = nic
                .queues
                .as_ref()
                .map_err(|why| PacketCpusError::Unreadable {
                    nic: nic.name.clone(),
                    why: why.clone(),
                })?;
            for queue in queues {
                if !queue.rps_cpus.is_empty() {
                    cpus.extend(&queue.rps_cpus);
                    continue;
                }
                match queue.irq_cpus.as_slice() {
                    &[cpu] => {
                        cpus.insert(cpu);
                    }
                    other => {
                        return Err(PacketCpusError::Ambiguous {
                            nic: nic.name.clone(),
                            irq: queue.irq,
                            cpus: other.to_vec(),
                        });
                    }
                }
            }
        }
        Ok(cpus.into_iter().collect())
    }
}

impl fmt::Display for Machine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "allowed CPUs   {}", cpu_list(&self.allowed))?;
        writeln!(f, "isolated       {}", or_none(&self.isolated))?;
        writeln!(f, "nohz_full      {}", or_none(&self.nohz_full))?;
        let smt = self.cores.iter().filter(|c| c.len() > 1).count();
        writeln!(
            f,
            "cores          {}, {smt} with SMT siblings",
            self.cores.len()
        )?;
        for (id, cpus) in &self.nodes {
            writeln!(f, "node {id:<9} CPUs {}", cpu_list(cpus))?;
        }
        for nic in &self.nics {
            let node = nic.numa_node.map_or("-".to_owned(), |n| n.to_string());
            let napi = if nic.threaded_napi {
                ", threaded NAPI"
            } else {
                ""
            };
            match &nic.queues {
                Ok(queues) => {
                    writeln!(
                        f,
                        "nic {:<10} node {node}, {} receive queue{}{napi}",
                        nic.name,
                        queues.len(),
                        if queues.len() == 1 { "" } else { "s" }
                    )?;
                    for (n, q) in queues.iter().enumerate() {
                        let rps = if q.rps_cpus.is_empty() {
                            String::new()
                        } else {
                            format!(", RPS to CPUs {}", cpu_list(&q.rps_cpus))
                        };
                        writeln!(
                            f,
                            "  rx-{n:<10} irq {} ({}) on CPUs {}{rps}",
                            q.irq,
                            q.irq_name,
                            cpu_list(&q.irq_cpus)
                        )?;
                    }
                }
                Err(why) => writeln!(f, "nic {:<10} node {node}{napi}: {why}", nic.name)?,
            }
        }
        writeln!(f, "RFS            {}", if self.rfs { "on" } else { "off" })?;
        writeln!(
            f,
            "irqbalance     {}",
            if self.irqbalance {
                "running"
            } else {
                "not seen"
            }
        )?;
        match self.packet_cpus() {
            Ok(cpus) => write!(f, "packet CPUs    {}", cpu_list(&cpus)),
            Err(e) => write!(f, "packet CPUs    unknown: {e}"),
        }
    }
}

/// The NIC under `path`, `/sys/class/net/<name>`.
fn nic(path: &Path, interrupts: &str, proc: &Path) -> Nic {
    let device = path.join("device");
    // virtio's vectors and node belong to the PCI device above the virtio one.
    let above = fs::canonicalize(&device).map(|d| d.join("..")).ok();
    let in_device = |name: &str| {
        std::iter::once(device.join(name))
            .chain(above.iter().map(|a| a.join(name)))
            .find(|p| p.exists())
    };
    let numa_node = in_device("numa_node").and_then(|p| read(&p).parse().ok());
    let rx: Vec<PathBuf> = {
        let mut rx: Vec<_> = entries(&path.join("queues"))
            .filter(|q| file_name(q).starts_with("rx-"))
            .collect();
        rx.sort_by_key(|q| file_name(q)[3..].parse::<usize>().unwrap_or(usize::MAX));
        rx
    };
    Nic {
        name: file_name(path),
        numa_node,
        threaded_napi: read(&path.join("threaded")) == "1",
        queues: in_device("msi_irqs")
            .ok_or_else(|| "no MSI-X interrupts".to_owned())
            .and_then(|msi| rx_queues(&msi, &rx, interrupts, proc)),
    }
}

fn rx_queues(
    msi: &Path,
    rx: &[PathBuf],
    interrupts: &str,
    proc: &Path,
) -> Result<Vec<RxQueue>, String> {
    let irqs: BTreeSet<u32> = entries(msi)
        .filter_map(|e| file_name(&e).parse().ok())
        .collect();
    let named: Vec<(u32, String)> = interrupts
        .lines()
        .filter_map(|line| {
            let (irq, rest) = line.trim_start().split_once(':')?;
            let irq = irq.parse().ok()?;
            let name = rest.split_whitespace().last()?;
            (irqs.contains(&irq) && carries_rx(name)).then(|| (irq, name.to_owned()))
        })
        .collect();
    if named.len() != rx.len() {
        return Err(format!(
            "{} receive interrupts for {} queues",
            named.len(),
            rx.len()
        ));
    }
    Ok(named
        .into_iter()
        .zip(rx)
        .map(|((irq, irq_name), queue)| {
            let dir = proc.join("irq").join(irq.to_string());
            let list = match read(&dir.join("effective_affinity_list")) {
                l if l.is_empty() => read(&dir.join("smp_affinity_list")),
                l => l,
            };
            RxQueue {
                irq,
                irq_name,
                irq_cpus: parse_cpu_list(&list),
                rps_cpus: parse_cpu_mask(&read(&queue.join("rps_cpus"))),
            }
        })
        .collect())
}

/// Whether an interrupt named `name` receives: ENA's `ens5-Tx-Rx-0`, virtio's
/// `virtio0-input.0`, mlx5's `mlx5_comp0@…`; not a management, async, config
/// or transmit-only vector.
fn carries_rx(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    ["rx", "input", "comp"].iter().any(|k| name.contains(k))
        && !["mgmnt", "async", "config"]
            .iter()
            .any(|k| name.contains(k))
}

fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .map(|s| s.trim().to_owned())
        .unwrap_or_default()
}

/// The entries of `dir`, sorted; none when it cannot be read.
fn entries(dir: &Path) -> impl Iterator<Item = PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .collect();
    paths.sort();
    paths.into_iter()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The CPUs the calling thread may run on, in ascending order. Not `0..n`: a
/// cpuset such as `0-2,4` leaves holes.
pub fn allowed_cpus() -> Vec<usize> {
    unsafe {
        let mut cpu_set: libc::cpu_set_t = std::mem::zeroed();
        let result =
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut cpu_set);
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

/// `0-3,8`, as in `/proc` and `/sys`.
pub fn parse_cpu_list(list: &str) -> Vec<usize> {
    list.split(',')
        .filter_map(|part| match part.trim().split_once('-') {
            Some((a, b)) => Some((a.parse().ok()?..=b.parse().ok()?).collect::<Vec<usize>>()),
            None => part.trim().parse().ok().map(|c| vec![c]),
        })
        .flatten()
        .collect()
}

/// A hex mask such as `00000000,0000000e`, most significant word first.
fn parse_cpu_mask(mask: &str) -> Vec<usize> {
    let digits: Vec<u32> = mask.chars().filter_map(|c| c.to_digit(16)).collect();
    let mut cpus = Vec::new();
    for (k, digit) in digits.iter().rev().enumerate() {
        cpus.extend(
            (0..4)
                .filter(|bit| digit & (1 << bit) != 0)
                .map(|bit| 4 * k + bit),
        );
    }
    cpus
}

/// The reverse of [`parse_cpu_list`], ranges folded.
pub fn cpu_list(cpus: &[usize]) -> String {
    let cpus: BTreeSet<usize> = cpus.iter().copied().collect();
    let mut parts = Vec::new();
    let mut iter = cpus.into_iter().peekable();
    while let Some(first) = iter.next() {
        let mut last = first;
        while iter.peek() == Some(&(last + 1)) {
            last = iter.next().unwrap();
        }
        parts.push(if first == last {
            first.to_string()
        } else {
            format!("{first}-{last}")
        });
    }
    parts.join(",")
}

fn or_none(cpus: &[usize]) -> String {
    if cpus.is_empty() {
        "none".to_owned()
    } else {
        cpu_list(cpus)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `/sys` and `/proc` pair under a fresh directory, removed on drop.
    struct Tree(PathBuf);

    impl Tree {
        fn new(name: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("runtime-machine-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }

        fn file(&self, path: &str, content: &str) -> &Self {
            let path = self.0.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
            self
        }

        fn dir(&self, path: &str) -> &Self {
            fs::create_dir_all(self.0.join(path)).unwrap();
            self
        }

        fn read(&self) -> Machine {
            Machine::read_from(&self.0.join("sys"), &self.0.join("proc"), vec![0, 1, 2, 3])
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// An ENA with one queue per entry of `cpus`, queue n's interrupt on `cpus[n]`.
    fn ena(name: &str, cpus: &[&str]) -> Tree {
        let tree = Tree::new(name);
        let mut interrupts = String::from("           CPU0       CPU1\n");
        interrupts += " 30:   1   0  PCI-MSIX-0000:00:05.0   0-edge   ena-mgmnt@pci:0000:00:05.0\n";
        tree.file("sys/class/net/lo/operstate", "unknown")
            .file("sys/class/net/ens5/operstate", "up\n")
            .file("sys/class/net/ens5/threaded", "0")
            .file("sys/class/net/ens5/device/numa_node", "0\n")
            .dir("sys/class/net/ens5/device/msi_irqs/30")
            .file("proc/1/comm", "systemd\n");
        for (n, cpu) in cpus.iter().enumerate() {
            let irq = 31 + n;
            interrupts += &format!(
                " {irq}:   5   7  PCI-MSIX-0000:00:05.0   {}-edge   ens5-Tx-Rx-{n}\n",
                n + 1
            );
            tree.dir(&format!("sys/class/net/ens5/device/msi_irqs/{irq}"))
                .file(
                    &format!("sys/class/net/ens5/queues/rx-{n}/rps_cpus"),
                    "00000000,00000000\n",
                )
                .dir(&format!("sys/class/net/ens5/queues/tx-{n}"))
                .file(
                    &format!("proc/irq/{irq}/effective_affinity_list"),
                    &format!("{cpu}\n"),
                );
        }
        tree.file("proc/interrupts", &interrupts);
        tree
    }

    #[test]
    fn an_ena_shows_each_queue_on_its_interrupts_cpu() {
        let machine = ena("ena", &["0", "1"]).read();
        let queues = machine.nics[0].queues.as_ref().unwrap();
        assert_eq!(machine.nics.len(), 1, "loopback has no device");
        assert_eq!(machine.nics[0].numa_node, Some(0));
        assert_eq!(queues[1].irq, 32);
        assert_eq!(queues[1].irq_name, "ens5-Tx-Rx-1");
        assert_eq!(queues[1].irq_cpus, [1]);
        assert_eq!(machine.packet_cpus(), Ok(vec![0, 1]));
    }

    #[test]
    fn rps_decides_where_a_queues_packets_are_processed() {
        let tree = ena("rps", &["0", "0-3"]);
        tree.file(
            "sys/class/net/ens5/queues/rx-1/rps_cpus",
            "00000000,0000000c\n",
        );
        assert_eq!(tree.read().packet_cpus(), Ok(vec![0, 2, 3]));
    }

    #[test]
    fn an_interrupt_allowed_on_several_cpus_cannot_be_placed() {
        let machine = ena("spread", &["0", "0-3"]).read();
        assert_eq!(
            machine.packet_cpus(),
            Err(PacketCpusError::Ambiguous {
                nic: "ens5".into(),
                irq: 32,
                cpus: vec![0, 1, 2, 3]
            })
        );
    }

    #[test]
    fn rfs_threaded_napi_and_irqbalance_are_seen() {
        let tree = ena("moved", &["0"]);
        tree.file("sys/class/net/ens5/threaded", "1\n")
            .file("proc/812/comm", "irqbalance\n");
        let machine = tree.read();
        assert!(machine.irqbalance);
        let nic = PacketCpusError::ThreadedNapi { nic: "ens5".into() };
        assert_eq!(machine.packet_cpus(), Err(nic));
        tree.file("proc/sys/net/core/rps_sock_flow_entries", "32768\n");
        assert_eq!(tree.read().packet_cpus(), Err(PacketCpusError::Rfs));
    }

    #[test]
    fn a_vector_count_off_from_the_queue_count_is_unreadable() {
        let tree = ena("short", &["0", "1"]);
        tree.dir("sys/class/net/ens5/queues/rx-2");
        assert_eq!(
            tree.read().packet_cpus(),
            Err(PacketCpusError::Unreadable {
                nic: "ens5".into(),
                why: "2 receive interrupts for 3 queues".into()
            })
        );
    }

    #[test]
    fn virtio_vectors_and_node_are_found_on_the_pci_device_above() {
        let tree = Tree::new("virtio");
        let pci = "sys/devices/pci0000:00/0000:00:01.0";
        tree.file("sys/class/net/enp0s1/operstate", "up")
            .dir("sys/class/net/enp0s1/queues/rx-0")
            .dir(&format!("{pci}/virtio0"))
            .file(&format!("{pci}/numa_node"), "-1\n")
            .dir(&format!("{pci}/msi_irqs/63"))
            .dir(&format!("{pci}/msi_irqs/64"))
            .dir(&format!("{pci}/msi_irqs/65"))
            .file(
                "proc/interrupts",
                " 63:  0  PCI-MSIX-0000:00:01.0  0-edge  virtio0-config\n \
                 64:  9  PCI-MSIX-0000:00:01.0  1-edge  virtio0-input.0\n \
                 65:  4  PCI-MSIX-0000:00:01.0  2-edge  virtio0-output.0\n",
            )
            .file("proc/irq/64/effective_affinity_list", "3");
        std::os::unix::fs::symlink(
            tree.0.join(format!("{pci}/virtio0")),
            tree.0.join("sys/class/net/enp0s1/device"),
        )
        .unwrap();
        let machine = tree.read();
        assert_eq!(
            machine.nics[0].numa_node, None,
            "-1: the machine does not say"
        );
        assert_eq!(machine.packet_cpus(), Ok(vec![3]));
    }

    #[test]
    fn without_a_nic_no_cpu_processes_packets() {
        let tree = Tree::new("none");
        tree.file("sys/class/net/lo/operstate", "unknown")
            .file("sys/class/net/veth0/operstate", "up");
        assert_eq!(tree.read().packet_cpus(), Err(PacketCpusError::NoNic));
    }

    #[test]
    fn cores_group_smt_siblings_and_nodes_list_their_cpus() {
        let tree = Tree::new("cpus");
        tree.file("sys/devices/system/cpu/online", "0-3\n")
            .file("sys/devices/system/cpu/isolated", "2-3\n")
            .file("sys/devices/system/cpu/nohz_full", "(null)\n")
            .file("sys/devices/system/node/node0/cpulist", "0-1\n")
            .file("sys/devices/system/node/node1/cpulist", "2-3\n")
            .dir("sys/devices/system/node/power");
        for (cpu, siblings) in [(0, "0,2"), (1, "1,3"), (2, "0,2"), (3, "1,3")] {
            tree.file(
                &format!("sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"),
                siblings,
            );
        }
        let machine = tree.read();
        assert_eq!(machine.cores, [vec![0, 2], vec![1, 3]]);
        assert_eq!(machine.isolated, [2, 3]);
        assert_eq!(machine.nohz_full, Vec::<usize>::new());
        assert_eq!(machine.nodes, [(0, vec![0, 1]), (1, vec![2, 3])]);
    }

    #[test]
    fn cpu_lists_and_masks_parse() {
        assert_eq!(parse_cpu_list("0-3,8,10-11"), [0, 1, 2, 3, 8, 10, 11]);
        assert_eq!(cpu_list(&[11, 0, 1, 2, 3, 8, 10]), "0-3,8,10-11");
        assert_eq!(parse_cpu_mask("00000000,0000000e"), [1, 2, 3]);
        assert_eq!(parse_cpu_mask("00000001,00000000"), [32]);
        assert_eq!(parse_cpu_mask("000"), Vec::<usize>::new());
    }
}
