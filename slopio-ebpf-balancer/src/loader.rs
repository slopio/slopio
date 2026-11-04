use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use object::LittleEndian;
use object::elf;
use object::read::StringTable;
use object::read::elf::{FileHeader as _, Rel as _, SectionHeader as _, Sym as _};

const BPF_PROG_LOAD: u32 = 5;
const BPF_MAP_CREATE: u32 = 0;
const BPF_MAP_UPDATE_ELEM: u32 = 2;
const BPF_MAP_TYPE_ARRAY: u32 = 2;
const BPF_MAP_TYPE_REUSEPORT_SOCKARRAY: u32 = 20;
const BPF_PROG_TYPE_SK_REUSEPORT: u32 = 21;
const BPF_SK_REUSEPORT_SELECT: u32 = 39;
const SO_ATTACH_REUSEPORT_EBPF: libc::c_int = 52;
const BPF_ANY: u64 = 0;

/// BPF instruction: LD_IMM64 uses opcode 0x18 and spans two 8-byte slots.
const BPF_LD_IMM64_OPCODE: u8 = 0x18;
/// Pseudo map-fd source register value that tells the kernel verifier
/// the imm field carries a map file descriptor.
const BPF_PSEUDO_MAP_FD: u8 = 1;

pub struct QuicBalancer {
    _prog_fd: OwnedFd,
    sockarray_fd: OwnedFd,
    config_fd: OwnedFd,
}

impl QuicBalancer {
    pub fn new(
        num_threads: u32,
        first_socket_fd: RawFd,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // The `object` crate requires the ELF data to be 8-byte aligned for
        // ELF64, but `include_bytes!` only guarantees 1-byte alignment.
        #[repr(C, align(8))]
        struct Aligned<const N: usize>([u8; N]);
        const BYTES: &Aligned<
            { include_bytes!(concat!(env!("OUT_DIR"), "/quic_reuseport.o")).len() },
        > = &Aligned(*include_bytes!(concat!(
            env!("OUT_DIR"),
            "/quic_reuseport.o"
        )));
        let ebpf_object = &BYTES.0;

        let sockarray_fd = bpf_create_map(BPF_MAP_TYPE_REUSEPORT_SOCKARRAY, 4, 8, 256)?;
        let config_fd = bpf_create_map(BPF_MAP_TYPE_ARRAY, 4, 4, 1)?;

        let key: u32 = 0;
        bpf_map_update(config_fd.as_raw_fd(), &key, &num_threads)?;

        let map_fds = &[
            ("sockarray", sockarray_fd.as_raw_fd()),
            ("config", config_fd.as_raw_fd()),
        ];
        let prog_fd = bpf_prog_load(ebpf_object, map_fds)?;

        let prog_raw = prog_fd.as_raw_fd();
        let ret = unsafe {
            libc::setsockopt(
                first_socket_fd,
                libc::SOL_SOCKET,
                SO_ATTACH_REUSEPORT_EBPF,
                &prog_raw as *const _ as *const libc::c_void,
                mem::size_of::<RawFd>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error().into());
        }

        // Register first socket in sockarray
        let index: u32 = 0;
        let sock_val: u64 = first_socket_fd as u64;
        bpf_map_update(sockarray_fd.as_raw_fd(), &index, &sock_val)?;

        Ok(Self {
            _prog_fd: prog_fd,
            sockarray_fd,
            config_fd,
        })
    }

    pub fn add_socket(
        &self,
        index: u32,
        socket_fd: RawFd,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let sock_val: u64 = socket_fd as u64;
        bpf_map_update(self.sockarray_fd.as_raw_fd(), &index, &sock_val)?;
        Ok(())
    }
}

fn bpf_create_map(
    map_type: u32,
    key_size: u32,
    value_size: u32,
    max_entries: u32,
) -> Result<OwnedFd, io::Error> {
    #[repr(C)]
    struct BpfMapCreateAttr {
        map_type: u32,
        key_size: u32,
        value_size: u32,
        max_entries: u32,
    }

    let attr = BpfMapCreateAttr {
        map_type,
        key_size,
        value_size,
        max_entries,
    };

    let fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_MAP_CREATE,
            &attr as *const _ as *const libc::c_void,
            mem::size_of::<BpfMapCreateAttr>(),
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

fn bpf_map_update<K, V>(map_fd: RawFd, key: &K, value: &V) -> Result<(), io::Error> {
    #[repr(C)]
    struct BpfMapUpdateAttr {
        map_fd: u32,
        key: u64,
        value: u64,
        flags: u64,
    }

    let attr = BpfMapUpdateAttr {
        map_fd: map_fd as u32,
        key: key as *const K as u64,
        value: value as *const V as u64,
        flags: BPF_ANY,
    };

    let ret = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_MAP_UPDATE_ELEM,
            &attr as *const _ as *const libc::c_void,
            mem::size_of::<BpfMapUpdateAttr>(),
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn elf_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Parse an eBPF ELF object, apply map-fd relocations, and load the program
/// into the kernel.
fn bpf_prog_load(elf_bytes: &[u8], map_fds: &[(&str, RawFd)]) -> Result<OwnedFd, io::Error> {
    let endian = LittleEndian;
    let header = elf::FileHeader64::<LittleEndian>::parse(elf_bytes).map_err(elf_err)?;
    let sections = header.sections(endian, elf_bytes).map_err(elf_err)?;

    // Find the program section: PROGBITS with EXECINSTR flag and nonzero size.
    let mut prog_section_index = None;
    let mut prog_data: Vec<u8> = Vec::new();
    for (idx, section) in sections.iter().enumerate() {
        if section.sh_type(endian) == elf::SHT_PROGBITS
            && section.sh_flags(endian) & u64::from(elf::SHF_EXECINSTR) != 0
            && section.sh_size(endian) > 0
        {
            let data = section.data(endian, elf_bytes).map_err(elf_err)?;
            prog_data = data.to_vec();
            prog_section_index = Some(idx);
            break;
        }
    }
    let prog_section_index =
        prog_section_index.ok_or_else(|| elf_err("no executable BPF section found"))?;

    // Process REL relocations targeting the program section.
    for section in sections.iter() {
        if section.sh_type(endian) != elf::SHT_REL {
            continue;
        }
        if section.sh_info(endian) as usize != prog_section_index {
            continue;
        }

        // Get symtab and strtab for this relocation section.
        let symtab_idx = section.sh_link(endian) as usize;
        let symtab_section = sections
            .section(object::read::SectionIndex(symtab_idx))
            .map_err(elf_err)?;
        let symtab_data = symtab_section.data(endian, elf_bytes).map_err(elf_err)?;

        let strtab_idx = symtab_section.sh_link(endian) as usize;
        let strtab_section = sections
            .section(object::read::SectionIndex(strtab_idx))
            .map_err(elf_err)?;
        let strtab_data = strtab_section.data(endian, elf_bytes).map_err(elf_err)?;
        let strtab = StringTable::new(strtab_data, 0, strtab_data.len() as u64);

        let rel_data = section.data(endian, elf_bytes).map_err(elf_err)?;

        // Each REL entry is 16 bytes for ELF64.
        let entry_size = mem::size_of::<elf::Rel64<LittleEndian>>();
        for chunk in rel_data.chunks_exact(entry_size) {
            let (rel, _): (&elf::Rel64<LittleEndian>, _) =
                object::pod::from_bytes(chunk).map_err(|_| elf_err("bad REL entry"))?;

            let offset = rel.r_offset(endian) as usize;
            let sym_idx = rel.r_sym(endian);

            // Look up the symbol.
            let sym_entry_size = mem::size_of::<elf::Sym64<LittleEndian>>();
            let sym_offset = sym_idx as usize * sym_entry_size;
            let (sym, _): (&elf::Sym64<LittleEndian>, _) =
                object::pod::from_bytes(&symtab_data[sym_offset..sym_offset + sym_entry_size])
                    .map_err(|_| elf_err("bad symbol entry"))?;

            let sym_name = strtab
                .get(sym.st_name(endian))
                .map_err(|_| elf_err("bad string table index"))?;

            // If the symbol name is empty, the symbol points into .maps.
            // Identify the map by its byte offset within that section.
            let map_name: &[u8] = if sym_name.is_empty() {
                let sec_idx = sym.st_shndx(endian) as usize;
                let target_sec = sections
                    .section(object::read::SectionIndex(sec_idx))
                    .map_err(elf_err)?;
                let sec_name = sections.section_name(endian, target_sec).map_err(elf_err)?;
                if sec_name == b".maps" {
                    match sym.st_value(endian) {
                        0 => b"sockarray",
                        32 => b"config",
                        v => {
                            return Err(elf_err(format!("unknown map at .maps offset {v}")));
                        }
                    }
                } else {
                    sec_name
                }
            } else {
                sym_name
            };

            // Find the map fd for this symbol.
            let map_fd = map_fds
                .iter()
                .find(|(n, _)| n.as_bytes() == map_name)
                .map(|(_, fd)| *fd)
                .ok_or_else(|| {
                    elf_err(format!(
                        "no map fd for symbol {:?}",
                        String::from_utf8_lossy(map_name)
                    ))
                })?;

            // Patch the LD_IMM64 instruction at `offset`.
            // BPF instruction layout (8 bytes each):
            //   [0] opcode  [1] dst:4|src:4  [2..4] off  [4..8] imm
            // LD_IMM64 spans two consecutive 8-byte slots.
            if offset + 16 > prog_data.len() || prog_data[offset] != BPF_LD_IMM64_OPCODE {
                return Err(elf_err(format!("expected LD_IMM64 at offset {offset}")));
            }
            // Set src_reg = BPF_PSEUDO_MAP_FD to tell the verifier this is a map fd.
            prog_data[offset + 1] = (prog_data[offset + 1] & 0x0F) | (BPF_PSEUDO_MAP_FD << 4);
            // Set imm (bytes 4..8 of the first slot) to the map fd.
            prog_data[offset + 4..offset + 8].copy_from_slice(&(map_fd as i32).to_le_bytes());
        }
    }

    // Load the program into the kernel.
    let insn_cnt = (prog_data.len() / 8) as u32;
    let insns_ptr = prog_data.as_ptr() as u64;

    let license = b"GPL\0";
    let mut log_buf = vec![0u8; 65536];

    #[repr(C)]
    #[derive(Default)]
    struct BpfProgLoadAttr {
        prog_type: u32,
        insn_cnt: u32,
        insns: u64,
        license: u64,
        log_level: u32,
        log_size: u32,
        log_buf: u64,
        kern_version: u32,
        prog_flags: u32,
        prog_name: [u8; 16],
        prog_ifindex: u32,
        expected_attach_type: u32,
    }

    let attr = BpfProgLoadAttr {
        prog_type: BPF_PROG_TYPE_SK_REUSEPORT,
        insn_cnt,
        insns: insns_ptr,
        license: license.as_ptr() as u64,
        log_level: 1,
        log_size: log_buf.len() as u32,
        log_buf: log_buf.as_mut_ptr() as u64,
        prog_name: {
            let mut name = [0u8; 16];
            let src = b"quic_reuseport";
            name[..src.len()].copy_from_slice(src);
            name
        },
        expected_attach_type: BPF_SK_REUSEPORT_SELECT,
        ..Default::default()
    };

    let fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_PROG_LOAD,
            &attr as *const _ as *const libc::c_void,
            mem::size_of::<BpfProgLoadAttr>(),
        )
    };

    if fd < 0 {
        let err = io::Error::last_os_error();
        let log_end = log_buf
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(log_buf.len());
        let log_str = String::from_utf8_lossy(&log_buf[..log_end]);
        eprintln!("BPF verifier log:\n{log_str}");
        return Err(err);
    }

    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ebpf_elf() {
        #[repr(C, align(8))]
        struct Aligned<const N: usize>([u8; N]);
        const BYTES: &Aligned<
            { include_bytes!(concat!(env!("OUT_DIR"), "/quic_reuseport.o")).len() },
        > = &Aligned(*include_bytes!(concat!(
            env!("OUT_DIR"),
            "/quic_reuseport.o"
        )));
        let endian = LittleEndian;
        let data: &[u8] = &BYTES.0;
        let header = elf::FileHeader64::<LittleEndian>::parse(data).expect("ELF parse failed");
        let sections = header.sections(endian, data).expect("sections");

        // Verify we can find an executable program section.
        let has_prog = sections.iter().any(|s| {
            s.sh_type(endian) == elf::SHT_PROGBITS
                && s.sh_flags(endian) & u64::from(elf::SHF_EXECINSTR) != 0
                && s.sh_size(endian) > 0
        });
        assert!(has_prog, "should find an executable BPF section");

        // Verify we can find a .maps section.
        let has_maps = sections.iter().any(|s| {
            sections
                .section_name(endian, s)
                .map(|n| n == b".maps")
                .unwrap_or(false)
        });
        assert!(has_maps, "should find a .maps section");
    }
}
