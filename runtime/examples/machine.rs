//! Prints what the runtime reads of this machine's CPUs and NICs, without
//! privilege: where it would find the CPUs that process the NICs' packets.
//!
//! ```text
//! cargo run --release --example machine
//! ```

fn main() {
    println!("{}", runtime::topology::Machine::read());
}
