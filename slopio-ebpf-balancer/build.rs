use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let src = "ebpf/quic_reuseport.c";
    let out = out_dir.join("quic_reuseport.o");

    println!("cargo:rerun-if-changed={src}");
    println!("cargo:rerun-if-env-changed=DOCS_RS");

    // docs.rs builds without clang; an empty object is enough to render the docs.
    if env::var_os("DOCS_RS").is_some() {
        std::fs::write(&out, []).unwrap();
        return;
    }

    let arch = match env::consts::ARCH {
        "x86_64" => "x86",
        "aarch64" => "arm64",
        other => other,
    };
    let arch_flag = format!("-D__TARGET_ARCH_{arch}");

    let status = Command::new("clang")
        .args([
            "-target",
            "bpf",
            "-O2",
            "-g",
            &arch_flag,
            "-isystem",
            &format!("/usr/include/{}-linux-gnu", env::consts::ARCH),
            "-c",
            src,
            "-o",
            out.to_str().unwrap(),
        ])
        .status()
        .expect("failed to run clang — is it installed?");

    if !status.success() {
        panic!("clang failed to compile {src}");
    }
}
