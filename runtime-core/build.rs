use std::path::Path;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest = format!("{out_dir}/ring_config.rs");

    // Look for ring_config.rs at the workspace root (one level above runtime-core/)
    let workspace_config = Path::new(&manifest_dir)
        .parent()
        .unwrap()
        .join("ring_config.rs");

    println!(
        "cargo:rerun-if-changed={}",
        workspace_config.display()
    );

    if workspace_config.exists() {
        std::fs::copy(&workspace_config, &dest).unwrap();
    } else {
        // Default: single 4 KB ring. Create ring_config.rs at the workspace root
        // with your own define_buffer_rings! invocation to override this.
        std::fs::write(
            &dest,
            "define_buffer_rings! {\n    Small { bgid: 0, buffer_size: 4096, ring_size: 1024 }\n}\n",
        )
        .unwrap();
    }
}
