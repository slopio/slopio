fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    let _ = rt.block_on(async { 3 });
}
