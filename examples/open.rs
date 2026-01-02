use runtime::io::files::open_file;

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    let k = rt.block_on(async {
        let f = open_file("src/lib.rs", 0, 0).await.unwrap();
        println!("{f:?}");
    });
}
