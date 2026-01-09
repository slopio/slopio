use runtime::thread_per_core;

fn main() {
    thread_per_core(|| async {
        println!("XD");
    });
}
