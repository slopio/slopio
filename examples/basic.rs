use runtime::io_driver::demo::Demo;

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    println!("begin");
    rt.block_on(async {
        let demo = Demo {};
        demo.await;
    });
    println!("end");
}
