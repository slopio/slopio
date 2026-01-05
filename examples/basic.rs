use runtime::runtime::spawn;

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    rt.block_on(async {
        let r = spawn(async {
            let uwu = spawn(async {
                println!("3");
            });
            println!("2");
            uwu.await;
            println!("4");
            5
        });
        println!("1");
        let x = r.await;
        println!("{x}");
    });
}
