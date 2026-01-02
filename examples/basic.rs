use runtime::runtime::spawn;

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    rt.block_on(async {
        let r = spawn(async {
            let uwu = spawn(async {
                println!("zuzu2");
            });
            println!("pololo");
            uwu.await;
            println!("pololo2");
            3
        });
        println!("owo");
        let x = r.await;
        println!(">>> {x}");
    });
}
