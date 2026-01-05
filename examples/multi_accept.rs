use runtime::io_driver::operations::StreamExt;
use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::runtime::spawn;

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    rt.block_on(async {
        let ma = MultiAccept::new(8080).unwrap();
        let mut ma = std::pin::pin!(ma);
        while let Some(c) = ma.next().await {
            let k = spawn(async move {
                println!("hello {c:?}");
                loop {}
            });
        }
    });
}
