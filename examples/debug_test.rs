use runtime::io_driver::operations::files::open::{flags, mode};
use runtime::io_driver::operations::files::{AsyncFileWrite, open_file};
use runtime::main_thread;

fn main() {
    let test_file = "/tmp/test_create_debug.txt";
    let _ = std::fs::remove_file(test_file);

    eprintln!("Starting main_thread...");
    main_thread(async {
        eprintln!("Inside async block, about to open file...");
        let file = open_file(test_file, flags::WRITE_ONLY | flags::CREATE, mode::RW_USER)
            .await
            .unwrap();
        eprintln!("File opened!");

        let data = b"Test content";
        eprintln!("About to write...");
        let n = file.write(data).await.unwrap();
        eprintln!("Write completed! bytes={}", n);

        assert_eq!(n, data.len());
    });
    eprintln!("main_thread finished");
}
