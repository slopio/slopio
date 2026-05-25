use runtime::{
    io_driver::operations::files::{
        open::{flags, mode},
        open_file,
    },
    main_thread,
    runtime::join::join,
};

#[test]
fn join_test() {
    let result = main_thread(async {
        let u = join(async { 2 }, async { 3 });
        u.await
    });
    assert_eq!(result, (2, 3));
}

#[test]
fn join_io_test() {
    main_thread(async {
        let test_file = "/tmp/test_write_read22.txt";
        let test_file2 = "/tmp/test_write_read23.txt";

        let file1 = open_file(
            test_file,
            flags::WRITE_ONLY | flags::CREATE | flags::TRUNCATE,
            mode::DEFAULT,
        );
        let file2 = open_file(
            test_file2,
            flags::WRITE_ONLY | flags::CREATE | flags::TRUNCATE,
            mode::DEFAULT,
        );
        let (Ok(f1), Ok(f2)) = join(file1, file2).await else {
            panic!("afdsf");
        };

        let w1 = f1.write(b"salut");
        let w2 = f2.write(b"salut");
        let ((r1_res, _), (r2_res, _)) = join(w1, w2).await;
        let r1_res = r1_res.unwrap();
        let r2_res = r2_res.unwrap();
        assert_eq!(r1_res, r2_res);
    });
}
