use runtime::io_driver::operations::files::open::{flags, mode};
use runtime::io_driver::operations::files::open_file;
use runtime::main_thread;
use std::fs;

#[test]
fn test_file_write_and_read() {
    let test_file = "/tmp/test_write_read.txt";

    let _ = fs::remove_file(test_file);

    main_thread(async {
        // Write to file
        let file = open_file(
            test_file,
            flags::WRITE_ONLY | flags::CREATE | flags::TRUNCATE,
            mode::DEFAULT,
        )
        .await
        .unwrap();

        let data = b"Hello from async runtime!";
        let bytes_written = file.write(data).await.0.unwrap();
        assert_eq!(bytes_written, data.len());

        drop(file);

        // Read from file
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        let buffer: Vec<u8> = Vec::with_capacity(128);
        let (result, buffer) = file.read(buffer).await;
        let bytes_read = result.unwrap();

        assert_eq!(bytes_read, data.len());
        assert_eq!(&buffer[..], data);
    });

    // Clean up
    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_create_write() {
    let test_file = "/tmp/test_create.txt";
    let _ = fs::remove_file(test_file);

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::CREATE, mode::RW_USER)
            .await
            .unwrap();

        let data = b"Test content";
        let n = file.write(data).await.0.unwrap();

        assert_eq!(n, data.len());
    });

    // Verify file exists and has correct content
    let content = fs::read_to_string(test_file).unwrap();
    assert_eq!(content, "Test content");

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_read_existing() {
    let test_file = "/tmp/test_read.txt";

    // Create file with std::fs
    fs::write(test_file, b"Existing content").unwrap();

    main_thread(async {
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        let buffer: Vec<u8> = Vec::with_capacity(64);
        let (result, buffer) = file.read(buffer).await;
        let _n = result.unwrap();

        assert_eq!(&buffer[..], b"Existing content");
    });

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_append() {
    let test_file = "/tmp/test_append.txt";

    // Create initial file
    fs::write(test_file, b"First line\n").unwrap();

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::APPEND, 0)
            .await
            .unwrap();

        let data = b"Second line\n";
        file.write(data).await.0.unwrap();
    });

    // Verify content
    let content = fs::read_to_string(test_file).unwrap();
    assert_eq!(content, "First line\nSecond line\n");

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_truncate() {
    let test_file = "/tmp/test_truncate.txt";

    // Create file with content
    fs::write(test_file, b"Old content that should be removed").unwrap();

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::TRUNCATE, 0)
            .await
            .unwrap();

        let data = b"New";
        file.write(data).await.0.unwrap();
    });

    // Verify file was truncated
    let content = fs::read_to_string(test_file).unwrap();
    assert_eq!(content, "New");

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_read_write() {
    let test_file = "/tmp/test_rw.txt";
    let _ = fs::remove_file(test_file);

    main_thread(async {
        // Open for reading and writing
        let file = open_file(test_file, flags::READ_WRITE | flags::CREATE, mode::DEFAULT)
            .await
            .unwrap();

        // Write
        let data = b"Read and write test";
        file.write(data).await.0.unwrap();

        drop(file);

        // Read back (need to reopen for now since we don't have seek)
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        let buffer: Vec<u8> = Vec::with_capacity(64);
        let (result, buffer) = file.read(buffer).await;
        let _n = result.unwrap();

        assert_eq!(&buffer[..], data);
    });

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_multiple_writes() {
    let test_file = "/tmp/test_multi_write.txt";
    let _ = fs::remove_file(test_file);

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::CREATE, mode::DEFAULT)
            .await
            .unwrap();

        // Multiple writes
        file.write(b"First ").await.0.unwrap();
        file.write(b"Second ").await.0.unwrap();
        file.write(b"Third").await.0.unwrap();
    });

    let content = fs::read_to_string(test_file).unwrap();
    assert_eq!(content, "First Second Third");

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_empty_write() {
    let test_file = "/tmp/test_empty.txt";
    let _ = fs::remove_file(test_file);

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::CREATE, mode::DEFAULT)
            .await
            .unwrap();

        let n = file.write(b"").await.0.unwrap();
        assert_eq!(n, 0);
    });

    let metadata = fs::metadata(test_file).unwrap();
    assert_eq!(metadata.len(), 0);

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_large_write() {
    let test_file = "/tmp/test_large.txt";
    let _ = fs::remove_file(test_file);

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::CREATE, mode::DEFAULT)
            .await
            .unwrap();

        // Write 1MB
        let data = vec![b'X'; 1024 * 1024];
        let expected_len = data.len();
        let n = file.write(data).await.0.unwrap();

        assert_eq!(n, expected_len);
    });

    let metadata = fs::metadata(test_file).unwrap();
    assert_eq!(metadata.len(), 1024 * 1024);

    let _ = fs::remove_file(test_file);
}

#[test]
fn test_file_read_chunks() {
    let test_file = "/tmp/test_chunks.txt";

    // Create file with known content
    fs::write(test_file, b"0123456789ABCDEF").unwrap();

    main_thread(async {
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        // Read in small chunks
        let buffer1: Vec<u8> = Vec::with_capacity(5);
        let (result1, buffer1) = file.read(buffer1).await;
        let n1 = result1.unwrap();
        assert_eq!(n1, 5);
        assert_eq!(&buffer1[..], b"01234");

        // Note: without seek, subsequent reads continue from current position
        let buffer2: Vec<u8> = Vec::with_capacity(5);
        let (result2, buffer2) = file.read(buffer2).await;
        let n2 = result2.unwrap();
        assert_eq!(n2, 5);
        assert_eq!(&buffer2[..], b"56789");
    });

    let _ = fs::remove_file(test_file);
}
