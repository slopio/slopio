use runtime::io_driver::operations::files::open::{flags, mode};
use runtime::io_driver::operations::files::{AsyncFileRead, AsyncFileWrite, open_file};
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
        let bytes_written = file.write(data).await.unwrap();
        assert_eq!(bytes_written, data.len());

        drop(file);

        // Read from file
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        let mut buffer = vec![0u8; 128];
        let bytes_read = file.read(&mut buffer, 128).await.unwrap();

        assert_eq!(bytes_read, data.len());
        assert_eq!(&buffer[..bytes_read], data);
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
        let n = file.write(data).await.unwrap();

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

        let mut buffer = vec![0u8; 64];
        let n = file.read(&mut buffer, 64).await.unwrap();

        assert_eq!(&buffer[..n], b"Existing content");
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
        file.write(data).await.unwrap();
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
        file.write(data).await.unwrap();
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
        file.write(data).await.unwrap();

        drop(file);

        // Read back (need to reopen for now since we don't have seek)
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        let mut buffer = vec![0u8; 64];
        let n = file.read(&mut buffer, 64).await.unwrap();

        assert_eq!(&buffer[..n], data);
    });

    let _ = fs::remove_file(test_file);
}

#[test]
#[ignore] // TODO: Requires file position tracking (seek support)
fn test_file_multiple_writes() {
    let test_file = "/tmp/test_multi_write.txt";
    let _ = fs::remove_file(test_file);

    main_thread(async {
        let file = open_file(test_file, flags::WRITE_ONLY | flags::CREATE, mode::DEFAULT)
            .await
            .unwrap();

        // Multiple writes
        file.write(b"First ").await.unwrap();
        file.write(b"Second ").await.unwrap();
        file.write(b"Third").await.unwrap();
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

        let n = file.write(b"").await.unwrap();
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
        let n = file.write(&data).await.unwrap();

        assert_eq!(n, data.len());
    });

    let metadata = fs::metadata(test_file).unwrap();
    assert_eq!(metadata.len(), 1024 * 1024);

    let _ = fs::remove_file(test_file);
}

#[test]
#[ignore] // TODO: Requires file position tracking (seek support)
fn test_file_read_chunks() {
    let test_file = "/tmp/test_chunks.txt";

    // Create file with known content
    fs::write(test_file, b"0123456789ABCDEF").unwrap();

    main_thread(async {
        let file = open_file(test_file, flags::READ_ONLY, 0).await.unwrap();

        // Read in small chunks
        let mut buffer1 = vec![0u8; 5];
        let n1 = file.read(&mut buffer1, 5).await.unwrap();
        assert_eq!(n1, 5);
        assert_eq!(&buffer1[..n1], b"01234");

        // Note: without seek, subsequent reads continue from current position
        let mut buffer2 = vec![0u8; 5];
        let n2 = file.read(&mut buffer2, 5).await.unwrap();
        assert_eq!(n2, 5);
        assert_eq!(&buffer2[..n2], b"56789");
    });

    let _ = fs::remove_file(test_file);
}
