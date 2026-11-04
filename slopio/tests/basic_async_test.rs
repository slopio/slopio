use slopio::main_thread;
use slopio::runtime::spawn;

#[test]
fn test_block_on_simple() {
    let result = main_thread(async { 42 });
    assert_eq!(result, 42);
}

#[test]
fn test_spawn_single_task() {
    let result = main_thread(async {
        let handle = spawn(async { 100 });
        handle.await
    });
    assert_eq!(result, 100);
}

#[test]
fn test_spawn_multiple_tasks() {
    let result = main_thread(async {
        let h1 = spawn(async { 1 });
        let h2 = spawn(async { 2 });
        let h3 = spawn(async { 3 });

        let r1 = h1.await;
        let r2 = h2.await;
        let r3 = h3.await;

        r1 + r2 + r3
    });
    assert_eq!(result, 6);
}

#[test]
fn test_nested_spawn() {
    let result = main_thread(async {
        let outer = spawn(async {
            let inner = spawn(async { 10 });
            let value = inner.await;
            value * 2
        });
        outer.await
    });
    assert_eq!(result, 20);
}

#[test]
fn test_async_computation() {
    let result = main_thread(async {
        let a = spawn(async { 5 });
        let b = spawn(async { 7 });

        a.await + b.await
    });
    assert_eq!(result, 12);
}
