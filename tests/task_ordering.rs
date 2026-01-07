use runtime::block_on;
use runtime::runtime::spawn;
use std::sync::{Arc, Mutex};

#[test]
fn test_task_execution_order() {
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let order_clone = execution_order.clone();

    block_on(async move {
        order_clone.lock().unwrap().push(1);

        let handle = spawn(async {
            order_clone.lock().unwrap().push(2);
        });

        order_clone.lock().unwrap().push(3);
        handle.await;
        order_clone.lock().unwrap().push(4);
    });

    let order = execution_order.lock().unwrap();
    assert_eq!(*order, vec![1, 3, 2, 4]);
}

#[test]
fn test_multiple_nested_tasks() {
    let result = block_on(async {
        let t1 = spawn(async {
            let t2 = spawn(async {
                let t3 = spawn(async { 1 });
                t3.await + 2
            });
            t2.await + 3
        });
        t1.await + 4
    });
    assert_eq!(result, 10);
}

#[test]
fn test_parallel_task_results() {
    let results = block_on(async {
        let mut handles = Vec::new();

        for i in 0..5 {
            let handle = spawn(async move { i * 2 });
            handles.push(handle);
        }

        let mut values = Vec::new();
        for handle in handles {
            values.push(handle.await);
        }
        values
    });

    assert_eq!(results, vec![0, 2, 4, 6, 8]);
}
