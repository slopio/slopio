use slopio::main_thread;
use slopio::runtime::{spawn, yield_now};
use std::sync::{Arc, Mutex};

#[test]
fn test_task_execution_order() {
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let order_clone = execution_order.clone();

    main_thread(async move {
        order_clone.lock().unwrap().push(1);

        let order_task = order_clone.clone();
        let handle = spawn(async move {
            order_task.lock().unwrap().push(2);
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
    let result = main_thread(async {
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
    let results = main_thread(async {
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

#[test]
fn test_yield_now_interleaves_tasks() {
    let order = Arc::new(Mutex::new(Vec::<u8>::new()));
    let order2 = order.clone();

    main_thread(async move {
        let handle = spawn(async move {
            order2.lock().unwrap().push(2);
        });

        order.lock().unwrap().push(1);
        yield_now().await;
        // The spawned task ran during the yield, so 2 is already in the vec
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);

        handle.await;
        order.lock().unwrap().push(3);
    });
}
