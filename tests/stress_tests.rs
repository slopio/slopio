use runtime::main_thread;
use runtime::runtime::spawn;
use std::sync::{Arc, Mutex};

// Test spawning a massive number of tasks
#[test]
fn test_spawn_many_tasks() {
    let result = main_thread(async {
        let mut handles = Vec::new();

        // Spawn 1000 tasks
        for i in 0..1000 {
            let handle = spawn(async move { i });
            handles.push(handle);
        }

        let mut sum = 0;
        for handle in handles {
            sum += handle.await;
        }
        sum
    });

    // Expected: sum of 0..1000 = 999*1000/2 = 499500
    assert_eq!(result, 499500);
}

// Test deeply nested spawns
#[test]
fn test_deeply_nested_spawns() {
    let result = main_thread(async {
        // Create a chain of 100 nested spawns
        let mut task = spawn(async { 1 });

        for _ in 0..99 {
            task = spawn(async move {
                let val = task.await;
                val + 1
            });
        }

        task.await
    });

    assert_eq!(result, 100);
}

// Test spawning tasks from multiple spawned tasks (exponential growth)
#[test]
fn test_exponential_task_spawn() {
    let result = main_thread(async {
        let counter = Arc::new(Mutex::new(0));

        let handle = spawn_recursive(counter.clone(), 0, 6); // 2^6 = 64 tasks
        handle.await;

        *counter.lock().unwrap()
    });

    // Each level doubles: 1 + 2 + 4 + 8 + 16 + 32 + 64 = 127
    assert_eq!(result, 127);
}

async fn spawn_recursive(counter: Arc<Mutex<i32>>, depth: u32, max_depth: u32) {
    *counter.lock().unwrap() += 1;

    if depth < max_depth {
        let c1 = counter.clone();
        let c2 = counter.clone();

        let h1 = spawn(async move {
            spawn_recursive(c1, depth + 1, max_depth).await;
        });

        let h2 = spawn(async move {
            spawn_recursive(c2, depth + 1, max_depth).await;
        });

        h1.await;
        h2.await;
    }
}

// Test immediately completing tasks
#[test]
fn test_immediate_completion() {
    let result = main_thread(async {
        let mut handles = Vec::new();

        for i in 0..100 {
            // Tasks that complete immediately
            let handle = spawn(async move { i * 2 });
            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await);
        }
        results
    });

    assert_eq!(result.len(), 100);
    for (i, &val) in result.iter().enumerate() {
        assert_eq!(val, i * 2);
    }
}

// Test interleaved spawning and awaiting
#[test]
fn test_interleaved_spawn_await() {
    let result = main_thread(async {
        let mut sum = 0;

        for i in 0..50 {
            let handle = spawn(async move { i });
            sum += handle.await;
            let handle2 = spawn(async move { i * 2 });
            sum += handle2.await;
        }

        sum
    });

    // Sum of i + 2*i for i in 0..50 = sum of 3*i for i in 0..50 = 3 * (49*50/2) = 3675
    assert_eq!(result, 3675);
}

// Test spawning tasks that share data through Arc<Mutex<>>
#[test]
fn test_shared_state_contention() {
    let result = main_thread(async {
        let shared = Arc::new(Mutex::new(0));
        let mut handles = Vec::new();

        for _ in 0..100 {
            let shared_clone = shared.clone();
            let handle = spawn(async move {
                for _ in 0..10 {
                    *shared_clone.lock().unwrap() += 1;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }

        *shared.lock().unwrap()
    });

    assert_eq!(result, 1000);
}

// Test tasks that create more tasks while others are running
#[test]
fn test_dynamic_task_creation() {
    let result = main_thread(async {
        let counter = Arc::new(Mutex::new(0));
        let mut handles = Vec::new();

        // Initial batch
        for _ in 0..10 {
            let c = counter.clone();
            let handle = spawn(async move {
                *c.lock().unwrap() += 1;

                // Each task spawns 2 more tasks
                let c2 = c.clone();
                let h1 = spawn(async move {
                    *c2.lock().unwrap() += 1;
                });

                let c3 = c.clone();
                let h2 = spawn(async move {
                    *c3.lock().unwrap() += 1;
                });

                h1.await;
                h2.await;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }

        *counter.lock().unwrap()
    });

    // 10 initial tasks + (10 * 2) spawned tasks = 30 increments
    assert_eq!(result, 30);
}

// Test empty async blocks
#[test]
fn test_empty_tasks() {
    main_thread(async {
        let mut handles = Vec::new();

        for _ in 0..100 {
            let handle = spawn(async {});
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }
    });
}

// Test tasks that return large values
#[test]
fn test_large_return_values() {
    let result = main_thread(async {
        let handle = spawn(async { vec![42u64; 1000] });

        handle.await
    });

    assert_eq!(result.len(), 1000);
    assert_eq!(result[0], 42);
    assert_eq!(result[999], 42);
}

// Test spawning tasks in a loop with dependencies
#[test]
fn test_sequential_dependencies() {
    let result = main_thread(async {
        let mut value = 0;

        for i in 0..50 {
            let handle = spawn(async move { i * 2 });
            value += handle.await;
        }

        value
    });

    // Sum of 2*i for i in 0..50 = 2 * (49*50/2) = 2450
    assert_eq!(result, 2450);
}

// Test rapid spawn and drop without awaiting
#[test]
fn test_spawn_without_await() {
    main_thread(async {
        for _ in 0..100 {
            let _handle = spawn(async { 42 });
            // Don't await, just drop the handle
        }

        // Give tasks a chance to complete
        let barrier = spawn(async { 1 });
        barrier.await;
    });
}

// Test alternating between block_on calls
#[test]
fn test_multiple_block_on_calls() {
    for i in 0..10 {
        let result = main_thread(async move {
            let handle = spawn(async move { i * 10 });
            handle.await
        });
        assert_eq!(result, i * 10);
    }
}

// Test nested block_on equivalent through multiple runtimes
#[test]
fn test_nested_runtime_simulation() {
    let result = main_thread(async {
        let outer_value = 10;

        let handle = spawn(async move {
            let inner_value = 20;
            let handle2 = spawn(async move { outer_value + inner_value });
            handle2.await
        });

        handle.await
    });

    assert_eq!(result, 30);
}

// Test spawning tasks that panic (should be handled gracefully)
#[test]
#[should_panic]
fn test_task_panic() {
    main_thread(async {
        let handle = spawn(async {
            panic!("Task intentionally panics");
        });

        handle.await
    });
}

// Test zero-sized types
#[test]
fn test_zero_sized_returns() {
    main_thread(async {
        let mut handles = Vec::new();

        for _ in 0..100 {
            let handle = spawn(async {});
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }
    });
}

// Test tasks with complex lifetime interactions
#[test]
fn test_complex_data_flow() {
    let result = main_thread(async {
        let data = vec![1, 2, 3, 4, 5];
        let sum = Arc::new(Mutex::new(0));

        let mut handles = Vec::new();

        for &value in &data {
            let sum_clone = sum.clone();
            let handle = spawn(async move {
                let squared = value * value;
                *sum_clone.lock().unwrap() += squared;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }

        *sum.lock().unwrap()
    });

    // 1^2 + 2^2 + 3^2 + 4^2 + 5^2 = 1 + 4 + 9 + 16 + 25 = 55
    assert_eq!(result, 55);
}

// Test tasks creating circular-ish dependencies through shared state
#[test]
fn test_task_coordination() {
    let result = main_thread(async {
        let flags = Arc::new(Mutex::new(vec![false; 10]));
        let mut handles = Vec::new();

        for i in 0..10 {
            let flags_clone = flags.clone();
            let handle = spawn(async move {
                // Mark this task as done
                flags_clone.lock().unwrap()[i] = true;

                // Wait for all tasks to be spawned (simple coordination)
                loop {
                    let all_spawned = flags_clone.lock().unwrap().len() == 10;
                    if all_spawned {
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }

        flags.lock().unwrap().iter().all(|&x| x)
    });

    assert!(result);
}

// Test mixed async and sync operations
#[test]
fn test_mixed_operations() {
    let result = main_thread(async {
        let mut results = Vec::new();

        for i in 0..20 {
            if i % 2 == 0 {
                // Async path
                let handle = spawn(async move { i * 2 });
                results.push(handle.await);
            } else {
                // Sync path
                results.push(i * 2);
            }
        }

        results
    });

    for (i, &val) in result.iter().enumerate() {
        assert_eq!(val, i * 2);
    }
}
