use runtime::main_thread;
use runtime::runtime::spawn;
use std::sync::{Arc, Mutex};

// Heavy allocation test - spawn many tasks that allocate memory
#[test]
fn test_heavy_allocations() {
    main_thread(async {
        let mut handles = Vec::new();

        // Spawn 500 tasks that each allocate significant memory
        for i in 0..500 {
            let handle = spawn(async move {
                // Allocate various sized buffers
                let _large_vec = vec![i as u64; 1000];
                let _boxed_data = Box::new([i as u8; 256]);
                let _string_data = format!("Task {} allocating memory", i).repeat(10);

                // Return some data to ensure it's used
                i * 2
            });
            handles.push(handle);
        }

        let mut sum = 0;
        for handle in handles {
            sum += handle.await;
        }

        assert!(sum > 0);
    });
}

// Test with nested spawns and allocations
#[test]
fn test_nested_allocations() {
    main_thread(async {
        let outer_handles: Vec<_> = (0..50)
            .map(|i| {
                spawn(async move {
                    let inner_handles: Vec<_> = (0..10)
                        .map(|j| {
                            spawn(async move {
                                let _data = vec![j as u64; 100];
                                let _boxed = Box::new(format!("Inner {}-{}", i, j));
                                j
                            })
                        })
                        .collect();

                    let mut sum = 0;
                    for h in inner_handles {
                        sum += h.await;
                    }
                    sum
                })
            })
            .collect();

        let mut total = 0;
        for h in outer_handles {
            total += h.await;
        }

        assert_eq!(total, 50 * 45); // Sum of 0..10 is 45, times 50 outer tasks
    });
}

// Test with shared allocations
#[test]
fn test_shared_allocations() {
    main_thread(async {
        let shared_data = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for i in 0..100 {
            let data_clone = shared_data.clone();
            let handle = spawn(async move {
                let mut guard = data_clone.lock().unwrap();
                // Each task allocates and stores data
                guard.push(vec![i as u64; 50]);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }

        let final_len = shared_data.lock().unwrap().len();
        assert_eq!(final_len, 100);
    });
}

// Test spawn without await (dropped handles)
#[test]
fn test_dropped_handles() {
    main_thread(async {
        // Spawn tasks but drop the handles
        for i in 0..200 {
            let _handle = spawn(async move {
                let _data = vec![i as u64; 100];
                let _boxed = Box::new([i as u8; 64]);
                i
            });
            // Handle dropped here
        }

        // Create a barrier to ensure all previous tasks complete
        let barrier = spawn(async { 42 });
        assert_eq!(barrier.await, 42);
    });
}

// Test rapid allocation/deallocation cycles
#[test]
fn test_allocation_cycles() {
    for _ in 0..10 {
        main_thread(async {
            let mut handles = Vec::new();

            for i in 0..100 {
                let handle = spawn(async move {
                    let _v1 = vec![i; 200];
                    let _v2 = Box::new(vec![i; 100]);
                    let _s = String::from("test").repeat(20);
                    i
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.await;
            }
        });
    }
}

// Test large allocations
#[test]
fn test_large_allocations() {
    main_thread(async {
        let mut handles = Vec::new();

        for i in 0..50 {
            let handle = spawn(async move {
                // Allocate 1MB per task
                let _large = vec![i as u8; 1024 * 1024];
                i
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }
    });
}

// Test mixed allocation patterns
#[test]
fn test_mixed_patterns() {
    main_thread(async {
        let counter = Arc::new(Mutex::new(0));
        let mut handles = Vec::new();

        for i in 0..200 {
            let c = counter.clone();
            let handle = spawn(async move {
                // Mix of stack and heap allocations
                let stack_array = [i; 10];
                let heap_vec = vec![i; 100];
                let boxed_string = Box::new(format!("Task {}", i));

                *c.lock().unwrap() += stack_array[0] + heap_vec[0];

                // Return complex type
                (heap_vec, boxed_string)
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }

        assert!(*counter.lock().unwrap() > 0);
    });
}

// Test string allocations
#[test]
fn test_string_allocations() {
    main_thread(async {
        let mut handles = Vec::new();

        for i in 0..300 {
            let handle = spawn(async move {
                let s1 = "Hello World".repeat(50);
                let s2 = format!("Task {} - {}", i, s1);
                let s3 = s2.clone();
                s3.len()
            });
            handles.push(handle);
        }

        let mut total_len = 0;
        for handle in handles {
            total_len += handle.await;
        }

        assert!(total_len > 0);
    });
}
