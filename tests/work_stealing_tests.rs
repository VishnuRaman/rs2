
use rs2::rs2::*;
use rs2::work_stealing::{WorkStealingExt, WorkStealingConfig};
use futures_util::stream::StreamExt;
use tokio::time::{sleep, timeout};
use std::time::Duration;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::sync::atomic::AtomicBool;

/// Test 1: Basic work stealing functionality
#[tokio::test]
async fn test_basic_work_stealing() {
    println!("🚀 Starting basic work stealing test");

    let item_count = 100; // Reduced for faster tests
    let source_data: Vec<usize> = (0..item_count).collect();
    let processed_count = Arc::new(AtomicUsize::new(0));

    // Add timeout to prevent hanging
    let result = timeout(Duration::from_secs(30), async {
        let stream = from_iter(source_data.clone())
            .par_eval_map_work_stealing_rs2({
                let processed_count = processed_count.clone();
                move |x| {
                    let processed_count = processed_count.clone();
                    async move {
                        // Simulate varying workloads
                        let sleep_duration = if x % 10 == 0 {
                            Duration::from_millis(5) // Reduced from 10ms
                        } else {
                            Duration::from_millis(1)
                        };

                        sleep(sleep_duration).await;
                        processed_count.fetch_add(1, Ordering::SeqCst);
                        x * 2
                    }
                }
            });

        stream.collect::<Vec<_>>().await
    }).await.expect("Test should complete within timeout");

    // Verify results
    assert_eq!(result.len(), item_count, "Should have processed all items");
    assert_eq!(processed_count.load(Ordering::SeqCst), item_count,
               "Processed count should equal the number of items");

    // Verify order preservation and correct transformation
    for (i, &result_value) in result.iter().enumerate() {
        assert_eq!(result_value, source_data[i] * 2,
                   "Result at index {} should be {} * 2", i, source_data[i]);
    }

    println!("✅ Basic work stealing test passed");
}

/// Test 2: Work stealing with CPU-intensive tasks
#[tokio::test]
async fn test_work_stealing_cpu_intensive() {
    println!("🚀 Starting work stealing CPU-intensive test");

    let item_count = 50; // Reduced for CPU-intensive tasks
    let source_data: Vec<usize> = (0..item_count).collect();
    let processed_count = Arc::new(AtomicUsize::new(0));

    let result = timeout(Duration::from_secs(45), async {
        let stream = from_iter(source_data.clone())
            .par_eval_map_cpu_intensive_rs2({
                let processed_count = processed_count.clone();
                move |x| {
                    let processed_count = processed_count.clone();
                    async move {
                        // Simulate CPU-intensive work (reduced iterations)
                        let mut result = x;
                        for _ in 0..5000 { // Reduced from 10000
                            result = result.wrapping_mul(31).wrapping_add(17);
                        }

                        processed_count.fetch_add(1, Ordering::SeqCst);
                        result
                    }
                }
            });

        stream.collect::<Vec<_>>().await
    }).await.expect("CPU-intensive test should complete within timeout");

    assert_eq!(result.len(), item_count, "Should have processed all items");
    assert_eq!(processed_count.load(Ordering::SeqCst), item_count,
               "Processed count should equal the number of items");

    println!("✅ Work stealing CPU-intensive test passed");
}

/// Test 3: Work stealing with custom configuration
#[tokio::test]
async fn test_work_stealing_custom_config() {
    println!("🚀 Starting work stealing custom config test");

    let item_count = 50; // Reduced for faster execution
    let source_data: Vec<usize> = (0..item_count).collect();
    let processed_count = Arc::new(AtomicUsize::new(0));

    let config = WorkStealingConfig {
        num_workers: Some(2), // Fewer workers for deterministic testing
        local_queue_size: 4,  // Small queue
        steal_interval_ms: 1, // Fast stealing
        use_blocking: false,
    };

    let result = timeout(Duration::from_secs(30), async {
        let stream = from_iter(source_data.clone())
            .par_eval_map_work_stealing_with_config_rs2(
                config,
                {
                    let processed_count = processed_count.clone();
                    move |x| {
                        let processed_count = processed_count.clone();
                        async move {
                            let sleep_duration = if x % 10 == 0 {
                                Duration::from_millis(10) // Some items take longer
                            } else {
                                Duration::from_millis(1)
                            };

                            sleep(sleep_duration).await;
                            processed_count.fetch_add(1, Ordering::SeqCst);
                            x * 3
                        }
                    }
                }
            );

        stream.collect::<Vec<_>>().await
    }).await.expect("Custom config test should complete within timeout");

    assert_eq!(result.len(), item_count, "Should have processed all items");
    assert_eq!(processed_count.load(Ordering::SeqCst), item_count,
               "Processed count should equal the number of items");

    // Verify transformation and order
    for (i, &result_value) in result.iter().enumerate() {
        assert_eq!(result_value, source_data[i] * 3,
                   "Result at index {} should be {} * 3", i, source_data[i]);
    }

    println!("✅ Work stealing custom config test passed");
}

/// Test 4: Work stealing load balancing (FIXED!)
#[tokio::test]
async fn test_work_stealing_load_balancing() {
    println!("🚀 Starting work stealing load balancing test");

    let item_count = 40;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Track which worker processes which tasks (by thread ID)
    let worker_assignments = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let processed_count = Arc::new(AtomicUsize::new(0));

    let config = WorkStealingConfig {
        num_workers: Some(4),
        local_queue_size: 2, // Very small queue to force work sharing
        steal_interval_ms: 1,
        use_blocking: false,
    };

    let result = timeout(Duration::from_secs(30), async {
        let stream = from_iter(source_data.clone())
            .par_eval_map_work_stealing_with_config_rs2(
                config,
                {
                    let worker_assignments = worker_assignments.clone();
                    let processed_count = processed_count.clone();
                    move |x| {
                        let worker_assignments = worker_assignments.clone();
                        let processed_count = processed_count.clone();
                        async move {
                            // Record which thread processes this task
                            let thread_id = std::thread::current().id();
                            {
                                let mut assignments = worker_assignments.lock().await;
                                assignments.push((x, thread_id));
                            }

                            // Create uneven workload - first 10 items are very slow
                            let sleep_duration = if x < 10 {
                                Duration::from_millis(20) // Very slow tasks
                            } else {
                                Duration::from_millis(1)  // Fast tasks
                            };

                            sleep(sleep_duration).await;
                            processed_count.fetch_add(1, Ordering::SeqCst);
                            x
                        }
                    }
                }
            );

        stream.collect::<Vec<_>>().await
    }).await.expect("Load balancing test should complete within timeout");

    // Verify all items processed
    assert_eq!(result.len(), item_count, "Should have processed all items");
    assert_eq!(processed_count.load(Ordering::SeqCst), item_count,
               "Processed count should equal the number of items");

    // Check work distribution
    let assignments = worker_assignments.lock().await;
    let unique_threads: std::collections::HashSet<_> =
        assignments.iter().map(|(_, thread_id)| *thread_id).collect();

    println!("Work distributed across {} threads", unique_threads.len());

    // With work stealing and uneven load, we should see multiple threads working
    assert!(unique_threads.len() >= 2,
            "Work stealing should distribute work across multiple threads");

    println!("✅ Work stealing load balancing test passed");
}

/// Test 5: Edge cases
#[tokio::test]
async fn test_work_stealing_edge_cases() {
    println!("🚀 Starting work stealing edge cases test");

    // Test empty stream
    let empty_result = timeout(Duration::from_secs(5), async {
        from_iter(std::iter::empty::<i32>())
            .par_eval_map_work_stealing_rs2(|x| async move { x * 2 })
            .collect::<Vec<_>>()
            .await
    }).await.expect("Empty stream test should complete quickly");

    assert_eq!(empty_result.len(), 0, "Empty stream should produce empty result");

    // Test single item
    let single_result = timeout(Duration::from_secs(5), async {
        from_iter(std::iter::once(42))
            .par_eval_map_work_stealing_rs2(|x| async move {
                sleep(Duration::from_millis(10)).await;
                x * 2
            })
            .collect::<Vec<_>>()
            .await
    }).await.expect("Single item test should complete");

    assert_eq!(single_result, vec![84], "Single item should be processed correctly");

    // Test with many workers, few items
    let few_items_result = timeout(Duration::from_secs(10), async {
        let config = WorkStealingConfig {
            num_workers: Some(8), // More workers than items
            local_queue_size: 1,
            steal_interval_ms: 1,
            use_blocking: false,
        };

        from_iter(0..3)
            .par_eval_map_work_stealing_with_config_rs2(config, |x| async move {
                sleep(Duration::from_millis(5)).await;
                x * 10
            })
            .collect::<Vec<_>>()
            .await
    }).await.expect("Few items test should complete");

    assert_eq!(few_items_result, vec![0, 10, 20], "Few items should be processed in order");

    println!("✅ Work stealing edge cases test passed");
}

/// Test 6: Performance comparison (optional)
#[tokio::test]
async fn test_work_stealing_vs_regular_parallel() {
    println!("🚀 Starting work stealing vs regular parallel test");

    let item_count = 50;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create highly uneven workload
    let create_workload = |x: usize| async move {
        let sleep_duration = if x < 10 {
            Duration::from_millis(50) // First 10 are very slow
        } else {
            Duration::from_millis(1)  // Rest are fast
        };
        sleep(sleep_duration).await;
        x
    };

    // Test regular parallel processing
    let start = std::time::Instant::now();
    let regular_result = timeout(Duration::from_secs(60), async {
        from_iter(source_data.clone())
            .par_eval_map_rs2(4, create_workload)
            .collect::<Vec<_>>()
            .await
    }).await.expect("Regular parallel should complete");
    let regular_time = start.elapsed();

    // Test work stealing
    let start = std::time::Instant::now();
    let ws_result = timeout(Duration::from_secs(60), async {
        from_iter(source_data.clone())
            .par_eval_map_work_stealing_rs2(create_workload)
            .collect::<Vec<_>>()
            .await
    }).await.expect("Work stealing should complete");
    let ws_time = start.elapsed();

    // Both should produce same results
    assert_eq!(regular_result, ws_result, "Both methods should produce same results");

    println!("Regular parallel: {:?}", regular_time);
    println!("Work stealing: {:?}", ws_time);

    // Work stealing should be competitive or better for uneven workloads
    // (This is more of an observation than a strict test)
    if ws_time < regular_time {
        println!("✨ Work stealing was faster for uneven workload!");
    } else {
        println!("📊 Regular parallel was faster (may vary by system load)");
    }

    println!("✅ Work stealing vs regular parallel test passed");
}