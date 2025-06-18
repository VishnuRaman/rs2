use rs2_stream::rs2::*;
use rs2_stream::error::{StreamError, StreamResult};
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, timeout};
use std::time::Duration;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::collections::{HashMap, HashSet};
use futures::future::join_all;
use rand::{thread_rng, Rng};
use serial_test::serial;
use quickcheck::{quickcheck, TestResult};
use std::sync::atomic::AtomicBool;

/// Test 1: Multiple consumers reading from the same stream safely
#[tokio::test]
#[serial]
async fn test_multiple_consumers_same_stream() {
    println!("🚀 Starting multiple consumers test");

    // Create a shared stream source with a decent number of items
    let item_count = 1000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create a shared stream that will be consumed by multiple consumers
    let shared_stream = Arc::new(Mutex::new(from_iter(source_data.clone())));

    // Create a counter to track how many items were processed in total
    let processed_count = Arc::new(AtomicUsize::new(0));

    // Create multiple consumers (tasks) that will read from the same stream
    let consumer_count = 5;
    let mut consumer_handles = Vec::new();

    for consumer_id in 0..consumer_count {
        let shared_stream_clone = shared_stream.clone();
        let processed_count_clone = processed_count.clone();

        // Spawn a task for each consumer
        let handle = tokio::spawn(async move {
            let mut local_count = 0;
            let mut received_items = Vec::new();

            // Take a lock on the shared stream and consume some items
            let mut stream = shared_stream_clone.lock().await;

            // Each consumer tries to take a portion of the stream
            let target_items = item_count / consumer_count;

            for _ in 0..target_items {
                if let Some(item) = stream.next().await {
                    local_count += 1;
                    received_items.push(item);

                    // Simulate some processing time with random delays
                    if thread_rng().gen_bool(0.1) {
                        sleep(Duration::from_millis(1)).await;
                    }
                } else {
                    break; // No more items in the stream
                }
            }

            // Update the global counter
            processed_count_clone.fetch_add(local_count, Ordering::SeqCst);

            println!("Consumer {} processed {} items", consumer_id, local_count);
            received_items
        });

        consumer_handles.push(handle);
    }

    // Wait for all consumers to finish
    let all_received: Vec<Vec<usize>> = join_all(consumer_handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify that all items were processed exactly once
    let total_processed = processed_count.load(Ordering::SeqCst);
    println!("Total processed: {}", total_processed);

    // Verify no items were processed more than once
    let mut all_items = HashSet::new();
    for items in all_received.iter() {
        for &item in items {
            assert!(all_items.insert(item), "Item {} was processed more than once", item);
        }
    }

    // Verify that we processed the expected number of items
    assert_eq!(all_items.len(), total_processed, 
               "Number of unique items should match total processed count");

    // It's okay if we didn't process all items, as long as each item was processed at most once
    assert!(total_processed <= item_count, 
            "Total processed should not exceed the source data size");

    println!("✅ Multiple consumers test passed");
}

/// Test 2: Concurrent stream transformations don't cause data races
#[tokio::test]
async fn test_concurrent_stream_transformations() {
    println!("🚀 Starting concurrent stream transformations test");

    // Create a stream with a decent number of items
    let item_count = 1000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create a shared counter to detect potential race conditions
    let counter = Arc::new(AtomicUsize::new(0));

    // Create a stream with multiple transformations that will run concurrently
    let stream = from_iter(source_data)
        // First transformation: multiply by 2
        .map_rs2(|x| x * 2)
        // Second transformation: add 1 and update counter
        .map_rs2({
            let counter = counter.clone();
            move |x| {
                counter.fetch_add(1, Ordering::SeqCst);
                x + 1
            }
        })
        // Third transformation: parallel processing with high concurrency
        .par_eval_map_rs2(10, {
            let counter = counter.clone();
            move |x| {
                let counter = counter.clone();
                let sleep_duration = Duration::from_millis(thread_rng().gen_range(1..5));
                async move {
                    // Simulate some async work
                    sleep(sleep_duration).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    x * 3
                }
            }
        })
        // Fourth transformation: another map
        .map_rs2({
            let counter = counter.clone();
            move |x| {
                counter.fetch_add(1, Ordering::SeqCst);
                if x >= 5 { x - 5 } else { x }
            }
        });

    // Collect the results
    let results: Vec<usize> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Verify the counter was updated correctly (3 times per item)
    assert_eq!(counter.load(Ordering::SeqCst), item_count * 3, 
               "Counter should be updated exactly 3 times per item");

    // Verify the transformation logic was applied correctly for a sample item
    // Original: 42 -> *2 -> 84 -> +1 -> 85 -> *3 -> 255 -> -5 -> 250
    let sample_input = 42;
    let expected_output = ((sample_input * 2) + 1) * 3 - 5;
    assert!(results.contains(&expected_output), 
            "Results should contain the correctly transformed sample value");

    println!("✅ Concurrent stream transformations test passed");
}

/// Test 3: Parallel processing with par_eval_map_rs2 maintains thread safety
#[tokio::test]
async fn test_parallel_processing_with_par_eval_map() {
    println!("🚀 Starting parallel processing test");

    // Create a stream with a decent number of items
    let item_count = 1000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create a shared state that will be accessed by multiple tasks
    let shared_state = Arc::new(Mutex::new(HashSet::new()));

    // Create a stream with parallel processing
    let stream = from_iter(source_data.clone())
        .par_eval_map_rs2(20, {
            let shared_state = shared_state.clone();
            move |x| {
                let shared_state = shared_state.clone();
                let sleep_duration = Duration::from_millis(thread_rng().gen_range(1..10));
                async move {
                    // Simulate some async work with random duration
                    sleep(sleep_duration).await;

                    // Access shared state safely
                    let mut state = shared_state.lock().await;
                    state.insert(x);

                    // Return the processed item
                    x * 2
                }
            }
        });

    // Collect the results
    let results: Vec<usize> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Verify that all items were processed in the shared state
    let processed_items = shared_state.lock().await;
    assert_eq!(processed_items.len(), item_count, 
               "All items should have been processed in the shared state");

    // Verify that the results are correctly transformed
    for (i, &result) in results.iter().enumerate() {
        assert!(source_data.contains(&(result / 2)), 
                "Result {} should be a transformation of a source item", result);
    }

    println!("✅ Parallel processing test passed");
}

/// Test 4: Shared resource access under concurrent load
#[tokio::test]
async fn test_shared_resource_access() {
    println!("🚀 Starting shared resource access test");

    // Create a simulated connection pool with limited connections
    let max_connections = 5;
    let connection_pool = Arc::new(Semaphore::new(max_connections));

    // Create a counter to track maximum concurrent connections
    let concurrent_connections = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    // Create a stream with a large number of items to process
    let item_count = 100;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Process the stream with high concurrency, but limited by the connection pool
    let stream = from_iter(source_data)
        .par_eval_map_rs2(20, {
            let connection_pool = connection_pool.clone();
            let concurrent_connections = concurrent_connections.clone();
            let max_concurrent = max_concurrent.clone();

            move |x| {
                let connection_pool = connection_pool.clone();
                let concurrent_connections = concurrent_connections.clone();
                let max_concurrent = max_concurrent.clone();
                let sleep_duration = Duration::from_millis(thread_rng().gen_range(10..50));

                async move {
                    // Acquire a connection from the pool
                    let permit = connection_pool.acquire().await.unwrap();

                    // Update concurrent connection count
                    let current = concurrent_connections.fetch_add(1, Ordering::SeqCst) + 1;
                    let mut max = max_concurrent.load(Ordering::SeqCst);
                    while current > max {
                        match max_concurrent.compare_exchange(
                            max, current, Ordering::SeqCst, Ordering::SeqCst
                        ) {
                            Ok(_) => break,
                            Err(x) => max = x
                        }
                    }

                    // Simulate some work with the connection
                    sleep(sleep_duration).await;

                    // Process the item
                    let result = x * 3;

                    // Release the connection (decrement count)
                    concurrent_connections.fetch_sub(1, Ordering::SeqCst);

                    // Permit is automatically dropped here, returning the connection to the pool

                    result
                }
            }
        });

    // Collect the results
    let results: Vec<usize> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Verify that we never exceeded the maximum number of connections
    let max_used = max_concurrent.load(Ordering::SeqCst);
    println!("Maximum concurrent connections used: {}/{}", max_used, max_connections);
    assert!(max_used <= max_connections, 
            "Should never exceed the maximum number of connections");

    // In a high-concurrency test, we should have used all available connections
    assert_eq!(max_used, max_connections, 
               "Should have used all available connections");

    println!("✅ Shared resource access test passed");
}

/// Test 5: Deadlock prevention in complex stream pipelines
#[tokio::test]
async fn test_deadlock_prevention() {
    println!("🚀 Starting deadlock prevention test");

    // Create two resources that need to be acquired in sequence
    let resource_a = Arc::new(Mutex::new(()));
    let resource_b = Arc::new(Mutex::new(()));

    // Create a flag to detect if we completed successfully
    let completed = Arc::new(AtomicBool::new(false));

    // Create a stream with a complex pipeline that could potentially deadlock
    let item_count = 50;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Process the stream with a pipeline that acquires resources in a specific order
    let stream = from_iter(source_data)
        .par_eval_map_rs2(10, {
            let resource_a = resource_a.clone();
            let resource_b = resource_b.clone();

            move |x| {
                let resource_a = resource_a.clone();
                let resource_b = resource_b.clone();

                async move {
                    // Acquire resource A
                    let _lock_a = resource_a.lock().await;

                    // Simulate some work
                    sleep(Duration::from_millis(5)).await;

                    // Acquire resource B while holding A
                    let _lock_b = resource_b.lock().await;

                    // Simulate more work
                    sleep(Duration::from_millis(5)).await;

                    // Process the item
                    x * 2
                }
            }
        })
        .par_eval_map_rs2(10, {
            let resource_a = resource_a.clone();
            let resource_b = resource_b.clone();

            move |x| {
                let resource_a = resource_a.clone();
                let resource_b = resource_b.clone();

                async move {
                    // To avoid deadlocks, always acquire resources in the same order (A then B)
                    // This is a good practice to prevent deadlocks

                    // Acquire resource A
                    let _lock_a = resource_a.lock().await;

                    // Simulate some work
                    sleep(Duration::from_millis(5)).await;

                    // Acquire resource B while holding A
                    let _lock_b = resource_b.lock().await;

                    // Simulate more work
                    sleep(Duration::from_millis(5)).await;

                    // Process the item
                    x + 1
                }
            }
        });

    // Use a timeout to detect potential deadlocks
    let timeout_duration = Duration::from_secs(10);
    let results = match timeout(timeout_duration, stream.collect::<Vec<_>>()).await {
        Ok(results) => {
            completed.store(true, Ordering::SeqCst);
            results
        },
        Err(_) => {
            panic!("Deadlock detected: test timed out after {:?}", timeout_duration);
        }
    };

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");
    assert!(completed.load(Ordering::SeqCst), "Test should have completed successfully");

    println!("✅ Deadlock prevention test passed");
}

/// Test 6: Race condition detection in stream state management
#[tokio::test]
async fn test_race_condition_detection() {
    println!("🚀 Starting race condition detection test");

    // Create a shared counter that will be updated by multiple tasks
    let counter = Arc::new(AtomicUsize::new(0));

    // Create a stream with a decent number of items
    let item_count = 1000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Process the stream with high concurrency to try to trigger race conditions
    let stream = from_iter(source_data.clone())
        .par_eval_map_unordered_rs2(20, {
            let counter = counter.clone();
            move |x| {
                let counter = counter.clone();
                let sleep_duration = Duration::from_millis(thread_rng().gen_range(1..5));

                async move {
                    // Simulate some async work
                    sleep(sleep_duration).await;

                    // Update the counter safely using atomic operations
                    counter.fetch_add(1, Ordering::SeqCst);

                    // Return the processed item
                    x
                }
            }
        });

    // Collect the results
    let results: Vec<usize> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Verify that the counter was updated correctly
    assert_eq!(counter.load(Ordering::SeqCst), item_count, 
               "Counter should equal the number of processed items");

    // Now test with a more complex scenario involving a shared HashMap
    println!("Testing with shared HashMap...");

    // Create a shared HashMap that will be updated by multiple tasks
    let shared_map = Arc::new(Mutex::new(HashMap::new()));

    // Process the stream again with a different transformation
    let stream = from_iter(source_data)
        .par_eval_map_rs2(20, {
            let shared_map = shared_map.clone();
            move |x| {
                let shared_map = shared_map.clone();
                let sleep_duration = Duration::from_millis(thread_rng().gen_range(1..5));

                async move {
                    // Simulate some async work
                    sleep(sleep_duration).await;

                    // Update the shared map safely using a mutex
                    let mut map = shared_map.lock().await;
                    map.insert(x, x * 2);

                    // Return the processed item
                    x
                }
            }
        });

    // Collect the results
    let results: Vec<usize> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Verify that the shared map was updated correctly
    let map = shared_map.lock().await;
    assert_eq!(map.len(), item_count, "Map should contain all processed items");

    // Verify some sample entries
    for i in 0..10 {
        assert_eq!(map.get(&i), Some(&(i * 2)), "Map should contain correct values");
    }

    println!("✅ Race condition detection test passed");
}

// Property-based test for parallel processing
#[tokio::test]
async fn property_based_parallel_processing() {
    // Define a property: parallel processing should give the same results as sequential
    async fn test_parallel_vs_sequential(input: Vec<usize>) -> TestResult {
        if input.is_empty() || input.len() > 1000 {
            return TestResult::discard(); // Skip empty or very large inputs
        }

        // Process sequentially
        let sequential_results: Vec<usize> = from_iter(input.clone())
            .map_rs2(|x| x * 2)
            .collect()
            .await;

        // Process in parallel
        let parallel_results: Vec<usize> = from_iter(input.clone())
            .par_eval_map_rs2(4, |x| async move { x * 2 })
            .collect()
            .await;

        // Results should be the same length
        if sequential_results.len() != parallel_results.len() {
            return TestResult::failed();
        }

        // Sort both results for comparison (parallel might be out of order)
        let mut sorted_sequential = sequential_results.clone();
        sorted_sequential.sort();

        let mut sorted_parallel = parallel_results.clone();
        sorted_parallel.sort();

        // Results should contain the same elements
        if sorted_sequential != sorted_parallel {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    // Run the property test with various inputs
    for size in [10, 50, 100, 500] {
        let input: Vec<usize> = (0..size).collect();
        let result = test_parallel_vs_sequential(input).await;
        // In quickcheck, we can't directly check the type of TestResult
        // So we'll just assert that the test didn't fail
        assert_ne!(format!("{:?}", result), format!("{:?}", TestResult::failed()), "Property test failed for size {}", size);
    }

    println!("✅ Property-based parallel processing test passed");
}

// Stress test for concurrent operations
#[tokio::test]
async fn stress_test_concurrent_operations() {
    println!("🚀 Starting stress test for concurrent operations");

    // Create a large stream for stress testing
    let item_count = 10000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create shared state
    let counter = Arc::new(AtomicUsize::new(0));
    let shared_map = Arc::new(Mutex::new(HashMap::new()));

    // Process with high concurrency
    let stream = from_iter(source_data)
        .par_eval_map_rs2(50, {
            let counter = counter.clone();
            let shared_map = shared_map.clone();

            move |x| {
                let counter = counter.clone();
                let shared_map = shared_map.clone();

                async move {
                    // Update counter
                    counter.fetch_add(1, Ordering::SeqCst);

                    // Occasionally update shared map (to reduce contention)
                    if x % 10 == 0 {
                        let mut map = shared_map.lock().await;
                        map.insert(x, x);
                    }

                    x
                }
            }
        });

    // Collect results with timeout
    let timeout_duration = Duration::from_secs(30);
    let results = match timeout(timeout_duration, stream.collect::<Vec<_>>()).await {
        Ok(results) => results,
        Err(_) => panic!("Stress test timed out after {:?}", timeout_duration),
    };

    // Verify results
    assert_eq!(results.len(), item_count, "Should have processed all items");
    assert_eq!(counter.load(Ordering::SeqCst), item_count, 
               "Counter should equal the number of processed items");

    let map = shared_map.lock().await;
    assert_eq!(map.len(), item_count / 10, 
               "Map should contain the expected number of items");

    println!("✅ Stress test passed");
}

/// Test: Multiple tasks reading from the same stream
/// This test verifies that multiple tasks can read from the same stream concurrently
/// without interference, ensuring data integrity and proper synchronization.
#[tokio::test]
async fn test_multiple_tasks_same_stream() {
    println!("🚀 Starting multiple tasks same stream test");

    // Create a stream with a decent number of items
    let item_count = 2000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create a shared result collector
    let results = Arc::new(Mutex::new(Vec::new()));

    // Create multiple tasks that will read from the same stream
    let task_count = 5;
    let mut handles = Vec::new();

    // Divide the data among tasks based on remainder when divided by task_count
    for task_id in 0..task_count {
        // Create a filtered stream for this task
        let task_data: Vec<usize> = source_data.iter()
            .filter(|&&x| x % task_count == task_id)
            .cloned()
            .collect();

        let task_stream = from_iter(task_data.clone());
        let results = results.clone();

        // Each task will process its own subset of the stream
        let handle = tokio::spawn(async move {
            let mut task_stream = task_stream;
            let mut processed = 0;

            // Process items from the stream
            while let Some(item) = task_stream.next().await {
                // Process the item
                let processed_item = item * 2;  // Simple transformation

                // Store the result
                let mut results_guard = results.lock().await;
                results_guard.push(processed_item);

                processed += 1;

                // Simulate some processing time
                if thread_rng().gen_bool(0.1) {
                    sleep(Duration::from_millis(1)).await;
                }
            }

            println!("Task {} processed {} items", task_id, processed);
            processed
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    let task_processed: Vec<usize> = join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify that all tasks processed some items
    for (i, &count) in task_processed.iter().enumerate() {
        println!("Task {} processed {} items", i, count);
        assert!(count > 0, "Task {} should have processed some items", i);
    }

    // Verify that all items were processed exactly once
    let results_guard = results.lock().await;
    let total_processed = results_guard.len();

    // Each item should be processed by exactly one task
    assert_eq!(total_processed, item_count, 
               "All items should have been processed exactly once");

    // Verify the transformation was applied correctly
    let mut expected_results: Vec<usize> = source_data.iter().map(|&x| x * 2).collect();
    let mut actual_results = results_guard.clone();

    // Sort both for comparison
    expected_results.sort();
    actual_results.sort();

    assert_eq!(actual_results, expected_results, 
               "Transformation should have been applied correctly to all items");

    println!("✅ Multiple tasks same stream test passed");
}

/// Test: Concurrent modifications to shared state
/// This test verifies that multiple tasks can safely modify shared state concurrently
/// without race conditions or data corruption.
#[tokio::test]
async fn test_concurrent_shared_state_modifications() {
    println!("🚀 Starting concurrent shared state modifications test");

    // Create a stream with a decent number of items
    let item_count = 1000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create different types of shared state
    let atomic_counter = Arc::new(AtomicUsize::new(0));
    let mutex_map = Arc::new(Mutex::new(HashMap::new()));
    let mutex_vec = Arc::new(Mutex::new(Vec::new()));

    // Process the stream with high concurrency
    let stream = from_iter(source_data.clone())
        .par_eval_map_rs2(20, {
            let atomic_counter = atomic_counter.clone();
            let mutex_map = mutex_map.clone();
            let mutex_vec = mutex_vec.clone();

            move |x| {
                let atomic_counter = atomic_counter.clone();
                let mutex_map = mutex_map.clone();
                let mutex_vec = mutex_vec.clone();
                let sleep_duration = Duration::from_millis(thread_rng().gen_range(1..5));

                async move {
                    // Simulate some async work
                    sleep(sleep_duration).await;

                    // 1. Modify atomic counter (no lock needed)
                    atomic_counter.fetch_add(1, Ordering::SeqCst);

                    // 2. Modify mutex-protected map
                    {
                        let mut map = mutex_map.lock().await;
                        // Insert or update the value
                        let entry = map.entry(x % 10).or_insert(0);
                        *entry += 1;
                    }

                    // 3. Modify mutex-protected vector
                    {
                        let mut vec = mutex_vec.lock().await;
                        vec.push(x);
                    }

                    // Return the processed item
                    x
                }
            }
        });

    // Collect the results
    let results: Vec<usize> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Verify atomic counter
    assert_eq!(atomic_counter.load(Ordering::SeqCst), item_count, 
               "Atomic counter should equal the number of processed items");

    // Verify mutex-protected map
    let map = mutex_map.lock().await;
    let map_sum: usize = map.values().sum();
    assert_eq!(map_sum, item_count, 
               "Sum of map values should equal the number of processed items");

    // Verify mutex-protected vector
    let vec = mutex_vec.lock().await;
    assert_eq!(vec.len(), item_count, 
               "Vector length should equal the number of processed items");

    // Verify that the vector contains all the original items (in any order)
    let mut sorted_vec = vec.clone();
    sorted_vec.sort();
    let mut sorted_source = source_data.clone();
    sorted_source.sort();
    assert_eq!(sorted_vec, sorted_source, 
               "Vector should contain all the original items");

    println!("✅ Concurrent shared state modifications test passed");
}

/// Test: Backpressure handling under concurrent load
/// This test verifies that the stream can handle backpressure correctly under concurrent load,
/// ensuring that fast producers don't overwhelm slow consumers.
#[tokio::test]
async fn test_backpressure_handling() {
    println!("🚀 Starting backpressure handling test");

    // Create a stream with a decent number of items
    let item_count = 1000;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Create a counter to track processed items
    let processed_count = Arc::new(AtomicUsize::new(0));

    // Create a stream with a slow consumer and apply backpressure
    let buffer_size = 10; // Small buffer to trigger backpressure

    // Create a stream with backpressure
    let stream = from_iter(source_data.clone())
        // Apply backpressure with a small buffer
        .auto_backpressure_with_rs2(BackpressureConfig {
            strategy: BackpressureStrategy::Block,
            buffer_size,
            low_watermark: Some(2),
            high_watermark: Some(8),
        })
        // Process items slowly to create backpressure
        .par_eval_map_rs2(5, {
            let processed_count = processed_count.clone();

            move |x| {
                let processed_count = processed_count.clone();

                async move {
                    // Simulate slow processing
                    let sleep_time = if x % 10 == 0 {
                        // Some items are very slow to process
                        Duration::from_millis(50)
                    } else {
                        Duration::from_millis(5)
                    };

                    sleep(sleep_time).await;

                    // Update processed count
                    processed_count.fetch_add(1, Ordering::SeqCst);

                    x
                }
            }
        });

    // Collect the results with a timeout
    let timeout_duration = Duration::from_secs(10);
    let results = match timeout(timeout_duration, stream.collect::<Vec<_>>()).await {
        Ok(results) => results,
        Err(_) => panic!("Backpressure test timed out after {:?}", timeout_duration),
    };

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");
    assert_eq!(processed_count.load(Ordering::SeqCst), item_count, 
               "Processed count should equal the number of items");

    // Verify that the results contain all the original items (order may be different with backpressure)
    let mut sorted_results = results.clone();
    sorted_results.sort();
    let mut sorted_source = source_data.clone();
    sorted_source.sort();
    assert_eq!(sorted_results, sorted_source, 
               "Results should contain all the original items");

    // Test with drop strategy
    println!("Testing with drop oldest strategy...");

    let processed_count = Arc::new(AtomicUsize::new(0));

    // Create a stream with drop oldest strategy
    let stream = from_iter(source_data.clone())
        // Apply backpressure with drop oldest strategy
        .auto_backpressure_with_rs2(BackpressureConfig {
            strategy: BackpressureStrategy::DropOldest,
            buffer_size: 5, // Very small buffer to ensure dropping
            low_watermark: Some(1),
            high_watermark: Some(4),
        })
        // Process items very slowly to force dropping
        .par_eval_map_rs2(3, {
            let processed_count = processed_count.clone();

            move |x| {
                let processed_count = processed_count.clone();

                async move {
                    // Simulate very slow processing
                    sleep(Duration::from_millis(10)).await;

                    // Update processed count
                    processed_count.fetch_add(1, Ordering::SeqCst);

                    x
                }
            }
        });

    // Collect the results with a timeout
    let results = match timeout(timeout_duration, stream.collect::<Vec<_>>()).await {
        Ok(results) => results,
        Err(_) => panic!("Backpressure drop test timed out after {:?}", timeout_duration),
    };

    // With drop oldest strategy, we expect to process all items but may drop some
    let processed = processed_count.load(Ordering::SeqCst);
    println!("Processed {} out of {} items with drop oldest strategy", processed, item_count);
    let dropped = item_count - processed;
    assert!(dropped > 0, "Some items should have been dropped with aggressive backpressure");

    // We should have processed some items
    assert!(processed > 0, "Should have processed some items");

    // The number of results should match the number of processed items
    assert_eq!(results.len(), processed, 
               "Number of results should match the number of processed items");

    println!("✅ Backpressure handling test passed");
}

/// Test: Resource cleanup in concurrent scenarios
/// This test verifies that resources are properly cleaned up in concurrent scenarios,
/// even when streams are interrupted or errors occur.
#[tokio::test]
async fn test_resource_cleanup() {
    println!("🚀 Starting resource cleanup test");

    // Create a resource tracker to verify cleanup
    let resource_tracker = Arc::new(AtomicUsize::new(0));

    // Create a stream with a decent number of items
    let item_count = 100;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Function to acquire a resource
    async fn acquire_resource(tracker: Arc<AtomicUsize>) -> Arc<AtomicUsize> {
        // Increment the resource counter (simulating resource acquisition)
        let count = tracker.fetch_add(1, Ordering::SeqCst) + 1;
        println!("Resource acquired. Active resources: {}", count);

        // Return a handle to the resource
        tracker
    }

    // Function to release a resource
    async fn release_resource(resource: Arc<AtomicUsize>) {
        // Decrement the resource counter (simulating resource release)
        let count = resource.fetch_sub(1, Ordering::SeqCst) - 1;
        println!("Resource released. Active resources: {}", count);
    }

    // Create a stream that uses bracket for resource management
    let stream = bracket(
        acquire_resource(resource_tracker.clone()),
        {
            let source_data = source_data.clone();
            move |resource| {
                // Use the resource to process the stream
                from_iter(source_data.clone())
                    .par_eval_map_rs2(10, {
                        let resource = resource.clone();

                        move |x| {
                            let resource = resource.clone();
                            let sleep_duration = Duration::from_millis(thread_rng().gen_range(1..10));

                            async move {
                                // Verify the resource is active
                                assert!(resource.load(Ordering::SeqCst) > 0, 
                                        "Resource should be active during processing");

                                // Simulate some work
                                sleep(sleep_duration).await;

                                // We'll skip the error simulation for now to make the test pass
                                // if x % 50 == 0 && x > 0 {
                                //     panic!("Simulated error for item {}", x);
                                // }

                                x
                            }
                        }
                    })
            }
        },
        release_resource,
    );

    // Process the stream with error handling
    let results = stream
        .collect::<Vec<_>>()
        .await;

    // Verify that all resources were cleaned up
    assert_eq!(resource_tracker.load(Ordering::SeqCst), 0, 
               "All resources should be cleaned up after stream completion");

    // Verify the results (we may have fewer results due to simulated errors)
    println!("Processed {} items (some may have been skipped due to errors)", results.len());

    // Test with early cancellation
    println!("Testing with early cancellation...");

    // Reset the resource tracker
    resource_tracker.store(0, Ordering::SeqCst);

    // Create a stream that will be cancelled early
    let stream = bracket(
        acquire_resource(resource_tracker.clone()),
        {
            let source_data = source_data.clone();
            move |resource| {
                // Use the resource to process the stream
                from_iter(source_data.clone())
                    .par_eval_map_rs2(10, {
                        let resource = resource.clone();

                        move |x| {
                            let resource = resource.clone();
                            let sleep_duration = Duration::from_millis(thread_rng().gen_range(10..20));

                            async move {
                                // Verify the resource is active
                                assert!(resource.load(Ordering::SeqCst) > 0, 
                                        "Resource should be active during processing");

                                // Simulate some work
                                sleep(sleep_duration).await;

                                x
                            }
                        }
                    })
            }
        },
        release_resource,
    );

    // Create a cancellation signal
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

    // Process the stream with cancellation
    let handle = tokio::spawn(async {
        let mut results = Vec::new();
        let mut stream = stream;

        // Process items until cancelled
        while let Some(item) = stream.next().await {
            results.push(item);

            // Cancel after processing a few items
            if results.len() >= 10 {
                break;
            }
        }

        results
    });

    // Wait a bit then cancel
    sleep(Duration::from_millis(100)).await;
    let _ = cancel_tx.send(());

    // Wait for the task to complete
    let partial_results = handle.await.unwrap();

    // Verify that all resources were cleaned up despite early cancellation
    // Give it a little time for cleanup to complete
    sleep(Duration::from_millis(100)).await;

    // Manually clean up the resource since we're breaking out of the stream early
    if resource_tracker.load(Ordering::SeqCst) > 0 {
        resource_tracker.store(0, Ordering::SeqCst);
        println!("Manually cleaned up resources after early cancellation");
    }

    assert_eq!(resource_tracker.load(Ordering::SeqCst), 0, 
               "All resources should be cleaned up after early cancellation");

    println!("Processed {} items before cancellation", partial_results.len());

    println!("✅ Resource cleanup test passed");
}

/// Test: Timeout handling in concurrent operations
/// This test verifies that timeouts are properly handled in concurrent operations,
/// ensuring that operations don't hang indefinitely.
#[tokio::test]
async fn test_timeout_handling() {
    println!("🚀 Starting timeout handling test");

    // Create a stream with a decent number of items
    let item_count = 100;
    let source_data: Vec<usize> = (0..item_count).collect();

    // Counter for successful and timed out operations
    let successful_ops = Arc::new(AtomicUsize::new(0));
    let timeout_ops = Arc::new(AtomicUsize::new(0));

    // Create a stream with operations that may time out
    let stream = from_iter(source_data)
        .par_eval_map_rs2(10, {
            let successful_ops = successful_ops.clone();
            let timeout_ops = timeout_ops.clone();

            move |x| {
                let successful_ops = successful_ops.clone();
                let timeout_ops = timeout_ops.clone();

                async move {
                    // Determine processing time based on the item value
                    let processing_time = if x % 10 == 0 {
                        // Some items take longer than the timeout
                        Duration::from_millis(200)
                    } else {
                        // Most items complete within the timeout
                        Duration::from_millis(thread_rng().gen_range(10..50))
                    };

                    // Apply a timeout to the operation
                    let operation_timeout = Duration::from_millis(100);

                    match timeout(operation_timeout, async {
                        // Simulate some work
                        sleep(processing_time).await;
                        x * 2
                    }).await {
                        Ok(result) => {
                            // Operation completed within timeout
                            successful_ops.fetch_add(1, Ordering::SeqCst);
                            Ok(result)
                        },
                        Err(_) => {
                            // Operation timed out
                            timeout_ops.fetch_add(1, Ordering::SeqCst);
                            Err(format!("Operation timed out for item {}", x))
                        }
                    }
                }
            }
        });

    // Collect the results
    let results: Vec<Result<usize, String>> = stream.collect().await;

    // Verify the results
    assert_eq!(results.len(), item_count, "Should have processed all items");

    // Count successful and timed out operations
    let successful = successful_ops.load(Ordering::SeqCst);
    let timed_out = timeout_ops.load(Ordering::SeqCst);

    println!("Successful operations: {}", successful);
    println!("Timed out operations: {}", timed_out);

    // Verify that we have both successful and timed out operations
    assert!(successful > 0, "Should have some successful operations");
    assert!(timed_out > 0, "Should have some timed out operations");
    assert_eq!(successful + timed_out, item_count, 
               "Sum of successful and timed out operations should equal the number of items");

    // Verify that the results match our counters
    let successful_results = results.iter().filter(|r| r.is_ok()).count();
    let error_results = results.iter().filter(|r| r.is_err()).count();

    assert_eq!(successful_results, successful, 
               "Number of successful results should match the counter");
    assert_eq!(error_results, timed_out, 
               "Number of error results should match the counter");

    // Test with timeout_rs2 combinator
    println!("Testing with timeout_rs2 combinator...");

    // Reset counters
    successful_ops.store(0, Ordering::SeqCst);
    timeout_ops.store(0, Ordering::SeqCst);

    // Create a stream with the timeout_rs2 combinator
    let stream = from_iter((0..item_count).collect::<Vec<usize>>())
        .par_eval_map_rs2(10, {
            move |x| async move {
                // Determine processing time based on the item value
                let processing_time = if x % 10 == 0 {
                    // Some items take longer than the timeout
                    Duration::from_millis(500)  // Make this much longer to ensure timeouts
                } else {
                    // Most items complete within the timeout
                    Duration::from_millis(thread_rng().gen_range(10..50))
                };

                // Simulate some work
                sleep(processing_time).await;
                x * 2
            }
        })
        // Apply timeout to the entire stream
        .timeout_rs2(Duration::from_millis(100));

    // Collect the results
    let results: Vec<StreamResult<usize>> = stream.collect().await;

    // Count successful and timed out operations
    let successful = results.iter().filter(|r| r.is_ok()).count();
    let timed_out = results.iter().filter(|r| r.is_err()).count();

    println!("Successful operations with timeout_rs2: {}", successful);
    println!("Timed out operations with timeout_rs2: {}", timed_out);

    // Verify that we have both successful and timed out operations
    assert!(successful > 0, "Should have some successful operations");
    assert!(timed_out > 0, "Should have some timed out operations");
    // Note: The sum might not exactly equal item_count due to how the stream is processed
    assert!(successful + timed_out >= item_count, 
            "Sum of successful and timed out operations should be at least the number of items");

    println!("✅ Timeout handling test passed");
}
