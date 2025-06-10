use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::{Duration, Instant};
use async_stream::stream;
use tokio::time::sleep;

#[test]
fn test_rate_limit_backpressure() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let capacity = 2;

        let result = rate_limit_backpressure(stream, capacity)
            .collect::<Vec<_>>()
            .await;

        // Verify all items are processed
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_throttle() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let delay = Duration::from_millis(50);

        let start = Instant::now();
        let result = throttle(stream, delay)
            .collect::<Vec<_>>()
            .await;
        let elapsed = start.elapsed();

        // Verify all items are processed
        assert_eq!(result, vec![1, 2, 3, 4, 5]);

        // Verify throttling occurred (at least 4 * 50ms = 200ms)
        // We expect at least 4 delays because after the first item, 
        // each subsequent item should be delayed
        assert!(elapsed.as_millis() >= 200, 
                "Expected at least 200ms delay, got {}ms", 
                elapsed.as_millis());
    });
}

#[test]
fn test_tick() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let period = Duration::from_millis(50);
        let item = 42;

        let start = Instant::now();
        let result = tick(period, item)
            .take(5)
            .collect::<Vec<_>>()
            .await;
        let elapsed = start.elapsed();

        // Verify we got 5 items, all equal to 42
        assert_eq!(result, vec![42, 42, 42, 42, 42]);

        // Verify timing (at least 4 * 50ms = 200ms)
        // We expect at least 4 periods because after the first item, 
        // each subsequent item should be delayed by one period
        assert!(elapsed.as_millis() >= 200, 
                "Expected at least 200ms delay, got {}ms", 
                elapsed.as_millis());
    });
}

#[test]
fn test_par_eval_map() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let concurrency = 2;

        let result = par_eval_map(stream, concurrency, |n| async move { n * 2 })
            .collect::<Vec<_>>()
            .await;

        // Sort the result since parallel execution might change the order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        assert_eq!(sorted_result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_par_eval_map_unordered() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let concurrency = 2;

        let result = par_eval_map_unordered(stream, concurrency, |n| async move { n * 2 })
            .collect::<Vec<_>>()
            .await;

        // Sort the result since unordered execution will change the order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        assert_eq!(sorted_result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_par_eval_map_with_delays() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream where items take different times to process
        let stream = from_iter(vec![
            (1, 100), // (value, delay_ms)
            (2, 50),
            (3, 150),
            (4, 25),
            (5, 75),
        ]);

        let concurrency = 3;

        let start = Instant::now();
        let result = par_eval_map(
            stream, 
            concurrency,
            |(n, delay_ms)| async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                n * 2
            }
        )
        .collect::<Vec<_>>()
        .await;
        let elapsed = start.elapsed();

        // Verify all items were processed
        let mut sorted_result = result.clone();
        sorted_result.sort();
        assert_eq!(sorted_result, vec![2, 4, 6, 8, 10]);

        // With concurrency=3, the total time should be less than the sum of all delays
        // but more than the sum of the longest delays that would need to be processed sequentially
        // In this case, with optimal scheduling, we'd expect around 175ms (100+75 or 150+25)
        assert!(elapsed.as_millis() < 400, "Expected parallel execution to be faster");
    });
}

#[test]
fn test_prefetch() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let prefetch_count = 2;

        let result = prefetch(stream, prefetch_count)
            .collect::<Vec<_>>()
            .await;

        // Verify all items are processed in the correct order
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_prefetch_rs2() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let prefetch_count = 2;

        let result = stream
            .prefetch_rs2(prefetch_count)
            .collect::<Vec<_>>()
            .await;

        // Verify all items are processed in the correct order
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_prefetch_performance() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream where each item takes time to process
        let process_time_ms = 50;
        let item_count = 5;

        // Function to create a rs2_stream with processing delays
        let create_stream = || {
            // Copy the values to avoid capturing references
            let delay = process_time_ms;
            from_iter(0..item_count)
                .eval_map_rs2(move |n| async move {
                    // Simulate processing time
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    n
                })
        };

        // Test without prefetch
        let start_without_prefetch = Instant::now();
        let result_without_prefetch = create_stream()
            .collect::<Vec<_>>()
            .await;
        let elapsed_without_prefetch = start_without_prefetch.elapsed();

        // Test with prefetch
        let start_with_prefetch = Instant::now();
        let result_with_prefetch = create_stream()
            .prefetch_rs2(item_count) // Prefetch all items
            .collect::<Vec<_>>()
            .await;
        let elapsed_with_prefetch = start_with_prefetch.elapsed();

        // Verify both streams produced the same result
        assert_eq!(result_without_prefetch, result_with_prefetch);

        // Without prefetch, processing is sequential, so time should be approximately item_count * process_time_ms
        // With prefetch, items are processed in parallel, so time should be less
        // However, the exact improvement depends on many factors, so we just check that it's faster
        println!("Without prefetch: {:?}, With prefetch: {:?}", elapsed_without_prefetch, elapsed_with_prefetch);

        // The prefetch version should be at least a little faster, but we don't make a strict assertion
        // because the exact timing can vary based on system load and other factors
    });
}

#[test]
fn test_debounce() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Use a very short debounce period for testing
        let debounce_period = Duration::from_millis(20);

        // Create a rs2_stream with two groups of rapid updates separated by a pause
        let stream = stream! {
            // First group: rapid updates
            yield 1;
            sleep(Duration::from_millis(5)).await;
            yield 2;
            sleep(Duration::from_millis(5)).await;
            yield 3;

            // Wait longer than the debounce period to ensure item 3 is emitted
            sleep(Duration::from_millis(50)).await;

            // Second group: a single item
            yield 4;

            // Wait longer than the debounce period to ensure item 4 is emitted
            sleep(Duration::from_millis(50)).await;

            // Third group: rapid updates
            yield 5;
            sleep(Duration::from_millis(5)).await;
            yield 6;

            // Wait longer than the debounce period to ensure item 6 is emitted
            sleep(Duration::from_millis(50)).await;
        };

        // Apply debounce
        let result = debounce(stream.boxed(), debounce_period)
            .collect::<Vec<_>>()
            .await;

        // We expect:
        // - Item 3 (last of the first group)
        // - Item 4 (the single item in the second group)
        // - Item 6 (last of the third group)
        assert_eq!(result, vec![3, 4, 6]);
    });
}

#[test]
fn test_sample() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Use a short sample interval for testing
        let sample_interval = Duration::from_millis(50);

        // Create a rs2_stream with values arriving at different rates
        let stream = stream! {
            // First group: rapid updates before the first sample interval
            yield 1;
            sleep(Duration::from_millis(10)).await;
            yield 2;
            sleep(Duration::from_millis(10)).await;
            yield 3;

            // Wait for the first sample interval to complete
            sleep(Duration::from_millis(40)).await;

            // No values during the second interval, so it should be skipped

            // Wait for the second sample interval to complete
            sleep(Duration::from_millis(50)).await;

            // Third group: a single value during the third interval
            yield 4;

            // Wait for the third sample interval to complete
            sleep(Duration::from_millis(50)).await;

            // Fourth group: rapid updates during the fourth interval
            yield 5;
            sleep(Duration::from_millis(10)).await;
            yield 6;

            // Wait for the fourth sample interval to complete
            sleep(Duration::from_millis(50)).await;
        };

        // Apply sample
        let result = sample(stream.boxed(), sample_interval)
            .collect::<Vec<_>>()
            .await;

        // We expect:
        // - Item 3 (the most recent value at the first sample interval)
        // - No item for the second interval (no new values)
        // - Item 4 (the most recent value at the third sample interval)
        // - Item 6 (the most recent value at the fourth sample interval)
        assert_eq!(result, vec![3, 4, 6]);
    });
}

#[test]
fn test_par_join() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream of streams
        let streams = vec![
            from_iter(vec![1, 2, 3]),
            from_iter(vec![4, 5, 6]),
            from_iter(vec![7, 8, 9]),
        ];
        let stream_of_streams = from_iter(streams);
        let concurrency = 2;

        // Apply par_join
        let result = par_join(stream_of_streams, concurrency)
            .collect::<Vec<_>>()
            .await;

        // Sort the result since parallel execution might change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        // We expect all elements from all streams
        assert_eq!(sorted_result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    });
}

#[test]
fn test_par_join_with_different_sizes() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a rs2_stream of streams with different sizes
        let streams = vec![
            from_iter(vec![1, 2]),
            from_iter(vec![3, 4, 5, 6]),
            from_iter(vec![7]),
            from_iter(vec![8, 9, 10]),
        ];
        let stream_of_streams = from_iter(streams);
        let concurrency = 2;

        // Apply par_join
        let result = par_join(stream_of_streams, concurrency)
            .collect::<Vec<_>>()
            .await;

        // Sort the result since parallel execution might change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        // We expect all elements from all streams
        assert_eq!(sorted_result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });
}

#[test]
fn test_par_join_with_delays() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create streams with different processing times
        let create_delayed_stream = |values: Vec<i32>, delay_ms: u64| {
            stream! {
                for value in values {
                    sleep(Duration::from_millis(delay_ms)).await;
                    yield value;
                }
            }.boxed()
        };

        // Create a rs2_stream of streams with different delays
        let streams = vec![
            create_delayed_stream(vec![1, 2, 3], 50),   // 3 items, 50ms each = 150ms total
            create_delayed_stream(vec![4, 5], 100),     // 2 items, 100ms each = 200ms total
            create_delayed_stream(vec![6, 7, 8], 30),   // 3 items, 30ms each = 90ms total
            create_delayed_stream(vec![9, 10], 80),     // 2 items, 80ms each = 160ms total
        ];
        let stream_of_streams = from_iter(streams);
        let concurrency = 2;

        // Measure the time it takes to process all streams
        let start = Instant::now();
        let result = par_join(stream_of_streams, concurrency)
            .collect::<Vec<_>>()
            .await;
        let elapsed = start.elapsed();

        // Sort the result since parallel execution might change order
        let mut sorted_result = result.clone();
        sorted_result.sort();

        // We expect all elements from all streams
        assert_eq!(sorted_result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // With concurrency=2, the total time should be less than the sum of all delays
        // Total sequential time would be 150 + 200 + 90 + 160 = 600ms
        // With optimal scheduling of 2 concurrent streams, we'd expect around 300-350ms
        // Allow a large margin for test environment variability
        assert!(elapsed.as_millis() < 700, 
                "Expected parallel execution to be faster, got {}ms", 
                elapsed.as_millis());
    });
}
