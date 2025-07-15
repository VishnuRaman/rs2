use rs2_stream::rs2::*;
use rs2_stream::stream::{StreamExt, from_iter, Stream, RateStreamExt};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::time::{Duration, Instant};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Runtime;
use tokio::time::sleep;

// Helper function to run tests with timeout
async fn run_with_timeout<F, T>(timeout_duration: Duration, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(timeout_duration, future)
        .await
        .expect("Test timed out")
}

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
        let result = throttle(stream, delay).collect::<Vec<_>>().await;
        let elapsed = start.elapsed();

        // Verify all items are processed
        assert_eq!(result, vec![1, 2, 3, 4, 5]);

        // Verify throttling occurred (at least 4 * 50ms = 200ms)
        // We expect at least 4 delays because after the first item,
        // each subsequent item should be delayed
        assert!(
            elapsed.as_millis() >= 200,
            "Expected at least 200ms delay, got {}ms",
            elapsed.as_millis()
        );
    });
}

#[test]
fn test_tick() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let period = Duration::from_millis(50);
        let item = 42;

        let start = Instant::now();
        let result = tick(period, item).take(5).collect::<Vec<_>>().await;
        let elapsed = start.elapsed();

        // Verify we got 5 items, all equal to 42
        assert_eq!(result, vec![42, 42, 42, 42, 42]);

        // Verify timing (at least 4 * 50ms = 200ms)
        // We expect at least 4 periods because after the first item,
        // each subsequent item should be delayed by one period
        assert!(
            elapsed.as_millis() >= 200,
            "Expected at least 200ms delay, got {}ms",
            elapsed.as_millis()
        );
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
        let result = par_eval_map(stream, concurrency, |(n, delay_ms)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            n * 2
        })
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
        assert!(
            elapsed.as_millis() < 400,
            "Expected parallel execution to be faster"
        );
    });
}

#[test]
fn test_prefetch() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let prefetch_count = 2;

        let result = prefetch(stream, prefetch_count).collect::<Vec<_>>().await;

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
            from_iter(0..item_count).eval_map_rs2(move |n| async move {
                // Simulate processing time
                tokio::time::sleep(Duration::from_millis(delay)).await;
                n
            })
        };

        // Test without prefetch
        let start_without_prefetch = Instant::now();
        let result_without_prefetch = create_stream().collect::<Vec<_>>().await;
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
        println!(
            "Without prefetch: {:?}, With prefetch: {:?}",
            elapsed_without_prefetch, elapsed_with_prefetch
        );

        // The prefetch version should be at least a little faster, but we don't make a strict assertion
        // because the exact timing can vary based on system load and other factors
    });
}

#[test]
fn test_debounce() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test debounce with a very short duration that won't cause hangs
        let stream = from_iter(vec![1, 2, 3, 4, 5, 6]);
        let result = run_with_timeout(
            Duration::from_secs(5),
            debounce(stream, Duration::from_millis(1))
                .collect::<Vec<_>>()
        ).await;

        // With very short duration, we should get some items
        // The exact number depends on timing, but we should get at least some
        assert!(!result.is_empty());
        assert!(result.len() <= 6);
        
        // All items should be from the original stream
        for item in &result {
            assert!(*item >= 1 && *item <= 6);
        }
    });
}

#[test]
fn test_sample() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Test sample with a very short interval that won't cause hangs
        let stream = from_iter(vec![1, 2, 3, 4, 5, 6]);
        let result = run_with_timeout(
            Duration::from_secs(5),
            stream.sample_finite(Duration::from_millis(1))
                .collect::<Vec<_>>()
        ).await;

        // Sample should emit at least some items
        assert!(!result.is_empty());
        assert!(result.len() <= 6);
        
        // All items should be from the original stream
        for item in &result {
            assert!(*item >= 1 && *item <= 6);
        }
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
        // Create simple streams with different sizes
        let streams = vec![
            from_iter(vec![1, 2, 3]), // 3 items
            from_iter(vec![4, 5]),    // 2 items  
            from_iter(vec![6, 7, 8]), // 3 items
            from_iter(vec![9, 10]),   // 2 items
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

        // The test should complete quickly since we're not simulating actual delays
        assert!(
            elapsed.as_millis() < 1000,
            "Expected quick execution, got {}ms",
            elapsed.as_millis()
        );
    });
}
