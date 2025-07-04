use rs2_stream::stream::{Stream, StreamExt, from_iter, once, empty};
use rs2_stream::stream::constructors::from_iter as stream_from_iter;
use rs2_stream::stream::rate::RateStreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

struct TestStream {
    values: Vec<i32>,
    index: usize,
}

impl TestStream {
    fn new(values: Vec<i32>) -> Self {
        Self { values, index: 0 }
    }
}

impl Stream for TestStream {
    type Item = i32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.values.len() {
            let value = self.values[self.index];
            self.index += 1;
            Poll::Ready(Some(value))
        } else {
            Poll::Ready(None)
        }
    }
}

#[tokio::test]
async fn test_basic_stream_operations() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_empty_stream() {
    let stream = TestStream::new(vec![]);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_single_item_stream() {
    let stream = TestStream::new(vec![42]);
    let result: Vec<i32> = stream.collect().await;
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn test_map_operation() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.map(|x| x * 2).collect().await;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_filter_operation() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.filter(|x| x % 2 == 0).collect().await;
    assert_eq!(result, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_take_operation() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.take(3).collect().await;
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_skip_operation() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = stream.skip(2).collect().await;
    assert_eq!(result, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_combined_operations() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream
        .filter(|x| x % 2 == 0)  // Keep even numbers
        .map(|x| x * 2)          // Double them
        .take(3)                 // Take first 3
        .collect()
        .await;
    assert_eq!(result, vec![4, 8, 12]); // (2*2), (4*2), (6*2)
}

#[tokio::test]
async fn test_throttle_direct_stream() {
    // Test using stream constructors directly
    println!("Starting direct stream throttle test...");
    
    let start = Instant::now();
    let stream = stream_from_iter(vec![1, 2, 3]);
    let throttled = stream.throttle(Duration::from_millis(2));
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Direct stream test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![1, 2, 3]);
    // Should take at least 4ms (2ms between each item for 3 items)
    assert!(elapsed >= Duration::from_millis(4));
}

#[tokio::test]
async fn test_throttle_single_item_direct() {
    // Test single item with direct stream
    println!("Starting single item direct stream test...");
    
    let start = Instant::now();
    let stream = stream_from_iter(vec![42]);
    let throttled = stream.throttle(Duration::from_millis(1));
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Single item direct test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![42]);
    // Should be fast since only one item
    assert!(elapsed < Duration::from_millis(10));
}

#[tokio::test]
async fn test_throttle_short_duration() {
    // Test with duration <= 1ms (should bypass throttling)
    let start = Instant::now();
    let stream = stream_from_iter(vec![1, 2, 3, 4, 5]);
    let throttled = stream.throttle(Duration::from_micros(100)); // 0.1ms
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Short duration test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    // Should be very fast since throttling is bypassed
    assert!(elapsed < Duration::from_millis(1));
}

#[tokio::test]
async fn test_throttle_long_duration() {
    // Test with duration > 1ms (should enforce throttling)
    let start = Instant::now();
    let stream = stream_from_iter(vec![1, 2, 3]);
    let throttled = stream.throttle(Duration::from_millis(2)); // 2ms > 1ms threshold
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Long duration test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![1, 2, 3]);
    // Should take at least 4ms (2ms between each item for 3 items)
    assert!(elapsed >= Duration::from_millis(4));
}

#[tokio::test]
async fn test_throttle_zero_duration() {
    // Test with zero duration (should bypass throttling)
    let start = Instant::now();
    let stream = stream_from_iter(vec![1, 2, 3, 4, 5]);
    let throttled = stream.throttle(Duration::ZERO);
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Zero duration test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    // Should be very fast since throttling is bypassed
    assert!(elapsed < Duration::from_millis(1));
}

#[tokio::test]
async fn test_throttle_simple() {
    // Simple test with just 2 items and 1ms throttling
    println!("Starting simple throttle test...");
    let start = Instant::now();
    let stream = stream_from_iter(vec![1, 2]);
    let throttled = stream.throttle(Duration::from_millis(1)); // 1ms throttling
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Simple throttle test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![1, 2]);
    // Should take at least 1ms (1ms between items)
    assert!(elapsed >= Duration::from_millis(1));
}

#[tokio::test]
async fn test_throttle_single_item() {
    // Test with just 1 item - should not hang
    println!("Starting single item throttle test...");
    let start = Instant::now();
    let stream = stream_from_iter(vec![42]);
    let throttled = stream.throttle(Duration::from_millis(1));
    let result: Vec<_> = throttled.collect().await;
    let elapsed = start.elapsed();
    
    println!("Single item test: elapsed={:?}, result={:?}", elapsed, result);
    assert_eq!(result, vec![42]);
    // Should be fast since only one item
    assert!(elapsed < Duration::from_millis(10));
}

#[tokio::test]
async fn test_throttle_debug() {
    // Minimal test to debug hanging
    println!("Starting debug throttle test...");
    
    // Test with 2ms duration to ensure it's above the bypass threshold
    let stream = stream_from_iter(vec![1, 2]);
    let throttled = stream.throttle(Duration::from_millis(2));
    
    println!("Created throttled stream, starting collection...");
    let result: Vec<_> = throttled.collect().await;
    println!("Collection completed: {:?}", result);
    
    assert_eq!(result, vec![1, 2]);
}

#[tokio::test]
async fn test_throttle_minimal() {
    // Minimal test to debug hanging - just 2 items with 1ms throttling
    println!("Starting minimal throttle test...");
    
    let stream = stream_from_iter(vec![1, 2]);
    println!("Created stream");
    
    let throttled = stream.throttle(Duration::from_millis(1));
    println!("Created throttled stream");
    
    println!("Starting collection...");
    let result: Vec<_> = throttled.collect().await;
    println!("Collection completed: {:?}", result);
    
    assert_eq!(result, vec![1, 2]);
}

#[tokio::test]
async fn test_throttle_debug_step_by_step() {
    // Very simple test to debug step by step
    println!("Starting step-by-step debug test...");
    
    // Create a simple stream with just 1 item
    let stream = stream_from_iter(vec![1]);
    println!("Created stream with 1 item");
    
    // Create throttled stream with 1ms duration
    let throttled = stream.throttle(Duration::from_millis(1));
    println!("Created throttled stream");
    
    // Try to collect just the first item
    println!("Starting collection...");
    let mut stream = throttled;
    let first_item = stream.next().await;
    println!("Got first item: {:?}", first_item);
    
    assert_eq!(first_item, Some(1));
}

#[tokio::test]
async fn test_throttle_two_items() {
    // Test with 2 items to see where hanging occurs
    println!("Starting two items test...");
    
    // Create a stream with 2 items
    let stream = stream_from_iter(vec![1, 2]);
    println!("Created stream with 2 items");
    
    // Create throttled stream with 1ms duration
    let throttled = stream.throttle(Duration::from_millis(1));
    println!("Created throttled stream");
    
    // Try to collect both items
    println!("Starting collection...");
    let mut stream = throttled;
    let first_item = stream.next().await;
    println!("Got first item: {:?}", first_item);
    
    let second_item = stream.next().await;
    println!("Got second item: {:?}", second_item);
    
    assert_eq!(first_item, Some(1));
    assert_eq!(second_item, Some(2));
}

#[tokio::test]
async fn test_throttle_timing_debug() {
    // Test with timing to see exactly where hanging occurs
    println!("Starting timing debug test...");
    
    let start = Instant::now();
    
    // Create a stream with 2 items
    let stream = stream_from_iter(vec![1, 2]);
    println!("Created stream at {:?}", start.elapsed());
    
    // Create throttled stream with 1ms duration
    let throttled = stream.throttle(Duration::from_millis(1));
    println!("Created throttled stream at {:?}", start.elapsed());
    
    // Try to collect both items with timing
    println!("Starting collection at {:?}", start.elapsed());
    let mut stream = throttled;
    
    let first_start = Instant::now();
    let first_item = stream.next().await;
    let first_elapsed = first_start.elapsed();
    println!("Got first item: {:?} in {:?}", first_item, first_elapsed);
    
    let second_start = Instant::now();
    let second_item = stream.next().await;
    let second_elapsed = second_start.elapsed();
    println!("Got second item: {:?} in {:?}", second_item, second_elapsed);
    
    let total_elapsed = start.elapsed();
    println!("Total elapsed: {:?}", total_elapsed);
    
    assert_eq!(first_item, Some(1));
    assert_eq!(second_item, Some(2));
    
    // The second item should take at least 1ms due to throttling
    assert!(second_elapsed >= Duration::from_millis(1));
}

#[tokio::test]
async fn test_throttle_above_threshold() {
    // Test with duration definitely above the bypass threshold
    println!("Starting above threshold test...");
    
    let start = Instant::now();
    
    // Create a stream with 2 items
    let stream = stream_from_iter(vec![1, 2]);
    println!("Created stream at {:?}", start.elapsed());
    
    // Create throttled stream with 2ms duration (definitely above 1ms threshold)
    let throttled = stream.throttle(Duration::from_millis(2));
    println!("Created throttled stream at {:?}", start.elapsed());
    
    // Try to collect both items with timing
    println!("Starting collection at {:?}", start.elapsed());
    let mut stream = throttled;
    
    let first_start = Instant::now();
    let first_item = stream.next().await;
    let first_elapsed = first_start.elapsed();
    println!("Got first item: {:?} in {:?}", first_item, first_elapsed);
    
    let second_start = Instant::now();
    let second_item = stream.next().await;
    let second_elapsed = second_start.elapsed();
    println!("Got second item: {:?} in {:?}", second_item, second_elapsed);
    
    let total_elapsed = start.elapsed();
    println!("Total elapsed: {:?}", total_elapsed);
    
    assert_eq!(first_item, Some(1));
    assert_eq!(second_item, Some(2));
    
    // The second item should take at least 2ms due to throttling
    assert!(second_elapsed >= Duration::from_millis(2));
}

#[tokio::test]
async fn test_basic_stream_no_throttle() {
    // Test basic stream without throttling
    println!("Starting basic stream test...");
    
    let start = Instant::now();
    
    // Create a stream with 2 items
    let stream = stream_from_iter(vec![1, 2]);
    println!("Created stream at {:?}", start.elapsed());
    
    // Try to collect both items without throttling
    println!("Starting collection at {:?}", start.elapsed());
    let mut stream = stream;
    
    let first_start = Instant::now();
    let first_item = stream.next().await;
    let first_elapsed = first_start.elapsed();
    println!("Got first item: {:?} in {:?}", first_item, first_elapsed);
    
    let second_start = Instant::now();
    let second_item = stream.next().await;
    let second_elapsed = second_start.elapsed();
    println!("Got second item: {:?} in {:?}", second_item, second_elapsed);
    
    let total_elapsed = start.elapsed();
    println!("Total elapsed: {:?}", total_elapsed);
    
    assert_eq!(first_item, Some(1));
    assert_eq!(second_item, Some(2));
}

#[tokio::test]
async fn test_throttle_bypass() {
    // Test with duration that should bypass throttling
    println!("Starting bypass test...");
    
    let start = Instant::now();
    
    // Create a stream with 2 items
    let stream = stream_from_iter(vec![1, 2]);
    println!("Created stream at {:?}", start.elapsed());
    
    // Create throttled stream with 0.5ms duration (should bypass throttling)
    let throttled = stream.throttle(Duration::from_micros(500));
    println!("Created throttled stream at {:?}", start.elapsed());
    
    // Try to collect both items with timing
    println!("Starting collection at {:?}", start.elapsed());
    let mut stream = throttled;
    
    let first_start = Instant::now();
    let first_item = stream.next().await;
    let first_elapsed = first_start.elapsed();
    println!("Got first item: {:?} in {:?}", first_item, first_elapsed);
    
    let second_start = Instant::now();
    let second_item = stream.next().await;
    let second_elapsed = second_start.elapsed();
    println!("Got second item: {:?} in {:?}", second_item, second_elapsed);
    
    let total_elapsed = start.elapsed();
    println!("Total elapsed: {:?}", total_elapsed);
    
    assert_eq!(first_item, Some(1));
    assert_eq!(second_item, Some(2));
    
    // Should be very fast since throttling is bypassed
    assert!(total_elapsed < Duration::from_millis(1));
} 