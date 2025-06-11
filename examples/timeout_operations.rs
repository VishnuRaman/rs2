use rs2::rs2::*;
use rs2::error::StreamError;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::{Duration, Instant};
use std::error::Error;
use async_stream::stream;

// Simulate a slow operation that might time out
async fn slow_operation(id: u32, delay_ms: u64) -> Result<String, Box<dyn Error + Send + Sync>> {
    println!("Starting operation {} with delay {}ms", id, delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    println!("Completed operation {}", id);
    Ok(format!("Result from operation {}", id))
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n=== 1. Timeout Example ===");

        // Create a stream of operations with different delays
        let operations = from_iter(vec![
            (1, 50),   // Fast operation (50ms)
            (2, 150),  // Medium operation (150ms)
            (3, 300),  // Slow operation (300ms)
            (4, 500),  // Very slow operation (500ms)
        ]);

        // Apply a timeout of 200ms to each operation
        let timeout_duration = Duration::from_millis(200);
        let results = operations
            .eval_map_rs2(move |(id, delay)| async move {
                // Add timeout to the operation
                match tokio::time::timeout(
                    timeout_duration,
                    slow_operation(id, delay)
                ).await {
                    Ok(Ok(result)) => (id, format!("Success: {}", result)),
                    Ok(Err(e)) => (id, format!("Error: {}", e)),
                    Err(_) => (id, format!("Timeout after {}ms", timeout_duration.as_millis())),
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("\nTimeout Results:");
        for (id, result) in results {
            println!("  Operation {}: {}", id, result);
        }

        // Alternative using timeout_rs2
        println!("\n=== 2. Using timeout_rs2 ===");

        let operations = from_iter(vec![
            (1, 50),   // Fast operation (50ms)
            (2, 250),  // Slow operation (250ms)
        ]);

        let results = operations
            .eval_map_rs2(|(id, delay)| slow_operation(id, delay))
            .timeout_rs2(Duration::from_millis(150))
            .collect::<Vec<_>>()
            .await;

        println!("\nTimeout Results using timeout_rs2:");
        for result in results {
            match result {
                Ok(Ok(value)) => println!("  Success: {}", value),
                Ok(Err(e)) => println!("  Error: {}", e),
                Err(StreamError::Timeout) => println!("  Operation timed out"),
                Err(e) => println!("  Other error: {:?}", e),
            }
        }

        println!("\n=== 3. Throttle Example ===");

        // Create a stream that emits values rapidly
        let start = Instant::now();
        let rapid_stream = from_iter(0..10);

        // Throttle the stream to emit at most one element per 100ms
        let throttled = rapid_stream
            .throttle_rs2(Duration::from_millis(100))
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("\nThrottled {} elements in {:?}", throttled.len(), elapsed);
        println!("Expected minimum time: {:?}", Duration::from_millis(100 * (throttled.len() as u64 - 1)));
        println!("Elements: {:?}", throttled);

        println!("\n=== 4. Debounce Example ===");

        // Simulate a stream of user input events with varying gaps
        let start = Instant::now();

        // Create a stream of (value, delay_before_next) pairs
        let input_events = vec![
            ("a", 10), // 10ms gap
            ("b", 20), // 20ms gap
            ("c", 30), // 30ms gap
            ("d", 200), // 200ms gap (longer than debounce period)
            ("e", 10), // 10ms gap
            ("f", 300), // 300ms gap (longer than debounce period)
        ];

        // Create a stream that emits these events with the specified timing
        let input_stream = stream! {
            for (value, delay) in input_events {
                yield value;
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
        }.boxed();

        // Debounce with a 100ms quiet period
        let debounced = input_stream
            .debounce_rs2(Duration::from_millis(100))
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("\nDebounced stream collected in {:?}", elapsed);
        println!("Original events: a, b, c, d, e, f");
        println!("Debounced events (expected 'd' and 'f'): {:?}", debounced);

        println!("\n=== 5. Sample Example ===");

        // Create a stream that emits values at varying rates
        let start = Instant::now();

        // Create a stream that emits a value every 10ms for 500ms
        let fast_stream = stream! {
            let mut i = 0;
            loop {
                yield i;
                i += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;

                // Stop after 500ms
                if start.elapsed() > Duration::from_millis(500) {
                    break;
                }
            }
        }.boxed();

        // Sample the stream every 100ms
        let sampled = fast_stream
            .sample_rs2(Duration::from_millis(100))
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("\nSampled stream collected in {:?}", elapsed);
        println!("Expected approximately {} samples", elapsed.as_millis() / 100);
        println!("Actual samples: {} - {:?}", sampled.len(), sampled);

        println!("\n=== 6. Emit After Example ===");

        // Create a stream that emits a value after a delay
        let start = Instant::now();
        let delayed_value = emit_after("Delayed value", Duration::from_millis(300))
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("\nEmit after collected in {:?}", elapsed);
        println!("Expected delay: 300ms");
        println!("Value: {:?}", delayed_value[0]);
    });
}
