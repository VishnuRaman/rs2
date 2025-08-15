
use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

// Simulate a slow operation that might time out
async fn slow_operation(id: u32, delay_ms: u64) -> Result<String, Box<dyn Error + Send + Sync>> {
    println!("  🔄 Starting operation {} with delay {}ms", id, delay_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    println!("  ✅ Completed operation {}", id);
    Ok(format!("Result from operation {}", id))
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("=== RS2 Timeout Operations Example ===\n");

        println!("1. Manual Timeout Example with tokio::time::timeout");
        
        // Create a stream of operations with different delays
        let operations = from_iter_rs2(vec![
            (1, 50),  // Fast operation (50ms)
            (2, 150), // Medium operation (150ms)
            (3, 300), // Slow operation (300ms)
            (4, 500), // Very slow operation (500ms)
        ]);

        // Apply a timeout of 200ms to each operation manually
        let timeout_duration = Duration::from_millis(200);
        let results = operations
            .eval_map_rs2(move |(id, delay)| async move {
                // Add timeout to the operation using tokio::time::timeout
                match tokio::time::timeout(timeout_duration, slow_operation(id, delay)).await {
                    Ok(Ok(result)) => (id, format!("✅ Success: {}", result)),
                    Ok(Err(e)) => (id, format!("❌ Error: {}", e)),
                    Err(_) => (
                        id,
                        format!("⏰ Timeout after {}ms", timeout_duration.as_millis()),
                    ),
                }
            })
            .collect_rs2()
            .await;

        println!("   Manual Timeout Results:");
        for (id, result) in results {
            println!("     Operation {}: {}", id, result);
        }

        println!("\n2. Throttle Example (simple)");

        // Create a simple stream without timing dependencies
        let numbers = from_iter_rs2(vec![1, 2, 3, 4, 5]);
        let start = Instant::now();
        
        // Throttle the stream to emit at most one element per 50ms  
        let throttled_results = throttle(numbers, Duration::from_millis(50))
            .collect_rs2()
            .await;

        let elapsed = start.elapsed();
        println!("   🐌 Throttled {} elements in {:?}", throttled_results.len(), elapsed);
        println!("   📊 Elements: {:?}", throttled_results);

        println!("\n3. Debounce Example (simple)");

        // Create a simple sequence to demonstrate debouncing concept
        let input_events = from_iter_rs2(vec!["a", "b", "c", "d", "e", "f"]);

        // Apply debounce - this will likely just pass through since we don't have real timing gaps
        let debounced_results = debounce(input_events, Duration::from_millis(10))
            .collect_rs2()
            .await;

        println!("   📥 Original events: a, b, c, d, e, f");
        println!("   📤 Debounced events: {:?}", debounced_results);

        println!("\n4. Emit After Example");

        // Create a stream that emits a value after a delay
        let start = Instant::now();
        let delayed_results = emit_after("🎉 Delayed value", Duration::from_millis(100))
            .collect_rs2()
            .await;

        let elapsed = start.elapsed();
        println!("   ⏰ Emit after collected in {:?}", elapsed);
        println!("   ⏱️  Expected delay: 100ms");
        println!("   📦 Value: {:?}", delayed_results[0]);

        println!("\n5. Sample Every Nth Example");

        // Create a stream and sample every 3rd element
        let numbers = from_iter_rs2(0..20);
        let sampled_nth = sample_every_nth(numbers, 3)
            .collect_rs2()
            .await;

        println!("   🔢 Original numbers: 0..20");
        println!("   🎯 Every 3rd element: {:?}", sampled_nth);

        println!("\n6. Sample First N Example");

        // Create a larger stream and take first 5 samples
        let numbers = from_iter_rs2(100..200);
        let first_samples = sample_first(numbers, 5)
            .collect_rs2()
            .await;

        println!("   📈 Original numbers: 100..200");
        println!("   🥇 First 5 samples: {:?}", first_samples);

        println!("\n=== Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Manual timeout handling with tokio::time::timeout");
        println!("2. Stream throttling with rs2::throttle");
        println!("3. Stream debouncing with rs2::debounce");
        println!("4. Delayed emission with rs2::emit_after");
        println!("5. Nth element sampling with rs2::sample_every_nth");
        println!("6. First N sampling with rs2::sample_first");
    });
}
