use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use async_stream::stream;

// Simulate a stream that emits items with delays
fn delayed_stream<T: Clone + Send + 'static>(
    items: Vec<T>,
    delay_ms: u64,
    name: &str,
) -> RS2Stream<(String, T)> {
    let name = name.to_string();
    stream! {
        for item in items {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            println!("Stream '{}' emitting item", name);
            yield (name.clone(), item);
        }
    }.boxed()
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n=== Basic Interleave Example ===");

        // Create three streams with different data
        let stream1 = from_iter(vec![1, 4, 7, 10]);
        let stream2 = from_iter(vec![2, 5, 8]);
        let stream3 = from_iter(vec![3, 6, 9, 12, 15]);

        // Interleave the streams in round-robin fashion
        let interleaved = stream1
            .interleave_rs2(vec![stream2, stream3])
            .collect::<Vec<_>>()
            .await;

        println!("Interleaved streams: {:?}", interleaved);

        println!("\n=== Interleave with Different Stream Lengths ===");

        // Create streams of different lengths
        let short_stream = from_iter(vec!["a", "b"]);
        let medium_stream = from_iter(vec!["c", "d", "e", "f"]);
        let long_stream = from_iter(vec!["g", "h", "i", "j", "k", "l"]);

        // Interleave the streams
        let interleaved = short_stream
            .interleave_rs2(vec![medium_stream, long_stream])
            .collect::<Vec<_>>()
            .await;

        println!("Interleaved streams with different lengths:");
        println!("  {:?}", interleaved);

        println!("\n=== Interleave with Timing Demonstration ===");

        // Create streams that emit items at different rates
        let fast_stream = delayed_stream(vec![1, 2, 3], 50, "fast");
        let medium_stream = delayed_stream(vec![10, 20, 30], 100, "medium");
        let slow_stream = delayed_stream(vec![100, 200, 300], 150, "slow");

        // Interleave the streams
        println!("Interleaving streams with different emission rates...");
        let interleaved = fast_stream
            .interleave_rs2(vec![medium_stream, slow_stream])
            .collect::<Vec<_>>()
            .await;

        println!("\nInterleaved result:");
        for (source, value) in interleaved {
            println!("  From '{}' stream: {}", source, value);
        }

        println!("\n=== Interleave for Multiplexing Data Sources ===");

        // Simulate multiple data sources
        let user_events = from_iter(vec![
            "User 1 logged in",
            "User 2 logged in",
            "User 1 updated profile",
            "User 3 logged in",
        ].into_iter().map(|s| ("USER".to_string(), s.to_string())));

        let system_events = from_iter(vec![
            "System started",
            "CPU usage at 80%",
            "Memory usage at 60%",
            "Disk space low",
            "System update available",
        ].into_iter().map(|s| ("SYSTEM".to_string(), s.to_string())));

        let application_events = from_iter(vec![
            "Application started",
            "Database connected",
            "Cache initialized",
            "Request processed",
        ].into_iter().map(|s| ("APP".to_string(), s.to_string())));

        // Interleave all event streams
        let all_events = user_events
            .interleave_rs2(vec![system_events, application_events])
            .collect::<Vec<_>>()
            .await;

        println!("Multiplexed event stream:");
        for (i, (source, event)) in all_events.iter().enumerate() {
            println!("  {}. [{}] {}", i + 1, source, event);
        }

        println!("\n=== Interleave with Empty Streams ===");

        // Create a mix of empty and non-empty streams
        let stream1 = from_iter(vec![1, 2, 3]);
        let empty_stream1: RS2Stream<i32> = from_iter(vec![]);
        let stream2 = from_iter(vec![4, 5]);
        let empty_stream2: RS2Stream<i32> = from_iter(vec![]);

        // Interleave the streams
        let interleaved = stream1
            .interleave_rs2(vec![empty_stream1, stream2, empty_stream2])
            .collect::<Vec<_>>()
            .await;

        println!("Interleaved with empty streams: {:?}", interleaved);
    });
}