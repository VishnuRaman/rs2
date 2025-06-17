use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::error::Error;

// Simulate a slow operation
async fn process_item(item: i32) -> Result<i32, Box<dyn Error + Send + Sync>> {
    // Simulate processing time that varies based on the item value
    let delay = 10 + (item % 5) * 20;
    tokio::time::sleep(Duration::from_millis(delay as u64)).await;
    Ok(item * 2)
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n=== Basic Metrics Collection Example ===");

        // Create a stream of numbers
        let numbers = from_iter(1..=20);

        // Apply metrics collection to the stream
        let (metrics_stream, metrics) = numbers.with_metrics_rs2("numbers_stream".to_string());

        // Process the stream
        let results = metrics_stream
            .map_rs2(|n| n * 2)
            .collect::<Vec<_>>()
            .await;

        // Print the results
        println!("Processed {} numbers", results.len());

        // Print the metrics
        let metrics_data = metrics.lock().await;
        println!("\nStream Metrics:");
        println!("  Items processed: {}", metrics_data.items_processed);
        println!("  Bytes processed: {}", metrics_data.bytes_processed);
        println!("  Processing time: {:?}", metrics_data.processing_time);
        println!("  Throughput: {:.2} items/sec", metrics_data.throughput_items_per_sec());
        println!("  Throughput: {:.2} bytes/sec", metrics_data.throughput_bytes_per_sec());

        println!("\n=== Metrics for Async Processing Example ===");

        // Create a stream of numbers
        let numbers = from_iter(1..=10);

        // Apply metrics collection to the stream
        let (metrics_stream, metrics) = numbers.with_metrics_rs2("async_processing".to_string());

        // Process the stream with async operations
        let results = metrics_stream
            .eval_map_rs2(|n| async move {
                process_item(n).await.unwrap_or_else(|_| 0)
            })
            .collect::<Vec<_>>()
            .await;

        // Print the results
        println!("Processed {} numbers asynchronously", results.len());

        // Print the metrics
        let metrics_data = metrics.lock().await;
        println!("\nAsync Processing Metrics:");
        println!("  Items processed: {}", metrics_data.items_processed);
        println!("  Processing time: {:?}", metrics_data.processing_time);
        println!("  Throughput: {:.2} items/sec", metrics_data.throughput_items_per_sec());

        println!("\n=== Metrics for Different Stream Transformations ===");

        // Create separate streams for each transformation

        // 1. Filter operation with metrics
        let (filter_stream, filter_metrics) = from_iter(1..=100)
            .with_metrics_rs2("filter_operation".to_string());

        let filter_results = filter_stream
            .filter_rs2(|n| n % 2 == 0)  // Keep only even numbers
            .collect::<Vec<_>>()
            .await;

        // 2. Map operation with metrics
        let (map_stream, map_metrics) = from_iter(1..=100)
            .with_metrics_rs2("map_operation".to_string());

        let map_results = map_stream
            .map_rs2(|n| n * 3)  // Triple each number
            .collect::<Vec<_>>()
            .await;

        // 3. Flat map operation with metrics
        let (flat_map_stream, flat_map_metrics) = from_iter(1..=100)
            .with_metrics_rs2("flat_map_operation".to_string());

        let flat_map_results = flat_map_stream
            .flat_map_rs2(|n| {
                if n % 10 == 0 {
                    from_iter(vec![n, n+1, n+2])  // Expand multiples of 10
                } else {
                    from_iter(vec![n])
                }
            })
            .collect::<Vec<_>>()
            .await;

        // Print comparison of metrics
        println!("\nComparison of Stream Transformation Metrics:");

        let filter_data = filter_metrics.lock().await;
        println!("\nFilter Operation (even numbers only):");
        println!("  Input items: 100");
        println!("  Output items: {}", filter_results.len());
        println!("  Processing time: {:?}", filter_data.processing_time);
        println!("  Throughput: {:.2} items/sec", filter_data.throughput_items_per_sec());

        let map_data = map_metrics.lock().await;
        println!("\nMap Operation (triple each number):");
        println!("  Input items: 100");
        println!("  Output items: {}", map_results.len());
        println!("  Processing time: {:?}", map_data.processing_time);
        println!("  Throughput: {:.2} items/sec", map_data.throughput_items_per_sec());

        let flat_map_data = flat_map_metrics.lock().await;
        println!("\nFlat Map Operation (expand multiples of 10):");
        println!("  Input items: 100");
        println!("  Output items: {}", flat_map_results.len());
        println!("  Processing time: {:?}", flat_map_data.processing_time);
        println!("  Throughput: {:.2} items/sec", flat_map_data.throughput_items_per_sec());
    });
}
