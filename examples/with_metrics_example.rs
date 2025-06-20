use futures_util::stream::StreamExt;
use rand::{thread_rng, Rng};
use rs2_stream::rs2::*;
use rs2_stream::stream_performance_metrics::HealthThresholds;
use std::error::Error;
use std::time::Duration;
use tokio::runtime::Runtime;

// Simulate a slow operation that may fail sometimes
async fn process_item(item: i32) -> Result<i32, Box<dyn Error + Send + Sync>> {
    // Simulate processing time that varies based on the item value
    let delay = 10 + (item % 5) * 20;
    tokio::time::sleep(Duration::from_millis(delay as u64)).await;

    // Increased error probability from 5% (1/20) to 20% (1/5)
    if thread_rng().gen_ratio(1, 2) {
        return Err("Random processing error".into());
    }

    Ok(item * 2)
}

// Enhanced metrics display function
fn print_enhanced_metrics(
    name: &str,
    metrics: &rs2_stream::stream_performance_metrics::StreamMetrics,
) {
    println!("\n📊 {} Metrics:", name);
    println!("  ✅ Items processed: {}", metrics.items_processed);
    println!("  📦 Bytes processed: {}", metrics.bytes_processed);
    println!("  ⏱️  Processing time: {:?}", metrics.processing_time);
    println!(
        "  🚀 Throughput (processing): {:.2} items/sec",
        metrics.throughput_items_per_sec()
    );
    println!(
        "  📈 Throughput (wall-clock): {:.2} items/sec",
        metrics.items_per_second
    );
    println!(
        "  💾 Bandwidth (processing): {:.2} KB/sec",
        metrics.throughput_bytes_per_sec() / 1000.0
    );
    println!(
        "  📊 Bandwidth (wall-clock): {:.2} KB/sec",
        metrics.bytes_per_second / 1000.0
    );
    println!(
        "  📏 Average item size: {:.1} bytes",
        metrics.average_item_size
    );
    println!(
        "  ❌ Errors: {} ({:.1}%)",
        metrics.errors,
        metrics.error_rate * 100.0
    );
    println!("  🔄 Retries: {}", metrics.retries);
    println!("  ⚠️  Consecutive errors: {}", metrics.consecutive_errors);
    println!(
        "  🐌 Peak processing time: {:?}",
        metrics.peak_processing_time
    );
    println!("  📊 Backpressure events: {}", metrics.backpressure_events);
    println!("  📋 Queue depth: {}", metrics.queue_depth);
    println!(
        "  🏥 Health: {}",
        if metrics.is_healthy() {
            "✅ Good"
        } else {
            "⚠️ Issues"
        }
    );
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Enhanced Stream Metrics Collection Example");

        println!("\n=== Basic Metrics Collection Example ===");

        // Create a stream of numbers
        let numbers = from_iter(1..=20);

        // Apply metrics collection to the stream
        let (metrics_stream, metrics) =
            numbers.with_metrics_rs2("numbers_stream".to_string(), HealthThresholds::default());

        // Process the stream with enhanced metrics tracking
        let mut results = Vec::new();
        let mut metrics_stream = std::pin::pin!(metrics_stream);

        while let Some(item) = metrics_stream.next().await {
            let start = std::time::Instant::now();

            // Simulate some processing
            let processed = item * 2;
            results.push(processed);

            // Record additional metrics
            {
                let mut m = metrics.lock().await;
                m.record_processing_time(start.elapsed());

                // Simulate occasional backpressure
                if thread_rng().gen_ratio(1, 10) {
                    m.record_backpressure();
                }

                // Update queue depth (simulated)
                m.update_queue_depth(thread_rng().gen_range(0..=10));
            }
        }

        println!("Processed {} numbers", results.len());

        // Print enhanced metrics
        let metrics_data = metrics.lock().await;
        print_enhanced_metrics("Basic Processing", &*metrics_data);

        println!("\n=== Error-Prone Async Processing Example ===");

        // Create a stream of numbers that might fail during processing
        let numbers = from_iter(1..=50);
        let (metrics_stream, metrics) =
            numbers.with_metrics_rs2("async_processing".to_string(), HealthThresholds::default());

        let mut success_count = 0;
        let mut error_count = 0;
        let mut retry_count = 0;

        let mut metrics_stream = std::pin::pin!(metrics_stream);

        while let Some(item) = metrics_stream.next().await {
            let start = std::time::Instant::now();

            // Try processing with retries
            let mut attempts = 0;
            let max_retries = 3;

            loop {
                match process_item(item).await {
                    Ok(_result) => {
                        success_count += 1;
                        {
                            let mut m = metrics.lock().await;
                            m.record_processing_time(start.elapsed());
                            if attempts > 0 {
                                m.retries += attempts; // Record total retry attempts
                            }
                        }
                        break;
                    }
                    Err(_e) => {
                        attempts += 1;
                        {
                            let mut m = metrics.lock().await;
                            m.record_error();
                            if attempts <= max_retries {
                                m.record_retry();
                            }
                        }

                        if attempts > max_retries {
                            error_count += 1;
                            println!(
                                "  ❌ Failed to process item {} after {} attempts",
                                item, attempts
                            );
                            break;
                        } else {
                            retry_count += 1;
                            // Exponential backoff
                            tokio::time::sleep(Duration::from_millis(
                                50 * 2_u64.pow(attempts as u32),
                            ))
                            .await;
                        }
                    }
                }
            }
        }

        println!(
            "✅ Successful: {} | ❌ Failed: {} | 🔄 Total retries: {}",
            success_count, error_count, retry_count
        );

        let metrics_data = metrics.lock().await;
        print_enhanced_metrics("Error-Prone Processing", &*metrics_data);

        println!("\n=== Stream Transformation Comparison ===");

        // Test different transformations with enhanced metrics

        // 1. Filter operation
        let (filter_stream, filter_metrics) = from_iter(1..=1000)
            .with_metrics_rs2("filter_operation".to_string(), HealthThresholds::default());

        // Clone metrics for use in the closure
        let filter_metrics_for_closure = filter_metrics.clone();

        let filter_results = filter_stream
            .filter_rs2(move |n| {
                // Simulate backpressure during heavy filtering
                if thread_rng().gen_ratio(1, 50) {
                    // In a real scenario, you'd record backpressure here
                    std::thread::sleep(Duration::from_micros(10));
                }

                // Simulate errors during filtering
                if thread_rng().gen_ratio(1, 10) {
                    // Record the error
                    tokio::spawn({
                        let metrics = filter_metrics_for_closure.clone();
                        async move {
                            let mut m = metrics.lock().await;
                            m.record_error();
                        }
                    });
                    return false; // Filter out this item due to "error"
                }

                n % 2 == 0 // Keep only even numbers
            })
            .collect::<Vec<_>>()
            .await;

        // Manually record some backpressure events for demonstration
        {
            let mut m = filter_metrics.lock().await;
            m.backpressure_events = filter_results.len() as u64 / 50; // Simulate some backpressure
        }

        // 2. Map operation with timing
        let (map_stream, map_metrics) = from_iter(1..=1000)
            .with_metrics_rs2("map_operation".to_string(), HealthThresholds::default());

        // Clone metrics for use in the closure
        let map_metrics_for_closure = map_metrics.clone();

        let map_results = map_stream
            .map_rs2(move |n| {
                // Simulate variable processing time
                std::thread::sleep(Duration::from_micros(n as u64 % 100));

                // Simulate errors during mapping
                if thread_rng().gen_ratio(1, 15) {
                    // Record the error
                    tokio::spawn({
                        let metrics = map_metrics_for_closure.clone();
                        async move {
                            let mut m = metrics.lock().await;
                            m.record_error();
                        }
                    });
                    // We still return a value since map_rs2 doesn't support filtering
                    return n * 3;
                }

                n * 3
            })
            .collect::<Vec<_>>()
            .await;

        // 3. Throttled operation
        let (throttled_stream, throttled_metrics) = from_iter(1..=100).with_metrics_rs2(
            "throttled_operation".to_string(),
            HealthThresholds::default(),
        );

        // We need to manually record errors for throttled operation
        // since throttle_rs2 doesn't take a closure where we could add error logic
        let throttled_metrics_for_errors = throttled_metrics.clone();

        // Spawn a task to simulate random errors during throttled processing
        tokio::spawn(async move {
            for _ in 0..20 {
                // Generate about 20 errors
                if thread_rng().gen_ratio(1, 5) {
                    let mut m = throttled_metrics_for_errors.lock().await;
                    m.record_error();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let throttled_results = throttled_stream
            .throttle_rs2(Duration::from_millis(10)) // Throttle to simulate load control
            .collect::<Vec<_>>()
            .await;

        // 4. Chunked operation with queue depth tracking
        let (chunked_stream, chunked_metrics) = from_iter(1..=200)
            .with_metrics_rs2("chunked_operation".to_string(), HealthThresholds::default());

        // Clone metrics before moving into closure
        let chunked_metrics_for_results = chunked_metrics.clone();
        let chunked_metrics_for_errors = chunked_metrics.clone();

        let chunked_results = chunked_stream
            .chunk_rs2(5) // Process in chunks of 5
            .enumerate()
            .map_rs2(move |(chunk_idx, chunk)| {
                // Simulate queue depth changes
                tokio::spawn({
                    let metrics = chunked_metrics.clone();
                    async move {
                        let mut m = metrics.lock().await;
                        m.update_queue_depth(chunk_idx as u64 % 10);
                    }
                });

                // Simulate errors during chunked processing
                if thread_rng().gen_ratio(1, 8) {
                    // Record the error
                    tokio::spawn({
                        let metrics = chunked_metrics_for_errors.clone();
                        async move {
                            let mut m = metrics.lock().await;
                            m.record_error();
                        }
                    });
                }

                chunk.len() // Return chunk size
            })
            .collect::<Vec<_>>()
            .await;

        // Print comparison
        println!("\n📊 Stream Transformation Comparison:");

        let filter_data = filter_metrics.lock().await;
        print_enhanced_metrics("Filter (even numbers)", &*filter_data);
        println!("  📉 Filtered from 1000 to {} items", filter_results.len());

        let map_data = map_metrics.lock().await;
        print_enhanced_metrics("Map (triple values)", &*map_data);
        println!("  🔢 Processed {} items", map_results.len());

        let throttled_data = throttled_metrics.lock().await;
        print_enhanced_metrics("Throttled Processing", &*throttled_data);
        println!("  🐌 Throttled {} items", throttled_results.len());

        let chunked_data = chunked_metrics_for_results.lock().await;
        print_enhanced_metrics("Chunked Processing", &*chunked_data);
        println!("  📦 Created {} chunks", chunked_results.len());

        println!("\n=== Performance Summary ===");
        println!(
            "🏆 Fastest throughput: Map operation ({:.2} items/sec)",
            map_data.throughput_items_per_sec()
        );
        println!(
            "🐌 Slowest throughput: Throttled operation ({:.2} items/sec)",
            throttled_data.throughput_items_per_sec()
        );
        println!(
            "📊 Most selective: Filter operation ({:.1}% pass rate)",
            filter_results.len() as f64 / 1000.0 * 100.0
        );

        // Health check summary
        println!("\n🏥 Health Check Summary:");
        println!(
            "  Metrics collection: {}",
            if metrics_data.is_healthy() {
                "✅ Healthy"
            } else {
                "⚠️ Issues"
            }
        );
        println!(
            "  Filter operation: {}",
            if filter_data.is_healthy() {
                "✅ Healthy"
            } else {
                "⚠️ Issues"
            }
        );
        println!(
            "  Map operation: {}",
            if map_data.is_healthy() {
                "✅ Healthy"
            } else {
                "⚠️ Issues"
            }
        );
        println!(
            "  Throttled operation: {}",
            if throttled_data.is_healthy() {
                "✅ Healthy"
            } else {
                "⚠️ Issues"
            }
        );
        println!(
            "  Chunked operation: {}",
            if chunked_data.is_healthy() {
                "✅ Healthy"
            } else {
                "⚠️ Issues"
            }
        );
    });
}
