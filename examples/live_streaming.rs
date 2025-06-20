//! Example of live streaming using the RS2 media streaming components
//!
//! This example demonstrates how to:
//! 1. Create a MediaStreamingService for live streaming
//! 2. Configure a live media stream
//! 3. Start a live stream
//! 4. Process and display the media chunks
//! 5. Monitor stream metrics in real-time

use chrono::Utc;
use futures_util::StreamExt;
use rs2_stream::media::streaming::StreamingServiceFactory;
use rs2_stream::media::types::{MediaChunk, MediaStream, MediaType, QualityLevel};
use rs2_stream::rs2::*;
use rs2_stream::stream_performance_metrics::StreamMetrics;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a streaming service optimized for live streaming
    let streaming_service = StreamingServiceFactory::create_low_latency_service();

    // Configure the live stream
    let stream_config = MediaStream {
        id: "example-live-stream".to_string(),
        user_id: 1,
        content_type: MediaType::Mixed,
        quality: QualityLevel::High,
        chunk_size: 32 * 1024, // 32KB chunks (smaller for lower latency)
        created_at: Utc::now(),
        metadata: HashMap::new(),
    };

    println!("Starting live stream with ID: {}", stream_config.id);

    // Start the live stream
    let chunk_stream = streaming_service.start_live_stream(stream_config).await;

    // Create a metrics monitor in a separate task
    let metrics_stream = streaming_service.get_metrics_stream();
    tokio::spawn(monitor_metrics(metrics_stream));

    // Process the chunks
    let mut chunk_count = 0;

    // Pin the stream to the stack
    let mut chunk_stream = std::pin::pin!(chunk_stream);

    // Process chunks for 10 seconds
    let start_time = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(10);

    println!("Processing live stream for 10 seconds...");

    while let Some(chunk) = chunk_stream.next().await {
        chunk_count += 1;

        // Process the chunk (in a real app, this would render the media)
        process_chunk(&chunk);

        // Print info every 10 chunks
        if chunk_count % 10 == 0 {
            println!(
                "Processed {} chunks, latest: type={:?}, size={} bytes",
                chunk_count,
                chunk.chunk_type,
                chunk.data.len()
            );
        }

        // Check if we've been running for the desired duration
        if start_time.elapsed() >= duration {
            println!("Time limit reached, stopping stream");
            break;
        }
    }

    // Get final metrics
    let metrics = streaming_service.get_metrics().await;
    println!("\nFinal Stream Metrics:");
    println!("  Name: {}", metrics.name.as_deref().unwrap_or("unknown"));
    println!("  Bytes processed: {}", metrics.bytes_processed);
    println!("  Items processed: {}", metrics.items_processed);
    println!("  Errors: {}", metrics.errors);
    println!(
        "  Average item size: {:.2} bytes",
        metrics.average_item_size
    );
    println!("  Processing time: {:?}", metrics.processing_time);

    // Shutdown the streaming service
    streaming_service.shutdown().await;
    println!("Streaming service shut down");

    // Wait a moment for the metrics monitor to finish
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

// Helper function to simulate processing a chunk
fn process_chunk(_chunk: &MediaChunk) {
    // In a real application, you would decode and render the chunk
    // For this example, we just simulate processing time
    std::thread::sleep(std::time::Duration::from_millis(5));
}

// Function to monitor metrics in real-time
async fn monitor_metrics(metrics_stream: RS2Stream<StreamMetrics>) {
    let mut metrics_stream = std::pin::pin!(metrics_stream);

    println!("Starting metrics monitor...");

    while let Some(metrics) = metrics_stream.next().await {
        println!(
            "[Metrics] Items: {}, Errors: {}, Avg size: {:.1} bytes",
            metrics.items_processed, metrics.errors, metrics.average_item_size
        );

        // Short sleep to avoid flooding the console
        sleep(Duration::from_secs(1)).await;
    }

    println!("Metrics monitor stopped");
}
