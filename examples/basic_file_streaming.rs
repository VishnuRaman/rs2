//! Example of basic file streaming using the RS2 media streaming components
//!
//! This example demonstrates how to:
//! 1. Create a MediaStreamingService
//! 2. Configure a media stream
//! 3. Start streaming from a file
//! 4. Process and display the media chunks

use rs2::media::streaming::{MediaStreamingService, StreamingServiceFactory};
use rs2::media::types::{MediaStream, MediaType, QualityLevel, MediaChunk};
use rs2::rs2::*;
use futures_util::StreamExt;
use std::path::PathBuf;
use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a streaming service using the factory
    let streaming_service = StreamingServiceFactory::create_file_streaming_service();

    // Configure the media stream
    let stream_config = MediaStream {
        id: "example-file-stream".to_string(),
        user_id: 1,
        content_type: MediaType::Mixed,
        quality: QualityLevel::High,
        chunk_size: 64 * 1024, // 64KB chunks
        created_at: Utc::now(),
        metadata: HashMap::new(),
    };

    // Path to the media file
    let file_path = PathBuf::from("path/to/your/media/file.mp4");

    println!("Starting file stream from: {:?}", file_path);

    // Start streaming from the file
    let chunk_stream = streaming_service.start_file_stream(file_path, stream_config).await;

    // Process the chunks
    let mut chunk_count = 0;
    let mut total_bytes = 0;

    // Pin the stream to the stack
    let mut chunk_stream = std::pin::pin!(chunk_stream);

    // Process up to 100 chunks or until the stream ends
    while let Some(chunk) = chunk_stream.next().await {
        chunk_count += 1;
        total_bytes += chunk.data.len();

        println!(
            "Received chunk #{}: type={:?}, size={} bytes, priority={:?}",
            chunk.sequence_number,
            chunk.chunk_type,
            chunk.data.len(),
            chunk.priority
        );

        // Stop after 100 chunks for this example
        if chunk_count >= 100 {
            break;
        }
    }

    // Get and display metrics
    let metrics = streaming_service.get_metrics().await;
    println!("\nStream Metrics:");
    println!("  Stream ID: {}", metrics.stream_id);
    println!("  Bytes processed: {}", metrics.bytes_processed);
    println!("  Chunks processed: {}", metrics.chunks_processed);
    println!("  Dropped chunks: {}", metrics.dropped_chunks);
    println!("  Average chunk size: {:.2} bytes", metrics.average_chunk_size);
    println!("  Buffer utilization: {:.2}%", metrics.buffer_utilization * 100.0);

    // Shutdown the streaming service
    streaming_service.shutdown().await;
    println!("Streaming service shut down");

    Ok(())
}

// Helper function to simulate processing a chunk
fn process_chunk(chunk: &MediaChunk) {
    // In a real application, you would decode and render the chunk
    // For this example, we just simulate processing time
    std::thread::sleep(Duration::from_millis(10));
}
