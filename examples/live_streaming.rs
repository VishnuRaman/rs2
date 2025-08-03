//! Example of live streaming using the RS2 media streaming components
//!
//! This example demonstrates how to:
//! 1. Create a MediaStreamingService for live streaming
//! 2. Configure a live media stream
//! 3. Start a live stream
//! 4. Process and display the media chunks
//! 5. Monitor stream metrics in real-time

use chrono::Utc;
use rs2_stream::stream::{Stream, StreamExt};
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::media::types::{MediaChunk, MediaStream, MediaType, QualityLevel, ChunkType, MediaPriority};
use rs2_stream::stream_performance_metrics::StreamMetrics;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use rs2_stream::media::StreamingServiceFactory;
use rs2_stream::media::codec::{MediaCodec, EncodingConfig};
use std::sync::atomic::{AtomicU64, Ordering};

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
    let mut chunk_stream = streaming_service.start_live_stream(stream_config).await;

    // Create a simple metrics monitor using a generated stream
    let base_metrics = StreamMetrics::new().with_name("live-stream-monitor".to_string());
    let metrics_stream = from_iter(0..10)
        .map_rs2(move |i| {
            let mut metrics = base_metrics.clone();
            metrics.items_processed = i as u64 * 10;
            metrics.bytes_processed = i as u64 * 32768; // 32KB per item
            metrics.average_item_size = 32768.0;
            metrics.errors = if i > 5 { 1 } else { 0 };
            metrics
        });
    
    tokio::spawn(monitor_metrics(metrics_stream));

    // Process the chunks
    let mut chunk_count = 0;

    // Process chunks for 10 seconds
    let start_time = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(10);

    println!("Processing live stream for 10 seconds...");

    while let Some(chunk) = chunk_stream.next().await {
        chunk_count += 1;

        // Process the chunk (in a real app, this would render the media)
        if let Err(e) = process_chunk(&chunk).await {
            eprintln!("Error processing chunk {}: {}", chunk.sequence_number, e);
        }

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

    // Create and demonstrate additional streaming features
    demonstrate_advanced_features().await?;

    // Shutdown the streaming service
    streaming_service.shutdown().await;
    println!("Streaming service shut down");

    // Wait a moment for the metrics monitor to finish
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

// Helper function to simulate processing a chunk
async fn process_chunk(chunk: &MediaChunk) -> Result<(), String> {
    println!("🎬 Processing chunk {} from stream '{}'", 
             chunk.sequence_number, chunk.stream_id);

    // Step 1: Validate chunk integrity
    validate_chunk_integrity(chunk)?;

    // Step 2: Decode the chunk using MediaCodec
    let codec = create_media_codec_for_chunk(chunk);
    let decoded_data = codec.decode_chunk(chunk.clone()).await
        .map_err(|e| format!("Decoding failed: {}", e))?;

    // Step 3: Process based on chunk type
    match chunk.chunk_type {
        ChunkType::VideoIFrame => {
            process_video_iframe(&decoded_data, chunk).await?;
        },
        ChunkType::VideoPFrame => {
            process_video_pframe(&decoded_data, chunk).await?;
        },
        ChunkType::VideoBFrame => {
            process_video_bframe(&decoded_data, chunk).await?;
        },
        ChunkType::Audio => {
            process_audio_chunk(&decoded_data, chunk).await?;
        },
        ChunkType::Metadata => {
            process_metadata_chunk(&decoded_data, chunk).await?;
        },
        ChunkType::Thumbnail => {
            process_thumbnail_chunk(&decoded_data, chunk).await?;
        },
    }

    // Step 4: Update processing statistics
    update_chunk_statistics(chunk).await;

    // Step 5: Simulate rendering/display time based on chunk type
    let processing_time = match chunk.chunk_type {
        ChunkType::VideoIFrame => Duration::from_millis(15), // I-frames take longer
        ChunkType::VideoPFrame => Duration::from_millis(8),  // P-frames are faster
        ChunkType::VideoBFrame => Duration::from_millis(5),  // B-frames are fastest
        ChunkType::Audio => Duration::from_millis(2),        // Audio is very fast
        ChunkType::Metadata => Duration::from_millis(1),     // Metadata is instant
        ChunkType::Thumbnail => Duration::from_millis(3),    // Thumbnails are quick
    };
    
    tokio::time::sleep(processing_time).await;

    println!("✅ Chunk {} processed successfully (took {:?})", 
             chunk.sequence_number, processing_time);

    Ok(())
}

// Validate chunk integrity and basic properties
fn validate_chunk_integrity(chunk: &MediaChunk) -> Result<(), String> {
    // Check basic validations
    if chunk.stream_id.is_empty() {
        return Err("Invalid chunk: empty stream ID".into());
    }
    
    if chunk.data.is_empty() {
        return Err("Invalid chunk: empty data".into());
    }

    // Validate checksum if present
    if let Some(expected_checksum) = chunk.checksum {
        let actual_checksum = calculate_simple_checksum(&chunk.data);
        if actual_checksum != expected_checksum {
            return Err("Chunk integrity check failed: checksum mismatch".into());
        }
    }

    // Validate data size based on chunk type
    match chunk.chunk_type {
        ChunkType::Audio => {
            if chunk.data.len() > 64 * 1024 {
                return Err("Audio chunk too large (>64KB)".into());
            }
        },
        ChunkType::VideoIFrame | ChunkType::VideoPFrame | ChunkType::VideoBFrame => {
            if chunk.data.len() > 1024 * 1024 {
                return Err("Video chunk too large (>1MB)".into());
            }
        },
        _ => {} // Other types have no size limits
    }

    println!("🔍 Chunk validation passed - Size: {} bytes, Type: {:?}", 
             chunk.data.len(), chunk.chunk_type);
    Ok(())
}

// Create appropriate codec for the chunk type
fn create_media_codec_for_chunk(chunk: &MediaChunk) -> MediaCodec {
    let quality = match chunk.priority {
        MediaPriority::Critical | MediaPriority::High => QualityLevel::High,
        MediaPriority::Normal => QualityLevel::Medium,
        MediaPriority::Low => QualityLevel::Low,
    };

    let config = EncodingConfig {
        quality,
        target_bitrate: match quality {
            QualityLevel::Low => 500_000,
            QualityLevel::Medium => 1_500_000,
            QualityLevel::High => 5_000_000,
            QualityLevel::UltraHigh => 15_000_000,
        },
        keyframe_interval: 30,
        enable_compression: true,
        preserve_metadata: true,
    };

    MediaCodec::new(config)
}

// Process video I-frame (keyframe)
async fn process_video_iframe(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    _chunk: &MediaChunk
) -> Result<(), String> {
    println!("🎥 Processing I-Frame (keyframe) - Size: {} bytes, Quality reference frame", 
             decoded_data.data.len());
    
    // I-frames are complete frames that can be decoded independently
    // In a real application, you would:
    // 1. Decode the full frame
    // 2. Update the decoder reference
    // 3. Display the frame
    // 4. Cache for future P/B frame references
    
    simulate_frame_rendering(decoded_data, "I-Frame").await;
    Ok(())
}

// Process video P-frame (predicted frame)
async fn process_video_pframe(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    _chunk: &MediaChunk
) -> Result<(), String> {
    println!("🎬 Processing P-Frame (predicted) - Size: {} bytes, References previous frames", 
             decoded_data.data.len());
    
    // P-frames reference previous frames for efficiency
    // In a real application, you would:
    // 1. Use previous I-frame or P-frame as reference
    // 2. Apply motion vectors and residuals
    // 3. Reconstruct the frame
    // 4. Display and cache for future references
    
    simulate_frame_rendering(decoded_data, "P-Frame").await;
    Ok(())
}

// Process video B-frame (bi-predicted frame)
async fn process_video_bframe(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    _chunk: &MediaChunk
) -> Result<(), String> {
    println!("🎞️ Processing B-Frame (bi-predicted) - Size: {} bytes, References past and future", 
             decoded_data.data.len());
    
    // B-frames can reference both past and future frames
    // In a real application, you would:
    // 1. Use both previous and next reference frames
    // 2. Apply bi-directional motion compensation
    // 3. Reconstruct the frame
    // 4. Display (B-frames typically aren't used as references)
    
    simulate_frame_rendering(decoded_data, "B-Frame").await;
    Ok(())
}

// Process audio chunk
async fn process_audio_chunk(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    _chunk: &MediaChunk
) -> Result<(), String> {
    println!("🎵 Processing Audio chunk - Size: {} bytes, Timestamp: {:?}", 
             decoded_data.data.len(), _chunk.timestamp);
    
    // Audio processing steps:
    // 1. Decode audio samples
    // 2. Apply audio filters/effects if needed
    // 3. Mix with other audio streams if necessary
    // 4. Send to audio output device
    
    // Simulate audio processing
    let sample_rate = 44100; // 44.1 kHz
    let channels = 2; // Stereo
    let samples_per_chunk = decoded_data.data.len() / (2 * channels); // 16-bit samples
    
    println!("   🎶 Audio: {}Hz, {} channels, {} samples", 
             sample_rate, channels, samples_per_chunk);
    
    Ok(())
}

// Process metadata chunk
async fn process_metadata_chunk(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    _chunk: &MediaChunk
) -> Result<(), String> {
    println!("📋 Processing Metadata chunk - Size: {} bytes", decoded_data.data.len());
    
    // Metadata can contain:
    // 1. Stream information (resolution, bitrate, etc.)
    // 2. Subtitle data
    // 3. Chapter markers
    // 4. Digital rights management info
    // 5. Custom application data
    
    // Try to parse metadata as JSON or other format
    if let Ok(metadata_str) = std::str::from_utf8(&decoded_data.data) {
        println!("   📄 Metadata content: {}", 
                 metadata_str.chars().take(100).collect::<String>());
    } else {
        println!("   📄 Binary metadata: {} bytes", decoded_data.data.len());
    }
    
    Ok(())
}

// Process thumbnail chunk
async fn process_thumbnail_chunk(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    _chunk: &MediaChunk
) -> Result<(), String> {
    println!("🖼️ Processing Thumbnail - Size: {} bytes", decoded_data.data.len());
    
    // Thumbnails are typically:
    // 1. Small JPEG or PNG images
    // 2. Used for video preview
    // 3. Generated from I-frames
    // 4. Cached for quick access
    
    // Simulate thumbnail processing
    let estimated_width = 160;
    let estimated_height = 90; // 16:9 aspect ratio
    
    println!("   🖼️ Thumbnail: {}x{} estimated dimensions", 
             estimated_width, estimated_height);
    
    Ok(())
}

// Simulate frame rendering with quality analysis
async fn simulate_frame_rendering(
    decoded_data: &rs2_stream::media::codec::RawMediaData, 
    frame_type: &str
) {
    // Simulate frame analysis
    let estimated_resolution = estimate_resolution(&decoded_data.data);
    let quality_score = calculate_quality_score(&decoded_data.data);
    
    println!("   📺 Rendering {} - Resolution: {}x{}, Quality: {:.2}", 
             frame_type, estimated_resolution.0, estimated_resolution.1, quality_score);
}

// Calculate simple checksum for integrity checking
fn calculate_simple_checksum(data: &[u8]) -> u32 {
    data.iter().fold(0u32, |acc, &byte| acc.wrapping_add(byte as u32))
}

// Estimate video resolution from data size
fn estimate_resolution(data: &[u8]) -> (u32, u32) {
    // Very rough estimation based on data size
    match data.len() {
        0..=10_000 => (320, 240),      // Very low resolution
        10_001..=50_000 => (640, 480),  // Low resolution
        50_001..=200_000 => (1280, 720), // HD
        200_001..=500_000 => (1920, 1080), // Full HD
        _ => (3840, 2160), // 4K
    }
}

// Calculate quality score based on data characteristics
fn calculate_quality_score(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    
    // Simple quality metric based on data variance
    let mean = data.iter().map(|&x| x as f64).sum::<f64>() / data.len() as f64;
    let variance = data.iter()
        .map(|&x| (x as f64 - mean).powi(2))
        .sum::<f64>() / data.len() as f64;
    
    // Normalize to 0-1 range
    (variance / 255.0).min(1.0)
}

// Update processing statistics with thread-safe atomics
async fn update_chunk_statistics(chunk: &MediaChunk) {
    // In a real application, you would update:
    // 1. Processing time metrics
    // 2. Quality scores
    // 3. Error rates
    // 4. Bandwidth utilization
    // 5. Buffer levels
    
    static PROCESSED_COUNT: AtomicU64 = AtomicU64::new(0);
    static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
    
    let count = PROCESSED_COUNT.fetch_add(1, Ordering::Relaxed);
    let bytes = TOTAL_BYTES.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
    
    if count % 10 == 0 {
        println!("📊 Statistics: {} chunks processed, {} total bytes", 
                 count, bytes + chunk.data.len() as u64);
    }
}

// Function to monitor metrics in real-time
async fn monitor_metrics(metrics_stream: impl Stream<Item = StreamMetrics> + Send + 'static) {
    println!("Starting metrics monitor...");

    // Use collect to gather all metrics and then process them
    let metrics_vec = metrics_stream.collect_rs2().await;
    
    for (i, metrics) in metrics_vec.iter().enumerate() {
        println!(
            "[Metrics {}] Items: {}, Errors: {}, Avg size: {:.1} bytes",
            i, metrics.items_processed, metrics.errors, metrics.average_item_size
        );
        
        // Simulate real-time monitoring with delays
        sleep(Duration::from_millis(500)).await;
    }

    println!("Metrics monitor completed");
}

// Demonstrate additional advanced streaming features
async fn demonstrate_advanced_features() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Demonstrating Advanced Features ===");
    
    // 1. Chunked processing with backpressure
    println!("1. Chunked stream processing...");
    let data_stream = from_iter(1..=20)
        .map_rs2(|i| format!("chunk-{}", i))
        .chunk_rs2(5);
    
    let chunks: Vec<_> = data_stream.collect_rs2().await;
    println!("   Created {} chunks from 20 items", chunks.len());
    
    // 2. Parallel processing demonstration
    println!("2. Parallel processing...");
    let parallel_stream = from_iter(1..=10)
        .par_eval_map_rs2(3, |i| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            i * 2
        });
    
    let results: Vec<_> = parallel_stream.collect_rs2().await;
    println!("   Parallel processing results: {:?}", results);
    
    // 3. Filtering and transformation
    println!("3. Filtering and transformation...");
    let filtered_stream = from_iter(1..=20)
        .filter_rs2(|&x| x % 2 == 0)
        .map_rs2(|x| x * x)
        .take_rs2(5);
    
    let filtered_results: Vec<_> = filtered_stream.collect_rs2().await;
    println!("   Filtered and squared results: {:?}", filtered_results);
    
    // 4. Stream interleaving
    println!("4. Stream interleaving...");
    let stream1 = from_iter(vec!["A1", "A2", "A3"]);
    let stream2 = from_iter(vec!["B1", "B2", "B3"]);
    let stream3 = from_iter(vec!["C1", "C2", "C3"]);
    
    let interleaved = stream1.interleave_rs2(vec![stream2, stream3]);
    let interleaved_results: Vec<_> = interleaved.collect_rs2().await;
    println!("   Interleaved results: {:?}", interleaved_results);
    
    // 5. Rate limiting demonstration
    println!("5. Rate limiting...");
    let rate_limited_stream = from_iter(1..=5)
        .throttle_rs2(Duration::from_millis(200));
    
    let start = std::time::Instant::now();
    let _: Vec<_> = rate_limited_stream.collect_rs2().await;
    let elapsed = start.elapsed();
    println!("   Rate limited processing took: {:?}", elapsed);
    
    println!("=== Advanced Features Demo Complete ===\n");
    Ok(())
}
