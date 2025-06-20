//! Example of custom codec configuration for media streaming
//!
//! This example demonstrates how to:
//! 1. Create a custom codec configuration
//! 2. Create a MediaCodec with the custom configuration
//! 3. Use the codec to encode and decode media data
//! 4. Monitor codec performance

use futures_util::StreamExt;
use rs2_stream::media::codec::{CodecFactory, EncodingConfig, MediaCodec, RawMediaData};
use rs2_stream::media::types::{MediaType, QualityLevel};
use rs2_stream::rs2::*;
use std::collections::HashMap;
use std::time::Duration;

// Define a struct to represent raw media data
struct RawVideoFrame {
    width: u32,
    height: u32,
    data: Vec<u8>,
    timestamp: Duration,
    is_keyframe: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Custom Codec Configuration Example");

    // 1. Create a custom encoding configuration
    let custom_config = EncodingConfig {
        quality: QualityLevel::High,
        target_bitrate: 4_000_000, // 4 Mbps
        keyframe_interval: 120,    // Every 2 seconds at 60fps
        enable_compression: true,
        preserve_metadata: true,
    };

    println!("Created custom codec configuration:");
    println!("  Quality: {:?}", custom_config.quality);
    println!("  Target bitrate: {} bps", custom_config.target_bitrate);
    println!(
        "  Keyframe interval: {} frames",
        custom_config.keyframe_interval
    );
    println!(
        "  Compression enabled: {}",
        custom_config.enable_compression
    );
    println!("  Preserve metadata: {}", custom_config.preserve_metadata);

    // 2. Create a codec with the custom configuration
    let custom_codec = MediaCodec::new(custom_config);

    // 3. Create a standard codec for comparison
    let standard_codec = CodecFactory::create_h264_codec(QualityLevel::High);

    // 4. Generate some test frames
    let test_frames = generate_test_frames(10);
    println!("Generated {} test frames", test_frames.len());

    // 5. Encode frames with both codecs and compare
    println!("\nEncoding test frames with both codecs...");

    let mut custom_encoded_size = 0;
    let mut standard_encoded_size = 0;

    for (i, frame) in test_frames.iter().enumerate() {
        // Convert to the format expected by the codec
        let raw_data = RawMediaData {
            data: frame.data.clone(),
            media_type: MediaType::Video,
            timestamp: frame.timestamp,
            metadata: {
                let mut metadata = HashMap::new();
                metadata.insert("width".to_string(), frame.width.to_string());
                metadata.insert("height".to_string(), frame.height.to_string());
                metadata.insert("is_keyframe".to_string(), frame.is_keyframe.to_string());
                metadata
            },
        };

        // Create a stream of raw data
        let raw_data_stream = from_iter(vec![raw_data.clone()]);

        // Encode with custom codec
        let mut custom_stream =
            custom_codec.encode_stream(raw_data_stream, "test-stream".to_string());

        // Get the first (and only) result
        if let Some(custom_result) = custom_stream.next().await {
            match custom_result {
                Ok(custom_chunk) => {
                    custom_encoded_size += custom_chunk.data.len();

                    println!(
                        "Frame {}: Custom codec: {} bytes",
                        i,
                        custom_chunk.data.len()
                    );

                    // Decode the custom-encoded chunk as a demonstration
                    if i == 0 {
                        match custom_codec.decode_chunk(custom_chunk).await {
                            Ok(decoded) => {
                                println!(
                                    "Successfully decoded frame: {} bytes",
                                    decoded.data.len()
                                );
                            }
                            Err(e) => {
                                println!("Failed to decode: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Custom codec encoding error: {:?}", e);
                }
            }
        }

        // Create another stream for the standard codec
        let raw_data_stream = from_iter(vec![raw_data]);

        // Encode with standard codec
        let mut standard_stream =
            standard_codec.encode_stream(raw_data_stream, "test-stream".to_string());

        // Get the first (and only) result
        if let Some(standard_result) = standard_stream.next().await {
            match standard_result {
                Ok(standard_chunk) => {
                    standard_encoded_size += standard_chunk.data.len();

                    println!(
                        "Frame {}: Standard codec: {} bytes",
                        i,
                        standard_chunk.data.len()
                    );
                }
                Err(e) => {
                    println!("Standard codec encoding error: {:?}", e);
                }
            }
        }
    }

    // 6. Compare overall performance
    println!("\nEncoding Performance Comparison:");
    println!("  Custom codec total size: {} bytes", custom_encoded_size);
    println!(
        "  Standard codec total size: {} bytes",
        standard_encoded_size
    );

    let size_diff_percent = if standard_encoded_size > 0 {
        ((custom_encoded_size as f64 - standard_encoded_size as f64) / standard_encoded_size as f64)
            * 100.0
    } else {
        0.0
    };

    println!(
        "  Size difference: {:.1}% ({} is more efficient)",
        size_diff_percent.abs(),
        if size_diff_percent < 0.0 {
            "Custom codec"
        } else {
            "Standard codec"
        }
    );

    // 7. Get codec stats
    let custom_stats = custom_codec.get_stats().await;
    println!("\nCustom Codec Statistics:");
    println!("  Frames encoded: {}", custom_stats.frames_encoded);
    println!("  Frames decoded: {}", custom_stats.frames_decoded);
    println!("  Bytes processed: {}", custom_stats.bytes_processed);
    println!("  Encoding time: {} ms", custom_stats.encoding_time_ms);
    println!(
        "  Average compression ratio: {:.2}",
        custom_stats.average_compression_ratio
    );
    println!("  Error count: {}", custom_stats.error_count);

    Ok(())
}

// Helper function to generate test frames
fn generate_test_frames(count: usize) -> Vec<RawVideoFrame> {
    let mut frames = Vec::with_capacity(count);

    for i in 0..count {
        // Every 3rd frame is a keyframe
        let is_keyframe = i % 3 == 0;

        // Create a frame with random data
        let frame = RawVideoFrame {
            width: 1920,
            height: 1080,
            // In a real application, this would be actual video data
            // For this example, we just create a buffer of the right size
            data: vec![0u8; 1920 * 1080 * 3 / 8], // Approximate size for a compressed frame
            timestamp: Duration::from_millis((i as u64) * 16), // ~60fps
            is_keyframe,
        };

        frames.push(frame);
    }

    frames
}
