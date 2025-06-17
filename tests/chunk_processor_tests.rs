use futures_util::StreamExt;
use tokio::runtime::Runtime;
use rs2_stream::media::chunk_processor::{ChunkProcessor, ChunkProcessorConfig, ChunkProcessingError};
use rs2_stream::media::codec::{MediaCodec, EncodingConfig};
use rs2_stream::media::types::{MediaChunk, ChunkType, MediaPriority};
use rs2_stream::queue::Queue;
use rs2_stream::rs2::*;
use std::sync::Arc;
use std::time::Duration;
use std::collections::VecDeque;

#[test]
fn test_chunk_processor_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create dependencies
        let codec = Arc::new(MediaCodec::new(EncodingConfig::default()));
        let output_queue = Arc::new(Queue::<MediaChunk>::bounded(10));

        // Create processor with optimized config for testing
        let mut config = ChunkProcessorConfig::default();
        config.enable_reordering = false;  // Disable reordering for faster processing
        config.enable_validation = false;  // Disable validation for faster processing
        config.parallel_processing = 1;    // Use single thread for simple test
        config.max_buffer_size = 10;       // Smaller buffer size

        let processor = ChunkProcessor::new(
            config,
            codec,
            output_queue.clone()
        );

        // Create a test chunk
        let chunk = create_test_chunk("test_stream", 1, ChunkType::VideoIFrame);

        // Create a stream with a single chunk
        let chunk_stream = from_iter(vec![chunk.clone()]);

        // Process the chunk stream
        let mut result_stream = processor.process_chunk_stream(chunk_stream);

        // Get the result
        let result = result_stream.next().await.unwrap();

        // Verify result
        assert!(result.is_ok());

        // Verify chunk was added to output queue
        let mut dequeue_stream = output_queue.dequeue();
        let output_chunk = dequeue_stream.next().await.unwrap();
        assert_eq!(output_chunk.stream_id, "test_stream");
        assert_eq!(output_chunk.sequence_number, 1);
    });
}

#[test]
fn test_chunk_processor_validation() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create dependencies
        let codec = Arc::new(MediaCodec::new(EncodingConfig::default()));
        let output_queue = Arc::new(Queue::<MediaChunk>::bounded(10));

        // Create processor with validation enabled
        let mut config = ChunkProcessorConfig::default();
        config.enable_validation = true;
        let processor = ChunkProcessor::new(config, codec, output_queue.clone());

        // Create an invalid chunk (empty stream_id)
        let invalid_chunk = create_test_chunk("", 1, ChunkType::VideoIFrame);

        // Create a stream with the invalid chunk
        let invalid_chunk_stream = from_iter(vec![invalid_chunk]);

        // Process the invalid chunk stream
        let mut result_stream = processor.process_chunk_stream(invalid_chunk_stream);

        // Get the result
        let result = result_stream.next().await.unwrap();

        // Verify validation failed
        assert!(result.is_err());
        match result {
            Err(ChunkProcessingError::ValidationFailed(_)) => {
                // Expected error
            },
            _ => panic!("Expected ValidationFailed error"),
        }

        // Create a valid chunk
        let valid_chunk = create_test_chunk("test_stream", 2, ChunkType::VideoIFrame);

        // Create a stream with the valid chunk
        let valid_chunk_stream = from_iter(vec![valid_chunk]);

        // Process the valid chunk stream
        let mut result_stream = processor.process_chunk_stream(valid_chunk_stream);

        // Get the result
        let result = result_stream.next().await.unwrap();

        // Verify success
        assert!(result.is_ok());
    });
}

#[test]
fn test_chunk_processor_stats() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create dependencies
        let codec = Arc::new(MediaCodec::new(EncodingConfig::default()));
        let output_queue = Arc::new(Queue::<MediaChunk>::bounded(10));

        // Create processor with optimized config for testing
        let mut config = ChunkProcessorConfig::default();
        config.enable_reordering = false;  // Disable reordering for more predictable stats
        config.parallel_processing = 1;    // Use single thread for deterministic processing
        config.max_buffer_size = 10;       // Smaller buffer size

        let processor = ChunkProcessor::new(
            config,
            codec,
            output_queue.clone()
        );

        // Process chunks one by one to ensure stats are updated correctly
        for i in 1..=5 {
            let chunk = create_test_chunk("test_stream", i, ChunkType::VideoIFrame);

            // Create a stream with a single chunk
            let chunk_stream = from_iter(vec![chunk.clone()]);

            // Process the chunk
            let mut result_stream = processor.process_chunk_stream(chunk_stream);

            // Get and verify the result
            let result = result_stream.next().await.unwrap();
            assert!(result.is_ok());

            // Verify chunk was added to output queue
            let mut dequeue_stream = output_queue.dequeue();
            let output_chunk = dequeue_stream.next().await.unwrap();
            assert_eq!(output_chunk.sequence_number, i);
        }

        // Small delay to ensure all stats are updated
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Get stats
        let stats = processor.get_stats().await;

        // Verify stats
        assert_eq!(stats.chunks_processed, 5, "Expected 5 chunks processed, got {}", stats.chunks_processed);
        assert_eq!(stats.chunks_dropped, 0, "Expected 0 chunks dropped, got {}", stats.chunks_dropped);
        assert!(stats.average_processing_time_ms > 0.0, "Expected positive processing time, got {}", stats.average_processing_time_ms);
    });
}

// Helper function to create test chunks
fn create_test_chunk(stream_id: &str, sequence: u64, chunk_type: ChunkType) -> MediaChunk {
    MediaChunk {
        stream_id: stream_id.to_string(),
        sequence_number: sequence,
        data: vec![0u8; 1024], // 1KB of test data
        chunk_type,
        priority: MediaPriority::Normal,
        timestamp: Duration::from_millis(sequence * 33),
        is_final: false,
        checksum: None,
    }
}
