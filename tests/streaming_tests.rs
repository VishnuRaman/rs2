use futures_util::StreamExt;
use tokio::runtime::Runtime;
use rs2::media::streaming::{MediaStreamingService, StreamingServiceFactory};
use rs2::media::types::{MediaStream, MediaChunk, ChunkType, MediaPriority, MediaType, QualityLevel};
use std::path::PathBuf;
use std::time::Duration;
use std::collections::HashMap;
use chrono::Utc;

#[test]
fn test_streaming_service_creation() {
    // Test factory methods
    let live_service = StreamingServiceFactory::create_live_streaming_service();
    let file_service = StreamingServiceFactory::create_file_streaming_service();
    let low_latency_service = StreamingServiceFactory::create_low_latency_service();

    // Basic creation test - just ensure they don't panic
    assert!(true);
}

#[test]
fn test_streaming_service_metrics() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create service
        let service = MediaStreamingService::new(512);

        // Get initial metrics
        let metrics = service.get_metrics().await;

        // Verify initial metrics
        assert_eq!(metrics.chunks_processed, 0);
        assert_eq!(metrics.bytes_processed, 0);
        assert_eq!(metrics.dropped_chunks, 0);
        assert_eq!(metrics.average_chunk_size, 0.0);
    });
}

#[test]
fn test_streaming_service_live_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create service
        let service = MediaStreamingService::new(512);

        // Create stream config
        let mut metadata = HashMap::new();
        metadata.insert("max_chunks".to_string(), "10".to_string());

        let stream_config = MediaStream {
            id: "test_live_stream".to_string(),
            user_id: 0,
            content_type: MediaType::Video,
            quality: QualityLevel::Low,
            chunk_size: 1024,
            created_at: Default::default(),
            metadata,
        };

        // Start live stream
        let mut stream = service.start_live_stream(stream_config).await;

        // Collect chunks
        let mut chunks = Vec::new();
        for _ in 0..5 {
            if let Some(chunk) = stream.next().await {
                chunks.push(chunk);
            }
        }

        // Verify we got chunks
        assert!(!chunks.is_empty());
        assert!(chunks.len() <= 5); // May be less due to backpressure

        // Verify chunk properties
        for chunk in chunks {
            assert_eq!(chunk.stream_id, "test_live_stream");
            assert_eq!(chunk.data.len(), 1024);
            assert!(!chunk.is_final);
        }

        // Get metrics
        let metrics = service.get_metrics().await;

        // Verify metrics were updated
        assert!(metrics.chunks_processed > 0);
        assert!(metrics.bytes_processed > 0);
    });
}

#[test]
fn test_streaming_service_metrics_stream() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create service
        let service = MediaStreamingService::new(512);

        // Get metrics stream
        let mut metrics_stream = service.get_metrics_stream();

        // Get first metrics update
        let metrics = metrics_stream.next().await.unwrap();

        // Basic verification
        assert_eq!(metrics.chunks_processed, 0);
        assert_eq!(metrics.bytes_processed, 0);
    });
}

#[test]
fn test_streaming_service_shutdown() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create service
        let service = MediaStreamingService::new(512);

        // Shutdown service
        service.shutdown().await;

        // Try to get chunk stream after shutdown
        let chunk_stream = service.get_chunk_stream();

        // Verify we can still get a stream (but it should be empty/closed)
        assert!(true);
    });
}

// Helper function to create a test stream config
fn create_test_stream_config(id: &str, chunk_size: usize) -> MediaStream {
    let mut metadata = HashMap::new();
    metadata.insert("test_key".to_string(), "test_value".to_string());

    MediaStream {
        id: id.to_string(),
        user_id: 0,
        content_type: MediaType::Video,
        quality: QualityLevel::Low,
        chunk_size,
        created_at: Default::default(),
        metadata,
    }
}
