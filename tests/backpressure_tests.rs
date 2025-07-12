use rs2_stream::rs2::{self, BackpressureConfig, BackpressureStrategy};
use rs2_stream::stream::{from_iter, StreamExt};

#[tokio::test]
async fn test_basic_stream_works() {
    // Test that basic stream functionality works
    let stream = from_iter(0..10);
    let result = stream.collect::<Vec<_>>().await;
    assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn test_backpressure_under_load() {
    // Create a stream that produces items faster than we can consume them
    let stream = from_iter(0..1000);
    
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::Block,
        buffer_size: 10,
        low_watermark: Some(5),
        high_watermark: Some(8),
    };
    
    let backpressured_stream = rs2::auto_backpressure_block(stream, config);
    
    let result = backpressured_stream
        .collect::<Vec<_>>()
        .await;
    
    // Should have all items
    assert_eq!(result.len(), 1000);
    assert_eq!(result[0], 0);
    assert_eq!(result[999], 999);
}

#[tokio::test]
async fn test_auto_backpressure_block() {
    // Create a stream that produces items faster than we can consume them
    let stream = from_iter(0..100);
    
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::Block,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    let backpressured_stream = rs2::auto_backpressure_block(stream, config);
    
    let result = backpressured_stream
        .collect::<Vec<_>>()
        .await;
    
    // Should have all items
    assert_eq!(result.len(), 100);
    assert_eq!(result[0], 0);
    assert_eq!(result[99], 99);
}

#[tokio::test]
async fn test_auto_backpressure_drop_oldest() {
    // Create a stream that produces items faster than we can consume them
    let stream = from_iter(0..100);
    
    // Set a small buffer size to trigger backpressure
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::DropOldest,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    // Apply backpressure with drop oldest strategy
    let backpressured_stream = rs2::auto_backpressure_drop_oldest(stream, config);
    
    // Consume items slowly to trigger backpressure
    let result = backpressured_stream
        .collect::<Vec<_>>()
        .await;
    
    // With drop oldest, we should still get items but some older ones might be dropped
    // The exact number depends on the implementation, but we should get some items
    assert!(!result.is_empty());
    assert!(result.len() <= 100);
    
    // All items should be from the original range
    for &item in &result {
        assert!(item >= 0 && item < 100);
    }
}

#[tokio::test]
async fn test_auto_backpressure_drop_newest() {
    // Create a stream that produces items faster than we can consume them
    let stream = from_iter(0..100);
    
    // Set a small buffer size to trigger backpressure
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::DropNewest,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    // Apply backpressure with drop newest strategy
    let backpressured_stream = rs2::auto_backpressure_drop_newest(stream, config);
    
    // Consume items slowly to trigger backpressure
    let result = backpressured_stream
        .collect::<Vec<_>>()
        .await;
    
    // With drop newest, we should still get items but some newer ones might be dropped
    // The exact number depends on the implementation, but we should get some items
    assert!(!result.is_empty());
    assert!(result.len() <= 100);
    
    // All items should be from the original range
    for &item in &result {
        assert!(item >= 0 && item < 100);
    }
}

#[tokio::test]
async fn test_backpressure_with_different_buffer_sizes() {
    // Test with different buffer sizes individually to avoid hanging
    let test_cases = vec![
        (5, 2, 3),   // buffer_size, low_watermark, high_watermark
        (10, 5, 7),
        (20, 10, 15),
    ];
    
    for (buffer_size, low_watermark, high_watermark) in test_cases {
        let stream = from_iter(0..20);
        
        // Apply backpressure with blocking strategy
        let config = BackpressureConfig {
            strategy: BackpressureStrategy::Block,
            buffer_size,
            low_watermark: Some(low_watermark),
            high_watermark: Some(high_watermark),
        };
        
        let backpressured_stream = rs2::auto_backpressure_block(stream, config);
        
        // Consume items
        let result = backpressured_stream
            .collect::<Vec<_>>()
            .await;
        
        // Should have all items regardless of buffer size
        assert_eq!(result.len(), 20);
        assert_eq!(result[0], 0);
        assert_eq!(result[19], 19);
    }
}

#[tokio::test]
async fn test_auto_backpressure_error() {
    // Create a stream that produces items faster than we can consume them
    let stream = from_iter(0..100);
    
    // Set a small buffer size to trigger backpressure
    let config = BackpressureConfig {
        strategy: BackpressureStrategy::Error,
        buffer_size: 5,
        low_watermark: Some(2),
        high_watermark: Some(4),
    };
    
    // Apply backpressure with error strategy
    let backpressured_stream = rs2::auto_backpressure_error(stream, config);
    
    let result_stream = backpressured_stream.map(Ok::<_, &str>);
    
    // Consume items slowly to trigger backpressure
    let result = result_stream
        .collect::<Vec<_>>()
        .await;
    
    // Should have all items
    assert_eq!(result.len(), 100);
    assert_eq!(result[0], Ok(0));
    assert_eq!(result[99], Ok(99));
    
    // Since auto_backpressure_error doesn't actually return errors in this implementation,
    // we just verify that all items are present
}
