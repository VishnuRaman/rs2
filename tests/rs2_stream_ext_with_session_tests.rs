use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::session::{SessionBuilder, SessionPreset, set_global_session, clear_global_session, get_global_parallel_config, get_global_buffer_config, get_global_backpressure_config};
use serial_test::serial;
use std::time::Duration;
use tokio;

#[tokio::test]
#[serial]
async fn test_par_eval_map_with_session() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Set up a session with custom parallel configuration
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 4;
            p.max_buffer_size = 2000;
            p.task_timeout = Duration::from_secs(60);
        })
        .build();
    
    set_global_session(session_config);

    let numbers = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let stream = from_iter_rs2(numbers);
    
    let result_stream = stream.par_eval_map_with_session_rs2(|n| async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        n * 2
    });

    let results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 8);
    
    // Verify that all numbers were processed
    for (i, &result) in results.iter().enumerate() {
        assert_eq!(result, (i as i32 + 1) * 2);
    }
}

#[tokio::test]
#[serial]
async fn test_par_eval_map_unordered_with_session() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Set up a session with custom parallel configuration
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 3;
            p.max_buffer_size = 1500;
            p.task_timeout = Duration::from_secs(45);
        })
        .build();
    
    set_global_session(session_config);

    let numbers = vec![1, 2, 3, 4, 5];
    let stream = from_iter_rs2(numbers);
    
    let result_stream = stream.par_eval_map_unordered_with_session_rs2(|n| async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        n * 3
    });

    let results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 5);
    
    // Results may be out of order due to unordered processing
    let mut sorted_results = results.clone();
    sorted_results.sort();
    assert_eq!(sorted_results, vec![3, 6, 9, 12, 15]);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_map_parallel_with_session() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Set up a session with custom parallel configuration
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 2;
            p.max_buffer_size = 1000;
            p.task_timeout = Duration::from_secs(30);
        })
        .build();
    
    set_global_session(session_config);

    let numbers = vec![1, 2, 3, 4];
    let stream = from_iter_rs2(numbers);
    
    let result_stream = stream.map_parallel_with_session_rs2(|n| n * 4);

    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 4);
    
    // Sort results since parallel operations don't guarantee order
    results.sort();
    
    // Verify that all numbers were processed (in sorted order)
    let expected = vec![4, 8, 12, 16];
    assert_eq!(results, expected);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_par_join_with_session() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Set up a session with custom parallel configuration
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 5;
            p.max_buffer_size = 3000;
            p.task_timeout = Duration::from_secs(90);
        })
        .build();
    
    set_global_session(session_config);

    let stream1 = from_iter_rs2(vec![1, 2, 3]);
    let stream2 = from_iter_rs2(vec![4, 5, 6]);
    let stream3 = from_iter_rs2(vec![7, 8, 9]);
    
    let streams = vec![stream1, stream2, stream3];
    let result_stream = from_iter_rs2(streams).par_join_with_session_rs2();

    let results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 9);
    
    // Results may be interleaved due to parallel processing
    let mut sorted_results = results.clone();
    sorted_results.sort();
    assert_eq!(sorted_results, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_session_config_fallback() {
    // Don't set global session - should use defaults
    let numbers = vec![1, 2, 3];
    let stream = from_iter_rs2(numbers);
    
    let result_stream = stream.par_eval_map_with_session_rs2(|n| async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        n * 2
    });

    let results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 3);
    
    // Verify that all numbers were processed
    for (i, &result) in results.iter().enumerate() {
        assert_eq!(result, (i as i32 + 1) * 2);
    }
}

#[tokio::test]
#[serial]
async fn test_session_presets() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Test with development preset
    let dev_session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    
    set_global_session(dev_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Development preset should have lower concurrency
    assert_eq!(parallel_config.concurrency, 2);

    let buffer_config = get_global_buffer_config();
    assert!(buffer_config.is_some());
    let buffer_config = buffer_config.unwrap();
    
    // Development preset should have reasonable buffer settings
    assert!(buffer_config.initial_capacity > 0);

    // Test with production preset
    let prod_session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    
    set_global_session(prod_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Production preset should have higher concurrency
    assert_eq!(parallel_config.concurrency, 16);

    let backpressure_config = get_global_backpressure_config();
    assert!(backpressure_config.is_some());
    let backpressure_config = backpressure_config.unwrap();
    
    // Production preset should use blocking strategy
    assert_eq!(backpressure_config.strategy, rs2_stream::BackpressureStrategy::Block);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_custom_session_configuration() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Test with custom configuration
    let custom_session = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 8;
            p.max_buffer_size = 4000;
            p.task_timeout = Duration::from_secs(120);
        })
        .stream_buffer(|b| {
            b.initial_capacity = 2048;
            b.max_capacity = Some(4 * 1024 * 1024); // 4MB
        })
        .backpressure(|b| {
            b.strategy = rs2_stream::BackpressureStrategy::DropNewest;
            b.buffer_size = 1500;
            b.low_watermark = Some(300);
            b.high_watermark = Some(1200);
        })
        .build();
    
    set_global_session(custom_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    assert_eq!(parallel_config.concurrency, 8);
    assert_eq!(parallel_config.max_buffer_size, 4000);
    assert_eq!(parallel_config.task_timeout, Duration::from_secs(120));

    let buffer_config = get_global_buffer_config();
    assert!(buffer_config.is_some());
    let buffer_config = buffer_config.unwrap();
    
    assert_eq!(buffer_config.initial_capacity, 2048);
    assert_eq!(buffer_config.max_capacity, Some(4 * 1024 * 1024));

    let backpressure_config = get_global_backpressure_config();
    assert!(backpressure_config.is_some());
    let backpressure_config = backpressure_config.unwrap();
    
    assert_eq!(backpressure_config.strategy, rs2_stream::BackpressureStrategy::DropNewest);
    assert_eq!(backpressure_config.buffer_size, 1500);
    assert_eq!(backpressure_config.low_watermark, Some(300));
    assert_eq!(backpressure_config.high_watermark, Some(1200));
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_high_performance_preset() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Test with high performance preset
    let high_perf_session = SessionBuilder::new()
        .preset(SessionPreset::HighPerformance)
        .build();
    
    set_global_session(high_perf_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // High performance preset should have high concurrency
    assert!(parallel_config.concurrency >= 8);

    let buffer_config = get_global_buffer_config();
    assert!(buffer_config.is_some());
    let buffer_config = buffer_config.unwrap();
    
    // High performance preset should have large buffers
    assert!(buffer_config.initial_capacity >= 1024);
}

#[tokio::test]
#[serial]
async fn test_low_memory_preset() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Test with low memory preset
    let low_mem_session = SessionBuilder::new()
        .preset(SessionPreset::LowMemory)
        .build();
    
    set_global_session(low_mem_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Low memory preset should have moderate concurrency
    assert!(parallel_config.concurrency <= 8);

    let buffer_config = get_global_buffer_config();
    assert!(buffer_config.is_some());
    let buffer_config = buffer_config.unwrap();
    
    // Low memory preset should have smaller buffers
    assert!(buffer_config.initial_capacity <= 1024);
}

#[tokio::test]
#[serial]
async fn test_parallel_processing_with_session() {
    // Clear any existing global session to ensure clean state
    clear_global_session();
    
    // Test parallel processing with session configuration
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 6;
            p.max_buffer_size = 2500;
        })
        .build();
    
    set_global_session(session_config);

    let numbers = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let stream = from_iter_rs2(numbers);
    
    let result_stream = stream.par_eval_map_with_session_rs2(|n| async move {
        // Simulate some async work
        tokio::time::sleep(Duration::from_millis(15)).await;
        n * n // Square the number
    });

    let results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 10);
    
    // Verify that all numbers were squared
    for (i, &result) in results.iter().enumerate() {
        let expected = (i as i32 + 1) * (i as i32 + 1);
        assert_eq!(result, expected);
    }
    
    // Clear the session at the end of the test
    clear_global_session();
} 