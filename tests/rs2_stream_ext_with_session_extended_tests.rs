use rs2_stream::rs2;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::session::{SessionBuilder, SessionPreset, clear_global_session, set_global_session};
use rs2_stream::stream_performance_metrics::HealthThresholds;
use serial_test::serial;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
struct TestItem {
    id: u32,
    value: String,
}

impl TestItem {
    fn new(id: u32, value: &str) -> Self {
        Self {
            id,
            value: value.to_string(),
        }
    }
}

#[tokio::test]
#[serial]
async fn test_collect_with_session() {
    clear_global_session();
    
    // Set up session with buffer configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![
        TestItem::new(1, "a"),
        TestItem::new(2, "b"),
        TestItem::new(3, "c"),
    ]);

    let results: Vec<TestItem> = stream.collect_with_session_rs2().await;
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], TestItem::new(1, "a"));
    assert_eq!(results[1], TestItem::new(2, "b"));
    assert_eq!(results[2], TestItem::new(3, "c"));
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_auto_backpressure_with_session() {
    clear_global_session();
    
    // Set up session with backpressure configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![
        TestItem::new(1, "a"),
        TestItem::new(2, "b"),
        TestItem::new(3, "c"),
    ]);

    let result_stream = stream.auto_backpressure_with_session_rs2();
    let results: Vec<TestItem> = result_stream.collect_rs2().await;
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], TestItem::new(1, "a"));
    assert_eq!(results[1], TestItem::new(2, "b"));
    assert_eq!(results[2], TestItem::new(3, "c"));
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_auto_backpressure_drop_oldest_with_session() {
    clear_global_session();
    
    // Set up session with backpressure configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![
        TestItem::new(1, "a"),
        TestItem::new(2, "b"),
        TestItem::new(3, "c"),
    ]);

    let result_stream = stream.auto_backpressure_drop_oldest_with_session_rs2();
    let results: Vec<TestItem> = result_stream.collect_rs2().await;
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], TestItem::new(1, "a"));
    assert_eq!(results[1], TestItem::new(2, "b"));
    assert_eq!(results[2], TestItem::new(3, "c"));
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_auto_backpressure_drop_newest_with_session() {
    clear_global_session();
    
    // Set up session with backpressure configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![
        TestItem::new(1, "a"),
        TestItem::new(2, "b"),
        TestItem::new(3, "c"),
    ]);

    let result_stream = stream.auto_backpressure_drop_newest_with_session_rs2();
    let results: Vec<TestItem> = result_stream.collect_rs2().await;
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], TestItem::new(1, "a"));
    assert_eq!(results[1], TestItem::new(2, "b"));
    assert_eq!(results[2], TestItem::new(3, "c"));
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_auto_backpressure_error_with_session() {
    clear_global_session();
    
    // Set up session with backpressure configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![
        TestItem::new(1, "a"),
        TestItem::new(2, "b"),
        TestItem::new(3, "c"),
    ]);

    let result_stream = stream.auto_backpressure_error_with_session_rs2();
    let results: Vec<TestItem> = result_stream.collect_rs2().await;
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], TestItem::new(1, "a"));
    assert_eq!(results[1], TestItem::new(2, "b"));
    assert_eq!(results[2], TestItem::new(3, "c"));
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_with_metrics_with_session() {
    clear_global_session();
    
    // Set up session with metrics configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![
        TestItem::new(1, "a"),
        TestItem::new(2, "b"),
        TestItem::new(3, "c"),
    ]);

    let health_thresholds = HealthThresholds::default();
    let (result_stream, metrics) = stream.with_metrics_with_session_rs2("test-stream".to_string(), health_thresholds);
    
    let results: Vec<TestItem> = result_stream.collect_rs2().await;
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], TestItem::new(1, "a"));
    assert_eq!(results[1], TestItem::new(2, "b"));
    assert_eq!(results[2], TestItem::new(3, "c"));
    
    // Give async tasks time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    // Check that metrics were collected
    let metrics_guard = metrics.lock().await;
    assert_eq!(metrics_guard.items_processed, 3);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_par_eval_map_with_session_config() {
    clear_global_session();
    
    // Set up session with parallel configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4]);

    let result_stream = stream.par_eval_map_with_session_config_rs2(|x| async move {
        tokio::task::yield_now().await;
        x * 2
    });
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 4);

    // Sort results since parallel operations don't guarantee order
    results.sort();
    
    // Verify that all numbers were processed (in sorted order)
    let expected = vec![2, 4, 6, 8];
    assert_eq!(results, expected);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_par_eval_map_unordered_with_session_config() {
    clear_global_session();
    
    // Set up session with parallel configuration
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4]);

    let result_stream = stream.par_eval_map_unordered_with_session_config_rs2(|x| async move {
        tokio::task::yield_now().await;
        x * 2
    });
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 4);

    // Sort results since parallel operations don't guarantee order
    results.sort();
    
    // Verify that all numbers were processed (in sorted order)
    let expected = vec![2, 4, 6, 8];
    assert_eq!(results, expected);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_session_presets() {
    clear_global_session();
    
    // Test Development preset
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4]);
    let result_stream = stream.par_eval_map_with_session_rs2(|x| async move { x * 2 });
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    results.sort();
    assert_eq!(results, vec![2, 4, 6, 8]);
    
    clear_global_session();
    
    // Test Production preset
    let session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4]);
    let result_stream = stream.par_eval_map_with_session_rs2(|x| async move { x * 2 });
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    results.sort();
    assert_eq!(results, vec![2, 4, 6, 8]);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_custom_session_configuration() {
    clear_global_session();
    
    // Test custom session configuration
    let session = SessionBuilder::new()
        .parallel(|config| {
            config.concurrency = 2;
            config.max_buffer_size = 512;
            config.timeout = Duration::from_secs(30);
            config.sequence_timeout = Duration::from_secs(10);
            config.task_timeout = Duration::from_secs(5);
        })
        .stream_buffer(|config| {
            config.initial_capacity = 100;
            config.max_capacity = Some(1000);
            config.growth_strategy = rs2_stream::stream_configuration::GrowthStrategy::Exponential(2.0);
        })
        .backpressure(|config| {
            config.strategy = rs2_stream::rs2::BackpressureStrategy::Block;
            config.buffer_size = 1000;
        })
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4]);
    
    // Test parallel processing with custom config
    let result_stream = stream.par_eval_map_with_session_config_rs2(|x| async move { x * 2 });
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    results.sort();
    assert_eq!(results, vec![2, 4, 6, 8]);
    
    // Test collection with custom buffer config
    let stream2 = rs2::from_iter_rs2(vec![1, 2, 3, 4]);
    let results2: Vec<i32> = stream2.collect_with_session_rs2().await;
    assert_eq!(results2, vec![1, 2, 3, 4]);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_high_performance_preset() {
    clear_global_session();
    
    // Test high performance preset
    let session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    
    // Test parallel processing with high performance config
    let result_stream = stream.par_eval_map_with_session_config_rs2(|x| async move {
        tokio::task::yield_now().await;
        x * 3
    });
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    results.sort();
    assert_eq!(results, vec![3, 6, 9, 12, 15, 18, 21, 24]);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_low_memory_preset() {
    clear_global_session();
    
    // Test low memory preset
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4]);
    
    // Test parallel processing with low memory config
    let result_stream = stream.par_eval_map_with_session_config_rs2(|x| async move {
        tokio::task::yield_now().await;
        x * 2
    });
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    results.sort();
    assert_eq!(results, vec![2, 4, 6, 8]);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_parallel_processing_with_session() {
    clear_global_session();
    
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    
    // Test various parallel operations with session config
    let result_stream = stream
        .par_eval_map_with_session_config_rs2(|x| async move { x * 2 })
        .par_eval_map_unordered_with_session_config_rs2(|x| async move { x + 1 })
        .map_parallel_with_session_rs2(|x| x * 3);
    
    let mut results: Vec<i32> = result_stream.collect_rs2().await;
    results.sort();
    
    // Expected: ((x * 2) + 1) * 3 = (2x + 1) * 3 = 6x + 3
    // For inputs [1,2,3,4,5,6,7,8]: [9, 15, 21, 27, 33, 39, 45, 51]
    let expected = vec![9, 15, 21, 27, 33, 39, 45, 51];
    assert_eq!(results, expected);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_backpressure_strategies_with_session() {
    clear_global_session();
    
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    // Test different backpressure strategies with session config
    let block_stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5]).auto_backpressure_with_session_rs2();
    let drop_oldest_stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5]).auto_backpressure_drop_oldest_with_session_rs2();
    let drop_newest_stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5]).auto_backpressure_drop_newest_with_session_rs2();
    let error_stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5]).auto_backpressure_error_with_session_rs2();
    
    let block_results: Vec<i32> = block_stream.collect_rs2().await;
    let drop_oldest_results: Vec<i32> = drop_oldest_stream.collect_rs2().await;
    let drop_newest_results: Vec<i32> = drop_newest_stream.collect_rs2().await;
    let error_results: Vec<i32> = error_stream.collect_rs2().await;
    
    assert_eq!(block_results, vec![1, 2, 3, 4, 5]);
    assert_eq!(drop_oldest_results, vec![1, 2, 3, 4, 5]);
    assert_eq!(drop_newest_results, vec![1, 2, 3, 4, 5]);
    assert_eq!(error_results, vec![1, 2, 3, 4, 5]);
    
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_metrics_integration_with_session() {
    clear_global_session();
    
    let session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(session);

    let stream = rs2::from_iter_rs2(vec![1, 2, 3, 4, 5]);
    
    // Test metrics with session config
    let health_thresholds = HealthThresholds {
        max_consecutive_errors: 5,
        max_error_rate: 0.1,
    };
    
    let (result_stream, metrics) = stream.with_metrics_with_session_rs2("test-metrics".to_string(), health_thresholds);
    
    let results: Vec<i32> = result_stream.collect_rs2().await;
    assert_eq!(results, vec![1, 2, 3, 4, 5]);
    
    // Give async tasks time to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    
    // Check metrics
    let metrics_guard = metrics.lock().await;
    assert_eq!(metrics_guard.items_processed, 5);
    assert_eq!(metrics_guard.errors, 0);
    
    clear_global_session();
} 