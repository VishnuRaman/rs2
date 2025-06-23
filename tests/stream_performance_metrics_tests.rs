use rs2_stream::stream_performance_metrics::*;
use std::time::Duration;

#[test]
fn test_stream_metrics_new() {
    // Test that new() creates a StreamMetrics instance with default values
    let metrics = StreamMetrics::new();

    // Check that start_time is set
    assert!(metrics.start_time.is_some());
    assert!(metrics.name.is_some());
    // Check that other fields have default values
    assert_eq!(metrics.items_processed, 0);
    assert_eq!(metrics.bytes_processed, 0);
    assert_eq!(metrics.errors, 0);
    assert_eq!(metrics.retries, 0);
    assert_eq!(metrics.items_per_second, 0.0);
    assert_eq!(metrics.bytes_per_second, 0.0);
    assert_eq!(metrics.average_item_size, 0.0);
    assert_eq!(metrics.peak_processing_time, Duration::from_secs(0));
    assert_eq!(metrics.consecutive_errors, 0);
    assert_eq!(metrics.error_rate, 0.0);
    assert_eq!(metrics.backpressure_events, 0);
    assert_eq!(metrics.queue_depth, 0);
}

#[test]
fn test_record_item() {
    let mut metrics = StreamMetrics::new();

    // Record an item
    metrics.record_item(100);

    // Check that metrics are updated
    assert_eq!(metrics.items_processed, 1);
    assert_eq!(metrics.bytes_processed, 100);
    assert!(metrics.last_activity.is_some());
    assert_eq!(metrics.consecutive_errors, 0);

    // Record another item
    metrics.record_item(200);

    // Check that metrics are updated
    assert_eq!(metrics.items_processed, 2);
    assert_eq!(metrics.bytes_processed, 300);
    assert!(metrics.last_activity.is_some());

    // Check derived metrics
    assert_eq!(metrics.average_item_size, 150.0);
}

#[test]
fn test_record_error() {
    let mut metrics = StreamMetrics::new();

    // Record an error
    metrics.record_error();

    // Check that metrics are updated
    assert_eq!(metrics.errors, 1);
    assert_eq!(metrics.consecutive_errors, 1);
    assert!(metrics.last_activity.is_some());

    // Record another error
    metrics.record_error();

    // Check that metrics are updated
    assert_eq!(metrics.errors, 2);
    assert_eq!(metrics.consecutive_errors, 2);

    // Record an item (should reset consecutive_errors)
    metrics.record_item(100);

    // Check that consecutive_errors is reset
    assert_eq!(metrics.consecutive_errors, 0);

    // Check error rate
    assert_eq!(metrics.error_rate, 2.0 / 3.0);
}

#[test]
fn test_record_retry() {
    let mut metrics = StreamMetrics::new();

    // Record retries
    metrics.record_retry();
    metrics.record_retry();

    // Check that metrics are updated
    assert_eq!(metrics.retries, 2);
}

#[test]
fn test_record_processing_time() {
    let mut metrics = StreamMetrics::new();

    // Record processing time
    metrics.record_processing_time(Duration::from_millis(100));

    // Check that metrics are updated
    assert_eq!(metrics.processing_time, Duration::from_millis(100));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(100));

    // Record a shorter processing time
    metrics.record_processing_time(Duration::from_millis(50));

    // Check that metrics are updated
    assert_eq!(metrics.processing_time, Duration::from_millis(150));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(100));

    // Record a longer processing time
    metrics.record_processing_time(Duration::from_millis(200));

    // Check that metrics are updated
    assert_eq!(metrics.processing_time, Duration::from_millis(350));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(200));
}

#[test]
fn test_record_backpressure() {
    let mut metrics = StreamMetrics::new();

    // Record backpressure events
    metrics.record_backpressure();
    metrics.record_backpressure();

    // Check that metrics are updated
    assert_eq!(metrics.backpressure_events, 2);
}

#[test]
fn test_update_queue_depth() {
    let mut metrics = StreamMetrics::new();

    // Update queue depth
    metrics.update_queue_depth(10);

    // Check that metrics are updated
    assert_eq!(metrics.queue_depth, 10);

    // Update queue depth again
    metrics.update_queue_depth(5);

    // Check that metrics are updated
    assert_eq!(metrics.queue_depth, 5);
}

#[test]
fn test_finalize() {
    let mut metrics = StreamMetrics::new();

    // Record some metrics
    metrics.record_item(100);
    metrics.record_item(200);
    metrics.record_error();

    // Sleep to ensure some time passes
    std::thread::sleep(Duration::from_millis(10));

    // Finalize metrics
    metrics.finalize();

    // Check that start_time is taken
    assert!(metrics.start_time.is_none());

    // Check that processing_time is set
    assert!(metrics.processing_time > Duration::from_millis(0));

    // Check derived metrics
    assert_eq!(metrics.items_processed, 2);
    assert_eq!(metrics.bytes_processed, 300);
    assert_eq!(metrics.errors, 1);
    assert_eq!(metrics.average_item_size, 150.0);
    assert!(metrics.items_per_second > 0.0);
    assert!(metrics.bytes_per_second > 0.0);
    assert_eq!(metrics.error_rate, 1.0 / 3.0);
}

#[test]
fn test_throughput_items_per_sec() {
    let mut metrics = StreamMetrics::new();

    // Set up metrics
    metrics.items_processed = 100;
    metrics.processing_time = Duration::from_secs(10);

    // Check throughput calculation
    assert_eq!(metrics.throughput_items_per_sec(), 10.0);

    // Test with zero processing time
    metrics.processing_time = Duration::from_secs(0);
    assert_eq!(metrics.throughput_items_per_sec(), 0.0);
}

#[test]
fn test_throughput_bytes_per_sec() {
    let mut metrics = StreamMetrics::new();

    // Set up metrics
    metrics.bytes_processed = 1000;
    metrics.processing_time = Duration::from_secs(10);

    // Check throughput calculation
    assert_eq!(metrics.throughput_bytes_per_sec(), 100.0);

    // Test with zero processing time
    metrics.processing_time = Duration::from_secs(0);
    assert_eq!(metrics.throughput_bytes_per_sec(), 0.0);
}

#[test]
fn test_is_healthy() {
    let mut metrics = StreamMetrics::new();

    // Initially should be healthy
    assert!(metrics.is_healthy());

    // Set error rate to 5% (below threshold)
    metrics.error_rate = 0.05;
    assert!(metrics.is_healthy());

    // Set error rate to 15% (above threshold)
    metrics.error_rate = 0.15;
    assert!(!metrics.is_healthy());

    // Reset error rate but set consecutive errors to 6 (above threshold)
    metrics.error_rate = 0.05;
    metrics.consecutive_errors = 6;
    assert!(!metrics.is_healthy());

    // Set both below threshold
    metrics.error_rate = 0.05;
    metrics.consecutive_errors = 3;
    assert!(metrics.is_healthy());
}

#[test]
fn test_throughput_summary() {
    let mut metrics = StreamMetrics::new();

    // Set up metrics
    metrics.items_per_second = 100.0;
    metrics.bytes_per_second = 10000.0;

    // Check summary format
    let summary = metrics.throughput_summary();
    assert_eq!(summary, "100.0 items/sec, 10.0 KB/sec");
}

#[test]
fn test_throughput_summary_processing_time() {
    let mut metrics = StreamMetrics::new();

    // Set up metrics
    metrics.items_processed = 1000;
    metrics.bytes_processed = 100000;
    metrics.processing_time = Duration::from_secs(10);

    // Check summary format
    let summary = metrics.throughput_summary_processing_time();
    assert_eq!(summary, "100.0 items/sec, 10.0 KB/sec");
}

#[test]
fn test_update_derived_metrics() {
    let mut metrics = StreamMetrics::new();

    // Set up metrics
    metrics.items_processed = 100;
    metrics.bytes_processed = 10000;
    metrics.errors = 10;

    // Update derived metrics
    metrics.update_derived_metrics();

    // Check derived metrics
    assert_eq!(metrics.average_item_size, 100.0);
    assert_eq!(metrics.error_rate, 10.0 / 110.0);
}

#[test]
fn test_with_name() {
    // Test that with_name() sets the name correctly
    let metrics = StreamMetrics::new().with_name("test_stream".to_string());

    // Check that name is set correctly
    assert_eq!(metrics.name.unwrap(), "test_stream");
}

#[test]
fn test_default_name_format() {
    let metrics = StreamMetrics::new();
    let name = metrics.name.unwrap();

    // Should start with "rs2-stream-" followed by timestamp
    assert!(name.starts_with("rs2-stream-"));

    // Should be longer than just the prefix (includes timestamp)
    assert!(name.len() > "rs2-stream-".len());

    // Should end with digits (timestamp)
    assert!(name.chars().last().unwrap().is_ascii_digit());
}

#[test]
fn test_multiple_instances_have_unique_names() {
    let metrics1 = StreamMetrics::new();
    std::thread::sleep(std::time::Duration::from_millis(1)); // Ensure different timestamps
    let metrics2 = StreamMetrics::new();

    let name1 = metrics1.name.unwrap();
    let name2 = metrics2.name.unwrap();

    // Both should start with correct prefix
    assert!(name1.starts_with("rs2-stream-"));
    assert!(name2.starts_with("rs2-stream-"));

    // But should be different (different timestamps)
    assert_ne!(name1, name2);
}

#[test]
fn test_set_name_changes_from_default() {
    let mut metrics = StreamMetrics::new();

    // Get the default name
    let default_name = metrics.name.clone();
    assert!(default_name.as_ref().unwrap().starts_with("rs2-stream-"));

    // Change to custom name
    metrics.set_name("custom-stream".to_string());

    // Should be completely different now
    assert_eq!(metrics.name.as_ref().unwrap(), "custom-stream");
    assert_ne!(metrics.name, default_name);
}

#[test]
fn test_set_name_changes_name() {
    let mut metrics = StreamMetrics::new();

    // Initial name should be the default timestamp-based name
    let initial_name = metrics.name.clone();
    assert!(initial_name.is_some());
    assert!(initial_name.as_ref().unwrap().starts_with("rs2-stream-"));

    // Change the name
    metrics.set_name("changed_name".to_string());

    // Verify the name changed
    assert_eq!(metrics.name.unwrap(), "changed_name");
}

#[test]
fn test_multiple_name_changes() {
    let mut metrics = StreamMetrics::new();

    // Change name multiple times
    metrics.set_name("first_name".to_string());
    assert_eq!(metrics.name.as_ref().unwrap(), "first_name");

    metrics.set_name("second_name".to_string());
    assert_eq!(metrics.name.as_ref().unwrap(), "second_name");

    metrics.set_name("final_name".to_string());
    assert_eq!(metrics.name.unwrap(), "final_name");
}

#[test]
fn test_with_health_thresholds() {
    // Test that with_health_thresholds() sets the thresholds correctly
    let thresholds = HealthThresholds::strict();
    let metrics = StreamMetrics::new().with_health_thresholds(thresholds.clone());

    // Check that thresholds are set correctly
    assert_eq!(
        metrics.health_thresholds.max_error_rate,
        thresholds.max_error_rate
    );
    assert_eq!(
        metrics.health_thresholds.max_consecutive_errors,
        thresholds.max_consecutive_errors
    );

    // Test with custom thresholds
    let custom_thresholds = HealthThresholds::custom(0.05, 3);
    let metrics = StreamMetrics::new().with_health_thresholds(custom_thresholds);

    // Check that thresholds are set correctly
    assert_eq!(metrics.health_thresholds.max_error_rate, 0.05);
    assert_eq!(metrics.health_thresholds.max_consecutive_errors, 3);
}

#[test]
fn test_set_health_thresholds() {
    // Test that set_health_thresholds() sets the thresholds correctly
    let mut metrics = StreamMetrics::new();
    let thresholds = HealthThresholds::relaxed();
    metrics.set_health_thresholds(thresholds.clone());

    // Check that thresholds are set correctly
    assert_eq!(
        metrics.health_thresholds.max_error_rate,
        thresholds.max_error_rate
    );
    assert_eq!(
        metrics.health_thresholds.max_consecutive_errors,
        thresholds.max_consecutive_errors
    );

    // Test changing the thresholds
    let new_thresholds = HealthThresholds::strict();
    metrics.set_health_thresholds(new_thresholds.clone());

    // Check that thresholds are updated correctly
    assert_eq!(
        metrics.health_thresholds.max_error_rate,
        new_thresholds.max_error_rate
    );
    assert_eq!(
        metrics.health_thresholds.max_consecutive_errors,
        new_thresholds.max_consecutive_errors
    );
}

#[test]
fn test_health_thresholds_default() {
    let thresholds = HealthThresholds::default();
    assert_eq!(thresholds.max_error_rate, 0.1); // 10%
    assert_eq!(thresholds.max_consecutive_errors, 5);
}

#[test]
fn test_health_thresholds_strict() {
    let thresholds = HealthThresholds::strict();
    assert_eq!(thresholds.max_error_rate, 0.01); // 1%
    assert_eq!(thresholds.max_consecutive_errors, 2);
}

#[test]
fn test_health_thresholds_relaxed() {
    let thresholds = HealthThresholds::relaxed();
    assert_eq!(thresholds.max_error_rate, 0.20); // 20%
    assert_eq!(thresholds.max_consecutive_errors, 20);
}

#[test]
fn test_health_thresholds_custom() {
    let thresholds = HealthThresholds::custom(0.05, 3);
    assert_eq!(thresholds.max_error_rate, 0.05);
    assert_eq!(thresholds.max_consecutive_errors, 3);
}

#[test]
fn test_edge_case_zero_items() {
    let mut metrics = StreamMetrics::new();

    // Test with no items processed
    metrics.update_derived_metrics();
    assert_eq!(metrics.average_item_size, 0.0);
    assert_eq!(metrics.error_rate, 0.0);

    // Test throughput with zero items
    assert_eq!(metrics.throughput_items_per_sec(), 0.0);
    assert_eq!(metrics.throughput_bytes_per_sec(), 0.0);
}

#[test]
fn test_edge_case_only_errors() {
    let mut metrics = StreamMetrics::new();

    // Record only errors, no successful items
    metrics.record_error();
    metrics.record_error();

    // Error rate should be 100% (2 errors, 0 successful items)
    assert_eq!(metrics.error_rate, 1.0);
    assert_eq!(metrics.consecutive_errors, 2);
    assert!(!metrics.is_healthy()); // Should be unhealthy due to high error rate
}

#[test]
fn test_throughput_calculation_precision() {
    let mut metrics = StreamMetrics::new();

    // Set up precise values
    metrics.items_processed = 1000;
    metrics.bytes_processed = 50000;
    metrics.processing_time = Duration::from_secs(2);

    // Test precise calculations
    assert_eq!(metrics.throughput_items_per_sec(), 500.0);
    assert_eq!(metrics.throughput_bytes_per_sec(), 25000.0);
}

#[test]
fn test_summary_formatting_edge_cases() {
    let mut metrics = StreamMetrics::new();

    // Test with zero values
    metrics.items_per_second = 0.0;
    metrics.bytes_per_second = 0.0;
    assert_eq!(metrics.throughput_summary(), "0.0 items/sec, 0.0 KB/sec");

    // Test with very small values
    metrics.items_per_second = 0.1;
    metrics.bytes_per_second = 50.0;
    assert_eq!(metrics.throughput_summary(), "0.1 items/sec, 0.1 KB/sec");

    // Test with large values
    metrics.items_per_second = 10000.0;
    metrics.bytes_per_second = 1000000.0;
    assert_eq!(
        metrics.throughput_summary(),
        "10000.0 items/sec, 1000.0 KB/sec"
    );
}

#[test]
fn test_consecutive_errors_reset_behavior() {
    let mut metrics = StreamMetrics::new();

    // Record multiple errors
    metrics.record_error();
    metrics.record_error();
    metrics.record_error();
    assert_eq!(metrics.consecutive_errors, 3);

    // Record a successful item - should reset consecutive errors
    metrics.record_item(100);
    assert_eq!(metrics.consecutive_errors, 0);

    // Record another error - should start counting again
    metrics.record_error();
    assert_eq!(metrics.consecutive_errors, 1);
}

#[test]
fn test_peak_processing_time_behavior() {
    let mut metrics = StreamMetrics::new();

    // Record increasing processing times
    metrics.record_processing_time(Duration::from_millis(100));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(100));

    metrics.record_processing_time(Duration::from_millis(200));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(200));

    // Record a shorter time - peak should remain the same
    metrics.record_processing_time(Duration::from_millis(150));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(200));

    // Record an even longer time - peak should update
    metrics.record_processing_time(Duration::from_millis(300));
    assert_eq!(metrics.peak_processing_time, Duration::from_millis(300));
}

#[test]
fn test_metrics_integration_scenario() {
    let mut metrics = StreamMetrics::new()
        .with_name("integration_test".to_string())
        .with_health_thresholds(HealthThresholds::strict());

    // Simulate a realistic processing scenario
    for i in 0..100 {
        if i % 10 == 0 {
            // Every 10th item fails
            metrics.record_error();
        } else {
            // Successful items
            metrics.record_item(100 + (i % 50));
        }

        // Record some processing time
        metrics.record_processing_time(Duration::from_millis(10 + (i % 20)));

        // Occasionally record backpressure
        if i % 25 == 0 {
            metrics.record_backpressure();
        }
    }

    // Verify final state
    assert_eq!(metrics.items_processed, 90); // 90 successful items
    assert_eq!(metrics.errors, 10); // 10 errors
    assert_eq!(metrics.backpressure_events, 4); // 4 backpressure events
    assert_eq!(metrics.error_rate, 10.0 / 100.0); // 10% error rate

    // Should be unhealthy due to 10% error rate > 1% threshold
    assert!(!metrics.is_healthy());

    // Verify name was set correctly
    assert_eq!(metrics.name.unwrap(), "integration_test");
}
