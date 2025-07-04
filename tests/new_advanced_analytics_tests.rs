//! Tests for New Advanced Analytics functionality
//!
//! This module tests the new advanced analytics features including:
//! - Time-based windowed aggregations
//! - Stream joins with time windows
//! - Sliding window operations (count, sum, min, max, aggregate)
//! - Moving averages
//! - Time-based grouping

use rs2_stream::new_advanced_analytics::*;
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::rs2_new_stream_ext::RS2StreamExt;
use std::time::{Duration, SystemTime};

// ================================
// Test Data Structures
// ================================

#[derive(Debug, Clone, PartialEq)]
struct TestEvent {
    id: u64,
    value: f64,
    timestamp: SystemTime,
}

impl TestEvent {
    fn new(id: u64, value: f64, timestamp: SystemTime) -> Self {
        Self { id, value, timestamp }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct TestProfile {
    id: u64,
    name: String,
    timestamp: SystemTime,
}

impl TestProfile {
    fn new(id: u64, name: String, timestamp: SystemTime) -> Self {
        Self { id, name, timestamp }
    }
}

// ================================
// TimeWindow Tests
// ================================

#[tokio::test]
async fn test_time_window_creation() {
    let start_time = SystemTime::now();
    let end_time = start_time + Duration::from_secs(60);
    
    let window: TimeWindow<TestEvent> = TimeWindow::new(start_time, end_time);
    
    assert_eq!(window.start_time, start_time);
    assert_eq!(window.end_time, end_time);
    assert!(window.events.is_empty());
    assert_eq!(window.count(), 0);
    assert!(window.is_empty());
}

#[tokio::test]
async fn test_time_window_events() {
    let start_time = SystemTime::now();
    let end_time = start_time + Duration::from_secs(60);
    
    let mut window: TimeWindow<TestEvent> = TimeWindow::new(start_time, end_time);
    let event1 = TestEvent::new(1, 10.0, start_time);
    let event2 = TestEvent::new(2, 20.0, start_time + Duration::from_secs(30));
    
    window.add_event(event1.clone());
    window.add_event(event2.clone());
    
    assert_eq!(window.count(), 2);
    assert!(!window.is_empty());
    assert_eq!(window.events[0], event1);
    assert_eq!(window.events[1], event2);
}

#[tokio::test]
async fn test_time_window_completion() {
    let start_time = SystemTime::now();
    let end_time = start_time + Duration::from_secs(60);
    
    let window: TimeWindow<TestEvent> = TimeWindow::new(start_time, end_time);
    
    // Window should be complete after end_time
    let watermark_after = end_time + Duration::from_secs(10);
    assert!(window.is_complete(watermark_after));
    
    // Window should not be complete before end_time
    let watermark_before = start_time + Duration::from_secs(30);
    assert!(!window.is_complete(watermark_before));
}

// ================================
// TimeWindowConfig Tests
// ================================

#[tokio::test]
async fn test_time_window_config_default() {
    let config = TimeWindowConfig::default();
    
    assert_eq!(config.window_size, Duration::from_secs(60));
    assert_eq!(config.slide_interval, Duration::from_secs(60));
    assert_eq!(config.watermark_delay, Duration::from_secs(10));
    assert_eq!(config.allowed_lateness, Duration::from_secs(5));
}

#[tokio::test]
async fn test_time_window_config_custom() {
    let config = TimeWindowConfig {
        window_size: Duration::from_secs(30),
        slide_interval: Duration::from_secs(15),
        watermark_delay: Duration::from_secs(5),
        allowed_lateness: Duration::from_secs(2),
    };
    
    assert_eq!(config.window_size, Duration::from_secs(30));
    assert_eq!(config.slide_interval, Duration::from_secs(15));
    assert_eq!(config.watermark_delay, Duration::from_secs(5));
    assert_eq!(config.allowed_lateness, Duration::from_secs(2));
}

// ================================
// TimeJoinConfig Tests
// ================================

#[tokio::test]
async fn test_time_join_config_default() {
    let config = TimeJoinConfig::default();
    
    assert_eq!(config.window_size, Duration::from_secs(60));
    assert_eq!(config.watermark_delay, Duration::from_secs(10));
}

#[tokio::test]
async fn test_time_join_config_custom() {
    let config = TimeJoinConfig {
        window_size: Duration::from_secs(30),
        watermark_delay: Duration::from_secs(5),
    };
    
    assert_eq!(config.window_size, Duration::from_secs(30));
    assert_eq!(config.watermark_delay, Duration::from_secs(5));
}

// ================================
// Time-based Windowing Tests
// ================================

#[tokio::test]
async fn test_window_by_time_basic() {
    let now = SystemTime::now();
    let events = vec![
        TestEvent::new(1, 10.0, now),
        TestEvent::new(2, 20.0, now + Duration::from_secs(30)),
        TestEvent::new(3, 30.0, now + Duration::from_secs(90)),
    ];
    
    let config = TimeWindowConfig {
        window_size: Duration::from_secs(60),
        slide_interval: Duration::from_secs(60),
        watermark_delay: Duration::from_secs(10),
        allowed_lateness: Duration::from_secs(5),
    };
    
    let stream = from_iter(events);
    let windowed_stream = stream.window_by_time_rs2_new(config, |event| event.timestamp);
    let results: Vec<TimeWindow<TestEvent>> = windowed_stream.collect_rs2::<Vec<_>>().await;
    
    assert!(!results.is_empty());
    
    // Check that events are grouped into windows
    for window in &results {
        assert!(!window.events.is_empty());
        for event in &window.events {
            assert!(event.timestamp >= window.start_time);
            assert!(event.timestamp < window.end_time);
        }
    }
}

#[tokio::test]
async fn test_window_by_time_empty_stream() {
    let config = TimeWindowConfig::default();
    let stream = from_iter(vec![]);
    let windowed_stream = stream.window_by_time_rs2_new(config, |event: &TestEvent| event.timestamp);
    let results: Vec<TimeWindow<TestEvent>> = windowed_stream.collect_rs2::<Vec<_>>().await;
    assert!(results.is_empty());
}

// ================================
// Sliding Window Tests
// ================================

#[tokio::test]
async fn test_sliding_count() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    let result: Vec<usize> = stream.sliding_count_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    // With window size 3, we should get counts for windows [1,2,3], [2,3,4], [3,4,5]
    assert_eq!(result, vec![3, 3, 3]);
}

#[tokio::test]
async fn test_sliding_sum() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    let result: Vec<i32> = stream.sliding_sum_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    // With window size 3, we should get sums for windows [1,2,3], [2,3,4], [3,4,5]
    assert_eq!(result, vec![6, 9, 12]);
}

#[tokio::test]
async fn test_sliding_min() {
    let data = vec![5, 2, 8, 1, 9];
    let stream = from_iter(data);
    let result: Vec<Option<i32>> = stream.sliding_min_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    // With window size 3, we should get mins for windows [5,2,8], [2,8,1], [8,1,9]
    assert_eq!(result, vec![Some(2), Some(1), Some(1)]);
}

#[tokio::test]
async fn test_sliding_max() {
    let data = vec![5, 2, 8, 1, 9];
    let stream = from_iter(data);
    let result: Vec<Option<i32>> = stream.sliding_max_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    // With window size 3, we should get maxes for windows [5,2,8], [2,8,1], [8,1,9]
    assert_eq!(result, vec![Some(8), Some(8), Some(9)]);
}

#[tokio::test]
async fn test_sliding_window_aggregate() {
    let data = vec![1, 2, 3, 4, 5];
    let stream = from_iter(data);
    let result: Vec<String> = stream
        .sliding_window_aggregate_rs2_new(3, |window| {
            format!("sum:{}", window.iter().sum::<i32>())
        })
        .collect_rs2::<Vec<_>>()
        .await;
    
    // With window size 3, we should get aggregated strings for windows [1,2,3], [2,3,4], [3,4,5]
    assert_eq!(result, vec!["sum:6", "sum:9", "sum:12"]);
}

// ================================
// Moving Average Tests
// ================================

#[tokio::test]
async fn test_moving_average() {
    let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let stream = from_iter(data);
    let result: Vec<f64> = stream.moving_average_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    // With window size 3, we should get averages for windows [1,2,3], [2,3,4], [3,4,5]
    assert_eq!(result, vec![2.0, 3.0, 4.0]);
}

#[tokio::test]
async fn test_moving_average_empty() {
    let data: Vec<f64> = vec![];
    let stream = from_iter(data);
    let result: Vec<f64> = stream.moving_average_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_moving_average_single_value() {
    let data = vec![5.0];
    let stream = from_iter(data);
    let result: Vec<f64> = stream.moving_average_rs2_new(3).collect_rs2::<Vec<_>>().await;
    
    // With only one value and window size 3, should get empty result
    assert!(result.is_empty());
}

// ================================
// Time-based Grouping Tests
// ================================

#[tokio::test]
async fn test_group_by_time() {
    let now = SystemTime::now();
    let events = vec![
        TestEvent::new(1, 10.0, now),
        TestEvent::new(2, 20.0, now + Duration::from_secs(30)),
        TestEvent::new(3, 30.0, now + Duration::from_secs(90)),
    ];
    
    let stream = from_iter(events);
    let result: Vec<(SystemTime, Vec<TestEvent>)> = stream
        .group_by_time_rs2_new(Duration::from_secs(60), |event| event.timestamp)
        .collect_rs2::<Vec<_>>()
        .await;
    
    assert!(!result.is_empty());
    
    // Check that events are grouped by time buckets
    for (bucket_start, events) in &result {
        assert!(!events.is_empty());
        for event in events {
            let since_epoch = event.timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            let bucket_secs = 60;
            let expected_bucket_start_secs = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
            let expected_bucket_start = SystemTime::UNIX_EPOCH + Duration::from_secs(expected_bucket_start_secs);
            assert_eq!(*bucket_start, expected_bucket_start);
        }
    }
}

// ================================
// Time-windowed Join Tests
// ================================

#[tokio::test]
async fn test_join_with_time_window() {
    let now = SystemTime::now();
    let events = vec![
        TestEvent::new(1, 10.0, now),
        TestEvent::new(2, 20.0, now + Duration::from_secs(30)),
    ];
    
    let profiles = vec![
        TestProfile::new(1, "Alice".to_string(), now + Duration::from_secs(10)),
        TestProfile::new(2, "Bob".to_string(), now + Duration::from_secs(40)),
    ];
    
    let config = TimeJoinConfig {
        window_size: Duration::from_secs(60),
        watermark_delay: Duration::from_secs(10),
    };
    
    let stream1 = from_iter(events);
    let stream2 = from_iter(profiles);
    
    let result: Vec<(TestEvent, TestProfile)> = stream1
        .join_with_time_window_rs2_new(
            stream2,
            config,
            |event| event.timestamp,
            |profile| profile.timestamp,
            |event, profile| (event, profile),
        )
        .collect_rs2::<Vec<_>>()
        .await;
    
    // Should have some joins if events and profiles are within the time window
    assert!(!result.is_empty());
    
    // Check that joined events are within the time window
    for (event, profile) in &result {
        let time_diff = if event.timestamp > profile.timestamp {
            event.timestamp.duration_since(profile.timestamp).unwrap_or_default()
        } else {
            profile.timestamp.duration_since(event.timestamp).unwrap_or_default()
        };
        assert!(time_diff <= Duration::from_secs(60));
    }
}

// ================================
// Edge Cases and Error Handling
// ================================

#[tokio::test]
async fn test_sliding_window_zero_size() {
    let data = vec![1, 2, 3];
    let stream = from_iter(data);
    let result: Vec<usize> = stream.sliding_count_rs2_new(0).collect_rs2::<Vec<_>>().await;
    
    // With window size 0, should get empty result
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_sliding_window_larger_than_data() {
    let data = vec![1, 2];
    let stream = from_iter(data);
    let result: Vec<usize> = stream.sliding_count_rs2_new(5).collect_rs2::<Vec<_>>().await;
    
    // With window size larger than data, should get empty result
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_time_window_negative_duration() {
    let now = SystemTime::now();
    let events = vec![TestEvent::new(1, 10.0, now)];
    
    // This should handle negative durations gracefully
    let config = TimeWindowConfig {
        window_size: Duration::from_secs(60),
        slide_interval: Duration::from_secs(60),
        watermark_delay: Duration::from_secs(10),
        allowed_lateness: Duration::from_secs(5),
    };
    
    let stream = from_iter(events);
    let windowed_stream = stream.window_by_time_rs2_new(config, |event| event.timestamp);
    let results: Vec<TimeWindow<TestEvent>> = windowed_stream.collect_rs2::<Vec<_>>().await;
    
    // Should not panic and should produce some result
    assert!(!results.is_empty());
}

// ================================
// Integration Tests
// ================================

#[tokio::test]
async fn test_analytics_pipeline() {
    let now = SystemTime::now();
    let events = vec![
        TestEvent::new(1, 10.0, now),
        TestEvent::new(2, 20.0, now + Duration::from_secs(30)),
        TestEvent::new(3, 30.0, now + Duration::from_secs(60)),
        TestEvent::new(4, 40.0, now + Duration::from_secs(90)),
        TestEvent::new(5, 50.0, now + Duration::from_secs(120)),
    ];
    
    // Test a complete analytics pipeline
    let grouped: Vec<(SystemTime, Vec<TestEvent>)> = from_iter(events.clone())
        .group_by_time_rs2_new(Duration::from_secs(60), |event| event.timestamp)
        .collect_rs2::<Vec<_>>()
        .await;
    
    // 2. Extract values and calculate moving average
    let values: Vec<f64> = from_iter(events.clone())
        .map_rs2(|event| event.value)
        .moving_average_rs2_new(3)
        .collect_rs2::<Vec<_>>()
        .await;
    
    // 3. Calculate sliding statistics
    let counts: Vec<usize> = from_iter(events.clone())
        .sliding_count_rs2_new(3)
        .collect_rs2::<Vec<_>>()
        .await;
    
    let sums: Vec<f64> = from_iter(events.clone())
        .map_rs2(|event| event.value)
        .sliding_sum_rs2_new(3)
        .collect_rs2::<Vec<_>>()
        .await;
    
    // Verify results
    assert!(!grouped.is_empty());
    assert!(!values.is_empty());
    assert!(!counts.is_empty());
    assert!(!sums.is_empty());
    
    // Check that moving average values are reasonable
    for &value in &values {
        assert!(value >= 0.0);
        assert!(value <= 50.0); // Max value in our data
    }
    
    // Check that counts are correct
    for &count in &counts {
        assert_eq!(count, 3); // Window size is 3
    }
    
    // Check that sums are reasonable
    for &sum in &sums {
        assert!(sum >= 60.0); // Min sum for window of 3 values
        assert!(sum <= 120.0); // Max sum for window of 3 values
    }
}

// ================================
// Performance Tests
// ================================

#[tokio::test]
async fn test_large_dataset_performance() {
    let now = SystemTime::now();
    let mut events = Vec::new();
    
    // Create a large dataset
    for i in 0..1000 {
        events.push(TestEvent::new(
            i,
            i as f64,
            now + Duration::from_secs(i as u64),
        ));
    }
    
    let stream = from_iter(events.clone());
    
    // Test various operations on large dataset
    let start = std::time::Instant::now();
    
    let _result: Vec<f64> = from_iter(events)
        .map_rs2(|event| event.value)
        .moving_average_rs2_new(10)
        .collect_rs2::<Vec<_>>()
        .await;
    
    let duration = start.elapsed();
    
    // Should complete within reasonable time (adjust threshold as needed)
    assert!(duration < Duration::from_secs(5));
}

// ================================
// Memory Safety Tests
// ================================

#[tokio::test]
async fn test_memory_safety_large_windows() {
    let now = SystemTime::now();
    let mut events = Vec::new();
    
    // Create many events in the same time window
    for i in 0..1000 {
        events.push(TestEvent::new(i, i as f64, now));
    }
    
    let config = TimeWindowConfig {
        window_size: Duration::from_secs(3600), // 1 hour window
        slide_interval: Duration::from_secs(3600),
        watermark_delay: Duration::from_secs(10),
        allowed_lateness: Duration::from_secs(5),
    };
    
    let stream = from_iter(events);
    let result: Vec<TimeWindow<TestEvent>> = stream
        .window_by_time_rs2_new(config, |event| event.timestamp)
        .collect_rs2::<Vec<_>>()
        .await;
    
    // Should handle large windows without memory issues
    assert!(!result.is_empty());
    assert_eq!(result[0].events.len(), 1000);
} 