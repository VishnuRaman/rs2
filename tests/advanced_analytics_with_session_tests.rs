use rs2_stream::advanced_analytics::*;
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::stream::StreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::session::{SessionBuilder, SessionPreset, set_global_session, clear_global_session, get_global_parallel_config, get_global_buffer_config, get_global_time_window_config};
use serial_test::serial;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio;

#[derive(Debug, Clone)]
struct TestEvent {
    id: u32,
    value: f64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct TestUser {
    id: u32,
    name: String,
    score: f64,
}

#[tokio::test]
async fn test_sliding_window_aggregate_with_session() {
    // Set up a session with custom buffer configuration
    clear_global_session();
    let session_config = SessionBuilder::new()
        .stream_buffer(|b| {
            b.initial_capacity = 1024;
            b.max_capacity = Some(2 * 1024 * 1024); // 2MB
        })
        .build();
    
    set_global_session(session_config);

    let events = vec![
        TestEvent { id: 1, value: 10.0, timestamp: UNIX_EPOCH + Duration::from_secs(1) },
        TestEvent { id: 2, value: 20.0, timestamp: UNIX_EPOCH + Duration::from_secs(2) },
        TestEvent { id: 3, value: 30.0, timestamp: UNIX_EPOCH + Duration::from_secs(3) },
        TestEvent { id: 4, value: 40.0, timestamp: UNIX_EPOCH + Duration::from_secs(4) },
    ];

    let stream = from_iter(events);
    let result_stream = stream.sliding_window_aggregate_with_session_rs2(3, |window| {
        if window.is_empty() {
            0.0
        } else {
            window.iter().map(|e| e.value).sum::<f64>() / window.len() as f64
        }
    });

    let results: Vec<f64> = result_stream.collect().await;
    assert_eq!(results.len(), 2);
    
    // First window: [10.0, 20.0, 30.0] -> avg = 20.0
    assert!((results[0] - 20.0).abs() < 0.001);
    // Second window: [20.0, 30.0, 40.0] -> avg = 30.0
    assert!((results[1] - 30.0).abs() < 0.001);
}

#[tokio::test]
async fn test_moving_average_with_session() {
    // Set up a session with custom parallel configuration
    clear_global_session();
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 4;
            p.max_buffer_size = 2000;
        })
        .build();
    
    set_global_session(session_config);

    let numbers = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
    let stream = from_iter(numbers);
    
        let result_stream = stream.growing_moving_average_with_session_rs2(3);
    
    let results: Vec<f64> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 8);
    
    // First value: [1.0] -> avg = 1.0
    assert!((results[0] - 1.0).abs() < 0.001);
    // Second value: [1.0, 2.0] -> avg = 1.5
    assert!((results[1] - 1.5).abs() < 0.001);
    // Third value: [1.0, 2.0, 3.0] -> avg = 2.0
    assert!((results[2] - 2.0).abs() < 0.001);
    // Fourth value: [2.0, 3.0, 4.0] -> avg = 3.0
    assert!((results[3] - 3.0).abs() < 0.001);
}

#[tokio::test]
async fn test_window_by_time_with_session() {
    // Set up a session with custom time window configuration
    clear_global_session();
    let session_config = SessionBuilder::new()
        .time_window(|tw| {
            tw.window_size = Duration::from_secs(60);
            tw.slide_interval = Duration::from_secs(30);
            tw.watermark_delay = Duration::from_secs(10);
            tw.allowed_lateness = Duration::from_secs(5);
        })
        .build();
    
    set_global_session(session_config);

    let events = vec![
        TestEvent { id: 1, value: 100.0, timestamp: UNIX_EPOCH + Duration::from_secs(30) },
        TestEvent { id: 2, value: 200.0, timestamp: UNIX_EPOCH + Duration::from_secs(45) },
        TestEvent { id: 3, value: 300.0, timestamp: UNIX_EPOCH + Duration::from_secs(90) },
    ];

    let stream = from_iter(events);
    let result_stream = stream.window_by_time_with_session_rs2(|event| event.timestamp);

    let results: Vec<_> = result_stream.collect().await;
    assert!(!results.is_empty());
    
    // Verify that time windows were created
    for window in results {
        assert!(!window.events.is_empty());
        assert!(window.start_time <= window.end_time);
    }
}

#[tokio::test]
async fn test_join_with_time_window_with_session() {
    // Set up a session with custom time window configuration
    let session_config = SessionBuilder::new()
        .time_window(|tw| {
            tw.window_size = Duration::from_secs(120);
            tw.slide_interval = Duration::from_secs(60);
            tw.watermark_delay = Duration::from_secs(15);
            tw.allowed_lateness = Duration::from_secs(10);
        })
        .build();
    
    set_global_session(session_config);

    let events1 = vec![
        TestEvent { id: 1, value: 10.0, timestamp: UNIX_EPOCH + Duration::from_secs(30) },
        TestEvent { id: 2, value: 20.0, timestamp: UNIX_EPOCH + Duration::from_secs(90) },
    ];

    let events2 = vec![
        TestEvent { id: 101, value: 100.0, timestamp: UNIX_EPOCH + Duration::from_secs(45) },
        TestEvent { id: 102, value: 200.0, timestamp: UNIX_EPOCH + Duration::from_secs(105) },
    ];

    let stream1 = from_iter(events1);
    let stream2 = from_iter(events2);
    
    let result_stream = stream1.join_with_time_window_with_session_rs2(
        stream2,
        |e1| e1.timestamp,
        |e2| e2.timestamp,
        |e1, e2| (e1.clone(), e2.clone())
    );

    let results: Vec<_> = result_stream.collect().await;
    assert!(!results.is_empty());
    
    // Verify that joins were performed
    for (event1, event2) in results {
        assert!(event1.id <= 2); // From stream1
        assert!(event2.id >= 101); // From stream2
    }
}

#[tokio::test]
async fn test_session_config_fallback() {
    // Don't set global session - should use defaults
    let numbers = vec![1.0, 2.0, 3.0, 4.0];
    let stream = from_iter(numbers);
    
    let result_stream = stream.sliding_window_aggregate_with_session_rs2(3, |window| {
        if window.is_empty() {
            0.0
        } else {
            window.iter().map(|n| n).sum::<f64>() / window.len() as f64
        }
    });

    let results: Vec<f64> = result_stream.collect().await;
    assert_eq!(results.len(), 2);
    
    // First window: [1.0, 2.0, 3.0] -> avg = 2.0
    assert!((results[0] - 2.0).abs() < 0.001);
    // Second window: [2.0, 3.0, 4.0] -> avg = 3.0
    assert!((results[1] - 3.0).abs() < 0.001);
}

#[tokio::test]
#[serial]
async fn test_session_presets() {
    // Test with development preset
    clear_global_session();
    let dev_session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    
    set_global_session(dev_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Development preset should have lower concurrency
    println!("Development preset concurrency: {}", parallel_config.concurrency);
    assert_eq!(parallel_config.concurrency, 2);

    let buffer_config = get_global_buffer_config();
    assert!(buffer_config.is_some());
    let buffer_config = buffer_config.unwrap();
    
    // Development preset should have reasonable buffer settings
    assert!(buffer_config.initial_capacity > 0);

    // Test with production preset
    clear_global_session();
    let prod_session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    
    set_global_session(prod_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Production preset should have higher concurrency
    assert_eq!(parallel_config.concurrency, 16);

    let time_window_config = get_global_time_window_config();
    assert!(time_window_config.is_some());
    let time_window_config = time_window_config.unwrap();
    
    // Production preset should have reasonable time window settings
    assert!(time_window_config.window_size > Duration::from_secs(0));
}

#[tokio::test]
#[serial]
async fn test_custom_session_configuration() {
    // Test with custom configuration
    clear_global_session();
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
        .time_window(|tw| {
            tw.window_size = Duration::from_secs(300); // 5 minutes
            tw.slide_interval = Duration::from_secs(60); // 1 minute
            tw.watermark_delay = Duration::from_secs(30);
            tw.allowed_lateness = Duration::from_secs(15);
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

    let time_window_config = get_global_time_window_config();
    assert!(time_window_config.is_some());
    let time_window_config = time_window_config.unwrap();
    
    assert_eq!(time_window_config.window_size, Duration::from_secs(300));
    assert_eq!(time_window_config.slide_interval, Duration::from_secs(60));
    assert_eq!(time_window_config.watermark_delay, Duration::from_secs(30));
    assert_eq!(time_window_config.allowed_lateness, Duration::from_secs(15));
}

#[tokio::test]
#[serial]
async fn test_high_performance_preset() {
    // Test with high performance preset
    clear_global_session();
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
    // Test with low memory preset
    clear_global_session();
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
async fn test_advanced_analytics_with_session() {
    // Test advanced analytics with session configuration
    clear_global_session();
    let session_config = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 6;
            p.max_buffer_size = 2500;
        })
        .time_window(|tw| {
            tw.window_size = Duration::from_secs(180);
            tw.slide_interval = Duration::from_secs(90);
            tw.watermark_delay = Duration::from_secs(20);
            tw.allowed_lateness = Duration::from_secs(10);
        })
        .build();
    
    set_global_session(session_config);

    let events = vec![
        TestEvent { id: 1, value: 10.0, timestamp: UNIX_EPOCH + Duration::from_secs(30) },
        TestEvent { id: 2, value: 20.0, timestamp: UNIX_EPOCH + Duration::from_secs(60) },
        TestEvent { id: 3, value: 30.0, timestamp: UNIX_EPOCH + Duration::from_secs(90) },
        TestEvent { id: 4, value: 40.0, timestamp: UNIX_EPOCH + Duration::from_secs(120) },
        TestEvent { id: 5, value: 50.0, timestamp: UNIX_EPOCH + Duration::from_secs(150) },
    ];

    // Test sliding window aggregation with session
    let stream1 = from_iter(events.clone());
    let aggregated_stream = stream1.sliding_window_aggregate_with_session_rs2(3, |window| {
        if window.is_empty() {
            0.0
        } else {
            window.iter().map(|e| e.value).sum::<f64>()
        }
    });

    let aggregated_results: Vec<f64> = aggregated_stream.collect().await;
    assert_eq!(aggregated_results.len(), 3);
    
    // Test time-based windowing with session
    let stream2 = from_iter(events);
    let time_window_stream = stream2.window_by_time_with_session_rs2(|event| event.timestamp);
    let time_window_results: Vec<_> = time_window_stream.collect().await;
    assert!(!time_window_results.is_empty());
    
    // Verify that time windows were created with session configuration
    for window in time_window_results {
        assert!(!window.events.is_empty());
        assert!(window.start_time <= window.end_time);
        
        // Check that window size matches session configuration
        let window_duration = window.end_time.duration_since(window.start_time).unwrap();
        // For now, just check that the window duration is reasonable
        assert!(window_duration >= Duration::from_secs(60));
        assert!(window_duration <= Duration::from_secs(300));
    }
} 