use futures_util::stream::StreamExt;
use rs2_stream::advanced_analytics::*;
use rs2_stream::rs2::*;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, PartialEq)]
struct TestEvent {
    id: u64,
    event_type: String,
    timestamp: SystemTime,
}

impl TestEvent {
    fn new(id: u64, event_type: &str, timestamp: SystemTime) -> Self {
        Self {
            id,
            event_type: event_type.to_string(),
            timestamp,
        }
    }
}

#[tokio::test]
async fn test_time_windowed_aggregations() {
    let now = SystemTime::now();
    let events = vec![
        TestEvent::new(1, "login", now),
        TestEvent::new(2, "purchase", now + Duration::from_secs(1)),
        TestEvent::new(1, "logout", now + Duration::from_secs(2)),
    ];

    let config = TimeWindowConfig {
        window_size: Duration::from_secs(60),
        slide_interval: Duration::from_secs(30),
        watermark_delay: Duration::from_secs(5),
        allowed_lateness: Duration::from_secs(2),
    };

    let windowed_stream = from_iter(events).window_by_time_rs2(config, |event| event.timestamp);

    let windows: Vec<_> = windowed_stream.collect().await;

    assert!(!windows.is_empty());
    for window in &windows {
        assert!(!window.events.is_empty());
        assert!(window.start_time < window.end_time);
    }
}

#[tokio::test]
async fn test_time_windowed_joins() {
    let now = SystemTime::now();
    let events1 = vec![
        TestEvent::new(1, "event1", now),
        TestEvent::new(2, "event2", now + Duration::from_secs(1)),
    ];

    let events2 = vec![
        TestEvent::new(1, "event3", now),
        TestEvent::new(2, "event4", now + Duration::from_secs(1)),
        TestEvent::new(1, "event5", now + Duration::from_secs(1)),
    ];

    let config = TimeJoinConfig {
        window_size: Duration::from_secs(60),
        watermark_delay: Duration::from_secs(5),
    };

    let joined_stream = from_iter(events1).join_with_time_window_rs2(
        from_iter(events2),
        config,
        |e| e.timestamp,
        |e| e.timestamp,
        |e1, e2| (e1, e2),
        Some((|e: &TestEvent| e.id, |e: &TestEvent| e.id)),
    );

    let results: Vec<_> = joined_stream.collect().await;
    // Should have all pairs with matching ids in the window
    assert_eq!(
        results
            .iter()
            .filter(|(e1, e2)| e1.id == 1 && e2.id == 1)
            .count(),
        2
    );
    assert_eq!(
        results
            .iter()
            .filter(|(e1, e2)| e1.id == 2 && e2.id == 2)
            .count(),
        1
    );
    for (e1, e2) in results {
        assert_eq!(e1.id, e2.id);
    }
}

#[tokio::test]
async fn test_window_config_default() {
    let config = TimeWindowConfig::default();
    assert_eq!(config.window_size, Duration::from_secs(60));
    assert_eq!(config.slide_interval, Duration::from_secs(60));
    assert_eq!(config.watermark_delay, Duration::from_secs(10));
    assert_eq!(config.allowed_lateness, Duration::from_secs(5));
}

#[tokio::test]
async fn test_time_join_config_default() {
    let config = TimeJoinConfig::default();
    assert_eq!(config.window_size, Duration::from_secs(60));
    assert_eq!(config.watermark_delay, Duration::from_secs(10));
}

#[tokio::test]
async fn test_time_window_operations() {
    let start_time = SystemTime::now();
    let end_time = start_time + Duration::from_secs(60);

    let mut window = TimeWindow::new(start_time, end_time);
    let event = TestEvent::new(1, "test", start_time);

    window.add_event(event);
    assert_eq!(window.events.len(), 1);

    let watermark = end_time + Duration::from_secs(5);
    assert!(window.is_complete(watermark));

    let early_watermark = start_time + Duration::from_secs(30);
    assert!(!window.is_complete(early_watermark));
}

#[tokio::test]
async fn test_extension_trait_methods() {
    let now = SystemTime::now();
    let events = vec![TestEvent::new(1, "test", now)];
    let stream = from_iter(events);
    // Test that extension trait methods are available
    let _windowed = stream.window_by_time_rs2(TimeWindowConfig::default(), |e| e.timestamp);
    let _joined = from_iter(vec![TestEvent::new(1, "test", now)])
        .join_with_time_window_rs2::<TestEvent, _, _, _, (), fn(&TestEvent) -> (), fn(&TestEvent) -> ()>(
            from_iter(vec![TestEvent::new(1, "test", now)]),
            TimeJoinConfig::default(),
            |e| e.timestamp,
            |e| e.timestamp,
            |e1, e2| (e1, e2),
            None::<(fn(&TestEvent) -> (), fn(&TestEvent) -> ())>,
        );
}
