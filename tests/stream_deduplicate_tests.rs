use rs2_stream::stream::Stream;
use rs2_stream::state::StateConfig;
use rs2_stream::state::stream_ext::StatefulStreamExt;
use rs2_stream::resource_manager::ResourceConfig;
use std::time::Duration;
use tokio::time::sleep;

// Include the test utilities directly in this file for now
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use rs2_stream::stream::StreamExt;

/// A simple test stream that implements the custom Stream trait
struct TestStream<T> {
    items: VecDeque<T>,
}

impl<T> TestStream<T> {
    fn new<I>(items: I) -> Self 
    where 
        I: IntoIterator<Item = T>
    {
        Self {
            items: items.into_iter().collect(),
        }
    }
}

impl<T> Stream for TestStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

impl<T> Unpin for TestStream<T> {}

/// Helper function to create a test stream from an iterator
fn test_stream<T, I>(items: I) -> TestStream<T>
where
    I: IntoIterator<Item = T>
{
    TestStream::new(items)
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct TestData {
    id: u32,
    value: String,
    count: u64,
    timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct ComplexData {
    user_id: String,
    event_type: String,
    payload: String,
    sequence: u64,
}

// Custom key extractor for complex data
#[derive(Clone)]
struct ComplexKeyExtractor;

impl rs2_stream::state::traits::KeyExtractor<ComplexData> for ComplexKeyExtractor {
    fn extract_key(&self, item: &ComplexData) -> String {
        format!("{}:{}", item.user_id, item.event_type)
    }
}

// Simple key extractor for TestData
#[derive(Clone)]
struct SimpleKeyExtractor;

impl rs2_stream::state::traits::KeyExtractor<TestData> for SimpleKeyExtractor {
    fn extract_key(&self, item: &TestData) -> String {
        item.id.to_string()
    }
}

#[tokio::test]
async fn test_basic_deduplication() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
        TestData { id: 1, value: "a".to_string(), count: 2, timestamp: 200 }, // duplicate
        TestData { id: 2, value: "b".to_string(), count: 3, timestamp: 300 },
        TestData { id: 1, value: "a".to_string(), count: 4, timestamp: 400 }, // duplicate
        TestData { id: 3, value: "c".to_string(), count: 5, timestamp: 500 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    
    // Should only have 3 unique items (ids: 1, 2, 3)
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().id, 1);
    assert_eq!(results[1].as_ref().unwrap().id, 2);
    assert_eq!(results[2].as_ref().unwrap().id, 3);
}

#[tokio::test]
async fn test_ttl_expiration() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
        TestData { id: 2, value: "b".to_string(), count: 2, timestamp: 200 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config.clone(),
        key_extractor.clone(),
        Duration::from_millis(50), // Short TTL
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 2);

    // Wait for TTL to expire
    sleep(Duration::from_millis(100)).await;

    // Send the same items again - they should not be considered duplicates
    let data2 = vec![
        TestData { id: 1, value: "a".to_string(), count: 3, timestamp: 300 },
        TestData { id: 2, value: "b".to_string(), count: 4, timestamp: 400 },
    ];

    let stream2 = test_stream(data2);
    let result_stream2 = stream2.stateful_deduplicate_rs2(
        config.clone(),
        key_extractor.clone(),
        Duration::from_millis(50),
        |item| item,
        ResourceConfig::default(),
    );

    let results2: Vec<_> = result_stream2.collect().await;
    assert_eq!(results2.len(), 2); // Should not be considered duplicates after TTL
}

#[tokio::test]
async fn test_custom_transformation() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
        TestData { id: 1, value: "a".to_string(), count: 2, timestamp: 200 }, // duplicate
        TestData { id: 2, value: "b".to_string(), count: 3, timestamp: 300 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| TestData { 
            id: item.id, 
            value: item.value.clone() + "_transformed", 
            count: item.count * 2, 
            timestamp: item.timestamp 
        },
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 2);
    
    // Check that transformation was applied
    assert_eq!(results[0].as_ref().unwrap().value, "a_transformed");
    assert_eq!(results[0].as_ref().unwrap().count, 2); // 1 * 2
    assert_eq!(results[1].as_ref().unwrap().value, "b_transformed");
    assert_eq!(results[1].as_ref().unwrap().count, 6); // 3 * 2
}

#[tokio::test]
async fn test_complex_key_extraction() {
    let data = vec![
        ComplexData { user_id: "user1".to_string(), event_type: "click".to_string(), payload: "data1".to_string(), sequence: 1 },
        ComplexData { user_id: "user1".to_string(), event_type: "click".to_string(), payload: "data2".to_string(), sequence: 2 }, // duplicate key
        ComplexData { user_id: "user1".to_string(), event_type: "scroll".to_string(), payload: "data3".to_string(), sequence: 3 },
        ComplexData { user_id: "user2".to_string(), event_type: "click".to_string(), payload: "data4".to_string(), sequence: 4 },
    ];

    let config = StateConfig::default();
    let key_extractor = ComplexKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 3); // user1:click, user1:scroll, user2:click
    
    // Verify the correct items were kept
    let keys: Vec<_> = results.iter().map(|item| format!("{}:{}", item.as_ref().unwrap().user_id, item.as_ref().unwrap().event_type)).collect();
    assert!(keys.contains(&"user1:click".to_string()));
    assert!(keys.contains(&"user1:scroll".to_string()));
    assert!(keys.contains(&"user2:click".to_string()));
}

#[tokio::test]
async fn test_empty_stream() {
    let data: Vec<TestData> = vec![];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_single_item() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_ref().unwrap().id, 1);
}

#[tokio::test]
async fn test_all_duplicates() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
        TestData { id: 1, value: "a".to_string(), count: 2, timestamp: 200 },
        TestData { id: 1, value: "a".to_string(), count: 3, timestamp: 300 },
        TestData { id: 1, value: "a".to_string(), count: 4, timestamp: 400 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 1); // Only one unique item
    assert_eq!(results[0].as_ref().unwrap().id, 1);
}

#[tokio::test]
async fn test_no_duplicates() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
        TestData { id: 2, value: "b".to_string(), count: 2, timestamp: 200 },
        TestData { id: 3, value: "c".to_string(), count: 3, timestamp: 300 },
        TestData { id: 4, value: "d".to_string(), count: 4, timestamp: 400 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 4); // All items should pass through
}

#[tokio::test]
async fn test_large_dataset() {
    let mut data = Vec::new();
    for i in 0..1000 {
        data.push(TestData { 
            id: i % 100, // Creates 100 unique IDs with duplicates
            value: format!("value_{}", i),
            count: i as u64,
            timestamp: i as u64,
        });
    }

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    let stream = test_stream(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let results: Vec<_> = result_stream.collect().await;
    assert_eq!(results.len(), 100); // Should have exactly 100 unique IDs
}

#[tokio::test]
async fn test_concurrent_access() {
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 1, timestamp: 100 },
        TestData { id: 2, value: "b".to_string(), count: 2, timestamp: 200 },
        TestData { id: 1, value: "a".to_string(), count: 3, timestamp: 300 }, // duplicate
        TestData { id: 3, value: "c".to_string(), count: 4, timestamp: 400 },
    ];

    let config = StateConfig::default();
    let key_extractor = SimpleKeyExtractor;
    
    // Create multiple streams with the same state storage
    let stream1 = test_stream(data.clone());
    let result_stream1 = stream1.stateful_deduplicate_rs2(
        config.clone(),
        key_extractor.clone(),
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let stream2 = test_stream(data);
    let result_stream2 = stream2.stateful_deduplicate_rs2(
        config.clone(),
        key_extractor.clone(),
        Duration::from_millis(1000),
        |item| item,
        ResourceConfig::default(),
    );

    let (results1, results2): (Vec<_>, Vec<_>) = tokio::join!(
        result_stream1.collect(),
        result_stream2.collect()
    );

    assert_eq!(results1.len(), 3);
    assert_eq!(results2.len(), 3);
} 