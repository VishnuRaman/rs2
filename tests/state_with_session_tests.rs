use rs2_stream::rs2::*;
use rs2_stream::state::{CustomKeyExtractor, KeyExtractor, StateConfig, StatefulStreamExt};
use rs2_stream::session::{SessionBuilder, SessionPreset, set_global_session, get_global_state_config, get_global_buffer_config};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestData {
    id: u32,
    value: String,
    count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestState {
    total_count: u64,
    last_value: String,
}

impl KeyExtractor<TestData> for fn(&TestData) -> String {
    fn extract_key(&self, item: &TestData) -> String {
        self(item)
    }
}

#[tokio::test]
async fn test_stateful_map_with_session() {
    // Set up a session with custom state configuration
    let session_config = SessionBuilder::new()
        .state(|s| {
            s.max_size = Some(1000);
            s.ttl = Duration::from_secs(3600);
        })
        .build();
    
    set_global_session(session_config);

    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData { id: 1, value: "hello".to_string(), count: 10 },
        TestData { id: 2, value: "world".to_string(), count: 20 },
    ];

    let stream = from_iter_rs2(data);
    let result_stream = stream.stateful_map_with_session_rs2(key_extractor, |item, state_access| {
        Box::pin(async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState { total_count: 0, last_value: String::new() }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(format!("{}: count={}, total={}", item.value, item.count, state.total_count))
        })
    });

    let results: Vec<String> = result_stream
        .collect_rs2()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|r| r.contains("hello: count=10, total=10")));
    assert!(results.iter().any(|r| r.contains("world: count=20, total=20")));
}

#[tokio::test]
async fn test_stateful_filter_with_session() {
    // Set up a session with custom state configuration
    let session_config = SessionBuilder::new()
        .state(|s| {
            s.max_size = Some(500);
            s.ttl = Duration::from_secs(1800);
        })
        .build();
    
    set_global_session(session_config);

    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData { id: 1, value: "small".to_string(), count: 5 },
        TestData { id: 2, value: "large".to_string(), count: 25 },
        TestData { id: 3, value: "medium".to_string(), count: 15 },
    ];

    let stream = from_iter_rs2(data);
    let result_stream = stream.stateful_filter_with_session_rs2(key_extractor, |item, state_access| {
        let item = item.clone();
        Box::pin(async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState { total_count: 0, last_value: String::new() }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            // Only keep items with count > 10
            Ok(item.count > 10)
        })
    });

    let results: Vec<TestData> = result_stream
        .collect_rs2()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|r| r.count == 25));
    assert!(results.iter().any(|r| r.count == 15));
}

#[tokio::test]
async fn test_stateful_aggregate_with_session() {
    // Set up a session with custom state and buffer configuration
    let session_config = SessionBuilder::new()
        .state(|s| {
            s.max_size = Some(2000);
            s.ttl = Duration::from_secs(7200);
        })
        .stream_buffer(|b| {
            b.initial_capacity = 2048;
            b.max_capacity = Some(1024 * 1024); // 1MB
        })
        .build();
    
    set_global_session(session_config);

    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData { id: 1, value: "a".to_string(), count: 10 },
        TestData { id: 1, value: "b".to_string(), count: 20 },
        TestData { id: 2, value: "c".to_string(), count: 30 },
    ];

    let stream = from_iter_rs2(data);
    let result_stream = stream.stateful_aggregate_with_session_rs2(
        key_extractor,
        0u64,
        |acc, item, _state_access| Box::pin(async move { Ok(acc + item.count) })
    );

    let results: Vec<Result<u64, _>> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 3);
    
    let values: Vec<u64> = results.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(values[0], 10);  // First item for id=1
    assert_eq!(values[1], 30);  // 10 + 20 for id=1
    assert_eq!(values[2], 30);  // First item for id=2
}

#[tokio::test]
async fn test_stateful_group_by_with_session() {
    // Set up a session with custom state and buffer configuration
    let session_config = SessionBuilder::new()
        .state(|s| {
            s.max_size = Some(1500);
            s.ttl = Duration::from_secs(5400);
        })
        .stream_buffer(|b| {
            b.initial_capacity = 1024;
            b.max_capacity = Some(512 * 1024); // 512KB
        })
        .build();
    
    set_global_session(session_config);

    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData { id: 1, value: "group1_a".to_string(), count: 10 },
        TestData { id: 1, value: "group1_b".to_string(), count: 20 },
        TestData { id: 2, value: "group2_a".to_string(), count: 30 },
    ];

    let stream = from_iter_rs2(data);
    let result_stream = stream.stateful_group_by_with_session_rs2(
        key_extractor,
        |_key, items, _state_access| Box::pin(async move { 
            Ok(items.iter().map(|item| item.value.clone()).collect())
        })
    );

    let results: Vec<Result<Vec<String>, _>> = result_stream.collect_rs2().await;
    assert_eq!(results.len(), 2); // Should be 2 groups: id=1 and id=2
    
    let values: Vec<Vec<String>> = results.into_iter().map(|r| r.unwrap()).collect();
    // Find the group with id=1 (should contain both items)
    let group1 = values.iter().find(|group| group.contains(&"group1_a".to_string())).unwrap();
    assert_eq!(group1.len(), 2);
    assert!(group1.contains(&"group1_a".to_string()));
    assert!(group1.contains(&"group1_b".to_string()));
    
    // Find the group with id=2 (should contain one item)
    let group2 = values.iter().find(|group| group.contains(&"group2_a".to_string())).unwrap();
    assert_eq!(group2.len(), 1);
    assert_eq!(group2[0], "group2_a");
}

#[tokio::test]
async fn test_session_config_fallback() {
    // Don't set global session - should use defaults
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData { id: 1, value: "test".to_string(), count: 10 },
    ];

    let stream = from_iter_rs2(data);
    let result_stream = stream.stateful_map_with_session_rs2(key_extractor, |item, state_access| {
        Box::pin(async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState { total_count: 0, last_value: String::new() }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(format!("{}: total={}", item.value, state.total_count))
        })
    });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 1);
    assert!(results[0].contains("test: total=10"));
}

#[tokio::test]
async fn test_session_presets() {
    // Test with development preset
    let dev_session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    
    set_global_session(dev_session);

    let state_config = get_global_state_config();
    assert!(state_config.is_some());
    let state_config = state_config.unwrap();
    
    // Development preset should have reasonable defaults
    assert!(state_config.max_size.is_some());
    assert!(state_config.ttl > Duration::from_secs(0));

    // Test with production preset
    let prod_session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    
    set_global_session(prod_session);

    let state_config = get_global_state_config();
    assert!(state_config.is_some());
    let state_config = state_config.unwrap();
    
    // Production preset should have higher limits
    assert!(state_config.max_size.is_some());
    assert!(state_config.ttl > Duration::from_secs(0));
} 