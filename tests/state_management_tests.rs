use futures::StreamExt;
use rs2_stream::state::config::StateConfigs;
use rs2_stream::state::stream_ext::StateAccess;
use rs2_stream::state::traits::StateStorageType;
use rs2_stream::state::{
    CustomKeyExtractor, InMemoryState, KeyExtractor, StateStorage, StatefulStreamExt,
};
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: u64,
    value: String,
    amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestState {
    count: u64,
    total_amount: f64,
    last_value: String,
}

#[tokio::test]
async fn test_stateful_map_basic() {
    let events = futures::stream::iter(vec![
        TestEvent {
            id: 1,
            value: "test1".to_string(),
            amount: 10.0,
        },
        TestEvent {
            id: 2,
            value: "test2".to_string(),
            amount: 20.0,
        },
        TestEvent {
            id: 1,
            value: "test3".to_string(),
            amount: 15.0,
        },
    ]);

    let config = rs2_stream::state::StateConfig::new();

    let results: Vec<_> = events
        .stateful_map_rs2(
            config,
            CustomKeyExtractor::new(|event: &TestEvent| event.id.to_string()),
            |event, state_access| {
                Box::pin(async move {
                    // Get current state or create new one
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(TestState {
                            count: 0,
                            total_amount: 0.0,
                            last_value: String::new(),
                        })
                    } else {
                        TestState {
                            count: 0,
                            total_amount: 0.0,
                            last_value: String::new(),
                        }
                    };

                    // Update state
                    state.count += 1;
                    state.total_amount += event.amount;
                    state.last_value = event.value.clone();

                    // Save state
                    let bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    Ok(state)
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    let results: Vec<TestState> = results.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].count, 1);
    assert_eq!(results[0].total_amount, 10.0);
    assert_eq!(results[1].count, 1);
    assert_eq!(results[1].total_amount, 20.0);
    assert_eq!(results[2].count, 2);
    assert_eq!(results[2].total_amount, 25.0);
}

#[tokio::test]
async fn test_stateful_filter() {
    let events = futures::stream::iter(vec![
        TestEvent {
            id: 1,
            value: "test1".to_string(),
            amount: 10.0,
        },
        TestEvent {
            id: 2,
            value: "test2".to_string(),
            amount: 20.0,
        },
        TestEvent {
            id: 1,
            value: "test3".to_string(),
            amount: 15.0,
        },
        TestEvent {
            id: 3,
            value: "test4".to_string(),
            amount: 5.0,
        },
    ]);

    let config = rs2_stream::state::StateConfig::new();

    let results: Vec<_> = events
        .stateful_filter_rs2(
            config,
            CustomKeyExtractor::new(|event: &TestEvent| event.id.to_string()),
            |event, state_access| {
                Box::pin(async move {
                    // Get current count
                    let count = if let Some(bytes) = state_access.get().await {
                        let state: TestState =
                            serde_json::from_slice(&bytes).unwrap_or(TestState {
                                count: 0,
                                total_amount: 0.0,
                                last_value: String::new(),
                            });
                        state.count
                    } else {
                        0
                    };

                    // Update count
                    let new_state = TestState {
                        count: count + 1,
                        total_amount: 0.0,
                        last_value: String::new(),
                    };
                    let bytes = serde_json::to_vec(&new_state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    // Only allow events with count <= 2
                    Ok(count < 2)
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    let results: Vec<TestEvent> = results.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].id, 1);
    assert_eq!(results[1].id, 2);
    assert_eq!(results[2].id, 1);
    assert_eq!(results[3].id, 3);
}

#[tokio::test]
async fn test_stateful_fold() {
    let events = futures::stream::iter(vec![
        TestEvent {
            id: 1,
            value: "test1".to_string(),
            amount: 10.0,
        },
        TestEvent {
            id: 2,
            value: "test2".to_string(),
            amount: 20.0,
        },
        TestEvent {
            id: 1,
            value: "test3".to_string(),
            amount: 15.0,
        },
    ]);

    let config = rs2_stream::state::StateConfig::new();

    let results: Vec<_> = events
        .stateful_fold_rs2(
            config,
            CustomKeyExtractor::new(|event: &TestEvent| event.id.to_string()),
            TestState {
                count: 0,
                total_amount: 0.0,
                last_value: String::new(),
            },
            |state, event, state_access| {
                Box::pin(async move {
                    // Get current state
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(TestState {
                            count: 0,
                            total_amount: 0.0,
                            last_value: String::new(),
                        })
                    } else {
                        TestState {
                            count: 0,
                            total_amount: 0.0,
                            last_value: String::new(),
                        }
                    };

                    // Update state
                    state.count += 1;
                    state.total_amount += event.amount;
                    state.last_value = event.value.clone();

                    // Save state
                    let bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    Ok(state)
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    let results: Vec<TestState> = results.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].count, 1);
    assert_eq!(results[0].total_amount, 10.0);
    assert_eq!(results[1].count, 1);
    assert_eq!(results[1].total_amount, 20.0);
    assert_eq!(results[2].count, 2);
    assert_eq!(results[2].total_amount, 25.0);
}

#[tokio::test]
async fn test_in_memory_storage() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test basic operations
    let key = "test_key";
    let value = TestState {
        count: 42,
        total_amount: 100.0,
        last_value: "test".to_string(),
    };

    // Set value
    storage
        .set(&key, &serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();

    // Get value
    let bytes = storage.get(&key).await.unwrap();
    let retrieved: TestState = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(retrieved, value);

    // Check exists
    assert!(storage.exists(&key).await);

    // Delete value
    storage.delete(&key).await.unwrap();
    assert!(!storage.exists(&key).await);
}

#[tokio::test]
async fn test_state_config_validation() {
    // Valid config
    let valid_config = StateConfigs::high_performance();
    assert!(valid_config.validate().is_ok());

    // Invalid config - zero TTL
    let mut invalid_config = StateConfigs::high_performance();
    invalid_config.ttl = Duration::from_secs(0);
    assert!(invalid_config.validate().is_err());

    // Invalid config - cleanup interval > TTL
    let mut invalid_config = StateConfigs::high_performance();
    invalid_config.cleanup_interval = Duration::from_secs(2 * 60 * 60); // 2 hours
    invalid_config.ttl = Duration::from_secs(60 * 60); // 1 hour
    assert!(invalid_config.validate().is_err());
}

#[tokio::test]
async fn test_state_config_builder() {
    let config = StateConfigBuilder::new()
        .in_memory()
        .ttl(Duration::from_secs(2 * 60 * 60)) // 2 hours
        .cleanup_interval(Duration::from_secs(10 * 60)) // 10 minutes
        .max_size(1000)
        .build()
        .unwrap();

    assert!(matches!(config.storage_type, StateStorageType::InMemory));
    assert_eq!(config.ttl, Duration::from_secs(2 * 60 * 60)); // 2 hours
    assert_eq!(config.cleanup_interval, Duration::from_secs(10 * 60)); // 10 minutes
    assert_eq!(config.max_size, Some(1000));
}

#[tokio::test]
async fn test_state_access_interface() {
    let config = rs2_stream::state::StateConfig::new();
    let storage = config.create_storage_arc();
    let state_access = StateAccess::new(storage, "test_key".to_string());

    // Test basic get/set operations
    let initial_state = TestState {
        count: 0,
        total_amount: 0.0,
        last_value: String::new(),
    };

    // Set initial state
    let state_bytes = serde_json::to_vec(&initial_state).unwrap();
    state_access.set(&state_bytes).await.unwrap();

    // Get and update state
    let state_bytes = state_access.get().await.unwrap_or(Vec::new());
    let mut state: TestState = if state_bytes.is_empty() {
        TestState {
            count: 0,
            total_amount: 0.0,
            last_value: String::new(),
        }
    } else {
        serde_json::from_slice(&state_bytes).unwrap()
    };

    state.count += 1;
    state.total_amount += 50.0;

    // Save updated state
    let state_bytes = serde_json::to_vec(&state).unwrap();
    state_access.set(&state_bytes).await.unwrap();

    assert_eq!(state.count, 1);

    // Verify the update by getting the state again
    let state_bytes = state_access.get().await.unwrap_or(Vec::new());
    let retrieved: TestState = if state_bytes.is_empty() {
        TestState {
            count: 0,
            total_amount: 0.0,
            last_value: String::new(),
        }
    } else {
        serde_json::from_slice(&state_bytes).unwrap()
    };

    assert_eq!(retrieved.count, 1);
    assert_eq!(retrieved.total_amount, 50.0);
}

#[tokio::test]
async fn test_stateful_window_processing() {
    let events = futures::stream::iter(vec![
        TestEvent {
            id: 1,
            value: "test1".to_string(),
            amount: 10.0,
        },
        TestEvent {
            id: 2,
            value: "test2".to_string(),
            amount: 20.0,
        },
        TestEvent {
            id: 1,
            value: "test3".to_string(),
            amount: 15.0,
        },
        TestEvent {
            id: 3,
            value: "test4".to_string(),
            amount: 25.0,
        },
    ]);

    let config = StateConfigs::high_performance();

    let results: Vec<_> = events
        .stateful_window_rs2(
            config,
            CustomKeyExtractor::new(|event: &TestEvent| event.id.to_string()),
            2,
            |window_events, _state_access| {
                Box::pin(async move {
                    let total_amount: f64 = window_events.iter().map(|e| e.amount).sum();
                    let count = window_events.len();
                    Ok((count, total_amount))
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    let results: Vec<(usize, f64)> = results.into_iter().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (2, 25.0)); // Only one window: id=1, 10.0 + 15.0
}

#[tokio::test]
async fn test_state_ttl_expiration() {
    // Use a very short TTL for testing
    let storage = InMemoryState::new(Duration::from_millis(10));

    let key = "expiring_key";
    let value = TestState {
        count: 1,
        total_amount: 100.0,
        last_value: "test".to_string(),
    };

    // Set the value
    storage
        .set(&key, &serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();

    // Verify it exists immediately
    assert!(storage.exists(&key).await);
    assert!(storage.get(&key).await.is_some());

    // Wait for expiration with a timeout
    let timeout = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if !storage.exists(&key).await {
                break;
            }
        }
    })
    .await;

    // Ensure we didn't timeout
    assert!(timeout.is_ok(), "TTL expiration timed out");

    // After expiration, exists() should return false and get() should return None
    assert!(!storage.exists(&key).await);
    assert!(storage.get(&key).await.is_none());
}

#[tokio::test]
async fn test_state_max_size_enforcement() {
    let mut config = StateConfigs::high_performance();
    config.max_size = Some(2);

    let storage = InMemoryState::new(config.ttl).with_max_size(2);

    // Add more items than max_size
    for i in 0..5 {
        let key = format!("key_{}", i);
        let value = TestState {
            count: i,
            total_amount: i as f64 * 10.0,
            last_value: format!("value_{}", i),
        };
        storage
            .set(&key, &serde_json::to_vec(&value).unwrap())
            .await
            .unwrap();
    }

    // Should only have the last 2 items
    assert!(storage.exists("key_3").await);
    assert!(storage.exists("key_4").await);
    assert!(!storage.exists("key_0").await);
    assert!(!storage.exists("key_1").await);
    assert!(!storage.exists("key_2").await);
}

#[tokio::test]
async fn test_custom_key_extractor() {
    let extractor = CustomKeyExtractor::new(|event: &TestEvent| format!("user_{}", event.id));

    let event = TestEvent {
        id: 42,
        value: "test".to_string(),
        amount: 100.0,
    };

    let key = extractor.extract_key(&event);
    assert_eq!(key, "user_42");
}

#[tokio::test]
async fn test_state_error_handling() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test with invalid JSON
    let invalid_json = b"invalid json";
    let result = storage.set("test_key", invalid_json).await;
    assert!(result.is_ok()); // Storage should accept any bytes

    // Test get with non-existent key
    let result = storage.get("non_existent").await;
    assert!(result.is_none());
}

// Builder pattern for StateConfig
struct StateConfigBuilder {
    config: rs2_stream::state::StateConfig,
}

impl StateConfigBuilder {
    fn new() -> Self {
        Self {
            config: rs2_stream::state::StateConfig::default(),
        }
    }

    fn in_memory(mut self) -> Self {
        self.config.storage_type = StateStorageType::InMemory;
        self
    }

    fn ttl(mut self, ttl: Duration) -> Self {
        self.config.ttl = ttl;
        self
    }

    fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.config.cleanup_interval = interval;
        self
    }

    fn max_size(mut self, max_size: usize) -> Self {
        self.config.max_size = Some(max_size);
        self
    }

    fn build(self) -> Result<rs2_stream::state::StateConfig, String> {
        self.config.validate().map_err(|e| e.to_string())?;
        Ok(self.config)
    }
}
