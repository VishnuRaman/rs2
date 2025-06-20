use futures::StreamExt;
use rs2_stream::state::{CustomKeyExtractor, KeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio;
use tokio::sync::mpsc;
use tokio_stream::wrappers;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestData {
    id: u32,
    value: String,
    count: u64,
    is_new_session: Option<bool>, // Optional to allow for events without session info
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
async fn test_stateful_map_basic() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "hello".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "world".to_string(),
            count: 20,
            is_new_session: None,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_map_rs2(config, key_extractor, |item, state_access| {
        let fut = async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState {
                    total_count: 0,
                    last_value: String::new(),
                }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(format!(
                "{}: count={}, total={}",
                item.value, item.count, state.total_count
            ))
        };
        Box::pin(fut)
    });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .any(|r| r.contains("hello: count=10, total=10")));
    assert!(results
        .iter()
        .any(|r| r.contains("world: count=20, total=20")));
}

#[tokio::test]
async fn test_stateful_filter() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "small".to_string(),
            count: 5,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "large".to_string(),
            count: 15,
            is_new_session: None,
        },
        TestData {
            id: 3,
            value: "medium".to_string(),
            count: 10,
            is_new_session: None,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_filter_rs2(config, key_extractor, |item, state_access| {
        let item = item.clone();
        let state_access = state_access.clone();
        Box::pin(async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState {
                    total_count: 0,
                    last_value: String::new(),
                }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(item.count >= 10)
        })
    });

    let results: Vec<TestData> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, "large");
    assert_eq!(results[1].value, "medium");
}

#[tokio::test]
async fn test_stateful_fold() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "hello".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "world".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "test".to_string(),
            count: 30,
            is_new_session: None,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream =
        stream.stateful_fold_rs2(config, key_extractor, 0u64, |acc, item, _state_access| {
            Box::pin(async move { Ok(acc + item.count as u64) })
        });
    let results: Vec<u64> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], 10);
    assert_eq!(results[1], 30);
    assert_eq!(results[2], 30);
}

#[tokio::test]
async fn test_stateful_window() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "a".to_string(),
            count: 1,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "b".to_string(),
            count: 2,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "c".to_string(),
            count: 3,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "d".to_string(),
            count: 4,
            is_new_session: None,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_window_rs2(
        config,
        key_extractor,
        2, // window size
        |window, state_access| {
            let fut = async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let mut state: TestState = if state_bytes.is_empty() {
                    TestState {
                        total_count: 0,
                        last_value: String::new(),
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };
                let window_sum: u64 = window.iter().map(|item| item.count).sum();
                state.total_count += window_sum;
                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();
                Ok(format!(
                    "Window sum: {}, Total: {}",
                    window_sum, state.total_count
                ))
            };
            Box::pin(fut)
        },
    );
    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 2); // Two complete windows of size 2
    assert!(results[0].contains("Window sum: 3")); // a+b
    assert!(results[1].contains("Window sum: 7")); // c+d
}

#[tokio::test]
async fn test_stateful_join() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let other_key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let left_data = vec![
        TestData {
            id: 1,
            value: "left1".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "left2".to_string(),
            count: 20,
            is_new_session: None,
        },
    ];
    let right_data = vec![
        TestData {
            id: 1,
            value: "right1".to_string(),
            count: 30,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "right2".to_string(),
            count: 40,
            is_new_session: None,
        },
    ];

    // Create interleaved streams to ensure deterministic behavior
    let (left_tx, left_rx) = mpsc::unbounded_channel();
    let (right_tx, right_rx) = mpsc::unbounded_channel();

    // Spawn a task to send items in an interleaved manner
    tokio::spawn(async move {
        let max_len = left_data.len().max(right_data.len());
        for i in 0..max_len {
            if i < left_data.len() {
                left_tx.send(left_data[i].clone()).unwrap();
            }
            if i < right_data.len() {
                right_tx.send(right_data[i].clone()).unwrap();
            }
            // Small yield to allow polling
            tokio::task::yield_now().await;
        }
    });

    let left_stream = wrappers::UnboundedReceiverStream::new(left_rx);
    let right_stream = wrappers::UnboundedReceiverStream::new(right_rx);

    let result_stream = left_stream.stateful_join_rs2(
        Box::pin(right_stream),
        config,
        key_extractor,
        other_key_extractor,
        Duration::from_secs(10), // Longer window for deterministic results
        |left, right, state_access| {
            let fut = async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let mut state: TestState = if state_bytes.is_empty() {
                    TestState {
                        total_count: 0,
                        last_value: String::new(),
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };

                state.total_count += left.count + right.count;
                state.last_value = format!("{}+{}", left.value, right.value);

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!("{} + {}", left.value, right.value))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("[JOIN_BASIC_DEBUG] Results: {:?}", results);

    // With interleaved streams, we should get consistent results
    assert!(!results.is_empty(), "Expected at least one join result");
    assert!(
        results.len() >= 2,
        "Expected at least 2 join results, got {}",
        results.len()
    );

    // Check that we get the expected join results
    let mut found_left1_right1 = false;
    let mut found_left2_right2 = false;

    for result in &results {
        if result.contains("left1 + right1") {
            found_left1_right1 = true;
        }
        if result.contains("left2 + right2") {
            found_left2_right2 = true;
        }
    }

    // We should have at least these specific joins
    assert!(found_left1_right1, "Expected 'left1 + right1' join result");
    assert!(found_left2_right2, "Expected 'left2 + right2' join result");

    // All results should contain both left and right items
    for result in &results {
        assert!(
            result.contains("left") && result.contains("right"),
            "All join results should contain both left and right items: {}",
            result
        );
    }
}

#[tokio::test]
async fn test_stateful_join_different_keys() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let other_key_extractor: fn(&TestData) -> String = |data| (data.id + 1).to_string(); // Different key mapping

    let left_data = vec![TestData {
        id: 1,
        value: "left1".to_string(),
        count: 10,
        is_new_session: None,
    }];
    let right_data = vec![
        TestData {
            id: 0,
            value: "right0".to_string(),
            count: 30,
            is_new_session: None,
        }, // id=0 maps to key="1"
    ];

    // Create interleaved streams to ensure deterministic behavior
    let (left_tx, left_rx) = mpsc::unbounded_channel();
    let (right_tx, right_rx) = mpsc::unbounded_channel();

    // Spawn a task to send items in an interleaved manner
    tokio::spawn(async move {
        let max_len = left_data.len().max(right_data.len());
        for i in 0..max_len {
            if i < left_data.len() {
                left_tx.send(left_data[i].clone()).unwrap();
            }
            if i < right_data.len() {
                right_tx.send(right_data[i].clone()).unwrap();
            }
            // Small yield to allow polling
            tokio::task::yield_now().await;
        }
    });

    let left_stream = wrappers::UnboundedReceiverStream::new(left_rx);
    let right_stream = wrappers::UnboundedReceiverStream::new(right_rx);

    let result_stream = left_stream.stateful_join_rs2(
        Box::pin(right_stream),
        config,
        key_extractor,
        other_key_extractor,
        Duration::from_secs(10), // Longer window for deterministic results
        |left: TestData, right: TestData, _state_access| {
            let fut = async move { Ok(format!("{} + {}", left.value, right.value)) };
            Box::pin(fut)
        },
    );

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("[JOIN_DIFFERENT_KEYS_DEBUG] Results: {:?}", results);

    // With interleaved streams, we should get consistent results
    assert!(!results.is_empty(), "Expected at least one join result");
    assert_eq!(
        results.len(),
        1,
        "Expected exactly 1 join result, got {}",
        results.len()
    );
    assert!(
        results[0].contains("left1 + right0"),
        "Expected 'left1 + right0' join result, got: {}",
        results[0]
    );
}

#[tokio::test]
async fn test_stateful_operations_with_empty_stream() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let empty_data: Vec<TestData> = vec![];

    let stream = futures::stream::iter(empty_data);
    let result_stream = stream.stateful_map_rs2(config, key_extractor, |item, state_access| {
        let fut = async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState {
                    total_count: 0,
                    last_value: String::new(),
                }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(format!(
                "{}: count={}, total={}",
                item.value, item.count, state.total_count
            ))
        };
        Box::pin(fut)
    });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_stateful_operations_with_single_item() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![TestData {
        id: 1,
        value: "single".to_string(),
        count: 100,
        is_new_session: None,
    }];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_filter_rs2(config, key_extractor, |item, state_access| {
        let item = item.clone();
        let state_access = state_access.clone();
        Box::pin(async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState {
                    total_count: 0,
                    last_value: String::new(),
                }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(item.count > 50) // Only pass items with count > 50
        })
    });

    let results: Vec<TestData> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value, "single");
}

#[tokio::test]
async fn test_stateful_operations_with_multiple_keys() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "key1_a".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_a".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "key1_b".to_string(),
            count: 30,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_b".to_string(),
            count: 40,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream =
        stream.stateful_fold_rs2(config, key_extractor, 0u64, |acc, item, _state_access| {
            Box::pin(async move { Ok(acc + item.count as u64) })
        });

    let results: Vec<u64> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], 10); // key1: 10
    assert_eq!(results[1], 20); // key2: 20
    assert_eq!(results[2], 40); // key1: 10 + 30
    assert_eq!(results[3], 60); // key2: 20 + 40
}

#[tokio::test]
async fn test_stateful_operations_with_large_data() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| (data.id % 10).to_string(); // Group into 10 keys

    let mut data = Vec::new();
    for i in 0..100 {
        data.push(TestData {
            id: i,
            value: format!("item_{}", i),
            count: i as u64,
            is_new_session: None,
        });
    }

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_window_rs2(
        config,
        key_extractor,
        5, // window size
        |window, state_access| {
            let fut = async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let mut state: TestState = if state_bytes.is_empty() {
                    TestState {
                        total_count: 0,
                        last_value: String::new(),
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };

                let window_sum: u64 = window.iter().map(|item| item.count).sum();
                state.total_count += window_sum;

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "Window: {} items, sum: {}, total: {}",
                    window.len(),
                    window_sum,
                    state.total_count
                ))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 20); // 100 items / 5 items per window = 20 windows
}

#[tokio::test]
async fn test_stateful_operations_error_handling() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![TestData {
        id: 1,
        value: "test".to_string(),
        count: 10,
        is_new_session: None,
    }];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_map_rs2(config, key_extractor, |item, state_access| {
        let fut = async move {
            // Simulate state access error
            let _state_bytes = state_access.get().await;
            // Continue processing even if state access fails
            Ok(format!("{}: processed", item.value))
        };
        Box::pin(fut)
    });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].contains("test: processed"));
}

#[tokio::test]
async fn test_stateful_operations_concurrent_access() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "concurrent1".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "concurrent2".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "concurrent3".to_string(),
            count: 30,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream =
        stream.stateful_reduce_rs2(config, key_extractor, 0u64, |acc, item, _state_access| {
            Box::pin(async move { Ok(acc + item.count as u64) })
        });

    let results: Vec<u64> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], 10); // key1: 10
    assert_eq!(results[1], 30); // key1: 10 + 20
    assert_eq!(results[2], 30); // key2: 30
}

#[tokio::test]
async fn test_stateful_operations_with_custom_key_extractor() {
    let config = StateConfig::new();
    let key_extractor =
        CustomKeyExtractor::new(|data: &TestData| format!("{}_{}", data.id, data.value.len()));

    let data = vec![
        TestData {
            id: 1,
            value: "short".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "longer".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "short".to_string(),
            count: 30,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_map_rs2(config, key_extractor, |item, state_access| {
        let fut = async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
            let mut state: TestState = if state_bytes.is_empty() {
                TestState {
                    total_count: 0,
                    last_value: String::new(),
                }
            } else {
                serde_json::from_slice(&state_bytes).unwrap()
            };

            state.total_count += item.count;
            state.last_value = item.value.clone();

            let state_bytes = serde_json::to_vec(&state).unwrap();
            state_access.set(&state_bytes).await.unwrap();

            Ok(format!("{} (total: {})", item.value, state.total_count))
        };
        Box::pin(fut)
    });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3);
    assert!(results[0].contains("short (total: 10)"));
    assert!(results[1].contains("longer (total: 20)")); // Different key due to different length
    assert!(results[2].contains("short (total: 30)")); // Different key due to different id
}
