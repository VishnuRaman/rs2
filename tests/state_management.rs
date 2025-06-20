use futures::StreamExt;
use rs2_stream::state::stream_ext::StateAccess;
use rs2_stream::state::StateError;
use rs2_stream::state::{CustomKeyExtractor, KeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    id: u64,
    value: String,
    count: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    data: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserState {
    total_events: u64,
    last_event_type: String,
    event_counts: std::collections::HashMap<String, u64>,
    last_seen: u64,
}

#[tokio::test]
async fn test_stateful_map() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "hello".to_string(),
            count: 10,
        },
        TestData {
            id: 1,
            value: "world".to_string(),
            count: 20,
        },
        TestData {
            id: 2,
            value: "test".to_string(),
            count: 30,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream =
        stream.stateful_map_rs2(config, key_extractor, |item, state_access: StateAccess| {
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
                state.total_count += item.count as u64;
                state.last_value = item.value.clone();
                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();
                Ok(TestData {
                    id: item.id,
                    value: format!("{} (total: {})", item.value, state.total_count),
                    count: item.count,
                })
            };
            Box::pin(fut)
        });
    let results: Vec<TestData> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].value, "hello (total: 10)");
    assert_eq!(results[1].value, "world (total: 30)");
    assert_eq!(results[2].value, "test (total: 30)");
}

#[tokio::test]
async fn test_stateful_filter() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![
        TestData {
            id: 1,
            value: "hello".to_string(),
            count: 10,
        },
        TestData {
            id: 1,
            value: "world".to_string(),
            count: 20,
        },
        TestData {
            id: 2,
            value: "test".to_string(),
            count: 30,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream =
        stream.stateful_filter_rs2(config, key_extractor, |item, state_access: StateAccess| {
            let item = item.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let state: TestState = if state_bytes.is_empty() {
                    TestState {
                        total_count: 0,
                        last_value: String::new(),
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };
                Ok(item.count as u64 > state.total_count)
            })
        });
    let results: Vec<TestData> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].count, 10);
    assert_eq!(results[1].count, 20);
    assert_eq!(results[2].count, 30);
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
        },
        TestData {
            id: 1,
            value: "world".to_string(),
            count: 20,
        },
        TestData {
            id: 2,
            value: "test".to_string(),
            count: 30,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_fold_rs2(
        config,
        key_extractor,
        0u64,
        |acc, item, _state_access: StateAccess| {
            let fut = async move { Ok(acc + item.count as u64) };
            Box::pin(fut)
        },
    );
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
            value: "hello".to_string(),
            count: 10,
        },
        TestData {
            id: 1,
            value: "world".to_string(),
            count: 20,
        },
        TestData {
            id: 1,
            value: "test".to_string(),
            count: 30,
        },
    ];
    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_window_rs2(
        config,
        key_extractor,
        2,
        |window, state_access: StateAccess| {
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
                let window_sum: u64 = window.iter().map(|item| item.count as u64).sum();
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
    assert_eq!(results.len(), 1);
    assert!(results[0].contains("Window sum: 30"));
}

#[tokio::test]
async fn test_stateful_join() {
    let config = StateConfig::new();
    let key_extractor1 = CustomKeyExtractor::new(|data: &TestData| data.id.to_string());
    let key_extractor2 = CustomKeyExtractor::new(|data: &TestData| data.id.to_string());

    // Create test data with matching keys
    let data1 = vec![
        TestData {
            id: 1,
            value: "hello".to_string(),
            count: 10,
        },
        TestData {
            id: 2,
            value: "world".to_string(),
            count: 20,
        },
    ];
    let data2 = vec![
        TestData {
            id: 1,
            value: "test".to_string(),
            count: 30,
        },
        TestData {
            id: 2,
            value: "join".to_string(),
            count: 40,
        },
    ];

    // Create interleaved streams to ensure deterministic behavior
    let (stream1_tx, stream1_rx) = tokio::sync::mpsc::unbounded_channel();
    let (stream2_tx, stream2_rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn a task to send items in an interleaved manner
    tokio::spawn(async move {
        let max_len = data1.len().max(data2.len());
        for i in 0..max_len {
            if i < data1.len() {
                stream1_tx.send(data1[i].clone()).unwrap();
            }
            if i < data2.len() {
                stream2_tx.send(data2[i].clone()).unwrap();
            }
            // Small yield to allow polling
            tokio::task::yield_now().await;
        }
    });

    let stream1 = tokio_stream::wrappers::UnboundedReceiverStream::new(stream1_rx);
    let stream2 = tokio_stream::wrappers::UnboundedReceiverStream::new(stream2_rx);

    let result_stream = stream1.stateful_join_rs2(
        Box::pin(stream2),
        config,
        key_extractor1,
        key_extractor2,
        std::time::Duration::from_secs(10), // Longer window for deterministic results
        |left: TestData, right: TestData, state_access: StateAccess| {
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
                state.total_count += left.count as u64 + right.count as u64;
                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();
                Ok(format!(
                    "{} + {} = {}",
                    left.value, right.value, state.total_count
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
    println!("JOIN RESULTS: {:?}", results);

    // With interleaved streams and longer window, we should get consistent results
    assert!(
        !results.is_empty(),
        "Expected at least one join result, got {}",
        results.len()
    );
    assert!(
        results.len() >= 2,
        "Expected at least 2 join results, got {}",
        results.len()
    );

    // Check that we have the expected join results
    let mut found_hello_test = false;
    let mut found_world_join = false;

    for result in &results {
        if result.contains("hello + test = 40") {
            found_hello_test = true;
        }
        if result.contains("world + join = 60") {
            found_world_join = true;
        }
    }

    // We should have at least one of the expected joins
    assert!(
        found_hello_test || found_world_join,
        "Expected at least one of the join results, got: {:?}",
        results
    );

    // If we got both results, that's great
    if results.len() >= 2 {
        assert!(
            found_hello_test,
            "Expected 'hello + test = 40' in results: {:?}",
            results
        );
        assert!(
            found_world_join,
            "Expected 'world + join = 60' in results: {:?}",
            results
        );
    }
}

#[tokio::test]
async fn test_stateful_map_user_events() {
    let events = futures::stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: std::collections::HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: std::collections::HashMap::new(),
        },
    ]);

    let config = StateConfig::default();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let results: Vec<Result<UserState, StateError>> = events
        .stateful_map_rs2(config, key_extractor, |event, state_access: StateAccess| {
            let event = event.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let mut state: UserState = if state_bytes.is_empty() {
                    UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: std::collections::HashMap::new(),
                        last_seen: 0,
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };

                state.total_events += 1;
                state.last_event_type = event.event_type.clone();
                state.last_seen = event.timestamp;
                *state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(state)
            })
        })
        .collect::<Vec<_>>()
        .await;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().total_events, 1);
    assert_eq!(results[1].as_ref().unwrap().total_events, 2);
}

#[tokio::test]
async fn test_stateful_filter_user_events() {
    let events = futures::stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: std::collections::HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: std::collections::HashMap::new(),
        },
    ]);

    let config = StateConfig::default();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let results: Vec<Result<UserEvent, StateError>> = events
        .stateful_filter_rs2(config, key_extractor, |event, state_access: StateAccess| {
            let event = event.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let state: UserState = if state_bytes.is_empty() {
                    UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: std::collections::HashMap::new(),
                        last_seen: 0,
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };

                let mut new_state = state;
                new_state.total_events += 1;
                new_state.last_event_type = event.event_type.clone();
                new_state.last_seen = event.timestamp;
                *new_state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                let state_bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(new_state.total_events <= 2)
            })
        })
        .collect::<Vec<_>>()
        .await;

    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.is_ok()));
    let events: Vec<UserEvent> = results.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(events[0].event_type, "login");
    assert_eq!(events[1].event_type, "purchase");
}

#[tokio::test]
async fn test_stateful_fold_user_events() {
    let events = futures::stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: std::collections::HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: std::collections::HashMap::new(),
        },
    ]);

    let config = StateConfig::default();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let results: Vec<Result<u64, StateError>> = events
        .stateful_fold_rs2(
            config,
            key_extractor,
            0u64,
            |acc, _event, _state_access: StateAccess| Box::pin(async move { Ok(acc + 1) }),
        )
        .collect::<Vec<_>>()
        .await;

    assert_eq!(results.len(), 2);
    assert_eq!(*results[0].as_ref().unwrap(), 1);
    assert_eq!(*results[1].as_ref().unwrap(), 2);
}

#[tokio::test]
async fn test_stateful_window_user_events() {
    let events = futures::stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: std::collections::HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: std::collections::HashMap::new(),
        },
    ]);

    let config = StateConfig::default();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let results: Vec<Result<usize, StateError>> = events
        .stateful_window_rs2(
            config,
            key_extractor,
            2,
            |window, state_access: StateAccess| {
                Box::pin(async move {
                    let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                    let mut state: UserState = if state_bytes.is_empty() {
                        UserState {
                            total_events: 0,
                            last_event_type: String::new(),
                            event_counts: std::collections::HashMap::new(),
                            last_seen: 0,
                        }
                    } else {
                        serde_json::from_slice(&state_bytes).unwrap()
                    };

                    state.total_events += window.len() as u64;
                    if let Some(last_event) = window.last() {
                        state.last_event_type = last_event.event_type.clone();
                        state.last_seen = last_event.timestamp;
                    }

                    let state_bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&state_bytes).await.unwrap();

                    Ok(window.len())
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

    assert_eq!(results.len(), 1);
    assert_eq!(*results[0].as_ref().unwrap(), 2);
}

#[tokio::test]
async fn test_stateful_join_user_events() {
    // Create interleaved data to ensure both streams are polled within the join window
    let stream1_data = vec![
        TestData {
            id: 1,
            value: "test1".to_string(),
            count: 10,
        },
        TestData {
            id: 2,
            value: "test2".to_string(),
            count: 20,
        },
    ];

    let stream2_data = vec![
        TestData {
            id: 1,
            value: "join1".to_string(),
            count: 30,
        },
        TestData {
            id: 3,
            value: "join3".to_string(),
            count: 40,
        },
    ];

    // Interleave the events
    let mut interleaved = Vec::new();
    let max_len = stream1_data.len().max(stream2_data.len());
    for i in 0..max_len {
        if i < stream1_data.len() {
            interleaved.push((Some(stream1_data[i].clone()), None));
        }
        if i < stream2_data.len() {
            interleaved.push((None, Some(stream2_data[i].clone())));
        }
    }

    // Split into two streams: one for stream1, one for stream2, but yield alternately
    let (stream1_tx, stream1_rx) = tokio::sync::mpsc::unbounded_channel();
    let (stream2_tx, stream2_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        for (data1, data2) in interleaved {
            if let Some(d1) = data1 {
                stream1_tx.send(d1).unwrap();
            }
            if let Some(d2) = data2 {
                stream2_tx.send(d2).unwrap();
            }
            // Small yield to allow polling
            tokio::task::yield_now().await;
        }
    });
    let stream1 = tokio_stream::wrappers::UnboundedReceiverStream::new(stream1_rx);
    let stream2 = tokio_stream::wrappers::UnboundedReceiverStream::new(stream2_rx);

    let config = StateConfig::default();
    let key_extractor1 = CustomKeyExtractor::new(|data: &TestData| data.id.to_string());
    let key_extractor2 = CustomKeyExtractor::new(|data: &TestData| data.id.to_string());

    let mut result_stream = stream1.stateful_join_rs2(
        Box::pin(stream2),
        config,
        key_extractor1,
        key_extractor2,
        std::time::Duration::from_secs(10),
        |data1: TestData, data2: TestData, state_access: StateAccess| {
            let fut = async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                let mut state: UserState = if state_bytes.is_empty() {
                    UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: std::collections::HashMap::new(),
                        last_seen: 0,
                    }
                } else {
                    serde_json::from_slice(&state_bytes).unwrap()
                };

                state.total_events += 1;
                state.last_event_type = format!("{}:{}", data1.value, data2.value);
                state.last_seen = 0;

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!("{}:{}", data1.value, data2.value))
            };
            Box::pin(fut)
        },
    );

    let mut results = Vec::new();
    while let Some(result) = result_stream.next().await {
        results.push(result);
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_ref().unwrap(), "test1:join1");
}
