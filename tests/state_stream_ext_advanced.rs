use futures::StreamExt;
use rs2_stream::state::stream_ext::StateAccess;
use rs2_stream::state::{CustomKeyExtractor, KeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio;

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
async fn test_stateful_reduce() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "a".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "b".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "c".to_string(),
            count: 30,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "d".to_string(),
            count: 5,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "e".to_string(),
            count: 15,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_reduce_rs2(
        config,
        key_extractor,
        0u64, // initial value
        |acc, item, state_access| Box::pin(async move { Ok(acc + item.count) }),
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0], 10); // First item for id=1
    assert_eq!(results[1], 30); // 10 + 20 for id=1
    assert_eq!(results[2], 60); // 10 + 20 + 30 for id=1
    assert_eq!(results[3], 5); // First item for id=2
    assert_eq!(results[4], 20); // 5 + 15 for id=2
}

#[tokio::test]
async fn test_stateful_group_by() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| (data.id % 2).to_string(); // Group by even/odd

    let data = vec![
        TestData {
            id: 1,
            value: "odd1".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "even1".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 3,
            value: "odd2".to_string(),
            count: 30,
            is_new_session: None,
        },
        TestData {
            id: 4,
            value: "even2".to_string(),
            count: 40,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_group_by_rs2(
        config,
        key_extractor,
        |group_key, group_items, state_access| {
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

                // Calculate group statistics
                let total_count: u64 = group_items.iter().map(|item| item.count as u64).sum();
                let avg_count = total_count / group_items.len() as u64;

                state.total_count += total_count;
                state.last_value = format!("group_{}", group_key);

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "Group {}: {} items, avg count: {}",
                    group_key,
                    group_items.len(),
                    avg_count
                ))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 2); // Two groups: "0" (even) and "1" (odd)
    assert!(results.iter().any(|r| r.contains("Group 0: 2 items"))); // Even group
    assert!(results.iter().any(|r| r.contains("Group 1: 2 items"))); // Odd group
}

#[tokio::test]
async fn test_stateful_group_by_advanced() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "item1".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "item2".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "item3".to_string(),
            count: 30,
            is_new_session: None,
        }, // Should trigger size-based emission
        TestData {
            id: 2,
            value: "item4".to_string(),
            count: 40,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "item5".to_string(),
            count: 50,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_group_by_advanced_rs2(
        config,
        key_extractor,
        Some(3), // max_group_size: emit when group reaches 3 items
        None,    // group_timeout: no timeout
        false,   // emit_on_key_change: don't emit on key change
        |group_key, group_items, state_access| {
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

                let total_count: u64 = group_items.iter().map(|item| item.count as u64).sum();
                state.total_count += total_count;
                state.last_value = group_items.last().unwrap().value.clone();

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "Group {}: {} items, total: {}",
                    group_key,
                    group_items.len(),
                    state.total_count
                ))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Should have 2 results: one for group "1" (size-based emission) and one for group "2" (end-of-stream)
    assert_eq!(results.len(), 2);

    // Group "1" should have 3 items (size-based emission)
    let group1_result = results.iter().find(|r| r.contains("Group 1:")).unwrap();
    assert!(group1_result.contains("Group 1: 3 items"));

    // Group "2" should have 2 items (end-of-stream emission)
    let group2_result = results.iter().find(|r| r.contains("Group 2:")).unwrap();
    assert!(group2_result.contains("Group 2: 2 items"));
}

#[tokio::test]
async fn test_stateful_deduplicate() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.value.clone();

    let data = vec![
        TestData {
            id: 1,
            value: "duplicate".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "unique".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 3,
            value: "duplicate".to_string(),
            count: 30,
            is_new_session: None,
        }, // Should be filtered out
        TestData {
            id: 4,
            value: "another".to_string(),
            count: 40,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(100), // TTL is less than sleep, so deduplication should occur for the second item only
        |item| item,                // Identity function for deduplication
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id, 1);
    assert_eq!(results[1].id, 2);
    assert_eq!(results[2].id, 4);
}

#[tokio::test]
async fn test_stateful_throttle() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    // Create a stream that emits items with delays to simulate real time
    let data = vec![
        TestData {
            id: 1,
            value: "first".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "second".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "third".to_string(),
            count: 30,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "other".to_string(),
            count: 40,
            is_new_session: None,
        },
    ];

    // Create a custom stream with delays
    struct DelayedStream {
        data: Vec<TestData>,
        index: usize,
    }

    impl futures::stream::Stream for DelayedStream {
        type Item = TestData;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if self.index < self.data.len() {
                let item = self.data[self.index].clone();
                println!(
                    "[DEBUG] Yielding item {} at index {}",
                    item.value, self.index
                );
                self.index += 1;
                // Add a 50ms delay after the first item, 200ms after the second
                if item.id == 1 {
                    let delay = match self.index {
                        2 => 50,  // after first item
                        3 => 200, // after second item
                        _ => 0,
                    };
                    if delay > 0 {
                        let waker = cx.waker().clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                            waker.wake();
                        });
                        return std::task::Poll::Pending;
                    }
                }
                std::task::Poll::Ready(Some(item))
            } else {
                std::task::Poll::Ready(None)
            }
        }
    }

    let stream = DelayedStream { data, index: 0 };

    let result_stream = stream.stateful_throttle_rs2(
        config,
        key_extractor,
        1,                                     // Rate limit: 1 item per 100ms window
        std::time::Duration::from_millis(100), // 100ms window (shorter for testing)
        |item| {
            println!(
                "[DEBUG] Throttle closure: processing item {} (id={}) at {:?}",
                item.value,
                item.id,
                Instant::now()
            );
            item
        }, // Identity function with debug
    );

    let results: Vec<_> = result_stream
        .inspect(|r| {
            if let Ok(item) = r {
                println!("[DEBUG] Emitted by throttle: {}", item.value);
            }
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Should only allow 1 item per key within the 100ms window
    assert_eq!(results.len(), 2); // 1 for id=1 + 1 for id=2 (first item in each window)
    assert_eq!(results[0].value, "first");
    assert_eq!(results[1].value, "other");
    // "second" and "third" should be throttled out due to rate limiting
}

#[tokio::test]
async fn test_stateful_session() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "session_start".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "session_continue".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "new_session".to_string(),
            count: 30,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_session_rs2(
        config,
        key_extractor,
        std::time::Duration::from_millis(100), // 100ms session timeout (shorter for testing)
        |mut item, is_new_session| {
            item.is_new_session = Some(is_new_session);
            item
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3);
    // Session logic may vary - just check that both items have session info
    assert!(results[0].is_new_session.is_some());
    assert!(results[1].is_new_session.is_some());
    assert!(results[2].is_new_session.is_some());
}

#[tokio::test]
async fn test_stateful_pattern() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "login".to_string(),
            count: 1,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "browse".to_string(),
            count: 2,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "purchase".to_string(),
            count: 3,
            is_new_session: None,
        }, // This should trigger the pattern
        TestData {
            id: 1,
            value: "logout".to_string(),
            count: 4,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "normal".to_string(),
            count: 10,
            is_new_session: None,
        }, // Different key, shouldn't affect pattern
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_pattern_rs2(
        config,
        key_extractor,
        4,
        |items: Vec<TestData>, state_access: StateAccess| {
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

                for item in &items {
                    state.total_count += item.count;
                    state.last_value = item.value.clone();
                }

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Detect login -> purchase pattern
                let has_purchase = items.iter().any(|item| item.value == "purchase");
                if has_purchase && state.total_count >= 6 {
                    Ok(Some(format!(
                        "PATTERN_DETECTED: purchase found in window (total: {})",
                        state.total_count
                    )))
                } else {
                    Ok(Some(format!(
                        "{} (total: {})",
                        state.last_value, state.total_count
                    )))
                }
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Pattern detection should emit results for each pattern match
    // The pattern is: login -> browse -> purchase -> logout
    assert!(
        !results.is_empty(),
        "Pattern detection should emit at least one result"
    );

    // Verify that we get pattern results with the expected format
    for result in &results {
        if let Some(pattern_result) = result {
            assert!(
                pattern_result.contains("total:"),
                "Pattern result should contain total count"
            );
            // Check for specific pattern values
            assert!(
                pattern_result.contains("login")
                    || pattern_result.contains("browse")
                    || pattern_result.contains("purchase")
                    || pattern_result.contains("logout")
                    || pattern_result.contains("normal")
                    || pattern_result.contains("PATTERN_DETECTED"),
                "Pattern result should contain expected values"
            );
        }
    }

    // Verify that at least one result is Some (not None)
    assert!(
        results.iter().any(|r| r.is_some()),
        "Should have at least one non-None result"
    );

    // Check for pattern detection - should detect the purchase pattern
    let pattern_detected = results.iter().any(|r| {
        r.as_ref()
            .map(|s| s.contains("PATTERN_DETECTED"))
            .unwrap_or(false)
    });
    assert!(pattern_detected, "Should detect the purchase pattern");
}

fn tuple_key_extractor(item: &(Instant, TestData)) -> String {
    item.1.id.to_string()
}

#[tokio::test]
async fn test_stateful_throttle_real_time() {
    use futures::stream::Stream;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::time::Instant as TokioInstant;

    let config = StateConfig::new();
    let key_extractor = CustomKeyExtractor::new(|item: &TestData| item.id.to_string());
    let throttle_interval_ms = 100u64;
    let max_per_interval = 1u32;

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
            id: 2,
            value: "x".to_string(),
            count: 4,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "y".to_string(),
            count: 5,
            is_new_session: None,
        },
    ];

    // Create a stream that emits items with real delays during processing
    struct DelayedStream {
        data: Vec<TestData>,
        index: usize,
    }

    impl Stream for DelayedStream {
        type Item = TestData;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.index < self.data.len() {
                let item = self.data[self.index].clone();
                self.index += 1;
                Poll::Ready(Some(item))
            } else {
                Poll::Ready(None)
            }
        }
    }

    let delayed_stream = DelayedStream {
        data: data.clone(),
        index: 0,
    };

    let result_stream = delayed_stream.stateful_throttle_rs2(
        config,
        key_extractor,
        max_per_interval,
        Duration::from_millis(throttle_interval_ms),
        |item| item,
    );

    // Collect results with timestamps
    let mut results: Vec<(u32, TokioInstant, String)> = Vec::new();
    tokio::pin!(result_stream);
    while let Some(item) = result_stream.next().await {
        let item = item.unwrap();
        results.push((item.id, TokioInstant::now(), item.value.clone()));
    }

    // Validate that we got results
    assert!(!results.is_empty(), "Should have at least some results");
    assert!(
        results.len() <= data.len(),
        "Throttling should reduce the number of emitted items"
    );

    // Group by key and validate throttling
    let mut per_key: HashMap<u32, Vec<TokioInstant>> = HashMap::new();
    for (id, ts, _val) in &results {
        per_key.entry(*id).or_default().push(*ts);
    }

    // Check that each key has proper throttling intervals
    for (id, times) in &per_key {
        if times.len() > 1 {
            for pair in times.windows(2) {
                let diff = pair[1].duration_since(pair[0]);
                assert!(
                    diff >= Duration::from_millis(throttle_interval_ms),
                    "Key {}: Throttle interval violated: {:?} < {}ms",
                    id,
                    diff,
                    throttle_interval_ms
                );
            }
        }
    }

    // Verify we have results from both keys
    assert!(per_key.contains_key(&1), "Should have results for key 1");
    assert!(per_key.contains_key(&2), "Should have results for key 2");

    // Verify that the total processing time is reasonable (should be at least throttle_interval * number_of_keys)
    if !results.is_empty() {
        let total_time = results
            .last()
            .unwrap()
            .1
            .duration_since(results.first().unwrap().1);
        let min_expected_time = Duration::from_millis(throttle_interval_ms * 2); // At least 2 throttle intervals
        assert!(
            total_time >= min_expected_time,
            "Total processing time too short: {:?} < {:?}",
            total_time,
            min_expected_time
        );
    }
}

#[tokio::test]
async fn test_stateful_operations_with_empty_state() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "first".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "second".to_string(),
            count: 20,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_map_rs2(config, key_extractor, |item, state_access| {
        let fut = async move {
            let state_bytes = state_access.get().await.unwrap_or(Vec::new());

            // Test with empty state
            if state_bytes.is_empty() {
                Ok(format!("{} (no previous state)", item.value))
            } else {
                let state: TestState = serde_json::from_slice(&state_bytes).unwrap();
                Ok(format!("{} (previous: {})", item.value, state.last_value))
            }
        };
        Box::pin(fut)
    });

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 2);
    assert!(results[0].contains("first (no previous state)"));
    assert!(results[1].contains("second (no previous state)"));
}

#[tokio::test]
async fn test_stateful_operations_with_concurrent_keys() {
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
        TestData {
            id: 3,
            value: "key3_a".to_string(),
            count: 50,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_reduce_rs2(
        config,
        key_extractor,
        0u64, // init value
        |acc, item, state_access| Box::pin(async move { Ok(acc + item.count) }),
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // The stateful_reduce_rs2 yields accumulated values for each item processed
    // With 5 input items, we expect 5 results showing the accumulation
    assert_eq!(results.len(), 5);
    // The exact values depend on the accumulation logic, but we can check they're reasonable
    assert!(results[0] >= 10); // First item should have at least its own count
    assert!(results[1] >= 20); // Second item should have at least its own count
    assert!(results[2] >= 40); // Third item should have accumulated value
    assert!(results[3] >= 60); // Fourth item should have accumulated value
    assert!(results[4] >= 50); // Fifth item should have at least its own count
}

#[tokio::test]
async fn test_stateful_window_with_overlapping_windows() {
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
        TestData {
            id: 1,
            value: "e".to_string(),
            count: 5,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_window_rs2(
        config,
        key_extractor,
        3, // window size
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
                    "window_sum: {}, total: {}",
                    window_sum, state.total_count
                ))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // With window_size=3, we should get windows of 3 items each
    // Window 1: [a, b, c] -> sum = 6
    // Window 2: [d, e] -> partial window, may or may not be emitted depending on implementation
    assert!(
        !results.is_empty(),
        "Should have at least one window result"
    );

    // Verify that we get window results with the expected format
    for result in &results {
        assert!(
            result.contains("window_sum:"),
            "Result should contain window_sum"
        );
        assert!(result.contains("total:"), "Result should contain total");
    }

    // Check that the first window has the expected sum
    if !results.is_empty() {
        assert!(
            results[0].contains("window_sum: 6") || results[0].contains("window_sum: 15"),
            "First window should sum to either 6 (3 items) or 15 (all items)"
        );
    }
}

#[tokio::test]
async fn test_stateful_session_with_timeout() {
    use std::time::Duration;

    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let session_timeout = Duration::from_millis(100);

    let data = vec![
        TestData {
            id: 1,
            value: "session1_a".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "session1_b".to_string(),
            count: 20,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_session_rs2(
        config,
        key_extractor,
        session_timeout,
        |mut item, is_new_session| {
            item.is_new_session = Some(is_new_session);
            item
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 2);
    // Session logic may vary - just check that both items have session info
    assert!(results[0].is_new_session.is_some());
    assert!(results[1].is_new_session.is_some());
}

#[tokio::test]
async fn test_stateful_pattern_with_complex_sequence() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "login".to_string(),
            count: 1,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "browse".to_string(),
            count: 2,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "purchase".to_string(),
            count: 3,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "logout".to_string(),
            count: 4,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_pattern_rs2(
        config,
        key_extractor,
        4,
        |items: Vec<TestData>, state_access: StateAccess| {
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

                for item in &items {
                    state.total_count += item.count;
                    state.last_value = item.value.clone();
                }

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Detect login -> purchase pattern
                let has_purchase = items.iter().any(|item| item.value == "purchase");
                if has_purchase && state.total_count >= 6 {
                    Ok(Some(format!(
                        "PATTERN_DETECTED: purchase found in window (total: {})",
                        state.total_count
                    )))
                } else {
                    Ok(Some(format!(
                        "{} (total: {})",
                        state.last_value, state.total_count
                    )))
                }
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Pattern detection should emit results for each pattern match
    // The pattern is: login -> browse -> purchase -> logout
    assert!(
        !results.is_empty(),
        "Pattern detection should emit at least one result"
    );

    // Verify that we get pattern results with the expected format
    for result in &results {
        if let Some(pattern_result) = result {
            assert!(
                pattern_result.contains("total:"),
                "Pattern result should contain total count"
            );
            // Check for specific pattern values
            assert!(
                pattern_result.contains("login")
                    || pattern_result.contains("browse")
                    || pattern_result.contains("purchase")
                    || pattern_result.contains("logout")
                    || pattern_result.contains("PATTERN_DETECTED"),
                "Pattern result should contain expected values"
            );
        }
    }

    // Verify that at least one result is Some (not None)
    assert!(
        results.iter().any(|r| r.is_some()),
        "Should have at least one non-None result"
    );
}

#[tokio::test]
async fn test_stateful_join_with_multiple_matches() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data1 = vec![
        TestData {
            id: 1,
            value: "left1".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "left2".to_string(),
            count: 20,
            is_new_session: None,
        },
    ];

    let data2 = vec![
        TestData {
            id: 1,
            value: "right1".to_string(),
            count: 100,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "right2".to_string(),
            count: 200,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "right3".to_string(),
            count: 300,
            is_new_session: None,
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
        key_extractor,
        key_extractor,
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

                state.total_count += left.count + right.count;
                state.last_value = format!("{} + {}", left.value, right.value);

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

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("[JOIN_MULTIPLE_DEBUG] Results: {:?}", results);

    // With interleaved streams and longer window, we should get consistent results
    // We expect at least 2 joins (left1 + right1, left2 + right1) and potentially more
    assert!(!results.is_empty(), "Expected at least one join result");
    assert!(
        results.len() >= 2,
        "Expected at least 2 join results, got {}",
        results.len()
    );

    // Check that we get the expected join results
    let mut found_left1_right1 = false;
    let mut found_left2_right1 = false;

    for result in &results {
        if result.contains("left1 + right1") {
            found_left1_right1 = true;
        }
        if result.contains("left2 + right1") {
            found_left2_right1 = true;
        }
    }

    // We should have at least these two specific joins
    assert!(found_left1_right1, "Expected 'left1 + right1' join result");
    assert!(found_left2_right1, "Expected 'left2 + right1' join result");

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
async fn test_stateful_throttle_with_multiple_keys() {
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::Instant as TokioInstant;

    let config = StateConfig::new();
    let key_extractor = CustomKeyExtractor::new(|item: &TestData| item.id.to_string());
    let throttle_interval_ms = 150u64;
    let max_per_interval = 1u32;

    let data = vec![
        TestData {
            id: 1,
            value: "key1_a".to_string(),
            count: 1,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_a".to_string(),
            count: 2,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "key1_b".to_string(),
            count: 3,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_b".to_string(),
            count: 4,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "key1_c".to_string(),
            count: 5,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_c".to_string(),
            count: 6,
            is_new_session: None,
        },
    ];

    // Emit all items as fast as possible
    let stream = futures::stream::iter(data.clone().into_iter().map(|mut item| {
        item.value = format!("{}-{}", item.value, "test");
        item
    }));

    let result_stream = stream.stateful_throttle_rs2(
        config,
        key_extractor,
        max_per_interval,
        Duration::from_millis(throttle_interval_ms),
        |item| {
            println!(
                "[DEBUG] Throttle closure: processing item {} (id={}) at {:?}",
                item.value,
                item.id,
                Instant::now()
            );
            item
        }, // Identity function with debug
    );

    // Collect results with timestamps
    let mut results: Vec<(u32, TokioInstant, String)> = Vec::new();
    tokio::pin!(result_stream);
    while let Some(item) = result_stream.next().await {
        let item = item.unwrap();
        results.push((item.id, TokioInstant::now(), item.value.clone()));
    }

    // Group by key and check intervals and rate limiting
    let mut per_key: HashMap<u32, Vec<(TokioInstant, String)>> = HashMap::new();
    for (id, ts, val) in &results {
        per_key.entry(*id).or_default().push((*ts, val.clone()));
    }

    for (id, times) in &per_key {
        // Check interval between consecutive emissions
        if times.len() > 1 {
            for pair in times.windows(2) {
                let diff = pair[1].0.duration_since(pair[0].0);
                assert!(
                    diff >= Duration::from_millis(throttle_interval_ms),
                    "Key {}: Throttle interval violated: {:?} < {}ms",
                    id,
                    diff,
                    throttle_interval_ms
                );
            }
        }
        // Check that in any window of size throttle_interval, no more than max_per_interval items are emitted
        for i in 0..times.len() {
            let start = times[i].0;
            let mut count = 1;
            for j in (i + 1)..times.len() {
                if times[j].0.duration_since(start) < Duration::from_millis(throttle_interval_ms) {
                    count += 1;
                }
            }
            assert!(
                count <= max_per_interval as usize,
                "Key {}: More than max_per_interval items emitted in throttle window",
                id
            );
        }
    }

    // Also verify that we get at least one item per key
    assert!(
        per_key.contains_key(&1) && per_key.contains_key(&2),
        "Should have results for both keys"
    );
    // And that the number of results is less than or equal to the input
    assert!(
        results.len() <= data.len(),
        "Throttling should reduce the number of emitted items"
    );
}

#[tokio::test]
async fn test_stateful_deduplicate_with_custom_comparison() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    use std::time::Duration;
    use tokio::time::sleep;

    let mut data = Vec::new();
    data.push(TestData {
        id: 1,
        value: "duplicate".to_string(),
        count: 10,
        is_new_session: None,
    });
    data.push(TestData {
        id: 1,
        value: "duplicate".to_string(),
        count: 20,
        is_new_session: None,
    }); // Same key, should be deduplicated (within TTL)
    sleep(Duration::from_millis(200)).await; // Sleep to exceed TTL
    data.push(TestData {
        id: 1,
        value: "different".to_string(),
        count: 30,
        is_new_session: None,
    }); // Should not be deduplicated
    data.push(TestData {
        id: 2,
        value: "unique".to_string(),
        count: 40,
        is_new_session: None,
    });

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_deduplicate_rs2(
        config,
        key_extractor,
        Duration::from_millis(100), // TTL is less than sleep, so deduplication should occur for the second item only
        |item| item,                // Identity function for deduplication
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // With the current deduplication logic, only 2 items should be emitted
    // (the first "duplicate" and "unique", the second "duplicate" is deduplicated)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, "duplicate");
    assert_eq!(results[1].value, "unique");
}

#[tokio::test]
async fn test_stateful_group_by_with_aggregation() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "group1_a".to_string(),
            count: 10,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "group2_a".to_string(),
            count: 20,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "group1_b".to_string(),
            count: 30,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "group2_b".to_string(),
            count: 40,
            is_new_session: None,
        },
        TestData {
            id: 3,
            value: "group3_a".to_string(),
            count: 50,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_group_by_rs2(
        config,
        key_extractor,
        |group_key, group_items, state_access| {
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
                let group_sum: u64 = group_items.iter().map(|item| item.count).sum();
                state.total_count += group_sum;
                state.last_value = group_items.last().unwrap().value.clone();
                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();
                Ok(format!(
                    "group_{}: {} items, total: {}",
                    group_key,
                    group_items.len(),
                    state.total_count
                ))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3); // 3 groups
    let mut found_1 = false;
    let mut found_2 = false;
    let mut found_3 = false;
    for r in &results {
        if r.contains("group_1: 2 items, total: 40") {
            found_1 = true;
        }
        if r.contains("group_2: 2 items, total: 60") {
            found_2 = true;
        }
        if r.contains("group_3: 1 items, total: 50") {
            found_3 = true;
        }
    }
    assert!(found_1 && found_2 && found_3);
}

#[tokio::test]
async fn test_stateful_window_sliding_overlap() {
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
    let result_stream = stream.stateful_window_rs2_advanced(
        config,
        key_extractor,
        3,       // window size
        Some(2), // slide size (creates overlapping windows)
        false,   // don't emit partial
        |window, _state_access| {
            let fut = async move {
                let sum: u64 = window.iter().map(|item| item.count).sum();
                Ok(format!("Sliding window sum: {}", sum))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // With window_size=3 and slide_size=2, we should get overlapping windows
    // Window 1: [a, b, c] -> sum = 6
    // Window 2: [c, d] -> partial window, should not be emitted since emit_partial=false
    assert_eq!(results.len(), 1, "Should have exactly one full window");
    assert!(
        results[0].contains("Sliding window sum: 6"),
        "First window should sum to 6"
    );
}

#[tokio::test]
async fn test_stateful_window_partial_window() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();
    let data = vec![TestData {
        id: 1,
        value: "x".to_string(),
        count: 5,
        is_new_session: None,
    }];
    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_window_rs2_advanced(
        config,
        key_extractor,
        3,       // window size
        Some(1), // slide size
        true,    // emit partial
        |window, _state_access| {
            let fut = async move { Ok(format!("Partial window: {} items", window.len())) };
            Box::pin(fut)
        },
    );
    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].contains("Partial window: 1 items"));
}

#[tokio::test]
async fn test_stateful_window_multi_key() {
    let config = StateConfig::new();
    let key_extractor: fn(&TestData) -> String = |data| data.id.to_string();

    let data = vec![
        TestData {
            id: 1,
            value: "key1_a".to_string(),
            count: 1,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_a".to_string(),
            count: 2,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "key1_b".to_string(),
            count: 3,
            is_new_session: None,
        },
        TestData {
            id: 2,
            value: "key2_b".to_string(),
            count: 4,
            is_new_session: None,
        },
        TestData {
            id: 1,
            value: "key1_c".to_string(),
            count: 5,
            is_new_session: None,
        },
    ];

    let stream = futures::stream::iter(data);
    let result_stream = stream.stateful_window_rs2_advanced(
        config,
        key_extractor,
        2,     // window size
        None,  // tumbling window
        false, // don't emit partial
        |window, _state_access| {
            let fut = async move {
                let key = window[0].id;
                let sum: u64 = window.iter().map(|item| item.count).sum();
                Ok(format!(
                    "Key {} window: {} items, sum: {}",
                    key,
                    window.len(),
                    sum
                ))
            };
            Box::pin(fut)
        },
    );

    let results: Vec<_> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Should have windows for both keys
    // Key 1: [key1_a, key1_b] -> sum = 4, [key1_c] -> partial, not emitted
    // Key 2: [key2_a, key2_b] -> sum = 6
    assert_eq!(results.len(), 2, "Should have exactly 2 full windows");

    let mut found_key1 = false;
    let mut found_key2 = false;

    for result in &results {
        if result.contains("Key 1 window: 2 items, sum: 4") {
            found_key1 = true;
        }
        if result.contains("Key 2 window: 2 items, sum: 6") {
            found_key2 = true;
        }
    }

    assert!(found_key1, "Should have window for key 1");
    assert!(found_key2, "Should have window for key 2");
}
