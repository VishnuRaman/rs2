use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rs2_stream::state::{
    CustomKeyExtractor, StatefulStreamExt, StateStorage,
};
use rs2_stream::state::config::{StateConfigs, StateConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use futures_util::stream::{self, StreamExt};
use async_trait::async_trait;

// Test data structures
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestEvent {
    id: u64,
    user_id: String,
    value: f64,
    timestamp: u64,
    category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestState {
    count: u64,
    sum: f64,
    last_value: f64,
    window_data: Vec<f64>,
}

// Custom storage backend for benchmarking - using std::sync::Mutex to avoid deadlocks
#[derive(Clone)]
struct BenchmarkStorage {
    data: Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>,
}

impl BenchmarkStorage {
    fn new() -> Self {
        Self {
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl StateStorage for BenchmarkStorage {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.lock().unwrap();
        data.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let data = self.data.lock().unwrap();
        data.contains_key(key)
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.lock().unwrap();
        data.clear();
        Ok(())
    }
}

// Helper functions
fn create_test_events(size: usize) -> Vec<TestEvent> {
    (0..size)
        .map(|i| TestEvent {
            id: i as u64,
            user_id: format!("user_{}", i % 100), // 100 unique users
            value: (i as f64) * 1.5,
            timestamp: i as u64,
            category: format!("cat_{}", i % 10), // 10 categories
        })
        .collect()
}

fn create_high_cardinality_events(size: usize) -> Vec<TestEvent> {
    (0..size)
        .map(|i| TestEvent {
            id: i as u64,
            user_id: format!("user_{}", i), // Each user is unique
            value: (i as f64) * 1.5,
            timestamp: i as u64,
            category: format!("cat_{}", i % 1000), // 1000 categories
        })
        .collect()
}

// ============================================================================
// Basic Stateful Operations Benchmarks
// ============================================================================

fn bench_stateful_map(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("stateful_map");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("basic_map", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_stateful_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("stateful_filter");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("rate_limiting", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_filter_rs2(config, key_extractor, |event, state_access| {
                        let event = event.clone();
                        let state_access = state_access.clone();
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut frequency_data: HashMap<String, u64> = if state_bytes.is_empty() {
                                HashMap::new()
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                            };

                            let current_count = frequency_data.get("count").unwrap_or(&0);
                            let window_start = frequency_data.get("window_start").unwrap_or(&event.timestamp);

                            let window_duration = 300;
                            let new_count = if event.timestamp - window_start > window_duration {
                                1
                            } else {
                                current_count + 1
                            };

                            frequency_data.insert("count".to_string(), new_count);
                            if new_count == 1 {
                                frequency_data.insert("window_start".to_string(), event.timestamp);
                            }

                            let state_bytes = serde_json::to_vec(&frequency_data).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(new_count <= 5) // Allow max 5 events per window
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_stateful_fold(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("stateful_fold");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("accumulation", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_fold_rs2(
                        config,
                        key_extractor,
                        TestState {
                            count: 0,
                            sum: 0.0,
                            last_value: 0.0,
                            window_data: Vec::new(),
                        },
                        |acc, event, state_access| {
                            Box::pin(async move {
                                let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                                let mut state: TestState = if state_bytes.is_empty() {
                                    acc
                                } else {
                                    serde_json::from_slice(&state_bytes).unwrap_or(acc)
                                };

                                state.count += 1;
                                state.sum += event.value;
                                state.last_value = event.value;

                                let state_bytes = serde_json::to_vec(&state).unwrap();
                                state_access.set(&state_bytes).await.unwrap();

                                Ok(state)
                            })
                        },
                    )
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Advanced Stateful Operations Benchmarks
// ============================================================================

fn bench_stateful_window(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("stateful_window");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("tumbling_window", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_window_rs2(config, key_extractor, 100, |window, state_access| {
                        Box::pin(async move {
                            let sum: f64 = window.iter().map(|e| e.value).sum();
                            let count = window.len() as f64;
                            let avg = sum / count;

                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += window.len() as u64;
                            state.sum += sum;
                            state.window_data.push(avg);

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(avg)
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_stateful_join(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("stateful_join");

    for size in [500, 1_000].iter() {
        group.bench_with_input(BenchmarkId::new("stream_join", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let left_events = create_test_events(size);
                let right_events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let left_stream = stream::iter(left_events);
                let right_stream = stream::iter(right_events);

                let result = left_stream
                    .stateful_join_rs2(
                        Box::pin(right_stream),
                        config,
                        key_extractor.clone(),
                        key_extractor,
                        Duration::from_secs(60),
                        |left, right, _state_access| {
                            Box::pin(async move {
                                let _combined_value = left.value + right.value;
                                Ok(format!("joined_{}_{}", left.id, right.id))
                            })
                        },
                    )
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_stateful_group_by(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("stateful_group_by");

    for size in [500, 1_000].iter() {
        group.bench_with_input(BenchmarkId::new("group_aggregation", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_group_by_rs2(config, key_extractor, |key, group, state_access| {
                        Box::pin(async move {
                            let sum: f64 = group.iter().map(|e| e.value).sum();
                            let count = group.len();
                            let avg = sum / count as f64;

                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += count as u64;
                            state.sum += sum;
                            state.window_data.push(avg);

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("group_{}_avg_{}", key, avg))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Storage Backend Benchmarks
// ============================================================================

fn bench_storage_backends(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("storage_backends");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("in_memory", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("custom_storage", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let storage = Arc::new(BenchmarkStorage::new());
                let config = StateConfig::new()
                    .with_custom_storage(storage);
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// State Configuration Benchmarks
// ============================================================================

fn bench_state_configurations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("state_configurations");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("session_config", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("persistent_config", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::long_lived();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("ttl_config", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfig::new()
                    .ttl(Duration::from_secs(300));
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Cardinality Impact Benchmarks
// ============================================================================

fn bench_cardinality_impact(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("cardinality_impact");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("low_cardinality", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size); // 100 unique users
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("high_cardinality", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_high_cardinality_events(size); // Each user is unique
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Specialized Operations Benchmarks (Simplified)
// ============================================================================

fn bench_specialized_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("specialized_operations");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("deduplicate", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_deduplicate_rs2(config, key_extractor, Duration::from_secs(60), |event| event)
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("throttle", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_throttle_rs2(
                        config,
                        key_extractor,
                        100, // 100 events per second
                        Duration::from_secs(1),
                        |event| event,
                    )
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("session", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_session_rs2(
                        config,
                        key_extractor,
                        Duration::from_secs(300),
                        |event, _is_new_session| event,
                    )
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Memory Usage Benchmarks (Simplified)
// ============================================================================

fn bench_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_usage");

    for size in [1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::new("stateful_operations", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let events = create_test_events(size);
                let config = StateConfigs::session();
                let key_extractor = CustomKeyExtractor::new(|event: &TestEvent| event.user_id.clone());

                let stream = stream::iter(events);
                let result = stream
                    .stateful_map_rs2(config, key_extractor, |event, state_access| {
                        Box::pin(async move {
                            let state_bytes = state_access.get().await.unwrap_or(Vec::new());
                            let mut state: TestState = if state_bytes.is_empty() {
                                TestState {
                                    count: 0,
                                    sum: 0.0,
                                    last_value: 0.0,
                                    window_data: Vec::new(),
                                }
                            } else {
                                serde_json::from_slice(&state_bytes).unwrap()
                            };

                            state.count += 1;
                            state.sum += event.value;
                            state.last_value = event.value;

                            // Simulate memory-intensive operation
                            state.window_data.push(event.value);
                            if state.window_data.len() > 1000 {
                                state.window_data.drain(0..state.window_data.len() - 1000);
                            }

                            let state_bytes = serde_json::to_vec(&state).unwrap();
                            state_access.set(&state_bytes).await.unwrap();

                            Ok(format!("processed_{}", event.id))
                        })
                    })
                    .collect::<Vec<_>>()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark Configuration
// ============================================================================

criterion_group!(
    benches,
    bench_stateful_map,
    bench_stateful_filter,
    bench_stateful_fold,
    bench_stateful_window,
    bench_stateful_join,
    bench_stateful_group_by,
    bench_storage_backends,
    bench_state_configurations,
    bench_cardinality_impact,
    bench_specialized_operations,
    bench_memory_usage,
);

criterion_main!(benches); 