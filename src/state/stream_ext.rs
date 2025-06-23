use crate::*;
use crate::resource_manager::{get_global_resource_manager, ResourceManager};
use async_stream::stream;
use futures_core::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use crate::state::traits::KeyExtractor;
use crate::state::{StateConfig, StateError, StateStorage};

// Memory management constants
const MAX_HASHMAP_KEYS: usize = 10_000;
const MAX_GROUP_SIZE: usize = 10_000; // Max items per group
const MAX_PATTERN_SIZE: usize = 1_000; // Max items per pattern
const CLEANUP_INTERVAL: u64 = 1000; // Cleanup every 1000 items (increased from 100)
const RESOURCE_TRACKING_INTERVAL: u64 = 100; // Track resources every 100 items
const DEFAULT_BUFFER_SIZE: usize = 1024;

// Optimized constants for different operation types
const MAP_CLEANUP_INTERVAL: u64 = 10000; // Much less frequent for simple operations
const WINDOW_CLEANUP_INTERVAL: u64 = 5000; // Less frequent for windowing
const JOIN_CLEANUP_INTERVAL: u64 = 500; // More frequent for complex operations
const THROTTLE_CLEANUP_INTERVAL: u64 = 10000; // Very infrequent for timing operations

// LRU eviction helper
fn evict_oldest_entries<K, V>(map: &mut HashMap<K, V>, max_keys: usize)
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Display + std::cmp::Ord,
    V: Clone,
{
    if map.len() > max_keys {
        let mut entries: Vec<_> = map.iter().map(|(k, _)| k.clone()).collect();
        entries.sort(); // Simple eviction strategy - could be improved with proper LRU
        let to_remove = entries.len() - max_keys;
        for key in entries.into_iter().take(to_remove) {
            map.remove(&key);
        }
    }
}

// Optimized resource tracking - batch operations
async fn track_resource_batch(
    resource_manager: &Arc<ResourceManager>,
    allocations: u64,
    deallocations: u64,
    buffer_overflows: u64,
) {
    if allocations > 0 {
        resource_manager.track_memory_allocation(allocations).await.ok();
    }
    if deallocations > 0 {
        resource_manager.track_memory_deallocation(deallocations).await;
    }
    for _ in 0..buffer_overflows {
        resource_manager.track_buffer_overflow().await.ok();
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct ThrottleState {
    count: u32,
    window_start: u64, // UNIX timestamp in milliseconds
}

#[derive(Serialize, Deserialize, Clone)]
struct SessionState {
    last_activity: u64, // UNIX timestamp
    is_new_session: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct LeftItemWithTime<T> {
    item: T,
    timestamp: u64,
    key: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct RightItemWithTime<U> {
    item: U,
    timestamp: u64,
    key: String,
}

/// Extension trait for adding stateful operations to streams
pub trait StatefulStreamExt<T>: Stream<Item = T> + Send + Sync + Sized + Unpin + 'static
where
    Self: 'static,
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Apply a stateful map operation
    fn stateful_map_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut seen_keys: HashSet<String> = HashSet::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Minimal cleanup - only when absolutely necessary
                item_count += 1;
                if item_count % MAP_CLEANUP_INTERVAL == 0 && seen_keys.len() > MAX_HASHMAP_KEYS {
                    seen_keys.clear(); // Simple and fast cleanup
                }

                // Optimized key tracking - single operation
                seen_keys.insert(key.clone());

                let state_access = StateAccess::new(storage.clone(), key);
                match f(item, state_access).await {
                    Ok(result) => yield Ok(result),
                    Err(e) => yield Err(e),
                }
            }
        })
    }

    /// Apply a stateful filter operation
    fn stateful_filter_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<T, StateError>> + Send>>
    where
        F: FnMut(
                &T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<bool, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut seen_keys: HashSet<String> = HashSet::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Optimized cleanup - only when necessary
                item_count += 1;
                if item_count % (CLEANUP_INTERVAL * 2) == 0 && seen_keys.len() > MAX_HASHMAP_KEYS {
                    // More efficient cleanup - clear all and let it rebuild
                    seen_keys.clear();
                }

                // Optimized key insertion - avoid double lookup
                let is_new_key = seen_keys.insert(key.clone());

                let state_access = StateAccess::new(storage.clone(), key);
                match f(&item, state_access).await {
                    Ok(should_emit) => {
                        if should_emit {
                            yield Ok(item);
                        }
                    }
                    Err(e) => yield Err(e),
                }
            }
        })
    }

    /// Apply a stateful fold operation
    fn stateful_fold_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        initial: R,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                R,
                T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut accumulators: HashMap<String, R> = HashMap::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % (CLEANUP_INTERVAL * 2) == 0 {
                    evict_oldest_entries(&mut accumulators, MAX_HASHMAP_KEYS);
                }

                let acc = accumulators.entry(key.clone()).or_insert_with(|| initial.clone());
                let state_access = StateAccess::new(storage.clone(), key);

                match f(acc.clone(), item, state_access).await {
                    Ok(new_acc) => {
                        *acc = new_acc.clone();
                        yield Ok(new_acc);
                    }
                    Err(e) => yield Err(e),
                }
            }
        })
    }

    /// Apply a stateful reduce operation
    fn stateful_reduce_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        initial: R,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                R,
                T,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + Clone + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut accumulators: HashMap<String, R> = HashMap::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % (CLEANUP_INTERVAL * 2) == 0 {
                    evict_oldest_entries(&mut accumulators, MAX_HASHMAP_KEYS);
                }

                let acc = accumulators.entry(key.clone()).or_insert_with(|| initial.clone());
                let state_access = StateAccess::new(storage.clone(), key);

                match f(acc.clone(), item, state_access).await {
                    Ok(new_acc) => {
                        *acc = new_acc.clone();
                        yield Ok(new_acc);
                    }
                    Err(e) => yield Err(e),
                }
            }
        })
    }

    /// Apply a stateful group by operation
    fn stateful_group_by_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                String,
                Vec<T>,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized,
    {
        self.stateful_group_by_advanced_rs2(config, key_extractor, None, None, false, f)
    }

    /// Apply a stateful group by operation with advanced configuration
    fn stateful_group_by_advanced_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        max_group_size: Option<usize>, // Emit when group reaches this size
        group_timeout: Option<std::time::Duration>, // Emit group after this timeout
        emit_on_key_change: bool,      // Emit previous group when key changes
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                String,
                Vec<T>,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();
        let timeout_ms = group_timeout.map(|d| d.as_millis() as u64);
        let max_group_size = max_group_size.unwrap_or(MAX_GROUP_SIZE);

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut groups: HashMap<String, Vec<T>> = HashMap::new();
            let mut group_timestamps: HashMap<String, u64> = HashMap::new();
            let mut last_key: Option<String> = None;
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let now = unix_timestamp_millis();

                // Reduced cleanup frequency for group operations
                item_count += 1;
                if item_count % (CLEANUP_INTERVAL * 2) == 0 {
                    evict_oldest_entries(&mut groups, MAX_HASHMAP_KEYS);
                    evict_oldest_entries(&mut group_timestamps, MAX_HASHMAP_KEYS);
                }

                // Check if we need to emit the previous group due to key change
                if emit_on_key_change {
                    if let Some(ref last_key_val) = last_key {
                        if last_key_val != &key {
                            // Key changed, emit the previous group
                            if let Some(group_items) = groups.remove(last_key_val) {
                                let state_access = StateAccess::new(storage.clone(), last_key_val.clone());
                                match f(last_key_val.clone(), group_items, state_access).await {
                                    Ok(result) => yield Ok(result),
                                    Err(e) => yield Err(e),
                                }
                            }
                            group_timestamps.remove(last_key_val);
                        }
                    }
                }

                // Optimized timeout check - only check current key instead of all groups
                if let (Some(timeout), Some(&group_start)) = (timeout_ms, group_timestamps.get(&key)) {
                    if now - group_start > timeout {
                        if let Some(group_items) = groups.remove(&key) {
                            let state_access = StateAccess::new(storage.clone(), key.clone());
                            match f(key.clone(), group_items, state_access).await {
                                Ok(result) => yield Ok(result),
                                Err(e) => yield Err(e),
                            }
                        }
                        group_timestamps.remove(&key);
                    }
                }

                // Add item to current group
                let group = groups.entry(key.clone()).or_insert_with(Vec::new);
                group_timestamps.entry(key.clone()).or_insert(now);
                group.push(item);

                // Check if we should emit this group due to size limit
                if group.len() >= max_group_size {
                    if let Some(group_items) = groups.remove(&key) {
                        let state_access = StateAccess::new(storage.clone(), key.clone());
                        match f(key.clone(), group_items, state_access).await {
                            Ok(result) => yield Ok(result),
                            Err(e) => yield Err(e),
                        }
                    }
                    group_timestamps.remove(&key);
                }

                last_key = Some(key);
            }

            // Final cleanup - check for any remaining groups that have timed out
            let now = unix_timestamp_millis();
            let mut expired_keys = Vec::new();

            if let Some(timeout) = timeout_ms {
                for (key, &group_start) in &group_timestamps {
                    if now - group_start > timeout {
                        expired_keys.push(key.clone());
                    }
                }
            }

            // Emit expired groups
            for key in expired_keys {
                let key_clone = key.clone();
                if let Some(group_items) = groups.remove(&key_clone) {
                    let state_access = StateAccess::new(storage.clone(), key_clone.clone());
                    match f(key_clone.clone(), group_items, state_access).await {
                        Ok(result) => yield Ok(result),
                        Err(e) => yield Err(e),
                    }
                }
                group_timestamps.remove(&key_clone);
            }

            // Emit any remaining groups at stream end
            for (key, group_items) in groups {
                let state_access = StateAccess::new(storage.clone(), key.clone());
                match f(key, group_items, state_access).await {
                    Ok(result) => yield Ok(result),
                    Err(e) => yield Err(e),
                }
            }
        })
    }

    /// Apply a stateful deduplication operation
    fn stateful_deduplicate_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        ttl: std::time::Duration,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<T, StateError>> + Send>>
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();
        let ttl_ms = ttl.as_millis() as u64;

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let state_access = StateAccess::new(storage.clone(), key.clone());

                let now = unix_timestamp_millis();
                let state_bytes = match state_access.get().await {
                    Some(bytes) => bytes,
                    None => Vec::new(),
                };

                let last_seen: u64 = if state_bytes.is_empty() {
                    0
                } else {
                    match serde_json::from_slice(&state_bytes) {
                        Ok(timestamp) => timestamp,
                        Err(_) => 0,
                    }
                };

                if now - last_seen > ttl_ms {
                    // Handle serialization error gracefully
                    match serde_json::to_vec(&now) {
                        Ok(timestamp_bytes) => {
                            if let Err(e) = state_access.set(&timestamp_bytes).await {
                                yield Err(StateError::Storage(format!("Failed to set state for deduplication: {}", e)));
                                continue;
                            }
                        }
                        Err(e) => {
                            yield Err(StateError::Serialization(e));
                            continue;
                        }
                    }

                    yield Ok(f(item));
                }
            }
        })
    }

    /// Apply a stateful throttle operation
    fn stateful_throttle_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        rate_limit: u32,
        window_duration: std::time::Duration,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<T, StateError>> + Send>>
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();
        let window_ms = window_duration.as_millis() as u64;

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let state_access = StateAccess::new(storage.clone(), key.clone());

                let now = unix_timestamp_millis();

                // Get current throttle state from storage
                let state_bytes = match state_access.get().await {
                    Some(bytes) => bytes,
                    None => Vec::new(),
                };

                let mut throttle_state: ThrottleState = if state_bytes.is_empty() {
                    ThrottleState { count: 0, window_start: now }
                } else {
                    match serde_json::from_slice(&state_bytes) {
                        Ok(state) => state,
                        Err(_) => ThrottleState { count: 0, window_start: now },
                    }
                };

                // If window expired, reset
                if now - throttle_state.window_start > window_ms {
                    throttle_state.count = 0;
                    throttle_state.window_start = now;
                }

                if throttle_state.count < rate_limit {
                    throttle_state.count += 1;

                    // Update state in storage
                    match serde_json::to_vec(&throttle_state) {
                        Ok(state_bytes) => {
                            if let Err(e) = state_access.set(&state_bytes).await {
                                yield Err(StateError::Storage(format!("Failed to set throttle state: {}", e)));
                                continue;
                            }
                        }
                        Err(e) => {
                            yield Err(StateError::Serialization(e));
                            continue;
                        }
                    }

                    yield Ok(f(item));
                } else {
                    // Optimized sleep - calculate remaining time more efficiently
                    let elapsed_ms = now.saturating_sub(throttle_state.window_start);
                    let remaining = if elapsed_ms >= window_ms {
                        Duration::from_millis(0)
                    } else {
                        Duration::from_millis(window_ms - elapsed_ms)
                    };

                    // Only sleep if necessary and for a reasonable duration
                    if remaining > Duration::from_millis(0) && remaining < Duration::from_secs(1) {
                        sleep(remaining).await;
                    }

                    // After sleep, reset window and count
                    let now2 = unix_timestamp_millis();
                    throttle_state.count = 1;
                    throttle_state.window_start = now2;

                    // Update state in storage
                    match serde_json::to_vec(&throttle_state) {
                        Ok(state_bytes) => {
                            if let Err(e) = state_access.set(&state_bytes).await {
                                yield Err(StateError::Storage(format!("Failed to set throttle state: {}", e)));
                                continue;
                            }
                        }
                        Err(e) => {
                            yield Err(StateError::Serialization(e));
                            continue;
                        }
                    }

                    yield Ok(f(item));
                }
            }
        })
    }

    /// Apply a stateful session operation
    fn stateful_session_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        session_timeout: std::time::Duration,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<T, StateError>> + Send>>
    where
        F: FnMut(T, bool) -> T + Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();
        let timeout_ms = session_timeout.as_millis() as u64;

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let state_access = StateAccess::new(storage.clone(), key.clone());

                let now = unix_timestamp_millis();
                let state_bytes = match state_access.get().await {
                    Some(bytes) => bytes,
                    None => Vec::new(),
                };

                let mut state: SessionState = if state_bytes.is_empty() {
                    SessionState { last_activity: now, is_new_session: true }
                } else {
                    match serde_json::from_slice(&state_bytes) {
                        Ok(session_state) => session_state,
                        Err(_) => SessionState { last_activity: now, is_new_session: true },
                    }
                };

                let is_new_session = now - state.last_activity > timeout_ms;
                state.last_activity = now;
                state.is_new_session = is_new_session;

                // Handle serialization and state setting errors gracefully
                match serde_json::to_vec(&state) {
                    Ok(state_bytes) => {
                        if let Err(e) = state_access.set(&state_bytes).await {
                            yield Err(StateError::Storage(format!("Failed to set session state: {}", e)));
                            continue;
                        }
                    }
                    Err(e) => {
                        yield Err(StateError::Serialization(e));
                        continue;
                    }
                }

                yield Ok(f(item, is_new_session));
            }
        })
    }

    /// Apply a stateful pattern operation
    fn stateful_pattern_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        pattern_size: usize,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<Option<String>, StateError>> + Send>>
    where
        F: FnMut(
                Vec<T>,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Option<String>, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut patterns: HashMap<String, Vec<T>> = HashMap::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % (CLEANUP_INTERVAL * 2) == 0 {
                    evict_oldest_entries(&mut patterns, MAX_HASHMAP_KEYS);
                }

                let pattern = patterns.entry(key.clone()).or_insert_with(Vec::new);
                pattern.push(item);

                // Limit pattern buffer size to prevent memory overflow
                if pattern.len() > MAX_PATTERN_SIZE {
                    pattern.drain(0..pattern.len() - MAX_PATTERN_SIZE);
                }

                if pattern.len() >= pattern_size {
                    let pattern_items = pattern.drain(..pattern_size).collect::<Vec<_>>();
                    let state_access = StateAccess::new(storage.clone(), key.clone());
                    match f(pattern_items, state_access).await {
                        Ok(result) => {
                            if let Some(pattern_str) = result {
                                yield Ok(Some(pattern_str));
                            }
                        }
                        Err(e) => yield Err(e),
                    }
                }
            }
        })
    }

    /// Join two streams based on keys with time-based windows (true streaming join)
    fn stateful_join_rs2<U, F, R>(
        self,
        other: Pin<Box<dyn Stream<Item = U> + Send>>,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        other_key_extractor: impl KeyExtractor<U> + Send + Sync + 'static,
        window_duration: std::time::Duration,
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                T,
                U,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        U: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
        R: Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();
        Box::pin(stream! {
            let left_stream = self;
            let right_stream = other;
            futures::pin_mut!(left_stream);
            futures::pin_mut!(right_stream);
            let mut left_buffer: HashMap<String, Vec<LeftItemWithTime<T>>> = HashMap::new();
            let mut right_buffer: HashMap<String, Vec<RightItemWithTime<U>>> = HashMap::new();
            let window_ms = window_duration.as_millis() as u64;
            let mut item_count = 0u64;

            loop {
                tokio::select! {
                    left_item = left_stream.next() => {
                        if let Some(item) = left_item {
                            let key = key_extractor.extract_key(&item);
                            let now = unix_timestamp_millis();

                            // Reduced cleanup frequency for complex operations
                            item_count += 1;
                            if item_count % JOIN_CLEANUP_INTERVAL == 0 {
                                evict_oldest_entries(&mut left_buffer, MAX_HASHMAP_KEYS);
                                evict_oldest_entries(&mut right_buffer, MAX_HASHMAP_KEYS);
                            }

                            // Clean up old left items
                            left_buffer.entry(key.clone()).or_default().retain(|x| now - x.timestamp <= window_ms);

                            // Add new left item
                            let left_entry = LeftItemWithTime { item: item.clone(), timestamp: now, key: key.clone() };
                            left_buffer.entry(key.clone()).or_default().push(left_entry.clone());

                            // Evict oldest if buffer is full
                            let max_size = config.max_size.unwrap_or(DEFAULT_BUFFER_SIZE);
                            let left_buf = left_buffer.get_mut(&key).unwrap();
                            if left_buf.len() > max_size {
                                left_buf.drain(0..left_buf.len() - max_size);
                            }

                            // Join with right items in window
                            if let Some(rights) = right_buffer.get(&key) {
                                for right in rights.iter().filter(|r| now - r.timestamp <= window_ms) {
                                    let state_access = StateAccess::new(storage.clone(), key.clone());
                                    match f(item.clone(), right.item.clone(), state_access).await {
                                        Ok(result) => yield Ok(result),
                                        Err(e) => yield Err(e),
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    right_item = right_stream.next() => {
                        if let Some(item) = right_item {
                            let key = other_key_extractor.extract_key(&item);
                            let now = unix_timestamp_millis();
                            
                            // Reduced cleanup frequency for complex operations
                            item_count += 1;
                            if item_count % JOIN_CLEANUP_INTERVAL == 0 {
                                evict_oldest_entries(&mut left_buffer, MAX_HASHMAP_KEYS);
                                evict_oldest_entries(&mut right_buffer, MAX_HASHMAP_KEYS);
                            }

                            // Clean up old right items
                            right_buffer.entry(key.clone()).or_default().retain(|x| now - x.timestamp <= window_ms);
                            // Clean up old left items
                            left_buffer.entry(key.clone()).or_default().retain(|x| now - x.timestamp <= window_ms);
                            
                            // Add new right item
                            let right_entry = RightItemWithTime { item: item.clone(), timestamp: now, key: key.clone() };
                            right_buffer.entry(key.clone()).or_default().push(right_entry.clone());
                            
                            // Evict oldest if buffer is full
                            let max_size = config.max_size.unwrap_or(DEFAULT_BUFFER_SIZE);
                            let right_buf = right_buffer.get_mut(&key).unwrap();
                            if right_buf.len() > max_size {
                                right_buf.drain(0..right_buf.len() - max_size);
                            }
                            
                            // Join with left items in window
                            if let Some(lefts) = left_buffer.get(&key) {
                                for left in lefts.iter().filter(|l| now - l.timestamp <= window_ms) {
                                    let state_access = StateAccess::new(storage.clone(), key.clone());
                                    match f(left.item.clone(), item.clone(), state_access).await {
                                        Ok(result) => yield Ok(result),
                                        Err(e) => yield Err(e),
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Apply a stateful window operation (tumbling window, no partial emission)
    fn stateful_window_rs2<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_size: usize,
        f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                Vec<T>,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized,
    {
        self.stateful_window_rs2_advanced(config, key_extractor, window_size, None, false, f)
    }

    /// Apply a stateful window operation with sliding window support
    fn stateful_window_rs2_advanced<F, R>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        window_size: usize,
        slide_size: Option<usize>, // None for tumbling, Some(n) for sliding
        emit_partial: bool,        // Whether to emit partial windows at stream end
        mut f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<R, StateError>> + Send>>
    where
        F: FnMut(
                Vec<T>,
                StateAccess,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>,
            > + Send
            + Sync
            + 'static,
        R: Send + Sync + 'static,
        Self: Sized,
    {
        let storage = config.create_storage_arc();
        let slide_size = slide_size.unwrap_or(window_size); // Default to tumbling window

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut windows: HashMap<String, Vec<T>> = HashMap::new();

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let window = windows.entry(key.clone()).or_insert_with(Vec::new);

                window.push(item);

                // Emit window when it reaches the required size
                if window.len() >= window_size {
                    let window_items = if slide_size >= window_size {
                        // Tumbling window - take all items and clear the window
                        window.drain(..).collect::<Vec<_>>()
                    } else {
                        // Sliding window - take window_size items, keep the sliding portion
                        let items = window.drain(..window_size).collect::<Vec<_>>();

                        // Calculate how many items to keep for the next window
                        let keep_count = window_size.saturating_sub(slide_size);
                        if keep_count > 0 && items.len() >= slide_size {
                            // Put back the items that should remain for the sliding window
                            let to_keep = items[slide_size..].to_vec();
                            window.splice(0..0, to_keep);
                        }

                        items
                    };

                    let state_access = StateAccess::new(storage.clone(), key.clone());
                    match f(window_items, state_access).await {
                        Ok(result) => yield Ok(result),
                        Err(e) => yield Err(e),
                    }
                }
            }

            // Emit remaining partial windows if requested
            if emit_partial {
                for (key, window) in windows {
                    if !window.is_empty() {
                        let state_access = StateAccess::new(storage.clone(), key.clone());
                        match f(window, state_access).await {
                            Ok(result) => yield Ok(result),
                            Err(e) => yield Err(e),
                        }
                    }
                }
            }
        })
    }
}

/// State access for managing persistent state
#[derive(Clone)]
pub struct StateAccess {
    storage: Arc<dyn StateStorage + Send + Sync>,
    key: String,
}

impl StateAccess {
    pub fn new(storage: Arc<dyn StateStorage + Send + Sync>, key: String) -> Self {
        Self { storage, key }
    }

    pub async fn get(&self) -> Option<Vec<u8>> {
        self.storage.get(&self.key).await
    }

    pub async fn set(&self, value: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.storage.set(&self.key, value).await
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// Blanket implementation for all streams that meet the trait bounds
impl<T, S> StatefulStreamExt<T> for S
where
    S: Stream<Item = T> + Send + Sync + Unpin + 'static,
    T: Send + Sync + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
{
}

