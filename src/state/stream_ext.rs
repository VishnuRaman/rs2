use crate::*;
use crate::resource_manager::{get_global_resource_manager, ResourceManager};
use async_stream::stream;
use futures_core::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
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

/// Evict the oldest keys until `map` holds at most `max_keys` entries.
///
/// "Oldest" means first-inserted: `order` records keys in insertion order and is
/// drained from the front. It may contain keys that were already removed by
/// other means (a group being emitted, say); those are skipped.
///
/// This replaces an earlier version that sorted keys lexicographically and
/// dropped the smallest, which evicted arbitrary *live* keys and silently reset
/// their accumulators.
///
/// Returns the number of entries actually removed.
fn evict_oldest_entries<V>(
    map: &mut HashMap<String, V>,
    order: &mut VecDeque<String>,
    max_keys: usize,
) -> usize {
    let mut removed = 0;
    while map.len() > max_keys {
        match order.pop_front() {
            Some(key) => {
                if map.remove(&key).is_some() {
                    removed += 1;
                }
            }
            // Nothing left to evict from; the map is only reachable via `order`,
            // so this should not happen, but never spin.
            None => break,
        }
    }

    // Cheap path: drop leading tombstones.
    while let Some(front) = order.front() {
        if map.contains_key(front) {
            break;
        }
        order.pop_front();
    }

    // Front-pruning alone is not enough. Callers such as `stateful_group_by`
    // remove keys mid-stream when a group is emitted, and a key that reappears
    // is pushed again — so tombstones and duplicates accumulate in the middle
    // while the map itself stays small. Without this rebuild, an alternating
    // key pattern grows `order` by one entry per item indefinitely.
    //
    // Retain keeps the *first* occurrence of each key, which is the correct
    // age for FIFO ordering.
    if order.len() > map.len().saturating_mul(2).saturating_add(64) {
        let mut seen: HashSet<String> = HashSet::with_capacity(map.len());
        order.retain(|k| map.contains_key(k) && seen.insert(k.clone()));
    }

    removed
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

/// Load a key's throttle state, returning `(state, is_new)`.
///
/// A missing or unreadable entry starts a fresh window at `now`.
async fn load_throttle_state(state_access: &StateAccess, now: u64) -> (ThrottleState, bool) {
    let bytes = state_access.get().await.unwrap_or_default();
    if bytes.is_empty() {
        return (
            ThrottleState {
                count: 0,
                window_start: now,
            },
            true,
        );
    }
    match serde_json::from_slice(&bytes) {
        Ok(state) => (state, false),
        Err(_) => (
            ThrottleState {
                count: 0,
                window_start: now,
            },
            true,
        ),
    }
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut state: HashMap<String, ()> = HashMap::new();
            let mut state_order: VecDeque<String> = VecDeque::new();
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup and resource tracking
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    let evicted = evict_oldest_entries(&mut state, &mut state_order, MAX_HASHMAP_KEYS);
                    if evicted > 0 {
                        pending_deallocations += evicted as u64;
                        pending_buffer_overflows += 1;
                    }
                }

                // Batch resource tracking
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                    pending_allocations = 0;
                    pending_deallocations = 0;
                    pending_buffer_overflows = 0;
                }

                let is_new_key = !state.contains_key(&key);
                state.entry(key.clone()).or_insert(());
                if is_new_key {
                    state_order.push_back(key.clone());
                    pending_allocations += 1;
                }

                let state_access = StateAccess::new(storage.clone(), key);
                match f(item, state_access).await {
                    Ok(result) => yield Ok(result),
                    Err(e) => yield Err(e),
                }
            }

            // Final resource tracking
            if pending_allocations > 0 || pending_deallocations > 0 || pending_buffer_overflows > 0 {
                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut seen_keys: HashSet<String> = HashSet::new();
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Optimized cleanup - only when necessary
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 && seen_keys.len() > MAX_HASHMAP_KEYS {
                    // More efficient cleanup - clear all and let it rebuild
                    let old_size = seen_keys.len();
                    seen_keys.clear();
                    pending_allocations = pending_allocations.saturating_sub(old_size as u64);
                }

                // Batch resource tracking
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    if pending_allocations > 0 {
                        resource_manager.track_memory_allocation(pending_allocations).await.ok();
                        pending_allocations = 0;
                    }
                }

                // Optimized key insertion - avoid double lookup
                let is_new_key = seen_keys.insert(key.clone());
                if is_new_key {
                    pending_allocations += 1;
                }

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

            // Final resource tracking
            if pending_allocations > 0 {
                resource_manager.track_memory_allocation(pending_allocations).await.ok();
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut accumulators: HashMap<String, R> = HashMap::new();
            let mut accumulator_order: VecDeque<String> = VecDeque::new();
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    let evicted = evict_oldest_entries(&mut accumulators, &mut accumulator_order, MAX_HASHMAP_KEYS);
                    if evicted > 0 {
                        pending_deallocations += evicted as u64;
                        pending_buffer_overflows += 1;
                    }
                }

                // Batch resource tracking
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                    pending_allocations = 0;
                    pending_deallocations = 0;
                    pending_buffer_overflows = 0;
                }

                let is_new_key = !accumulators.contains_key(&key);
                if is_new_key {
                    accumulator_order.push_back(key.clone());
                    pending_allocations += 1;
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

            // Final resource tracking
            if pending_allocations > 0 || pending_deallocations > 0 || pending_buffer_overflows > 0 {
                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut accumulators: HashMap<String, R> = HashMap::new();
            let mut accumulator_order: VecDeque<String> = VecDeque::new();
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    let evicted = evict_oldest_entries(&mut accumulators, &mut accumulator_order, MAX_HASHMAP_KEYS);
                    if evicted > 0 {
                        pending_deallocations += evicted as u64;
                        pending_buffer_overflows += 1;
                    }
                }

                // Batch resource tracking
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                    pending_allocations = 0;
                    pending_deallocations = 0;
                    pending_buffer_overflows = 0;
                }

                let is_new_key = !accumulators.contains_key(&key);
                if is_new_key {
                    accumulator_order.push_back(key.clone());
                    pending_allocations += 1;
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

            // Final resource tracking
            if pending_allocations > 0 || pending_deallocations > 0 || pending_buffer_overflows > 0 {
                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut groups: HashMap<String, Vec<T>> = HashMap::new();
            let mut group_timestamps: HashMap<String, u64> = HashMap::new();
            let mut group_order: VecDeque<String> = VecDeque::new();
            let mut last_key: Option<String> = None;
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let now = unix_timestamp_millis();

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    // `groups` and `group_timestamps` share a key space, so evict
                    // `groups` by age and keep the timestamps in step.
                    let mut evict_order = group_order.clone();
                    let evicted_groups = evict_oldest_entries(&mut groups, &mut group_order, MAX_HASHMAP_KEYS);
                    let evicted_timestamps = evict_oldest_entries(&mut group_timestamps, &mut evict_order, MAX_HASHMAP_KEYS);
                    if evicted_groups > 0 || evicted_timestamps > 0 {
                        pending_deallocations += (evicted_groups + evicted_timestamps) as u64;
                        pending_buffer_overflows += 1;
                    }
                }

                // Batch resource tracking
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                    pending_allocations = 0;
                    pending_deallocations = 0;
                    pending_buffer_overflows = 0;
                }

                // Check if we need to emit the previous group due to key change
                if emit_on_key_change {
                    if let Some(ref last_key_val) = last_key {
                        if last_key_val != &key {
                            // Key changed, emit the previous group
                            if let Some(group_items) = groups.remove(last_key_val) {
                                pending_deallocations += group_items.len() as u64;
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
                            pending_deallocations += group_items.len() as u64;
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
                let is_new_group = !groups.contains_key(&key);
                if is_new_group {
                    group_order.push_back(key.clone());
                    pending_allocations += 1;
                }
                let group = groups.entry(key.clone()).or_insert_with(Vec::new);
                group_timestamps.entry(key.clone()).or_insert(now);
                group.push(item);
                pending_allocations += 1;

                // Check if we should emit this group due to size limit
                if group.len() >= max_group_size {
                    if let Some(group_items) = groups.remove(&key) {
                        pending_deallocations += group_items.len() as u64;
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
                    pending_deallocations += group_items.len() as u64;
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
                pending_deallocations += group_items.len() as u64;
                let state_access = StateAccess::new(storage.clone(), key.clone());
                match f(key, group_items, state_access).await {
                    Ok(result) => yield Ok(result),
                    Err(e) => yield Err(e),
                }
            }

            // Final resource tracking
            if pending_allocations > 0 || pending_deallocations > 0 || pending_buffer_overflows > 0 {
                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;

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
                    // Track memory allocation for new state entry
                    pending_allocations += 1;
                    
                    // Batch resource tracking
                    item_count += 1;
                    if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                        if pending_allocations > 0 {
                            resource_manager.track_memory_allocation(pending_allocations).await.ok();
                            pending_allocations = 0;
                        }
                    }
                    
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

            // Final resource tracking
            if pending_allocations > 0 {
                resource_manager.track_memory_allocation(pending_allocations).await.ok();
            }
        })
    }

    /// Apply a stateful throttle operation
    ///
    /// # Deprecated
    ///
    /// This previously enforced nothing at all: items over the limit reset the
    /// window and were emitted anyway, so the rate limit was a no-op for any
    /// window of one second or more. It now delegates to
    /// [`stateful_throttle_drop_rs2`], which sheds the excess.
    ///
    /// Call [`stateful_throttle_drop_rs2`] directly instead, so the shedding
    /// behaviour is explicit at the call site.
    #[deprecated(
        since = "0.4.0",
        note = "previously enforced no limit at all; call stateful_throttle_drop_rs2 explicitly"
    )]
    fn stateful_throttle_rs2<F>(
        self,
        config: StateConfig,
        key_extractor: impl KeyExtractor<T> + Send + Sync + 'static,
        rate_limit: u32,
        window_duration: std::time::Duration,
        f: F,
    ) -> Pin<Box<dyn Stream<Item = Result<T, StateError>> + Send>>
    where
        F: FnMut(T) -> T + Send + Sync + 'static,
        Self: Sized,
    {
        self.stateful_throttle_drop_rs2(config, key_extractor, rate_limit, window_duration, f)
    }

    fn stateful_throttle_drop_rs2<F>(
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let state_access = StateAccess::new(storage.clone(), key.clone());

                let now = unix_timestamp_millis();
                let (mut throttle_state, is_new) = load_throttle_state(&state_access, now).await;
                if is_new {
                    pending_allocations += 1;
                }

                if now.saturating_sub(throttle_state.window_start) >= window_ms {
                    throttle_state.count = 0;
                    throttle_state.window_start = now;
                }

                let admit = throttle_state.count < rate_limit;
                if admit {
                    throttle_state.count += 1;
                } else {
                    pending_buffer_overflows += 1;
                }

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

                if admit {
                    yield Ok(f(item));
                }

                item_count += 1;
                if item_count % (RESOURCE_TRACKING_INTERVAL * 2) == 0 {
                    if pending_allocations > 0 {
                        resource_manager.track_memory_allocation(pending_allocations).await.ok();
                        pending_allocations = 0;
                    }
                    for _ in 0..pending_buffer_overflows {
                        resource_manager.track_buffer_overflow().await.ok();
                    }
                    pending_buffer_overflows = 0;
                }
            }

            if pending_allocations > 0 {
                resource_manager.track_memory_allocation(pending_allocations).await.ok();
            }
            for _ in 0..pending_buffer_overflows {
                resource_manager.track_buffer_overflow().await.ok();
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let state_access = StateAccess::new(storage.clone(), key.clone());

                let now = unix_timestamp_millis();
                let state_bytes = match state_access.get().await {
                    Some(bytes) => bytes,
                    None => Vec::new(),
                };

                let mut state: SessionState = if state_bytes.is_empty() {
                    // Track memory allocation for new session state
                    pending_allocations += 1;
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

                // Batch resource tracking
                item_count += 1;
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    if pending_allocations > 0 {
                        resource_manager.track_memory_allocation(pending_allocations).await.ok();
                        pending_allocations = 0;
                    }
                }

                yield Ok(f(item, is_new_session));
            }

            // Final resource tracking
            if pending_allocations > 0 {
                resource_manager.track_memory_allocation(pending_allocations).await.ok();
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut patterns: HashMap<String, Vec<T>> = HashMap::new();
            let mut pattern_order: VecDeque<String> = VecDeque::new();
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    let evicted = evict_oldest_entries(&mut patterns, &mut pattern_order, MAX_HASHMAP_KEYS);
                    if evicted > 0 {
                        pending_deallocations += evicted as u64;
                        pending_buffer_overflows += 1;
                    }
                }

                // Batch resource tracking
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                    pending_allocations = 0;
                    pending_deallocations = 0;
                    pending_buffer_overflows = 0;
                }

                let is_new_pattern = !patterns.contains_key(&key);
                if is_new_pattern {
                    pattern_order.push_back(key.clone());
                    pending_allocations += 1;
                }
                let pattern = patterns.entry(key.clone()).or_insert_with(Vec::new);
                pattern.push(item);
                pending_allocations += 1;

                // Limit pattern buffer size to prevent memory overflow
                if pattern.len() > MAX_PATTERN_SIZE {
                    let drained = pattern.len() - MAX_PATTERN_SIZE;
                    pattern.drain(0..drained);
                    pending_deallocations += drained as u64;
                    pending_buffer_overflows += 1;
                }

                if pattern.len() >= pattern_size {
                    let pattern_items = pattern.drain(..pattern_size).collect::<Vec<_>>();
                    pending_deallocations += pattern_size as u64;
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

            // Final resource tracking
            if pending_allocations > 0 || pending_deallocations > 0 || pending_buffer_overflows > 0 {
                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
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
        let resource_manager = get_global_resource_manager();
        Box::pin(stream! {
            let left_stream = self;
            let right_stream = other;
            futures::pin_mut!(left_stream);
            futures::pin_mut!(right_stream);
            let mut left_buffer: HashMap<String, Vec<LeftItemWithTime<T>>> = HashMap::new();
            let mut right_buffer: HashMap<String, Vec<RightItemWithTime<U>>> = HashMap::new();
            let mut left_order: VecDeque<String> = VecDeque::new();
            let mut right_order: VecDeque<String> = VecDeque::new();
            let window_ms = window_duration.as_millis() as u64;
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;
            let mut pending_buffer_overflows = 0u64;

            // Track each side separately: one input ending must not discard the
            // items still buffered and pending on the other.
            let mut left_done = false;
            let mut right_done = false;

            while !(left_done && right_done) {
                tokio::select! {
                    left_item = left_stream.next(), if !left_done => {
                        if let Some(item) = left_item {
                            let key = key_extractor.extract_key(&item);
                            let now = unix_timestamp_millis();

                            // Periodic cleanup to prevent memory leaks
                            item_count += 1;
                            if item_count % CLEANUP_INTERVAL == 0 {
                                let evicted_left = evict_oldest_entries(&mut left_buffer, &mut left_order, MAX_HASHMAP_KEYS);
                                let evicted_right = evict_oldest_entries(&mut right_buffer, &mut right_order, MAX_HASHMAP_KEYS);
                                if evicted_left > 0 || evicted_right > 0 {
                                    pending_deallocations += (evicted_left + evicted_right) as u64;
                                    pending_buffer_overflows += 1;
                                }
                            }

                            // Batch resource tracking
                            if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                                pending_allocations = 0;
                                pending_deallocations = 0;
                                pending_buffer_overflows = 0;
                            }

                            // Single lookup: check newness before `entry` inserts the key.
                            if !left_buffer.contains_key(&key) {
                                left_order.push_back(key.clone());
                                pending_allocations += 1;
                            }
                            let left_buf = left_buffer.entry(key.clone()).or_default();

                            // Drop left items that have fallen out of the window.
                            // `saturating_sub` because SystemTime is not monotonic.
                            let before = left_buf.len();
                            left_buf.retain(|x| now.saturating_sub(x.timestamp) <= window_ms);
                            let after = left_buf.len();
                            if before > after {
                                pending_deallocations += (before - after) as u64;
                            }

                            // Add new left item
                            left_buf.push(LeftItemWithTime { item: item.clone(), timestamp: now, key: key.clone() });
                            pending_allocations += 1;

                            // Evict oldest if buffer is full
                            let max_size = config.max_size.unwrap_or(DEFAULT_BUFFER_SIZE);
                            if left_buf.len() > max_size {
                                let removed = left_buf.len() - max_size;
                                left_buf.drain(0..removed);
                                pending_deallocations += removed as u64;
                                pending_buffer_overflows += 1;
                            }

                            // Join with right items in window
                            if let Some(rights) = right_buffer.get(&key) {
                                for right in rights.iter().filter(|r| now.saturating_sub(r.timestamp) <= window_ms) {
                                    let state_access = StateAccess::new(storage.clone(), key.clone());
                                    match f(item.clone(), right.item.clone(), state_access).await {
                                        Ok(result) => yield Ok(result),
                                        Err(e) => yield Err(e),
                                    }
                                }
                            }
                        } else {
                            left_done = true;
                        }
                    }
                    right_item = right_stream.next(), if !right_done => {
                        if let Some(item) = right_item {
                            let key = other_key_extractor.extract_key(&item);
                            let now = unix_timestamp_millis();
                            // Periodic cleanup to prevent memory leaks
                            item_count += 1;
                            if item_count % CLEANUP_INTERVAL == 0 {
                                let evicted_left = evict_oldest_entries(&mut left_buffer, &mut left_order, MAX_HASHMAP_KEYS);
                                let evicted_right = evict_oldest_entries(&mut right_buffer, &mut right_order, MAX_HASHMAP_KEYS);
                                if evicted_left > 0 || evicted_right > 0 {
                                    pending_deallocations += (evicted_left + evicted_right) as u64;
                                    pending_buffer_overflows += 1;
                                }
                            }

                            // Batch resource tracking
                            if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
                                pending_allocations = 0;
                                pending_deallocations = 0;
                                pending_buffer_overflows = 0;
                            }

                            // Drop left items that have fallen out of the window.
                            if let Some(left_buf) = left_buffer.get_mut(&key) {
                                let before = left_buf.len();
                                left_buf.retain(|x| now.saturating_sub(x.timestamp) <= window_ms);
                                let after = left_buf.len();
                                if before > after {
                                    pending_deallocations += (before - after) as u64;
                                }
                            }

                            // Single lookup: check newness before `entry` inserts the key.
                            if !right_buffer.contains_key(&key) {
                                right_order.push_back(key.clone());
                                pending_allocations += 1;
                            }
                            let right_buf = right_buffer.entry(key.clone()).or_default();

                            // Drop right items that have fallen out of the window.
                            let before = right_buf.len();
                            right_buf.retain(|x| now.saturating_sub(x.timestamp) <= window_ms);
                            let after = right_buf.len();
                            if before > after {
                                pending_deallocations += (before - after) as u64;
                            }

                            // Add new right item
                            right_buf.push(RightItemWithTime { item: item.clone(), timestamp: now, key: key.clone() });
                            pending_allocations += 1;
                            // Evict oldest if buffer is full
                            let max_size = config.max_size.unwrap_or(DEFAULT_BUFFER_SIZE);
                            if right_buf.len() > max_size {
                                let removed = right_buf.len() - max_size;
                                right_buf.drain(0..removed);
                                pending_deallocations += removed as u64;
                                pending_buffer_overflows += 1;
                            }
                            // Join with left items in window
                            if let Some(lefts) = left_buffer.get(&key) {
                                for left in lefts.iter().filter(|l| now.saturating_sub(l.timestamp) <= window_ms) {
                                    let state_access = StateAccess::new(storage.clone(), key.clone());
                                    match f(left.item.clone(), item.clone(), state_access).await {
                                        Ok(result) => yield Ok(result),
                                        Err(e) => yield Err(e),
                                    }
                                }
                            }
                        } else {
                            right_done = true;
                        }
                    }
                }
            }

            // Final resource tracking
            if pending_allocations > 0 || pending_deallocations > 0 || pending_buffer_overflows > 0 {
                track_resource_batch(&resource_manager, pending_allocations, pending_deallocations, pending_buffer_overflows).await;
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
        let resource_manager = get_global_resource_manager();

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut windows: HashMap<String, Vec<T>> = HashMap::new();
            let mut item_count = 0u64;
            let mut pending_allocations = 0u64;
            let mut pending_deallocations = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let is_new_window = !windows.contains_key(&key);
                let window = windows.entry(key.clone()).or_insert_with(Vec::new);
                if is_new_window {
                    pending_allocations += 1;
                }

                window.push(item);
                pending_allocations += 1;

                // Batch resource tracking
                item_count += 1;
                if item_count % RESOURCE_TRACKING_INTERVAL == 0 {
                    if pending_allocations > 0 {
                        resource_manager.track_memory_allocation(pending_allocations).await.ok();
                        pending_allocations = 0;
                    }
                    if pending_deallocations > 0 {
                        resource_manager.track_memory_deallocation(pending_deallocations).await;
                        pending_deallocations = 0;
                    }
                }

                // Emit window when it reaches the required size
                if window.len() >= window_size {
                    let window_items = if slide_size >= window_size {
                        // Tumbling window - take all items and clear the window
                        let items = window.drain(..).collect::<Vec<_>>();
                        pending_deallocations += items.len() as u64;
                        items
                    } else {
                        // Sliding window - take window_size items, keep the sliding portion
                        let items = window.drain(..window_size).collect::<Vec<_>>();
                        pending_deallocations += window_size as u64;

                        // Calculate how many items to keep for the next window
                        let keep_count = window_size.saturating_sub(slide_size);
                        if keep_count > 0 && items.len() >= slide_size {
                            // Put back the items that should remain for the sliding window
                            let to_keep = items[slide_size..].to_vec();
                            let to_keep_len = to_keep.len();
                            window.splice(0..0, to_keep);
                            pending_allocations += to_keep_len as u64;
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
                        pending_deallocations += window.len() as u64;
                        let state_access = StateAccess::new(storage.clone(), key.clone());
                        match f(window, state_access).await {
                            Ok(result) => yield Ok(result),
                            Err(e) => yield Err(e),
                        }
                    }
                }
            }

            // Final resource tracking
            if pending_allocations > 0 {
                resource_manager.track_memory_allocation(pending_allocations).await.ok();
            }
            if pending_deallocations > 0 {
                resource_manager.track_memory_deallocation(pending_deallocations).await;
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


#[cfg(test)]
mod evict_tests {
    use super::*;

    fn insert(map: &mut HashMap<String, u32>, order: &mut VecDeque<String>, key: &str, v: u32) {
        if !map.contains_key(key) {
            order.push_back(key.to_string());
        }
        map.insert(key.to_string(), v);
    }

    #[test]
    fn evicts_in_insertion_order_not_key_order() {
        let mut map = HashMap::new();
        let mut order = VecDeque::new();
        // Inserted newest-name-first, so lexicographic order is the reverse of age.
        for (i, k) in ["zzz", "mmm", "aaa"].iter().enumerate() {
            insert(&mut map, &mut order, k, i as u32);
        }

        let removed = evict_oldest_entries(&mut map, &mut order, 2);

        assert_eq!(removed, 1);
        assert!(!map.contains_key("zzz"), "oldest insertion should go first");
        assert!(map.contains_key("aaa"), "newest insertion must survive");
        assert!(map.contains_key("mmm"));
    }

    #[test]
    fn skips_tombstones_when_evicting() {
        let mut map = HashMap::new();
        let mut order = VecDeque::new();
        for (i, k) in ["a", "b", "c", "d"].iter().enumerate() {
            insert(&mut map, &mut order, k, i as u32);
        }
        // "a" and "b" removed elsewhere (a group being emitted).
        map.remove("a");
        map.remove("b");

        let removed = evict_oldest_entries(&mut map, &mut order, 1);

        // Only "c" is a real eviction; the tombstones must not be counted.
        assert_eq!(removed, 1);
        assert!(!map.contains_key("c"));
        assert!(map.contains_key("d"));
    }

    #[test]
    fn order_queue_stays_bounded_under_churn() {
        // Reproduces the stateful_group_by pattern. The critical detail is a
        // long-lived key pinned at the FRONT of the queue: front-pruning then
        // stops immediately, and every short-lived key removed behind it
        // becomes a permanent tombstone. The map stays tiny, so the eviction
        // loop never runs either — without the rebuild, `order` grows by one
        // entry per item forever.
        let mut map: HashMap<String, u32> = HashMap::new();
        let mut order: VecDeque<String> = VecDeque::new();

        insert(&mut map, &mut order, "long_lived", 0);

        for i in 0..10_000u32 {
            let key = format!("group{}", i);
            insert(&mut map, &mut order, &key, i);
            // Emit the group: key leaves the map but stays in `order`.
            map.remove(&key);
            evict_oldest_entries(&mut map, &mut order, 10_000);
        }

        assert!(
            map.contains_key("long_lived"),
            "the live key must not be evicted"
        );
        assert!(
            order.len() < 256,
            "order queue grew without bound: {} entries for a map of {}",
            order.len(),
            map.len()
        );
    }

    #[test]
    fn rebuild_preserves_oldest_first_ordering() {
        let mut map: HashMap<String, u32> = HashMap::new();
        let mut order: VecDeque<String> = VecDeque::new();

        // "old" is inserted first and stays live throughout.
        insert(&mut map, &mut order, "old", 0);
        // Churn enough distinct short-lived keys to trigger the rebuild.
        for i in 0..500u32 {
            let k = format!("tmp{}", i);
            insert(&mut map, &mut order, &k, i);
            map.remove(&k);
        }
        insert(&mut map, &mut order, "new", 1);
        evict_oldest_entries(&mut map, &mut order, 10_000);

        // Rebuild must keep both live keys, oldest first.
        let live: Vec<&String> = order.iter().collect();
        assert_eq!(live, vec!["old", "new"], "rebuild lost or reordered keys");

        // And a subsequent eviction must still take "old" before "new".
        evict_oldest_entries(&mut map, &mut order, 1);
        assert!(!map.contains_key("old"));
        assert!(map.contains_key("new"));
    }
}
