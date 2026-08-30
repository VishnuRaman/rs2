use async_stream::stream;
use futures_core::Stream;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::state::traits::KeyExtractor;
use crate::state::{StateConfig, StateError, StateStorage};

// Memory management constants
const MAX_HASHMAP_KEYS: usize = 10_000;
const MAX_GROUP_SIZE: usize = 10_000; // Max items per group
const MAX_PATTERN_SIZE: usize = 1_000; // Max items per pattern
const CLEANUP_INTERVAL: u64 = 1000; // Cleanup every 1000 items (increased from 100)
const DEFAULT_BUFFER_SIZE: usize = 1024;

/// Tracks key recency so eviction can drop the least-recently-*used* key.
///
/// History: the original sorted key names and dropped the lexicographically
/// smallest, evicting arbitrary live keys and silently resetting their
/// accumulators. A FIFO replacement bounded memory correctly but still evicted a
/// long-lived hot key ahead of a recently-created idle one. This is LRU.
///
/// `touch` runs on every item, so it is kept allocation-free for keys already
/// present — only a first sighting allocates. Finding victims is an O(n) select
/// rather than a sorted index, which costs nothing in the common case because
/// eviction only runs every `CLEANUP_INTERVAL` items and only above the cap.
#[derive(Default)]
struct KeyRecency {
    tick: u64,
    last_seen: HashMap<String, u64>,
}

impl KeyRecency {
    fn new() -> Self {
        Self::default()
    }

    /// Record a sighting of `key`, making it the most recently used.
    fn touch(&mut self, key: &str) {
        self.tick += 1;
        match self.last_seen.get_mut(key) {
            Some(slot) => *slot = self.tick,
            None => {
                self.last_seen.insert(key.to_string(), self.tick);
            }
        }
    }

    /// Forget `key` entirely (it was removed from the map by other means).
    fn forget(&mut self, key: &str) {
        self.last_seen.remove(key);
    }

    /// The `count` least-recently-used keys.
    fn least_recent(&self, count: usize) -> Vec<String> {
        if count == 0 {
            return Vec::new();
        }
        let mut entries: Vec<(u64, &String)> =
            self.last_seen.iter().map(|(k, t)| (*t, k)).collect();
        if count >= entries.len() {
            return entries.into_iter().map(|(_, k)| k.clone()).collect();
        }
        entries.select_nth_unstable_by_key(count, |(t, _)| *t);
        entries[..count].iter().map(|(_, k)| (*k).clone()).collect()
    }
}

/// Evict least-recently-used keys until `map` holds at most `max_keys` entries.
///
/// Callers that remove keys by other means should call [`KeyRecency::forget`],
/// but the index tolerates stale entries: if a pass selects only tombstones and
/// the map is still over the cap, it prunes and retries once. That keeps the
/// common path free of the O(n) prune.
///
/// Returns the number of entries actually removed.
fn evict_oldest_entries<V>(
    map: &mut HashMap<String, V>,
    recency: &mut KeyRecency,
    max_keys: usize,
) -> usize {
    let mut removed = 0;

    for attempt in 0..2 {
        if map.len() <= max_keys {
            break;
        }
        if attempt == 1 {
            // First pass did not free enough — the index holds tombstones.
            recency.last_seen.retain(|k, _| map.contains_key(k));
        }
        let excess = map.len() - max_keys;
        for key in recency.least_recent(excess) {
            if map.remove(&key).is_some() {
                removed += 1;
            }
            recency.forget(&key);
        }
    }

    // Keep the index from outgrowing the map when callers drop keys silently.
    if recency.last_seen.len() > map.len().saturating_mul(2).saturating_add(64) {
        recency.last_seen.retain(|k, _| map.contains_key(k));
    }

    removed
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

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);
            let mut state: HashMap<String, ()> = HashMap::new();
            let mut state_order = KeyRecency::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup and resource tracking
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    evict_oldest_entries(&mut state, &mut state_order, MAX_HASHMAP_KEYS);
                }

                state_order.touch(&key);
                state.entry(key.clone()).or_insert(());

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
            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // No per-key bookkeeping here: the filter's state lives in
                // `storage`, keyed per item. A `seen_keys` set used to be
                // maintained purely to feed a resource counter that no longer
                // exists, costing a String clone and a hash insert per item.

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
            let mut accumulator_order = KeyRecency::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    evict_oldest_entries(&mut accumulators, &mut accumulator_order, MAX_HASHMAP_KEYS);
                }

                accumulator_order.touch(&key);
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
            let mut accumulator_order = KeyRecency::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    evict_oldest_entries(&mut accumulators, &mut accumulator_order, MAX_HASHMAP_KEYS);
                }

                accumulator_order.touch(&key);
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
            let mut group_order = KeyRecency::new();
            let mut last_key: Option<String> = None;
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let now = unix_timestamp_millis();

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    // `groups` and `group_timestamps` share a key space: evict
                    // `groups` by recency, then drop any timestamp left orphaned.
                    evict_oldest_entries(&mut groups, &mut group_order, MAX_HASHMAP_KEYS);
                    group_timestamps.retain(|k, _| groups.contains_key(k));
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
                    if now.saturating_sub(group_start) > timeout {
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
                group_order.touch(&key);
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
                    if now.saturating_sub(group_start) > timeout {
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

                if now.saturating_sub(last_seen) > ttl_ms {
                    
                    
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

        Box::pin(stream! {
            let stream = self;
            futures::pin_mut!(stream);

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);
                let state_access = StateAccess::new(storage.clone(), key.clone());

                let now = unix_timestamp_millis();
                let (mut throttle_state, _is_new) = load_throttle_state(&state_access, now).await;

                if now.saturating_sub(throttle_state.window_start) >= window_ms {
                    throttle_state.count = 0;
                    throttle_state.window_start = now;
                }

                let admit = throttle_state.count < rate_limit;
                if admit {
                    throttle_state.count += 1;
                } else {
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

                let is_new_session = now.saturating_sub(state.last_activity) > timeout_ms;
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
            let mut pattern_order = KeyRecency::new();
            let mut item_count = 0u64;

            while let Some(item) = StreamExt::next(&mut stream).await {
                let key = key_extractor.extract_key(&item);

                // Periodic cleanup to prevent memory leaks
                item_count += 1;
                if item_count % CLEANUP_INTERVAL == 0 {
                    evict_oldest_entries(&mut patterns, &mut pattern_order, MAX_HASHMAP_KEYS);
                }

                pattern_order.touch(&key);
                let pattern = patterns.entry(key.clone()).or_insert_with(Vec::new);
                pattern.push(item);

                // Limit pattern buffer size to prevent memory overflow
                if pattern.len() > MAX_PATTERN_SIZE {
                    let drained = pattern.len() - MAX_PATTERN_SIZE;
                    pattern.drain(0..drained);
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
            let mut left_order = KeyRecency::new();
            let mut right_order = KeyRecency::new();
            let window_ms = window_duration.as_millis() as u64;
            let mut item_count = 0u64;

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
                                evict_oldest_entries(&mut left_buffer, &mut left_order, MAX_HASHMAP_KEYS);
                                evict_oldest_entries(&mut right_buffer, &mut right_order, MAX_HASHMAP_KEYS);
                            }

                            // Single lookup: check newness before `entry` inserts the key.
                            left_order.touch(&key);
                            let left_buf = left_buffer.entry(key.clone()).or_default();

                            // Drop left items that have fallen out of the window.
                            // `saturating_sub` because SystemTime is not monotonic.
                            let _before = left_buf.len();
                            left_buf.retain(|x| now.saturating_sub(x.timestamp) <= window_ms);
                            let _after = left_buf.len();

                            // Add new left item
                            left_buf.push(LeftItemWithTime { item: item.clone(), timestamp: now, key: key.clone() });

                            // Evict oldest if buffer is full
                            let max_size = config.max_size.unwrap_or(DEFAULT_BUFFER_SIZE);
                            if left_buf.len() > max_size {
                                let removed = left_buf.len() - max_size;
                                left_buf.drain(0..removed);
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
                                evict_oldest_entries(&mut left_buffer, &mut left_order, MAX_HASHMAP_KEYS);
                                evict_oldest_entries(&mut right_buffer, &mut right_order, MAX_HASHMAP_KEYS);
                            }

                            // Drop left items that have fallen out of the window.
                            if let Some(left_buf) = left_buffer.get_mut(&key) {
                                let _before = left_buf.len();
                                left_buf.retain(|x| now.saturating_sub(x.timestamp) <= window_ms);
                                let _after = left_buf.len();
                            }

                            // Single lookup: check newness before `entry` inserts the key.
                            right_order.touch(&key);
                            let right_buf = right_buffer.entry(key.clone()).or_default();

                            // Drop right items that have fallen out of the window.
                            let _before = right_buf.len();
                            right_buf.retain(|x| now.saturating_sub(x.timestamp) <= window_ms);
                            let _after = right_buf.len();

                            // Add new right item
                            right_buf.push(RightItemWithTime { item: item.clone(), timestamp: now, key: key.clone() });
                            // Evict oldest if buffer is full
                            let max_size = config.max_size.unwrap_or(DEFAULT_BUFFER_SIZE);
                            if right_buf.len() > max_size {
                                let removed = right_buf.len() - max_size;
                                right_buf.drain(0..removed);
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
                let _is_new_window = !windows.contains_key(&key);
                let window = windows.entry(key.clone()).or_insert_with(Vec::new);

                window.push(item);


                // Emit window when it reaches the required size
                if window.len() >= window_size {
                    let window_items = if slide_size >= window_size {
                        // Tumbling window - take all items and clear the window
                        let items = window.drain(..).collect::<Vec<_>>();
                        items
                    } else {
                        // Sliding window - take window_size items, keep the sliding portion
                        let items = window.drain(..window_size).collect::<Vec<_>>();

                        // Calculate how many items to keep for the next window
                        let keep_count = window_size.saturating_sub(slide_size);
                        if keep_count > 0 && items.len() >= slide_size {
                            // Put back the items that should remain for the sliding window
                            let to_keep = items[slide_size..].to_vec();
                            let _to_keep_len = to_keep.len();
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

#[cfg(test)]
mod evict_tests {
    use super::*;

    fn insert(map: &mut HashMap<String, u32>, order: &mut KeyRecency, key: &str, v: u32) {
        order.touch(key);
        map.insert(key.to_string(), v);
    }

    #[test]
    fn evicts_least_recently_used_not_lexicographically_smallest() {
        let mut map = HashMap::new();
        let mut order = KeyRecency::new();
        // Inserted newest-name-first, so lexicographic order is the reverse of age.
        for (i, k) in ["zzz", "mmm", "aaa"].iter().enumerate() {
            insert(&mut map, &mut order, k, i as u32);
        }

        let removed = evict_oldest_entries(&mut map, &mut order, 2);

        assert_eq!(removed, 1);
        assert!(!map.contains_key("zzz"), "least recently used goes first");
        assert!(map.contains_key("aaa"), "most recent must survive");
        assert!(map.contains_key("mmm"));
    }

    #[test]
    fn a_re_seen_key_survives_eviction() {
        // This is what FIFO got wrong: "old" is inserted first but stays hot,
        // so it must outlive keys created after it.
        let mut map = HashMap::new();
        let mut order = KeyRecency::new();

        insert(&mut map, &mut order, "old", 0);
        insert(&mut map, &mut order, "b", 1);
        insert(&mut map, &mut order, "c", 2);
        // Touch "old" again — it is now the most recently used.
        order.touch("old");

        evict_oldest_entries(&mut map, &mut order, 2);

        assert!(
            map.contains_key("old"),
            "a recently used key was evicted despite being touched last"
        );
        assert!(!map.contains_key("b"), "least recently used should go");
    }

    #[test]
    fn skips_tombstones_when_evicting() {
        let mut map = HashMap::new();
        let mut order = KeyRecency::new();
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
    fn recency_index_stays_bounded_under_churn() {
        // Reproduces the stateful_group_by pattern: a long-lived key plus a
        // stream of short-lived ones removed as their groups are emitted.
        // `forget` must keep the index from growing without bound.
        let mut map: HashMap<String, u32> = HashMap::new();
        let mut order = KeyRecency::new();

        insert(&mut map, &mut order, "long_lived", 0);

        for i in 0..10_000u32 {
            let key = format!("group{}", i);
            insert(&mut map, &mut order, &key, i);
            map.remove(&key);
            order.forget(&key);
            evict_oldest_entries(&mut map, &mut order, 10_000);
        }

        assert!(map.contains_key("long_lived"));
        assert!(
            order.last_seen.len() < 256,
            "recency index grew without bound: {} entries for a map of {}",
            order.last_seen.len(),
            map.len()
        );
    }
}
