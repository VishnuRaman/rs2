//! Advanced analytics for RS2 streams
//!
//! Provides time-based windowed aggregations and advanced stream joins for building sophisticated real-time analytics.

use crate::*;
use async_stream::stream;
use futures_core::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

// ================================
// Time-based Windowed Aggregations
// ================================

/// Configuration for time-based windowing
#[derive(Debug, Clone)]
pub struct TimeWindowConfig {
    pub window_size: Duration,
    pub slide_interval: Duration,
    pub watermark_delay: Duration,
    pub allowed_lateness: Duration,
}

impl Default for TimeWindowConfig {
    fn default() -> Self {
        Self {
            window_size: Duration::from_secs(60),
            slide_interval: Duration::from_secs(60),
            watermark_delay: Duration::from_secs(10),
            allowed_lateness: Duration::from_secs(5),
        }
    }
}

/// A time-based window of events
#[derive(Debug)]
pub struct TimeWindow<T> {
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub events: Vec<T>,
}

impl<T> TimeWindow<T> {
    pub fn new(start_time: SystemTime, end_time: SystemTime) -> Self {
        Self {
            start_time,
            end_time,
            events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: T) {
        self.events.push(event);
    }

    pub fn is_complete(&self, watermark: SystemTime) -> bool {
        watermark >= self.end_time
    }
}

/// Create time-based windows from a stream of timestamped events
pub fn window_by_time<T, F>(
    stream: RS2Stream<T>,
    config: TimeWindowConfig,
    timestamp_fn: F,
) -> RS2Stream<TimeWindow<T>>
where
    T: Clone + Send + 'static,
    F: Fn(&T) -> SystemTime + Send + 'static,
{
    stream! {
        let mut windows: HashMap<u64, TimeWindow<T>> = HashMap::new();
        let mut watermark = SystemTime::UNIX_EPOCH;
        pin_mut!(stream);

        while let Some(event) = stream.next().await {
            let event_time = timestamp_fn(&event);
            if event_time > watermark {
                watermark = event_time;
            }

            // Calculate window boundaries.
            // Bucket in milliseconds: `as_secs()` truncated sub-second windows
            // to zero and panicked on the division.
            let since_epoch = event_time.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            let window_size_ms = config.window_size.as_millis().max(1) as u64;
            let window_start_ms = (since_epoch.as_millis() as u64 / window_size_ms) * window_size_ms;
            let window_start = SystemTime::UNIX_EPOCH + Duration::from_millis(window_start_ms);
            let window_end = window_start + config.window_size;
            let window_id = window_start_ms;

            // Add event to appropriate window
            windows
                .entry(window_id)
                .or_insert_with(|| TimeWindow::new(window_start, window_end))
                .add_event(event);

            // Emit completed windows
            let mut to_remove = Vec::new();
            for (id, window) in &windows {
                // `checked_sub`: the watermark starts at the epoch, so subtracting
                // the delay can underflow before any event has arrived.
                let cutoff = watermark.checked_sub(config.watermark_delay).unwrap_or(SystemTime::UNIX_EPOCH);
                if window.is_complete(cutoff) {
                    to_remove.push(*id);
                }
            }
            for id in to_remove {
                if let Some(window) = windows.remove(&id) {
                    yield window;
                }
            }
        }

        // Emit remaining windows
        for (_, window) in windows {
            yield window;
        }
    }
    .boxed()
}

// ================================
// Stream Joins with Time Windows
// ================================

/// Whether two event times fall within `window` of each other, in either direction.
fn within_window(a: SystemTime, b: SystemTime, window: Duration) -> bool {
    let diff = if a > b {
        a.duration_since(b).unwrap_or_default()
    } else {
        b.duration_since(a).unwrap_or_default()
    };
    diff <= window
}

/// Configuration for time-windowed joins
#[derive(Debug, Clone)]
pub struct TimeJoinConfig {
    pub window_size: Duration,
    pub watermark_delay: Duration,
}

impl Default for TimeJoinConfig {
    fn default() -> Self {
        Self {
            window_size: Duration::from_secs(60),
            watermark_delay: Duration::from_secs(10),
        }
    }
}

/// Join two streams with time-based windowing
/// If key_selector is provided, only join on matching keys; otherwise, cross join within the window.
pub fn join_with_time_window<T1, T2, F, G1, G2, K, FK1, FK2>(
    stream1: RS2Stream<T1>,
    stream2: RS2Stream<T2>,
    config: TimeJoinConfig,
    timestamp_fn1: G1,
    timestamp_fn2: G2,
    join_fn: F,
    key_selector: Option<(FK1, FK2)>,
) -> RS2Stream<(T1, T2)>
where
    T1: Clone + Send + Sync + 'static,
    T2: Clone + Send + Sync + 'static,
    F: Fn(T1, T2) -> (T1, T2) + Send + 'static,
    G1: Fn(&T1) -> SystemTime + Send + 'static,
    G2: Fn(&T2) -> SystemTime + Send + 'static,
    K: Eq + std::hash::Hash,
    FK1: Fn(&T1) -> K + Send + Sync + 'static,
    FK2: Fn(&T2) -> K + Send + Sync + 'static,
{
    enum Either<L, R> {
        Left(L),
        Right(R),
    }

    /// Bucket key for the join index.
    ///
    /// Indexing on the *hash* of the join key rather than the key itself keeps
    /// the public bounds unchanged (`K: Eq + Hash` already), since the key never
    /// has to be stored. Hash collisions only widen the candidate set — the
    /// existing `fk1(e1) == fk2(e2)` check still decides every match.
    fn bucket_of<K: std::hash::Hash>(key: &K) -> u64 {
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Drop buffered events that have fallen behind the watermark.
    fn prune<V>(buffer: &mut HashMap<u64, Vec<(V, SystemTime)>>, min_time: SystemTime) {
        buffer.retain(|_, items| {
            items.retain(|(_, t)| *t >= min_time);
            !items.is_empty()
        });
    }

    /// Events between watermark-eviction passes.
    ///
    /// Pruning used to run on every event, which was O(n) per event and
    /// quadratic overall on its own. Correctness does not depend on it —
    /// `within_window` still gates every pair — so it is pure memory hygiene and
    /// safe to amortise.
    const EVICT_INTERVAL: u32 = 256;

    stream! {
        // Bucketed by join-key hash. The unkeyed (cross join) case puts
        // everything in bucket 0, which is the correct behaviour there: every
        // pair genuinely has to be considered.
        let mut buffer1: HashMap<u64, Vec<(T1, SystemTime)>> = HashMap::new();
        let mut buffer2: HashMap<u64, Vec<(T2, SystemTime)>> = HashMap::new();
        let mut watermark = SystemTime::UNIX_EPOCH;
        let mut since_evict: u32 = 0;

        let s1 = stream1.map(Either::Left);
        let s2 = stream2.map(Either::Right);
        let merged = merge(s1, s2);
        pin_mut!(merged);

        while let Some(either) = merged.next().await {
            // Each arriving event is joined against the *opposite* buffer only,
            // so every pair is considered exactly once. With the hash index that
            // is one bucket rather than the whole buffer.
            match either {
                Either::Left(e1) => {
                    let t1 = timestamp_fn1(&e1);
                    if t1 > watermark { watermark = t1; }

                    let bucket = match key_selector {
                        Some((ref fk1, _)) => bucket_of(&fk1(&e1)),
                        None => 0,
                    };

                    if let Some(candidates) = buffer2.get(&bucket) {
                        for (e2, t2) in candidates {
                            if within_window(t1, *t2, config.window_size) {
                                let key_match = match key_selector {
                                    Some((ref fk1, ref fk2)) => fk1(&e1) == fk2(e2),
                                    None => true,
                                };
                                if key_match {
                                    yield join_fn(e1.clone(), e2.clone());
                                }
                            }
                        }
                    }

                    buffer1.entry(bucket).or_default().push((e1, t1));
                }
                Either::Right(e2) => {
                    let t2 = timestamp_fn2(&e2);
                    if t2 > watermark { watermark = t2; }

                    let bucket = match key_selector {
                        Some((_, ref fk2)) => bucket_of(&fk2(&e2)),
                        None => 0,
                    };

                    if let Some(candidates) = buffer1.get(&bucket) {
                        for (e1, t1) in candidates {
                            if within_window(*t1, t2, config.window_size) {
                                let key_match = match key_selector {
                                    Some((ref fk1, ref fk2)) => fk1(e1) == fk2(&e2),
                                    None => true,
                                };
                                if key_match {
                                    yield join_fn(e1.clone(), e2.clone());
                                }
                            }
                        }
                    }

                    buffer2.entry(bucket).or_default().push((e2, t2));
                }
            }

            since_evict += 1;
            if since_evict >= EVICT_INTERVAL {
                since_evict = 0;
                // `checked_sub`: the watermark can sit near the epoch.
                if let Some(min_time) = watermark.checked_sub(config.window_size) {
                    prune(&mut buffer1, min_time);
                    prune(&mut buffer2, min_time);
                }
            }
        }
    }
    .boxed()
}

// ================================
// Extension Traits
// ================================

/// Extension trait for advanced analytics
pub trait AdvancedAnalyticsExt: Stream + Send + Sized + 'static {
    /// Apply time-based windowing to the stream
    fn window_by_time_rs2<F>(
        self,
        config: TimeWindowConfig,
        timestamp_fn: F,
    ) -> RS2Stream<TimeWindow<<Self as Stream>::Item>>
    where
        <Self as Stream>::Item: Clone + Send + 'static,
        F: Fn(&<Self as Stream>::Item) -> SystemTime + Send + 'static,
    {
        window_by_time(self.boxed(), config, timestamp_fn)
    }
    /// Join with another stream using time windows
    fn join_with_time_window_rs2<T2, F, G1, G2, K, FK1, FK2>(
        self,
        other: RS2Stream<T2>,
        config: TimeJoinConfig,
        timestamp_fn1: G1,
        timestamp_fn2: G2,
        join_fn: F,
        key_selector: Option<(FK1, FK2)>,
    ) -> RS2Stream<(Self::Item, T2)>
    where
        Self::Item: Clone + Send + Sync + 'static,
        T2: Clone + Send + Sync + 'static,
        F: Fn(Self::Item, T2) -> (Self::Item, T2) + Send + 'static,
        G1: Fn(&Self::Item) -> SystemTime + Send + 'static,
        G2: Fn(&T2) -> SystemTime + Send + 'static,
        K: Eq + std::hash::Hash,
        FK1: Fn(&Self::Item) -> K + Send + Sync + 'static,
        FK2: Fn(&T2) -> K + Send + Sync + 'static,
    {
        join_with_time_window(
            self.boxed(),
            other,
            config,
            timestamp_fn1,
            timestamp_fn2,
            join_fn,
            key_selector,
        )
    }
}

impl<S> AdvancedAnalyticsExt for S where S: Stream + Send + Sized + 'static {}
