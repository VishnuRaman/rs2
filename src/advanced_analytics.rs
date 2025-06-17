//! Advanced analytics for RS2 streams
//!
//! Provides time-based windowed aggregations and advanced stream joins for building sophisticated real-time analytics.

use crate::rs2::*;
use async_stream::stream;
use futures_util::stream::StreamExt;
use futures_util::pin_mut;
use futures_core::Stream;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use std::collections::HashSet;

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

            // Calculate window boundaries
            let since_epoch = event_time.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            let window_size_secs = config.window_size.as_secs();
            let window_start_secs = (since_epoch.as_secs() / window_size_secs) * window_size_secs;
            let window_start = SystemTime::UNIX_EPOCH + Duration::from_secs(window_start_secs);
            let window_end = window_start + config.window_size;
            let window_id = window_start_secs;

            // Add event to appropriate window
            let window = windows.entry(window_id).or_insert_with(|| {
                TimeWindow::new(window_start, window_end)
            });
            window.add_event(event);

            // Emit completed windows
            let mut to_remove = Vec::new();
            for (id, window) in &windows {
                if window.is_complete(watermark - config.watermark_delay) {
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
    enum Either<L, R> { Left(L), Right(R) }
    stream! {
        let mut buffer1: Vec<(T1, SystemTime)> = Vec::new();
        let mut buffer2: Vec<(T2, SystemTime)> = Vec::new();
        let mut watermark = SystemTime::UNIX_EPOCH;
        let mut yielded: HashSet<(u128, u128)> = HashSet::new();
        let s1 = stream1.map(|e| Either::Left(e));
        let s2 = stream2.map(|e| Either::Right(e));
        let merged = merge(s1, s2);
        pin_mut!(merged);
        while let Some(either) = merged.next().await {
            match either {
                Either::Left(e1) => {
                    let t1 = timestamp_fn1(&e1);
                    if t1 > watermark { watermark = t1; }
                    buffer1.push((e1, t1));
                }
                Either::Right(e2) => {
                    let t2 = timestamp_fn2(&e2);
                    if t2 > watermark { watermark = t2; }
                    buffer2.push((e2, t2));
                }
            }
            // Clean old events
            let min_time = watermark - config.window_size;
            buffer1.retain(|(_, t)| *t >= min_time);
            buffer2.retain(|(_, t)| *t >= min_time);
            // Perform joins
            for (e1, t1) in &buffer1 {
                for (e2, t2) in &buffer2 {
                    let diff = if t1 > t2 {
                        t1.duration_since(*t2).unwrap_or_default()
                    } else {
                        t2.duration_since(*t1).unwrap_or_default()
                    };
                    if diff <= config.window_size {
                        let key_match = if let Some((ref fk1, ref fk2)) = key_selector {
                            fk1(e1) == fk2(e2)
                        } else {
                            true
                        };
                        if key_match {
                            // Deduplicate by timestamps
                            let t1n = t1.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos();
                            let t2n = t2.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos();
                            let key = (t1n, t2n);
                            if !yielded.contains(&key) {
                                yielded.insert(key);
                                yield join_fn(e1.clone(), e2.clone());
                            }
                        }
                    }
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
        join_with_time_window(self.boxed(), other, config, timestamp_fn1, timestamp_fn2, join_fn, key_selector)
    }
}

impl<S> AdvancedAnalyticsExt for S where S: Stream + Send + Sized + 'static {} 