//! New Advanced Analytics for RS2 Streams
//!
//! Provides time-based windowed aggregations and advanced stream joins using our custom rs2_new implementation.
//! No external stream dependencies, no futures_util, no unsafe code.

use crate::rs2_new;
use crate::rs2_new_stream_ext::RS2StreamExt;
use crate::stream::Stream;
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
#[derive(Debug, Clone)]
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

    pub fn count(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// State for windowing operations
#[derive(Debug)]
struct WindowState<T> {
    windows: HashMap<u64, TimeWindow<T>>,
    watermark: SystemTime,
    config: TimeWindowConfig,
}

impl<T> WindowState<T> {
    fn new(config: TimeWindowConfig) -> Self {
        Self {
            windows: HashMap::new(),
            watermark: SystemTime::UNIX_EPOCH,
            config,
        }
    }

    fn process_event(&mut self, event: T, timestamp: SystemTime) -> Vec<TimeWindow<T>>
    where
        T: Clone,
    {
        // Update watermark
        if timestamp > self.watermark {
            self.watermark = timestamp;
        }

        // Calculate window boundaries
        let since_epoch = timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
        let window_size_secs = self.config.window_size.as_secs();
        let window_start_secs = (since_epoch.as_secs() / window_size_secs) * window_size_secs;
        let window_start = SystemTime::UNIX_EPOCH + Duration::from_secs(window_start_secs);
        let window_end = window_start + self.config.window_size;
        let window_id = window_start_secs;

        // Add event to appropriate window
        let window = self.windows.entry(window_id).or_insert_with(|| {
            TimeWindow::new(window_start, window_end)
        });
        window.add_event(event);

        // Check for completed windows
        self.emit_completed_windows()
    }

    fn emit_completed_windows(&mut self) -> Vec<TimeWindow<T>> {
        let watermark_threshold = self.watermark - self.config.watermark_delay;
        let mut completed = Vec::new();
        let mut to_remove = Vec::new();

        for (id, window) in &self.windows {
            if window.is_complete(watermark_threshold) {
                to_remove.push(*id);
            }
        }

        for id in to_remove {
            if let Some(window) = self.windows.remove(&id) {
                completed.push(window);
            }
        }

        completed
    }

    fn finalize(self) -> Vec<TimeWindow<T>> {
        self.windows.into_values().collect()
    }
}

/// Create time-based windows from a stream of timestamped events
pub fn window_by_time<T, F>(
    stream: impl Stream<Item = T> + Send + 'static,
    config: TimeWindowConfig,
    timestamp_fn: F,
) -> impl Stream<Item = TimeWindow<T>> + Send + 'static
where
    T: Clone + Send + 'static,
    F: Fn(&T) -> SystemTime + Send + 'static,
{
    // Simplified implementation: group events into time-based windows
    stream
        .map_rs2(move |event| {
            let timestamp = timestamp_fn(&event);
            let since_epoch = timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            let window_size_secs = config.window_size.as_secs();
            let window_start_secs = (since_epoch.as_secs() / window_size_secs) * window_size_secs;
            let window_start = SystemTime::UNIX_EPOCH + Duration::from_secs(window_start_secs);
            let window_end = window_start + config.window_size;
            (window_start, window_end, event)
        })
        .group_by_rs2(|(window_start, _, _)| *window_start)
        .map_rs2(|(window_start, events)| {
            if events.is_empty() {
                TimeWindow::new(window_start, window_start)
            } else {
                let (_, window_end, _) = &events[0];
                let mut window = TimeWindow::new(window_start, *window_end);
                for (_, _, event) in events {
                    window.add_event(event);
                }
                window
            }
        })
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

/// Either type for merging two streams
#[derive(Debug, Clone)]
enum Either<L, R> {
    Left(L),
    Right(R),
}

/// Join two streams with time-based windowing (simplified implementation)
pub fn join_with_time_window<T1, T2, F, G1, G2>(
    stream1: impl Stream<Item = T1> + Send + 'static,
    stream2: impl Stream<Item = T2> + Send + 'static,
    config: TimeJoinConfig,
    timestamp_fn1: G1,
    timestamp_fn2: G2,
    join_fn: F,
) -> impl Stream<Item = (T1, T2)> + Send + 'static
where
    T1: Clone + Send + 'static,
    T2: Clone + Send + 'static,
    F: Fn(T1, T2) -> (T1, T2) + Send + 'static,
    G1: Fn(&T1) -> SystemTime + Send + 'static,
    G2: Fn(&T2) -> SystemTime + Send + 'static,
{
    // Simplified join: collect items from both streams in windows and join them
    let s1 = stream1.map_rs2(move |e| {
        let t = timestamp_fn1(&e);
        Either::Left((e, t))
    });
    
    let s2 = stream2.map_rs2(move |e| {
        let t = timestamp_fn2(&e);
        Either::Right((e, t))
    });
    
    let merged = rs2_new::merge(s1, s2);
    
    // Use chunking to process in batches for efficiency
    merged
        .chunk_rs2(20)
        .map_rs2(move |batch| {
            let mut left_items = Vec::new();
            let mut right_items = Vec::new();
            
            // Separate left and right items
            for item in batch {
                match item {
                    Either::Left((e, t)) => left_items.push((e, t)),
                    Either::Right((e, t)) => right_items.push((e, t)),
                }
            }
            
            // Perform joins within the time window
            let mut joins = Vec::new();
            for (e1, t1) in &left_items {
                for (e2, t2) in &right_items {
                    let diff = if t1 > t2 {
                        t1.duration_since(*t2).unwrap_or_default()
                    } else {
                        t2.duration_since(*t1).unwrap_or_default()
                    };
                    
                    if diff <= config.window_size {
                        joins.push(join_fn(e1.clone(), e2.clone()));
                    }
                }
            }
            
            joins
        })
        .flat_map_rs2(|joins| rs2_new::from_iter_rs2(joins))
}

// ================================
// Aggregation Functions
// ================================

/// Sliding window aggregation
pub fn sliding_window_aggregate<T, R, F>(
    stream: impl Stream<Item = T> + Send + 'static,
    window_size: usize,
    aggregate_fn: F,
) -> impl Stream<Item = R> + Send + 'static
where
    T: Send + 'static + Clone,
    R: Send + 'static,
    F: Fn(&[T]) -> R + Send + 'static,
{
    stream
        .sliding_window_rs2(window_size)
        .map_rs2(move |window| aggregate_fn(&window))
}

/// Moving average calculation
pub fn moving_average(
    stream: impl Stream<Item = f64> + Send + 'static,
    window_size: usize,
) -> impl Stream<Item = f64> + Send + 'static {
    sliding_window_aggregate(stream, window_size, |values| {
        if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<f64>() / values.len() as f64
        }
    })
}

/// Count events in sliding window
pub fn sliding_count<T>(
    stream: impl Stream<Item = T> + Send + 'static,
    window_size: usize,
) -> impl Stream<Item = usize> + Send + 'static
where
    T: Send + 'static + Clone,
{
    sliding_window_aggregate(stream, window_size, |values| values.len())
}

/// Sum values in sliding window
pub fn sliding_sum<T>(
    stream: impl Stream<Item = T> + Send + 'static,
    window_size: usize,
) -> impl Stream<Item = T> + Send + 'static
where
    T: Send + 'static + std::iter::Sum + Clone + Default,
{
    sliding_window_aggregate(stream, window_size, |values| {
        values.iter().cloned().sum()
    })
}

/// Find minimum in sliding window
pub fn sliding_min<T>(
    stream: impl Stream<Item = T> + Send + 'static,
    window_size: usize,
) -> impl Stream<Item = Option<T>> + Send + 'static
where
    T: Send + 'static + Ord + Clone,
{
    sliding_window_aggregate(stream, window_size, |values| {
        values.iter().min().cloned()
    })
}

/// Find maximum in sliding window
pub fn sliding_max<T>(
    stream: impl Stream<Item = T> + Send + 'static,
    window_size: usize,
) -> impl Stream<Item = Option<T>> + Send + 'static
where
    T: Send + 'static + Ord + Clone,
{
    sliding_window_aggregate(stream, window_size, |values| {
        values.iter().max().cloned()
    })
}

// ================================
// Time-based Aggregations
// ================================

/// Group events by time buckets
pub fn group_by_time<T, F>(
    stream: impl Stream<Item = T> + Send + 'static,
    bucket_size: Duration,
    timestamp_fn: F,
) -> impl Stream<Item = (SystemTime, Vec<T>)> + Send + 'static
where
    T: Clone + Send + 'static,
    F: Fn(&T) -> SystemTime + Send + 'static,
{
    stream
        .map_rs2(move |event| {
            let timestamp = timestamp_fn(&event);
            let since_epoch = timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            let bucket_secs = bucket_size.as_secs();
            let bucket_start_secs = (since_epoch.as_secs() / bucket_secs) * bucket_secs;
            let bucket_start = SystemTime::UNIX_EPOCH + Duration::from_secs(bucket_start_secs);
            (bucket_start, event)
        })
        .group_by_rs2(|(bucket_start, _)| *bucket_start)
        .map_rs2(|(bucket_start, events)| {
            let values = events.into_iter().map(|(_, event)| event).collect();
            (bucket_start, values)
        })
}

// ================================
// Extension Traits
// ================================

/// Extension trait for new advanced analytics
pub trait NewAdvancedAnalyticsExt: Stream + Send + Sized + 'static {
    /// Apply time-based windowing to the stream
    fn window_by_time_rs2_new<F>(
        self,
        config: TimeWindowConfig,
        timestamp_fn: F,
    ) -> impl Stream<Item = TimeWindow<Self::Item>> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
        F: Fn(&Self::Item) -> SystemTime + Send + 'static,
    {
        window_by_time(self, config, timestamp_fn)
    }

    /// Join with another stream using time windows (simplified)
    fn join_with_time_window_rs2_new<T2, F, G1, G2>(
        self,
        other: impl Stream<Item = T2> + Send + 'static,
        config: TimeJoinConfig,
        timestamp_fn1: G1,
        timestamp_fn2: G2,
        join_fn: F,
    ) -> impl Stream<Item = (Self::Item, T2)> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
        T2: Clone + Send + 'static,
        F: Fn(Self::Item, T2) -> (Self::Item, T2) + Send + 'static,
        G1: Fn(&Self::Item) -> SystemTime + Send + 'static,
        G2: Fn(&T2) -> SystemTime + Send + 'static,
    {
        join_with_time_window(
            self,
            other,
            config,
            timestamp_fn1,
            timestamp_fn2,
            join_fn,
        )
    }

    /// Apply sliding window aggregation
    fn sliding_window_aggregate_rs2_new<R, F>(
        self,
        window_size: usize,
        aggregate_fn: F,
    ) -> impl Stream<Item = R> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
        R: Send + 'static,
        F: Fn(&[Self::Item]) -> R + Send + 'static,
    {
        sliding_window_aggregate(self, window_size, aggregate_fn)
    }

    /// Calculate moving average (for numeric streams)
    fn moving_average_rs2_new(
        self,
        window_size: usize,
    ) -> impl Stream<Item = f64> + Send + 'static
    where
        Self::Item: Into<f64> + Send + 'static,
    {
        let numeric_stream = self.map_rs2(|x| x.into());
        moving_average(numeric_stream, window_size)
    }

    /// Count items in sliding window
    fn sliding_count_rs2_new(
        self,
        window_size: usize,
    ) -> impl Stream<Item = usize> + Send + 'static
    where
        Self::Item: Send + 'static + Clone,
    {
        sliding_count(self, window_size)
    }

    /// Sum items in sliding window
    fn sliding_sum_rs2_new(
        self,
        window_size: usize,
    ) -> impl Stream<Item = Self::Item> + Send + 'static
    where
        Self::Item: Send + 'static + std::iter::Sum + Clone + Default,
    {
        sliding_sum(self, window_size)
    }

    /// Find minimum in sliding window
    fn sliding_min_rs2_new(
        self,
        window_size: usize,
    ) -> impl Stream<Item = Option<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static + Ord + Clone,
    {
        sliding_min(self, window_size)
    }

    /// Find maximum in sliding window
    fn sliding_max_rs2_new(
        self,
        window_size: usize,
    ) -> impl Stream<Item = Option<Self::Item>> + Send + 'static
    where
        Self::Item: Send + 'static + Ord + Clone,
    {
        sliding_max(self, window_size)
    }

    /// Group events by time buckets
    fn group_by_time_rs2_new<F>(
        self,
        bucket_size: Duration,
        timestamp_fn: F,
    ) -> impl Stream<Item = (SystemTime, Vec<Self::Item>)> + Send + 'static
    where
        Self::Item: Clone + Send + 'static,
        F: Fn(&Self::Item) -> SystemTime + Send + 'static,
    {
        group_by_time(self, bucket_size, timestamp_fn)
    }
}

impl<S> NewAdvancedAnalyticsExt for S where S: Stream + Send + Sized + 'static {} 