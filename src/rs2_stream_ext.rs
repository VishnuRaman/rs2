use futures_core::Stream;
use futures_util::future;
use futures_util::stream::StreamExt;
use log;
use num_cpus;
use serde;
use serde::Serialize;
use serde_json;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::error::{StreamError, StreamResult};
use crate::schema_validation::SchemaValidator;
use crate::stream_configuration::{BufferConfig, GrowthStrategy};
use crate::stream_performance_metrics::{HealthThresholds, StreamMetrics};
use crate::{
    auto_backpressure, batch_process, bracket, chunk, debounce, distinct_until_changed,
    distinct_until_changed_by, drop_while, fold, group_adjacent_by, skip, Either,
    interleave, interleave_all, interleave_many, interrupt_when, merge, merge_either,
    par_eval_map, par_eval_map_unordered, par_join, prefetch, race,
    sample, scan, sliding_window, take, take_while, throttle, timeout, with_metrics,
    with_metrics_sized, zip_with, zip_with_index, BackpressureConfig, RS2Stream,
    chunk_n, eval_tap, group_within, metered, on_finalize, on_finalize_case, ExitCase,
};

/// Extension trait providing RS2-like combinators on Streams
pub trait RS2StreamExt: Stream + Sized + Unpin + Send + 'static {
    /// Apply automatic backpressure with default configuration
    fn auto_backpressure_rs2(self) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        auto_backpressure(self.boxed(), BackpressureConfig::default())
    }

    /// Apply automatic backpressure with custom configuration
    fn auto_backpressure_with_rs2(self, config: BackpressureConfig) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        auto_backpressure(self.boxed(), config)
    }

    /// Map elements of the rs2_stream with a function
    fn map_rs2<U, F>(self, f: F) -> RS2Stream<U>
    where
        F: FnMut(Self::Item) -> U + Send + 'static,
        U: Send + 'static,
    {
        self.map(f).boxed()
    }

    /// Transforms each element of the stream in parallel using all available CPU cores.
    ///
    /// This method applies the given synchronous function to each element concurrently,
    /// automatically detecting the number of CPU cores and using that as the concurrency limit.
    /// Perfect for CPU-bound operations that benefit from parallelization.
    ///
    /// # Arguments
    ///
    /// * `f` - A synchronous function that transforms each stream element. Must be `Send + Sync + Clone`.
    ///
    /// # Returns
    ///
    /// A new `RS2Stream` containing the transformed elements. Order may not be preserved.
    ///
    /// # Performance
    ///
    /// - **Concurrency**: Automatically uses `num_cpus::get()` concurrent tasks
    /// - **Execution**: Each call runs on Tokio's blocking pool via
    ///   `spawn_blocking`, so CPU work genuinely runs on multiple threads
    /// - **Best for**: CPU-intensive computations (math, parsing, compression)
    /// - **Not for**: trivial closures — `spawn_blocking` costs more than the
    ///   work itself; use `map_rs2` there
    /// - **Memory**: Uses one task per CPU core, moderate memory overhead
    /// - **Backpressure**: Inherits from underlying `par_eval_map_rs2`
    ///
    /// # When to Use
    ///
    /// - ✅ **CPU-bound work**: Mathematical calculations, data parsing, compression
    /// - ✅ **Simple parallelization**: Don't want to think about optimal concurrency
    /// - ✅ **Balanced workloads**: Each task takes roughly the same time
    /// - ❌ **I/O-bound work**: Use `par_eval_map_rs2` with higher concurrency instead
    /// - ❌ **Memory-intensive**: May overwhelm system with too many concurrent tasks
    ///
    /// # See Also
    ///
    /// * [`map_parallel_with_concurrency_rs2`] - For custom concurrency control
    /// * [`par_eval_map_rs2`] - For async functions and fine-tuned concurrency
    fn map_parallel_rs2<O, F>(self, f: F) -> RS2Stream<O>
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static,
        Self::Item: Send + 'static,
        O: Send + 'static,
    {
        let concurrency = num_cpus::get();
        self.map_parallel_with_concurrency_rs2(concurrency, f)
    }

    /// Transforms each element of the stream in parallel with custom concurrency control.
    ///
    /// This method applies the given synchronous function to each element concurrently,
    /// using exactly the specified number of concurrent tasks. Ideal when you need precise
    /// control over resource usage or when the optimal concurrency differs from CPU count.
    ///
    /// # Arguments
    ///
    /// * `concurrency` - Maximum number of concurrent tasks (must be > 0)
    /// * `f` - A synchronous function that transforms each stream element. Must be `Send + Sync + Clone`.
    ///
    /// # Returns
    ///
    /// A new `RS2Stream` containing the transformed elements. Order may not be preserved.
    ///
    /// # Performance
    ///
    /// - **Concurrency**: Uses exactly `concurrency` concurrent tasks
    /// - **Best for**: I/O-bound operations, memory-constrained environments, fine-tuning
    /// - **Memory**: Scales with concurrency parameter
    /// - **Backpressure**: Inherits from underlying `par_eval_map_rs2`
    ///
    /// # Concurrency Guidelines
    ///
    /// | **Workload Type** | **Recommended Concurrency** | **Reasoning** |
    /// |-------------------|------------------------------|---------------|
    /// | **CPU-bound** | `num_cpus::get()` | Match CPU cores |
    /// | **I/O-bound** | `50-200` | Network can handle many concurrent requests |
    /// | **Memory-heavy** | `1-4` | Prevent out-of-memory errors |
    /// | **Database queries** | `10-50` | Respect connection pool limits |
    /// | **File I/O** | `4-16` | Balance throughput vs file handle limits |
    ///
    /// # When to Use
    ///
    /// - ✅ **I/O-bound operations**: Network requests, file operations, database queries
    /// - ✅ **Resource constraints**: Limited memory, connection pools, rate limits
    /// - ✅ **Performance tuning**: Benchmarked optimal concurrency for your workload
    /// - ✅ **Mixed workloads**: Some tasks much slower/faster than others
    /// - ❌ **Simple CPU-bound work**: Use `map_parallel_rs2` for automatic optimization
    ///
    /// # Panics
    ///
    /// Re-raises a panic from `f`, matching what would happen if it ran inline.
    /// A `concurrency` of 0 is treated as 1.
    ///
    /// # See Also
    ///
    /// * [`map_parallel_rs2`] - For automatic concurrency based on CPU cores
    /// * [`par_eval_map_rs2`] - For async functions with the same concurrency control
    fn map_parallel_with_concurrency_rs2<O, F>(self, concurrency: usize, f: F) -> RS2Stream<O>
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static,
        Self::Item: Send + 'static,
        O: Send + 'static,
    {
        // `f` is synchronous, so it must go to the blocking pool to achieve any
        // parallelism at all. Wrapping it in a plain `async move { f(x) }` — as
        // this used to — produces a future that completes on its first poll, on
        // the polling thread: `buffered` then interleaves futures that have
        // nothing left to interleave. Measured on 8 x 100ms of CPU work across
        // 16 cores: 800ms before, 108ms after, against an 800ms serial baseline.
        self.par_eval_map_rs2(concurrency.max(1), move |x| {
            let f = f.clone();
            async move {
                tokio::task::spawn_blocking(move || f(x))
                    .await
                    // Re-raise a panic from the user's closure, which is what
                    // would happen if it ran inline.
                    .expect("map_parallel closure panicked")
            }
        })
    }

    /// Filter elements of the rs2_stream with a predicate
    fn filter_rs2<F>(self, mut f: F) -> RS2Stream<Self::Item>
    where
        F: FnMut(&Self::Item) -> bool + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.filter(move |item| future::ready(f(item))).boxed()
    }

    /// Flat map elements of the rs2_stream with a function that returns a rs2_stream
    fn flat_map_rs2<U, St, F>(self, f: F) -> RS2Stream<U>
    where
        F: FnMut(Self::Item) -> St + Send + 'static,
        St: Stream<Item = U> + Send + 'static,
        U: Send + 'static,
    {
        self.flat_map(f).boxed()
    }

    /// Map elements of the rs2_stream with an async function
    fn eval_map_rs2<U, Fut, F>(self, f: F) -> RS2Stream<U>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
    {
        self.then(f).boxed()
    }

    /// Merge this rs2_stream with another rs2_stream
    fn merge_rs2(self, other: RS2Stream<Self::Item>) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        merge(self, other)
    }

    /// Zip this rs2_stream with another rs2_stream
    fn zip_rs2<U>(self, other: RS2Stream<U>) -> RS2Stream<(Self::Item, U)>
    where
        Self::Item: Send + 'static,
        U: Send + 'static,
    {
        self.zip(other).boxed()
    }

    /// Zip this rs2_stream with another rs2_stream, applying a function to each pair
    fn zip_with_rs2<U, O, F>(self, other: RS2Stream<U>, f: F) -> RS2Stream<O>
    where
        Self::Item: Send + 'static,
        U: Send + 'static,
        O: Send + 'static,
        F: FnMut(Self::Item, U) -> O + Send + 'static,
    {
        zip_with(self, other, f)
    }

    /// Throttle this rs2_stream to emit at most one element per duration
    fn throttle_rs2(self, duration: Duration) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        throttle(self.boxed(), duration)
    }

    /// Debounce this rs2_stream, only emitting an element after a specified quiet period has passed
    /// without receiving another element
    fn debounce_rs2(self, duration: Duration) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        debounce(self.boxed(), duration)
    }

    /// Sample this rs2_stream at regular intervals, emitting the most recent value
    ///
    /// This combinator samples the most recent value from a rs2_stream at a regular interval.
    /// It only emits a value if at least one new value has arrived since the last emission.
    /// If no new value has arrived during an interval, that interval is skipped.
    fn sample_rs2(self, interval: Duration) -> RS2Stream<Self::Item>
    where
        Self::Item: Clone + Send + 'static,
    {
        sample(self.boxed(), interval)
    }

    /// Process elements in parallel with bounded concurrency, preserving order
    /// ### Use when: `par_eval_map_rs2`
    /// - Already have async functions**
    /// - Need custom concurrency control**
    /// - Want maximum control/performance**
    fn par_eval_map_rs2<U, Fut, F>(self, concurrency: usize, f: F) -> RS2Stream<U>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
        Self::Item: Send + 'static,
    {
        par_eval_map(self.boxed(), concurrency, f)
    }

    /// Process elements in parallel with bounded concurrency, without preserving order
    fn par_eval_map_unordered_rs2<U, Fut, F>(self, concurrency: usize, f: F) -> RS2Stream<U>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
        Self::Item: Send + 'static,
    {
        par_eval_map_unordered(self.boxed(), concurrency, f)
    }

    /// Run multiple streams concurrently and combine their outputs
    ///
    /// This combinator takes a rs2_stream of streams and a concurrency limit, and runs
    /// up to n inner streams concurrently. It emits all elements from the inner streams,
    /// and starts new inner streams as others complete.
    fn par_join_rs2<S, O>(self, concurrency: usize) -> RS2Stream<O>
    where
        Self: Stream<Item = S>,
        S: Stream<Item = O> + Send + 'static + Unpin,
        O: Send + 'static,
    {
        par_join(self.boxed(), concurrency)
    }

    /// Add timeout to rs2_stream operations
    fn timeout_rs2(self, duration: Duration) -> RS2Stream<StreamResult<Self::Item>>
    where
        Self::Item: Send + 'static,
    {
        timeout(self.boxed(), duration)
    }

    /// Prefetch a specified number of elements ahead of consumption
    ///
    /// This can improve performance by starting to process the next elements
    /// before they're actually needed.
    fn prefetch_rs2(self, prefetch_count: usize) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        prefetch(self.boxed(), prefetch_count)
    }

    /// Filter out consecutive duplicate elements from this rs2_stream
    ///
    /// This combinator only emits elements that are different from the previous element.
    /// It uses the default equality operator (`==`) to compare elements.
    /// The first element is always emitted.
    fn distinct_until_changed_rs2(self) -> RS2Stream<Self::Item>
    where
        Self::Item: Clone + Send + PartialEq + 'static,
    {
        distinct_until_changed(self.boxed())
    }

    /// Filter out consecutive duplicate elements from this rs2_stream using a custom equality function
    ///
    /// This combinator only emits elements that are different from the previous element.
    /// It uses the provided equality function to compare elements.
    /// The first element is always emitted.
    fn distinct_until_changed_by_rs2<F>(self, eq: F) -> RS2Stream<Self::Item>
    where
        Self::Item: Clone + Send + 'static,
        F: FnMut(&Self::Item, &Self::Item) -> bool + Send + 'static,
    {
        distinct_until_changed_by(self.boxed(), eq)
    }

    /// Interrupt this rs2_stream when a signal is received
    ///
    /// This combinator stops processing the rs2_stream when the signal future completes.
    /// Resources are properly cleaned up when the rs2_stream is interrupted.
    fn interrupt_when_rs2<F>(self, signal: F) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        interrupt_when(self.boxed(), signal)
    }

    /// Take elements from this rs2_stream while a predicate returns true
    ///
    /// This combinator yields elements from the stream as long as the predicate returns true.
    /// It stops (and does not yield) the first element where the predicate returns false.
    fn take_while_rs2<F, Fut>(self, predicate: F) -> RS2Stream<Self::Item>
    where
        F: FnMut(&Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send + 'static,
        Self::Item: Send + 'static,
    {
        take_while(self.boxed(), predicate)
    }

    /// Skip elements from this rs2_stream while a predicate returns true
    ///
    /// This combinator skips elements from the stream as long as the predicate returns true.
    /// Once the predicate returns false, it yields that element and all remaining elements.
    fn drop_while_rs2<F, Fut>(self, predicate: F) -> RS2Stream<Self::Item>
    where
        F: FnMut(&Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send + 'static,
        Self::Item: Send + 'static,
    {
        drop_while(self.boxed(), predicate)
    }

    /// Group adjacent elements that share a common key
    ///
    /// This combinator groups consecutive elements that produce the same key.
    /// It emits groups as they complete (when the key changes or the rs2_stream ends).
    /// Each emitted item is a tuple containing the key and a vector of elements.
    fn group_adjacent_by_rs2<K, F>(self, key_fn: F) -> RS2Stream<(K, Vec<Self::Item>)>
    where
        Self::Item: Clone + Send + 'static,
        K: Eq + Clone + Send + 'static,
        F: FnMut(&Self::Item) -> K + Send + 'static,
    {
        group_adjacent_by(self.boxed(), key_fn)
    }

    /// Group consecutive elements that share a common key
    ///
    /// This combinator groups consecutive elements that produce the same key.
    /// It emits groups as they complete (when the key changes or the rs2_stream ends).
    /// Each emitted item is a tuple containing the key and a vector of elements.
    #[deprecated(
        since = "0.4.0",
        note = "groups only adjacent runs despite the name; use group_adjacent_by_rs2"
    )]
    fn group_by_rs2<K, F>(self, key_fn: F) -> RS2Stream<(K, Vec<Self::Item>)>
    where
        Self::Item: Clone + Send + 'static,
        K: Eq + Clone + Send + 'static,
        F: FnMut(&Self::Item) -> K + Send + 'static,
    {
        group_adjacent_by(self.boxed(), key_fn)
    }

    /// Fold operation that accumulates a value over a stream
    ///
    /// This combinator applies a function to each element in the stream, accumulating a single result.
    /// It returns a Future that resolves to the final accumulated value.
    fn fold_rs2<A, F, Fut>(self, init: A, f: F) -> impl Future<Output = A>
    where
        F: FnMut(A, Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = A> + Send + 'static,
        Self::Item: Send + 'static,
        A: Send + 'static,
    {
        fold(self.boxed(), init, f)
    }

    /// Scan operation that applies a function to each element and emits intermediate accumulated values
    ///
    /// This combinator is similar to fold but emits each intermediate accumulated value.
    /// It applies a function to each element in the stream, accumulating a result and yielding
    /// each intermediate accumulated value.
    fn scan_rs2<U, F>(self, init: U, f: F) -> RS2Stream<U>
    where
        F: FnMut(U, Self::Item) -> U + Send + 'static,
        Self::Item: Send + 'static,
        U: Clone + Send + 'static,
    {
        scan(self.boxed(), init, f)
    }

    /// Apply a function to each element in the stream
    ///
    /// This combinator applies a function to each element in the stream without accumulating a result.
    /// It returns a Future that completes when the stream is exhausted.
    fn for_each_rs2<F, Fut>(self, mut f: F) -> impl Future<Output = ()>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        Self::Item: Send + 'static,
    {
        let mut stream = self.boxed();
        async move {
            while let Some(item) = stream.next().await {
                f(item).await;
            }
        }
    }

    /// Take the first n elements from the stream
    ///
    /// This combinator yields the first n elements from the stream and then stops.
    fn take_rs2(self, n: usize) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        take(self.boxed(), n)
    }

    /// Skip the first n elements from the stream
    ///
    /// FS2 calls this `drop`; RS2 uses `skip` to match Rust convention and to
    /// avoid shadowing `std::mem::drop` under a glob import.
    fn skip_rs2(self, n: usize) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        skip(self.boxed(), n)
    }

    /// Select between this rs2_stream and another rs2_stream based on which one produces a value first
    ///
    /// This combinator emits values from whichever rs2_stream produces a value first.
    /// Once a value is received from one rs2_stream, the other rs2_stream is cancelled.
    /// If either rs2_stream completes (returns None), the combinator switches to the other rs2_stream exclusively.
    /// Emit from whichever stream produces a value first
    fn race_rs2(self, other: RS2Stream<Self::Item>) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        race(self, other)
    }

    /// Emit from whichever stream produces a value first
    ///
    /// # Deprecated
    ///
    /// Renamed to [`race_rs2`]; the name collided with FS2's `either`, which
    /// tags values by branch. That combinator is [`merge_either_rs2`].
    #[deprecated(since = "0.4.0", note = "renamed to `race_rs2`; FS2's `either` is `merge_either_rs2`")]
    fn either_rs2(self, other: RS2Stream<Self::Item>) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        race(self, other)
    }

    /// Merge with another stream, tagging each value with its branch (FS2's `either`)
    fn merge_either_rs2<B>(self, other: RS2Stream<B>) -> RS2Stream<Either<Self::Item, B>>
    where
        Self::Item: Send + 'static,
        B: Send + 'static,
    {
        merge_either(self, other)
    }

    /// Collect all items from the stream into a collection
    ///
    /// This combinator collects all items from the stream into a collection of type B.
    /// It returns a Future that resolves to the collection.
    ///
    /// # Examples
    /// ```
    /// use rs2_stream::rs2::*;
    /// use futures_util::stream::StreamExt;
    ///
    /// # async fn example() {
    /// let stream = from_iter(vec![1, 2, 3, 4, 5]);
    /// let result = stream.collect_rs2::<Vec<_>>().await;
    /// assert_eq!(result, vec![1, 2, 3, 4, 5]);
    /// # }
    /// ```
    fn collect_rs2<B>(self) -> impl Future<Output = B>
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        let mut stream = self.boxed();
        async move {
            let mut collection = B::default();
            while let Some(item) = stream.next().await {
                collection.extend(std::iter::once(item));
            }
            collection
        }
    }

    /// Collect at most `max_items` items, failing if the stream yields more
    ///
    /// Use this instead of [`collect_rs2`] when you need a hard bound on memory
    /// and want to be *told* when the stream exceeded it, rather than silently
    /// receiving a truncated result.
    ///
    /// # Examples
    /// ```
    /// use rs2_stream::rs2::*;
    /// use futures_util::stream::StreamExt;
    ///
    /// # async fn example() {
    /// let ok = from_iter(0..5).try_collect_bounded_rs2::<Vec<_>>(10).await;
    /// assert!(ok.is_ok());
    ///
    /// let too_many = from_iter(0..100).try_collect_bounded_rs2::<Vec<_>>(10).await;
    /// assert!(too_many.is_err());
    /// # }
    /// ```
    fn try_collect_bounded_rs2<B>(self, max_items: usize) -> impl Future<Output = StreamResult<B>>
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        let mut stream = self.boxed();
        async move {
            let mut collection = B::default();
            let mut count = 0usize;
            while let Some(item) = stream.next().await {
                if count == max_items {
                    return Err(StreamError::Custom(format!(
                        "stream yielded more than the {} item limit passed to try_collect_bounded_rs2",
                        max_items
                    )));
                }
                collection.extend(std::iter::once(item));
                count += 1;
            }
            Ok(collection)
        }
    }

    /// Collect into a `Vec`, sizing it from `config`
    ///
    /// Honours [`BufferConfig`] as the allocation policy its fields describe:
    ///
    /// - `initial_capacity` — reserved up front. Worth ~20% on a large collect
    ///   versus starting empty.
    /// - `growth_strategy` — how much to reserve each time the buffer fills.
    /// - `max_capacity` — a ceiling on *reservation* for the growing
    ///   strategies. It never truncates the stream.
    ///
    /// # Errors
    ///
    /// [`GrowthStrategy::Fixed`] means what it says: the buffer is fixed at
    /// `initial_capacity` and does not grow. A stream with more items than that
    /// returns [`StreamError::Custom`] rather than silently dropping the
    /// remainder — which is exactly what the previous implementation did, via
    /// `max_capacity`, past 1,048,576 items.
    ///
    /// The growing strategies never return an error, so
    /// `.expect("Linear never errors")` is safe there if you prefer.
    /// See also [`try_collect_bounded_rs2`] for a bound stated directly rather
    /// than derived from a buffer config.
    fn collect_vec_with_config_rs2(
        self,
        config: BufferConfig,
    ) -> impl Future<Output = StreamResult<Vec<Self::Item>>>
    where
        Self::Item: Send + 'static,
    {
        let mut stream = self.boxed();
        async move {
            let fixed = matches!(config.growth_strategy, GrowthStrategy::Fixed);
            let ceiling = config.max_capacity.unwrap_or(usize::MAX);
            let mut buffer = Vec::with_capacity(config.initial_capacity.min(ceiling));

            while let Some(item) = stream.next().await {
                if fixed && buffer.len() == config.initial_capacity {
                    return Err(StreamError::Custom(format!(
                        "GrowthStrategy::Fixed buffer of {} items overflowed; \
                         use Linear/Exponential to grow, or try_collect_bounded_rs2",
                        config.initial_capacity
                    )));
                }

                if buffer.len() == buffer.capacity() {
                    let current = buffer.capacity();
                    let target = match config.growth_strategy {
                        GrowthStrategy::Linear(step) => current.saturating_add(step),
                        GrowthStrategy::Exponential(factor) => {
                            // `saturating_sub` below guards factor <= 1.0, which
                            // previously underflowed and panicked.
                            ((current as f64) * factor) as usize
                        }
                        GrowthStrategy::Fixed => current,
                    };
                    let target = target.min(ceiling);
                    // Zero when the strategy asks for no growth beyond the
                    // ceiling; `Vec` then grows on its own.
                    buffer.reserve(target.saturating_sub(current));
                }
                buffer.push(item);
            }

            Ok(buffer)
        }
    }

    /// Collect all items from the stream into a collection with custom buffer configuration
    ///
    /// # Deprecated
    ///
    /// The previous implementation treated [`BufferConfig::max_capacity`] as an
    /// item-count limit and silently truncated the stream once it was reached
    /// (1,048,576 items by default).
    ///
    /// A `BufferConfig` cannot be applied through the generic `Extend` bound —
    /// there is no stable way to reserve capacity in an arbitrary collection —
    /// so this method ignores it and forwards to [`collect_rs2`].
    ///
    /// Use [`collect_vec_with_config_rs2`] if you want the config honoured,
    /// [`collect_rs2`] to collect everything, or [`try_collect_bounded_rs2`]
    /// for an explicit, *erroring* bound.
    #[deprecated(
        since = "0.4.0",
        note = "BufferConfig cannot be honoured for a generic collection; use collect_vec_with_config_rs2"
    )]
    fn collect_with_config_rs2<B>(self, _config: BufferConfig) -> impl Future<Output = B>
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        self.collect_rs2()
    }

    /// Create a sliding window of elements from the stream
    ///
    /// This combinator creates a sliding window of the specified size over the stream.
    /// It yields a vector of items for each window position.
    fn sliding_window_rs2(self, size: usize) -> RS2Stream<Vec<Self::Item>>
    where
        Self::Item: Clone + Send + 'static,
    {
        sliding_window(self.boxed(), size)
    }

    /// Process items in batches for better throughput
    ///
    /// This combinator processes items in batches of the specified size,
    /// applying the processor function to each batch.
    fn batch_process_rs2<U, F>(self, batch_size: usize, processor: F) -> RS2Stream<U>
    where
        F: FnMut(Vec<Self::Item>) -> Vec<U> + Send + 'static,
        Self::Item: Send + 'static,
        U: Send + 'static,
    {
        batch_process(self.boxed(), batch_size, processor)
    }

    /// Collect metrics while processing the stream
    ///
    /// This combinator collects metrics while processing the stream,
    /// returning both the stream and the metrics.
    fn with_metrics_rs2(
        self,
        name: String,
        health_thresholds: HealthThresholds,
    ) -> (RS2Stream<Self::Item>, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        with_metrics(self.boxed(), name, health_thresholds)
    }

    /// Collect metrics, sizing each item with the supplied function
    ///
    /// Use this when you need `bytes_processed`; [`with_metrics_rs2`] leaves it
    /// at zero rather than reporting the shallow `size_of_val`, which was 24
    /// bytes for every `String` regardless of contents.
    fn with_metrics_sized_rs2<F>(
        self,
        name: String,
        health_thresholds: HealthThresholds,
        size_of: F,
    ) -> (RS2Stream<Self::Item>, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
        F: Fn(&Self::Item) -> u64 + Send + 'static,
    {
        with_metrics_sized(self.boxed(), name, health_thresholds, size_of)
    }

    /// Interleave multiple streams in a round-robin fashion
    ///
    /// This combinator takes a vector of streams and interleaves their elements
    /// in a round-robin fashion.
    /// Deterministically interleave with another stream, stopping at the shorter
    ///
    /// FS2's `interleave`. This used to take a `Vec` and round-robin until every
    /// stream was exhausted; that behaviour is now [`interleave_many_rs2`].
    fn interleave_rs2(self, other: RS2Stream<Self::Item>) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        interleave(self, other)
    }

    /// Deterministically interleave, continuing with whichever stream is longer
    ///
    /// FS2's `interleaveAll`.
    fn interleave_all_rs2(self, other: RS2Stream<Self::Item>) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        interleave_all(self, other)
    }

    /// Round-robin across this stream and others, dropping each as it ends
    fn interleave_many_rs2<S>(self, streams: Vec<S>) -> RS2Stream<Self::Item>
    where
        S: Stream<Item = Self::Item> + Send + 'static + Unpin,
        Self::Item: Send + 'static,
    {
        let mut all_streams = vec![self.boxed()];
        all_streams.extend(streams.into_iter().map(|s| s.boxed()));
        interleave_many(all_streams)
    }

    /// Run an effect on each element for its side effects, passing it through
    ///
    /// FS2's `evalTap`.
    fn eval_tap_rs2<F, Fut>(self, f: F) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
        F: FnMut(&Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        eval_tap(self.boxed(), f)
    }

    /// Pair each element with its zero-based index (FS2's `zipWithIndex`)
    fn zip_with_index_rs2(self) -> RS2Stream<(Self::Item, u64)>
    where
        Self::Item: Send + 'static,
    {
        zip_with_index(self.boxed())
    }

    /// Buffer into chunks of up to `chunk_size`, emitting early on `timeout`
    ///
    /// FS2's `groupWithin`.
    fn group_within_rs2(self, chunk_size: usize, timeout: Duration) -> RS2Stream<Vec<Self::Item>>
    where
        Self::Item: Send + 'static,
    {
        group_within(self.boxed(), chunk_size, timeout)
    }

    /// Chunk into vectors of size `n`, optionally dropping a short final chunk
    ///
    /// FS2's `chunkN(n, allowFewer)`.
    fn chunk_n_rs2(self, n: usize, allow_fewer: bool) -> RS2Stream<Vec<Self::Item>>
    where
        Self::Item: Send + 'static,
    {
        chunk_n(self.boxed(), n, allow_fewer)
    }

    /// Emit at most one element per `rate`, dropping none (FS2's `metered`)
    fn metered_rs2(self, rate: Duration) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        metered(self.boxed(), rate)
    }

    /// Run an action when the stream ends, however it ends (FS2's `onFinalize`)
    fn on_finalize_rs2<F, Fut>(self, f: F) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        on_finalize(self.boxed(), f)
    }

    /// Run an action when the stream ends, told how it ended
    ///
    /// FS2's `onFinalizeCase`.
    fn on_finalize_case_rs2<F, Fut>(self, f: F) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
        F: FnOnce(ExitCase<()>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        on_finalize_case(self.boxed(), f)
    }

    /// Chunk the stream into vectors of the specified size
    ///
    /// This combinator collects elements from the stream into vectors of the specified size.
    /// If the stream ends before a chunk is filled, the final chunk may contain fewer elements.
    fn chunk_rs2(self, size: usize) -> RS2Stream<Vec<Self::Item>>
    where
        Self::Item: Send + 'static,
    {
        chunk(self.boxed(), size)
    }

    /// Bracket for resource management
    ///
    /// This combinator ensures that a resource is properly released after use.
    /// It takes three parameters:
    /// 1. A future that acquires a resource
    /// 2. A function that uses the resource and returns a stream
    /// 3. A function that releases the resource
    fn bracket_rs<A, O, St, FAcq, FUse, FRel, R>(
        self,
        acquire: FAcq,
        use_fn: FUse,
        release: FRel,
    ) -> RS2Stream<O>
    where
        FAcq: Future<Output = A> + Send + 'static,
        FUse: FnOnce(A) -> St + Send + 'static,
        St: Stream<Item = O> + Send + 'static,
        FRel: FnOnce(A) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
        O: Send + 'static,
        A: Clone + Send + 'static,
    {
        bracket(acquire, use_fn, release)
    }

    fn with_schema_validation_rs2<V, T>(
        self,
        validator: V,
    ) -> Pin<Box<dyn futures_util::Stream<Item = T> + Send>>
    where
        V: SchemaValidator + 'static,
        T: serde::de::DeserializeOwned + Serialize + Send + 'static,
        Self: futures_util::Stream<Item = T> + Send + 'static,
    {
        use futures_util::StreamExt;
        let validator = std::sync::Arc::new(validator);
        self.filter_map(move |item| {
            let validator = validator.clone();
            async move {
                let bytes = match serde_json::to_vec(&item) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("Schema validation: failed to serialize item: {}", e);
                        return None;
                    }
                };
                match validator.validate(&bytes).await {
                    Ok(()) => Some(item),
                    Err(e) => {
                        log::warn!("Schema validation failed: {}", e);
                        None
                    }
                }
            }
        })
        .boxed()
    }
}

impl<S> RS2StreamExt for S where S: Stream + Sized + Unpin + Send + 'static {}
