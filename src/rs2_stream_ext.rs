use async_stream::stream;
use futures_core::Stream;
use futures_util::pin_mut;
use futures_util::stream::{BoxStream, StreamExt};
use futures_util::future;
use std::future::Future;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use num_cpus;

use crate::error::StreamResult;
use crate::stream_performance_metrics::StreamMetrics;
use crate::stream_configuration::{BufferConfig, GrowthStrategy};
use crate::rs2::{
    RS2Stream, BackpressureConfig, auto_backpressure, merge, zip_with, throttle, debounce, sample,
    par_eval_map, par_eval_map_unordered, par_join, timeout, prefetch, distinct_until_changed,
    distinct_until_changed_by, interrupt_when, take_while, drop_while, group_adjacent_by, group_by,
    fold, scan, take, drop, either, sliding_window, batch_process, with_metrics, interleave, chunk,
    tick, bracket
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
    /// - **Best for**: CPU-intensive computations (math, parsing, compression)
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
        self.par_eval_map_rs2(concurrency, move |x| {
            let f = f.clone();
            async move { f(x) }
        })
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
    /// This function will panic if `concurrency` is 0. Always use a positive value.
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
        self.par_eval_map_rs2(concurrency, move |x| {
            let f = f.clone();
            async move { f(x) }
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
    fn group_by_rs2<K, F>(self, key_fn: F) -> RS2Stream<(K, Vec<Self::Item>)>
    where
        Self::Item: Clone + Send + 'static,
        K: Eq + Clone + Send + 'static,
        F: FnMut(&Self::Item) -> K + Send + 'static,
    {
        group_by(self.boxed(), key_fn)
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

    /// Drop the first n elements from the stream
    ///
    /// This combinator skips the first n elements from the stream and yields all remaining elements.
    fn drop_rs2(self, n: usize) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        drop(self.boxed(), n)
    }

    /// Skip the first n elements from the stream
    ///
    /// This combinator skips the first n elements from the stream and yields all remaining elements.
    fn skip_rs2(self, n: usize) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        drop(self.boxed(), n)
    }

    /// Select between this rs2_stream and another rs2_stream based on which one produces a value first
    ///
    /// This combinator emits values from whichever rs2_stream produces a value first.
    /// Once a value is received from one rs2_stream, the other rs2_stream is cancelled.
    /// If either rs2_stream completes (returns None), the combinator switches to the other rs2_stream exclusively.
    fn either_rs2(self, other: RS2Stream<Self::Item>) -> RS2Stream<Self::Item>
    where
        Self::Item: Send + 'static,
    {
        either(self, other)
    }

    /// Collect all items from the stream into a collection
    ///
    /// This combinator collects all items from the stream into a collection of type B.
    /// It returns a Future that resolves to the collection.
    ///
    /// # Examples
    /// ```
    /// use rs2::rs2::*;
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
        self.collect_with_config_rs2(BufferConfig::default())
    }

    /// Collect all items from the stream into a collection with custom buffer configuration
    ///
    /// This combinator collects all items from the stream into a collection of type B.
    /// It returns a Future that resolves to the collection.
    /// The buffer configuration allows for optimized memory allocation and growth strategies.
    // Enhanced version that uses all BufferConfig fields
    fn collect_with_config_rs2<B>(self, config: BufferConfig) -> impl Future<Output = B>
    where
        B: Default + Extend<Self::Item> + Send + 'static,
        Self::Item: Send + 'static,
    {
        let mut stream = self.boxed();
        async move {
            if std::any::TypeId::of::<B>() == std::any::TypeId::of::<Vec<Self::Item>>() {
                // Create Vec with smart capacity management
                let mut vec = Vec::with_capacity(config.initial_capacity);
                let mut items_collected = 0;

                while let Some(item) = stream.next().await {
                    // Check max_capacity limit
                    if let Some(max_cap) = config.max_capacity {
                        if items_collected >= max_cap {
                            break; // Respect size limit
                        }
                    }

                    // Apply growth strategy when needed
                    if vec.len() == vec.capacity() {
                        let new_capacity = match config.growth_strategy {
                            GrowthStrategy::Linear(step) => vec.capacity() + step,
                            GrowthStrategy::Exponential(factor) =>
                                (vec.capacity() as f64 * factor) as usize,
                            GrowthStrategy::Fixed => vec.capacity(), // No growth
                        };

                        let capped_capacity = if let Some(max_cap) = config.max_capacity {
                            new_capacity.min(max_cap)
                        } else {
                            new_capacity
                        };

                        vec.reserve(capped_capacity - vec.capacity());
                    }

                    vec.push(item);
                    items_collected += 1;
                }

                // Safe transmute back to B
                let result = unsafe {
                    let ptr = &vec as *const Vec<Self::Item> as *const B;
                    let result = std::ptr::read(ptr);
                    std::mem::forget(vec);
                    result
                };
                result
            } else {
                // Fallback for other collection types
                let mut collection = B::default();
                while let Some(item) = stream.next().await {
                    collection.extend(std::iter::once(item));
                }
                collection
            }
        }
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
    fn with_metrics_rs2(self, name: String) -> (RS2Stream<Self::Item>, Arc<Mutex<StreamMetrics>>)
    where
        Self::Item: Send + 'static,
    {
        with_metrics(self.boxed(), name)
    }

    /// Interleave multiple streams in a round-robin fashion
    ///
    /// This combinator takes a vector of streams and interleaves their elements
    /// in a round-robin fashion.
    fn interleave_rs2<S>(self, streams: Vec<S>) -> RS2Stream<Self::Item>
    where
        S: Stream<Item = Self::Item> + Send + 'static + Unpin,
        Self::Item: Send + 'static,
    {
        let mut all_streams = vec![self.boxed()];
        all_streams.extend(streams.into_iter().map(|s| s.boxed()));
        interleave(all_streams)
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

    /// Create a stream that emits values at a fixed rate
    ///
    /// This combinator creates a stream that emits the provided item at a fixed rate.
    fn tick_rs<O>(self, period: Duration, item: O) -> RS2Stream<O>
    where
        O: Clone + Send + 'static,
    {
        tick(period, item)
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
}

impl<S> RS2StreamExt for S
where
    S: Stream + Sized + Unpin + Send + 'static,
{
}
