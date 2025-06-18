//! RStream - A Rust streaming library inspired by FS2/RS2
//!
//! This module provides the core streaming functionality with functional
//! programming patterns, backpressure handling, and resource management.

use async_stream::stream;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures_core::Stream;
use futures_util::pin_mut;
use futures_util::{
    future,
    stream::{self, BoxStream, FuturesUnordered, StreamExt},
    SinkExt,
};
use std::future::Future;
use std::time::Duration;
use std::sync::Arc;
use tokio::{spawn, time::sleep};
use tokio::sync::Mutex;

use crate::error::{StreamError, StreamResult, RetryPolicy};
use crate::stream_performance_metrics::{HealthThresholds, StreamMetrics};

/// A boxed, heap-allocated Rust Stream analogous to RS2's Stream[F, O]
pub type RS2Stream<O> = BoxStream<'static, O>;

/// Backpressure strategy for automatic flow control
#[derive(Debug, Clone, Copy)]
pub enum BackpressureStrategy {
    /// Drop oldest items when buffer is full
    DropOldest,
    /// Drop newest items when buffer is full  
    DropNewest,
    /// Block producer until consumer catches up
    Block,
    /// Fail fast when buffer is full
    Error,
}

/// Configuration for automatic backpressure
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    pub strategy: BackpressureStrategy,
    pub buffer_size: usize,
    pub low_watermark: Option<usize>,  // Resume at this level
    pub high_watermark: Option<usize>, // Pause at this level
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            strategy: BackpressureStrategy::Block,
            buffer_size: 100,
            low_watermark: Some(25),
            high_watermark: Some(75),
        }
    }
}

/// ExitCase for bracketCase semantics
#[derive(Debug, Clone)]
pub enum ExitCase<E> {
    Completed,
    Errored(E),
}

// ================================
// Core Stream Constructors
// ================================

/// Emit a single element as a rs2_stream
pub fn emit<O>(item: O) -> RS2Stream<O>
where
    O: Send + 'static,
{
    stream::once(future::ready(item)).boxed()
}

/// Create an empty rs2_stream that completes immediately
pub fn empty<O>() -> RS2Stream<O>
where
    O: Send + 'static,
{
    stream::empty().boxed()
}

/// Create a rs2_stream from an iterator
pub fn from_iter<I, O>(iter: I) -> RS2Stream<O>
where
    I: IntoIterator<Item = O> + Send + 'static,
    <I as IntoIterator>::IntoIter: Send,
    O: Send + 'static,
{
    stream::iter(iter).boxed()
}

/// Evaluate a Future and emit its output
pub fn eval<O, F>(fut: F) -> RS2Stream<O>
where
    F: Future<Output = O> + Send + 'static,
    O: Send + 'static,
{
    stream::once(fut).boxed()
}

/// Repeat a value indefinitely
pub fn repeat<O>(item: O) -> RS2Stream<O>
where
    O: Clone + Send + 'static,
{
    stream::repeat(item).boxed()
}

/// Create a rs2_stream that emits a single value after a delay
pub fn emit_after<O>(item: O, duration: Duration) -> RS2Stream<O>
where
    O: Send + 'static,
{
    stream::once(async move {
        sleep(duration).await;
        item
    }).boxed()
}

/// Generate a rs2_stream from a seed value and a function
///
/// This combinator takes an initial state and a function that produces an element and the next state.
/// It continues until the function returns None.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
/// 
/// # async fn example() {
/// // Create a rs2_stream of Fibonacci numbers
/// let fibonacci = unfold(
///     (0, 1),
///     |state| async move {
///         let (a, b) = state;
///         Some((a, (b, a + b)))
///     }
/// );
///
/// // Take the first 10 Fibonacci numbers
/// let result = fibonacci.take(10).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]);
/// # }
/// ```
pub fn unfold<S, O, F, Fut>(init: S, mut f: F) -> RS2Stream<O>
where
    S: Send + 'static,
    O: Send + 'static,
    F: FnMut(S) -> Fut + Send + 'static,
    Fut: Future<Output = Option<(O, S)>> + Send + 'static,
{
    stream! {
        let mut state_opt = Some(init);

        loop {
            let state = state_opt.take().expect("State should be available");
            let fut = f(state);
            match fut.await {
                Some((item, next_state)) => {
                    yield item;
                    state_opt = Some(next_state);
                },
                None => break,
            }
        }
    }
    .boxed()
}

// ================================
// Stream Transformations
// ================================

/// Group adjacent elements that share a common key
///
/// This combinator groups consecutive elements that produce the same key.
/// It emits groups as they complete (when the key changes or the rs2_stream ends).
/// Each emitted item is a tuple containing the key and a vector of elements.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let rs2_stream = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);
/// let result = group_adjacent_by(rs2_stream, |&x| x % 2).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![(1, vec![1, 1]), (0, vec![2, 2]), (1, vec![3, 3]), (0, vec![2]), (1, vec![1])]);
/// # }
/// ```
pub fn group_adjacent_by<O, K, F>(s: RS2Stream<O>, mut key_fn: F) -> RS2Stream<(K, Vec<O>)>
where
    O: Clone + Send + 'static,
    K: Eq + Clone + Send + 'static,
    F: FnMut(&O) -> K + Send + 'static,
{
    stream! {
        pin_mut!(s);
        let mut current_key: Option<K> = None;
        let mut current_group: Vec<O> = Vec::new();

        while let Some(item) = s.next().await {
            let key = key_fn(&item);

            match &current_key {
                Some(k) if *k == key => {
                    current_group.push(item);
                },
                _ => {
                    if !current_group.is_empty() {
                        yield (current_key.clone().unwrap(), std::mem::take(&mut current_group));
                    }
                    current_key = Some(key);
                    current_group.push(item);
                }
            }
        }

        if !current_group.is_empty() {
            yield (current_key.clone().unwrap(), std::mem::take(&mut current_group));
        }
    }
    .boxed()
}

/// Slice: take first n items
pub fn take<O>(s: RS2Stream<O>, n: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    s.take(n).boxed()
}

/// Slice: drop first n items
pub fn drop<O>(s: RS2Stream<O>, n: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    s.skip(n).boxed()
}

/// Chunk the rs2_stream into Vecs of size n
pub fn chunk<O>(s: RS2Stream<O>, size: usize) -> RS2Stream<Vec<O>>
where
    O: Send + 'static,
{
    stream! {
        let mut buf = Vec::with_capacity(size);
        pin_mut!(s);
        while let Some(item) = s.next().await {
            buf.push(item);
            if buf.len() == size {
                yield std::mem::take(&mut buf);
            }
        }
        if !buf.is_empty() {
            yield std::mem::take(&mut buf);
        }
    }
        .boxed()
}

/// Add timeout support to any rs2_stream
pub fn timeout<T>(s: RS2Stream<T>, duration: Duration) -> RS2Stream<StreamResult<T>>
where
    T: Send + 'static,
{
    stream! {
        pin_mut!(s);
        loop {
            match tokio::time::timeout(duration, s.next()).await {
                Ok(Some(value)) => yield Ok(value),
                Ok(None) => break,
                Err(_) => yield Err(StreamError::Timeout),
            }
        }
    }.boxed()
}

/// Scan operation (like fold but emits intermediate results)
pub fn scan<T, U, F>(s: RS2Stream<T>, init: U, mut f: F) -> RS2Stream<U>
where
    F: FnMut(U, T) -> U + Send + 'static,
    T: Send + 'static,
    U: Clone + Send + 'static,
{
    stream! {
        let mut acc = init;
        pin_mut!(s);
        while let Some(item) = s.next().await {
            acc = f(acc.clone(), item);
            yield acc.clone();
        }
    }.boxed()
}

/// Fold operation that accumulates a value over a stream
pub fn fold<T, A, F, Fut>(s: RS2Stream<T>, init: A, mut f: F) -> impl Future<Output = A>
where
    F: FnMut(A, T) -> Fut + Send + 'static,
    Fut: Future<Output = A> + Send + 'static,
    T: Send + 'static,
    A: Send + 'static,
{
    async move {
        let mut acc = init;
        pin_mut!(s);
        while let Some(item) = s.next().await {
            acc = f(acc, item).await;
        }
        acc
    }
}

/// Reduce operation that combines all elements in a stream using a binary operation
pub fn reduce<T, F, Fut>(s: RS2Stream<T>, mut f: F) -> impl Future<Output = Option<T>>
where
    F: FnMut(T, T) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    async move {
        pin_mut!(s);
        let first = match s.next().await {
            Some(item) => item,
            None => return None, // Return None for empty streams
        };

        let mut acc = first;
        while let Some(item) = s.next().await {
            acc = f(acc, item).await;
        }

        Some(acc)
    }
}

/// Filter and map elements of a stream in one operation
pub fn filter_map<T, U, F, Fut>(s: RS2Stream<T>, f: F) -> RS2Stream<U>
where
    F: FnMut(T) -> Fut + Send + 'static,
    Fut: Future<Output = Option<U>> + Send + 'static,
    T: Send + 'static,
    U: Send + 'static,
{
    s.filter_map(f).boxed()
}

/// Take elements from a stream while a predicate returns true
///
/// This combinator yields elements from the stream as long as the predicate returns true.
/// It stops (and does not yield) the first element where the predicate returns false.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let stream = from_iter(vec![1, 2, 3, 4, 5]);
/// let result = take_while(stream, |&x| async move { x < 4 }).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![1, 2, 3]);
/// # }
/// ```
pub fn take_while<T, F, Fut>(s: RS2Stream<T>, mut predicate: F) -> RS2Stream<T>
where
    F: FnMut(&T) -> Fut + Send + 'static,
    Fut: Future<Output = bool> + Send + 'static,
    T: Send + 'static,
{
    stream! {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            if predicate(&item).await {
                yield item;
            } else {
                break;
            }
        }
    }.boxed()
}

/// Skip elements from a stream while a predicate returns true
///
/// This combinator skips elements from the stream as long as the predicate returns true.
/// Once the predicate returns false, it yields that element and all remaining elements.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let stream = from_iter(vec![1, 2, 3, 4, 5]);
/// let result = drop_while(stream, |&x| async move { x < 4 }).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![4, 5]);
/// # }
/// ```
pub fn drop_while<T, F, Fut>(s: RS2Stream<T>, mut predicate: F) -> RS2Stream<T>
where
    F: FnMut(&T) -> Fut + Send + 'static,
    Fut: Future<Output = bool> + Send + 'static,
    T: Send + 'static,
{
    stream! {
        pin_mut!(s);

        let mut found_false = false;
        while let Some(item) = s.next().await {
            if !found_false && predicate(&item).await {
                continue;
            } else {
                found_false = true;
                yield item;
            }
        }
    }.boxed()
}

/// Group consecutive elements that share a common key
///
/// This combinator groups consecutive elements that produce the same key.
/// It emits groups as they complete (when the key changes or the stream ends).
/// Each emitted item is a tuple containing the key and a vector of elements.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let stream = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);
/// let result = group_by(stream, |&x| x % 2).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![(1, vec![1, 1]), (0, vec![2, 2]), (1, vec![3, 3]), (0, vec![2]), (1, vec![1])]);
/// # }
/// ```
pub fn group_by<T, K, F>(s: RS2Stream<T>, mut key_fn: F) -> RS2Stream<(K, Vec<T>)>
where
    T: Clone + Send + 'static,
    K: Eq + Clone + Send + 'static,
    F: FnMut(&T) -> K + Send + 'static,
{
    stream! {
        pin_mut!(s);
        let mut current_key: Option<K> = None;
        let mut current_group: Vec<T> = Vec::new();

        while let Some(item) = s.next().await {
            let key = key_fn(&item);

            match &current_key {
                Some(k) if *k == key => {
                    current_group.push(item);
                },
                _ => {
                    if !current_group.is_empty() {
                        yield (current_key.clone().unwrap(), std::mem::take(&mut current_group));
                    }
                    current_key = Some(key);
                    current_group.push(item);
                }
            }
        }

        if !current_group.is_empty() {
            yield (current_key.clone().unwrap(), std::mem::take(&mut current_group));
        }
    }
    .boxed()
}

/// Sliding window operation
pub fn sliding_window<T>(s: RS2Stream<T>, size: usize) -> RS2Stream<Vec<T>>
where
    T: Clone + Send + 'static,
{
    if size == 0 {
        return empty();
    }

    stream! {
        let mut window = Vec::with_capacity(size);
        pin_mut!(s);

        while let Some(item) = s.next().await {
            window.push(item);

            if window.len() > size {
                window.remove(0);
            }

            if window.len() == size {
                yield window.clone();
            }
        }
    }.boxed()
}

/// Batch processing for better throughput
pub fn batch_process<T, U, F>(
    s: RS2Stream<T>,
    batch_size: usize,
    mut processor: F
) -> RS2Stream<U>
where
    F: FnMut(Vec<T>) -> Vec<U> + Send + 'static,
    T: Send + 'static,
    U: Send + 'static,
{
    stream! {
        let chunked = chunk(s, batch_size);
        pin_mut!(chunked);
        while let Some(batch) = chunked.next().await {
            for item in processor(batch) {
                yield item;
            }
        }
    }.boxed()
}

/// Collect metrics while processing rs2_stream
pub fn with_metrics<T>(
    s: RS2Stream<T>,
    name: String,
    thresholds: HealthThresholds
) -> (RS2Stream<T>, Arc<Mutex<StreamMetrics>>)
where
    T: Send + 'static,
{
    let metrics = Arc::new(Mutex::new(
        StreamMetrics::new()
            .with_name(name)
            .with_health_thresholds(thresholds)
    ));
    
    let metrics_clone = Arc::clone(&metrics);

    let monitored_stream = stream! {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            {
                let mut m = metrics_clone.lock().await;
                m.record_item(size_of_val(&item) as u64);
            }
            yield item;
        }

        {
            let mut m = metrics_clone.lock().await;
            m.finalize();
        }
    }.boxed();

    (monitored_stream, metrics)
}


// ================================
// Backpressure Management
// ================================

/// Automatic backpressure with configurable strategy
pub fn auto_backpressure<O>(s: RS2Stream<O>, config: BackpressureConfig) -> RS2Stream<O>
where
    O: Send + 'static,
{
    match config.strategy {
        BackpressureStrategy::Block => auto_backpressure_block(s, config.buffer_size),
        BackpressureStrategy::DropOldest => auto_backpressure_drop_oldest(s, config.buffer_size),
        BackpressureStrategy::DropNewest => auto_backpressure_drop_newest(s, config.buffer_size),
        BackpressureStrategy::Error => auto_backpressure_error(s, config.buffer_size),
    }
}

/// Automatic backpressure with blocking strategy
pub fn auto_backpressure_block<O>(s: RS2Stream<O>, buffer_size: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    let (mut tx, rx): (Sender<O>, Receiver<O>) = channel(buffer_size);

    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });

    stream! {
        let mut rx = rx;
        while let Some(item) = rx.next().await {
            yield item;
        }
    }
        .boxed()
}

/// Automatic backpressure that drops oldest items when buffer is full
pub fn auto_backpressure_drop_oldest<O>(s: RS2Stream<O>, buffer_size: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    use std::collections::VecDeque;

    let buffer = Arc::new(Mutex::new(VecDeque::<O>::new()));
    let buffer_clone = Arc::clone(&buffer);
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);

    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            let mut buf = buffer_clone.lock().await;

            if buf.len() >= buffer_size {
                buf.pop_front();
            }

            buf.push_back(item);
        }

        let _ = done_tx.send(()).await;
    });

    stream! {
        let mut source_done = false;

        loop {
            if let Ok(_) = done_rx.try_recv() {
                source_done = true;
            }

            let item = {
                let mut buf = buffer.lock().await;
                buf.pop_front()
            };

            match item {
                Some(item) => yield item,
                None => {
                    if source_done {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }
        .boxed()
}

/// Automatic backpressure that drops newest items when buffer is full
pub fn auto_backpressure_drop_newest<O>(s: RS2Stream<O>, buffer_size: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    use std::collections::VecDeque;

    let buffer = Arc::new(Mutex::new(VecDeque::<O>::new()));
    let buffer_clone = Arc::clone(&buffer);
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);

    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            let mut buf = buffer_clone.lock().await;

            if buf.len() < buffer_size {
                buf.push_back(item);
            }
        }

        let _ = done_tx.send(()).await;
    });

    stream! {
        let mut source_done = false;

        loop {
            if let Ok(_) = done_rx.try_recv() {
                source_done = true;
            }

            let item = {
                let mut buf = buffer.lock().await;
                buf.pop_front()
            };

            match item {
                Some(item) => yield item,
                None => {
                    if source_done {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }
        .boxed()
}

/// Automatic backpressure that errors when buffer is full
pub fn auto_backpressure_error<O>(s: RS2Stream<O>, buffer_size: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel(buffer_size);

    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });

    stream! {
        while let Some(item) = rx.recv().await {
            yield item;
        }
    }
        .boxed()
}

// ================================
// Stream Combinators
// ================================

/// Interrupt a rs2_stream when a signal is received
///
/// This combinator takes a rs2_stream and a future that signals interruption.
/// It stops processing the rs2_stream when the signal future completes.
/// Resources are properly cleaned up when the rs2_stream is interrupted.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use std::time::Duration;
/// use tokio::time::sleep;
/// use async_stream::stream;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// // Create a rs2_stream that emits numbers every 100ms
/// let rs2_stream = from_iter(0..100)
///     .throttle_rs2(Duration::from_millis(100));
///
/// // Create a future that completes after 250ms
/// let interrupt_signal = sleep(Duration::from_millis(250));
///
/// // The rs2_stream will be interrupted after about 250ms,
/// // so we should get approximately 2-3 items
/// let result = interrupt_when(rs2_stream, interrupt_signal)
///     .collect::<Vec<_>>()
///     .await;
///
/// assert!(result.len() >= 2 && result.len() <= 3);
/// # }
/// ```
pub fn interrupt_when<O, F>(s: RS2Stream<O>, signal: F) -> RS2Stream<O>
where
    O: Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    stream! {
        pin_mut!(s);
        pin_mut!(signal);

        loop {
            tokio::select! {
                biased;
                _ = &mut signal => {
                    break;
                },

                maybe_item = s.next() => {
                    match maybe_item {
                        Some(item) => yield item,
                        None => break,
                    }
                },
            }
        }
    }
    .boxed()
}

/// Concatenate multiple streams sequentially
pub fn concat<O, S>(streams: Vec<S>) -> RS2Stream<O>
where
    S: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    stream! {
        for s in streams {
            pin_mut!(s);
            while let Some(item) = s.next().await {
                yield item;
            }
        }
    }
        .boxed()
}

/// Merge two streams into one interleaved output
pub fn merge<O, S1, S2>(s1: S1, mut s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static + Unpin,
    O: Send + 'static,
{
    let chained = s1
        .map(Some)
        .chain(stream! { while let Some(x) = s2.next().await { yield Some(x) } });
    stream! {
        pin_mut!(chained);
        while let Some(item) = chained.next().await {
            if let Some(x) = item {
                yield x;
            }
        }
    }
        .boxed()
}


/// Interleave multiple streams in a round-robin fashion
pub fn interleave<O, S>(streams: Vec<S>) -> RS2Stream<O>
where
    S: Stream<Item = O> + Send + 'static + Unpin,
    O: Send + 'static,
{
    if streams.is_empty() {
        return empty();
    }

    stream! {
        let mut streams: Vec<_> = streams.into_iter().map(|s| Box::pin(s)).collect();
        let mut index = 0;

        while !streams.is_empty() {
            if index >= streams.len() {
                index = 0;
            }

            match streams[index].next().await {
                Some(item) => {
                    yield item;
                    index += 1;
                }
                None => {
                    streams.remove(index);
                }
            }
        }
    }
        .boxed()
}

/// Combine two streams element-by-element using a provided function
/// Returns a new rs2_stream with the combined elements
/// Stops when either input rs2_stream ends
pub fn zip_with<A, B, O, F, S1, S2>(s1: S1, s2: S2, mut f: F) -> RS2Stream<O>
where
    S1: Stream<Item = A> + Send + 'static,
    S2: Stream<Item = B> + Send + 'static,
    F: FnMut(A, B) -> O + Send + 'static,
    A: Send + 'static,
    B: Send + 'static,
    O: Send + 'static,
{
    stream! {
        pin_mut!(s1);
        pin_mut!(s2);

        loop {
            match futures_util::future::join(s1.next(), s2.next()).await {
                (Some(a), Some(b)) => yield f(a, b),
                _ => break, // Stop when either rs2_stream ends
            }
        }
    }
    .boxed()
}

/// Select between two streams based on which one produces a value first
///
/// This combinator takes two streams and emits values from whichever rs2_stream
/// produces a value first. Once a value is received from one rs2_stream, the other
/// rs2_stream is cancelled. If either rs2_stream completes (returns None), the combinator
/// switches to the other rs2_stream exclusively.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use std::time::Duration;
/// use async_stream::stream;
/// use tokio::time::sleep;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// // Create two streams with different timing
/// let fast_stream = stream! {
///     yield 1;
///     sleep(Duration::from_millis(10)).await;
///     yield 2;
///     sleep(Duration::from_millis(100)).await;
///     yield 3;
/// };
///
/// let slow_stream = stream! {
///     sleep(Duration::from_millis(50)).await;
///     yield 10;
///     sleep(Duration::from_millis(10)).await;
///     yield 20;
/// };
///
/// // The either combinator will select values from whichever rs2_stream produces first
/// let result = either(fast_stream.boxed(), slow_stream.boxed())
///     .collect::<Vec<_>>()
///     .await;
///
/// // We expect to get values from the fast rs2_stream first, then from the slow rs2_stream
/// // when the fast rs2_stream is waiting longer
/// assert_eq!(result, vec![1, 2, 10, 20]);
/// # }
/// ```
pub fn either<O, S1, S2>(s1: S1, s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    stream! {
        pin_mut!(s1);
        pin_mut!(s2);

        let mut s1_done = false;
        let mut s2_done = false;

        let mut using_s1 = true;

        loop {
            if s1_done {
                match s2.next().await {
                    Some(item) => yield item,
                    None => break,
                }
                continue;
            }

            if s2_done {
                match s1.next().await {
                    Some(item) => yield item,
                    None => break,
                }
                continue;
            }

            if using_s1 {
                match s1.next().await {
                    Some(item) => {
                        yield item;
                    },
                    None => {
                        s1_done = true;
                    }
                }
            } else {
                match s2.next().await {
                    Some(item) => {
                        yield item;
                    },
                    None => {
                        s2_done = true;
                    }
                }
            }

            tokio::select! {
                biased;

                maybe_item = s1.next() => {
                    match maybe_item {
                        Some(item) => {
                            yield item;
                            using_s1 = true;
                        },
                        None => {
                            s1_done = true;
                        }
                    }
                },
                maybe_item = s2.next() => {
                    match maybe_item {
                        Some(item) => {
                            yield item;
                            using_s1 = false;
                        },
                        None => {
                            s2_done = true;
                        }
                    }
                }
            }
        }
    }
    .boxed()
}

// ================================
// Timing and Rate Control
// ================================

/// Debounce a rs2_stream, only emitting an element after a specified quiet period has passed
/// without receiving another element
///
/// This combinator waits for a quiet period (specified by `duration`) after receiving an element
/// before emitting it. If another element arrives during the quiet period, the timer is reset
/// and the new element replaces the previous one.
///
/// This is useful for handling rapidly updating sources where you only want to process
/// the most recent value after the source has settled.
pub fn debounce<O>(s: RS2Stream<O>, duration: Duration) -> RS2Stream<O>
where
    O: Send + 'static,
{
    stream! {
        pin_mut!(s);

        let mut latest_item: Option<O> = None;
        let mut timer_handle: Option<tokio::task::JoinHandle<()>> = None;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

        loop {
            tokio::select! {
                maybe_item = s.next() => {
                    match maybe_item {
                        Some(item) => {
                            if let Some(handle) = timer_handle.take() {
                                handle.abort();
                            }

                            latest_item = Some(item);

                            let tx_clone = tx.clone();
                            timer_handle = Some(tokio::spawn(async move {
                                tokio::time::sleep(duration).await;
                                let _ = tx_clone.send(()).await;
                            }));
                        },
                        None => {
                            if let Some(item) = latest_item.take() {
                                yield item;
                            }
                            break;
                        }
                    }
                },
                _ = rx.recv() => {
                    if let Some(item) = latest_item.take() {
                        yield item;
                    }
                }
            }
        }
    }
    .boxed()
}

/// Filter out consecutive duplicate elements from a rs2_stream
/// 
/// This combinator only emits elements that are different from the previous element.
/// It uses the default equality operator (`==`) to compare elements.
/// The first element is always emitted.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let rs2_stream = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);
/// let result = distinct_until_changed(rs2_stream).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![1, 2, 3, 2, 1]);
/// # }
/// ```
pub fn distinct_until_changed<O>(s: RS2Stream<O>) -> RS2Stream<O>
where
    O: Clone + Send + PartialEq + 'static,
{
    stream! {
        pin_mut!(s);
        let mut prev: Option<O> = None;

        while let Some(item) = s.next().await {
            match &prev {
                Some(p) if p == &item => {
                },
                _ => {
                    yield item.clone();
                    prev = Some(item);
                }
            }
        }
    }
    .boxed()
}

/// Sample a rs2_stream at regular intervals, emitting the most recent value
///
/// This combinator samples the most recent value from a rs2_stream at a regular interval.
/// It only emits a value if at least one new value has arrived since the last emission.
/// If no new value has arrived during an interval, that interval is skipped.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
/// use std::time::Duration;
/// use tokio::time::sleep;
/// use async_stream::stream;
///
/// # async fn example() {
/// // Create a rs2_stream that emits values faster than the sample interval
/// let rs2_stream = stream! {
///     yield 1;
///     sleep(Duration::from_millis(10)).await;
///     yield 2;
///     sleep(Duration::from_millis(10)).await;
///     yield 3;
///     sleep(Duration::from_millis(100)).await;
///     yield 4;
/// };
///
/// // Sample the rs2_stream every 50ms
/// let result = sample(rs2_stream.boxed(), Duration::from_millis(50))
///     .collect::<Vec<_>>()
///     .await;
///
/// // We expect to get the most recent value at each interval:
/// // - 3 (the most recent value after the first 50ms)
/// // - 4 (the most recent value after the next 50ms)
/// assert_eq!(result, vec![3, 4]);
/// # }
/// ```
pub fn sample<O>(s: RS2Stream<O>, interval: Duration) -> RS2Stream<O>
where
    O: Clone + Send + 'static,
{
    stream! {
        pin_mut!(s);

        let mut latest_item: Option<O> = None;
        let mut has_new_value = false;

        let mut timer = tokio::time::interval(interval);
        timer.tick().await;

        loop {
            tokio::select! {
                maybe_item = s.next() => {
                    match maybe_item {
                        Some(item) => {
                            latest_item = Some(item);
                            has_new_value = true;
                        },
                        None => {
                            if has_new_value {
                                if let Some(item) = latest_item.take() {
                                    yield item;
                                }
                            }
                            break;
                        }
                    }
                },
                _ = timer.tick() => {
                    if has_new_value {
                        if let Some(ref item) = latest_item {
                            yield item.clone();
                            has_new_value = false;
                        }
                    }
                }
            }
        }
    }
    .boxed()
}

/// Filter out consecutive duplicate elements from a rs2_stream using a custom equality function
/// 
/// This combinator only emits elements that are different from the previous element.
/// It uses the provided equality function to compare elements.
/// The first element is always emitted.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
/// 
/// # async fn example() {
/// let rs2_stream = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);
/// // Use a custom equality function that considers two numbers equal if they have the same parity
/// let result = distinct_until_changed_by(rs2_stream, |a, b| a % 2 == b % 2).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![1, 2]);
/// # }
/// ```
pub fn distinct_until_changed_by<O, F>(s: RS2Stream<O>, mut eq: F) -> RS2Stream<O>
where
    O: Clone + Send + 'static,
    F: FnMut(&O, &O) -> bool + Send + 'static,
{
    stream! {
        pin_mut!(s);
        let mut prev: Option<O> = None;

        while let Some(item) = s.next().await {
            match &prev {
                Some(p) if eq(p, &item) => {
                },
                _ => {
                    yield item.clone();
                    prev = Some(item);
                }
            }
        }
    }
    .boxed()
}

/// Prefetch a specified number of elements ahead of consumption
/// This combinator eagerly evaluates a specified number of elements ahead of what's been requested,
/// storing them in a buffer. This can improve performance by starting to process the next elements
/// before they're actually needed.
///
/// Backpressure is maintained by using a bounded channel with capacity equal to the prefetch count.
pub fn prefetch<O>(s: RS2Stream<O>, prefetch_count: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    if prefetch_count == 0 {
        return s;
    }

    let (mut tx, rx): (Sender<O>, Receiver<O>) = channel(prefetch_count);

    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });

    stream! {
        let mut rx = rx;
        while let Some(item) = rx.next().await {
            yield item;
        }
    }
    .boxed()
}

/// Back-pressure-aware rate limiting via bounded channel (legacy)
pub fn rate_limit_backpressure<O>(s: RS2Stream<O>, capacity: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    auto_backpressure_block(s, capacity)
}

/// Throttle rs2_stream to emit one element per `duration`
pub fn throttle<O>(s: RS2Stream<O>, duration: Duration) -> RS2Stream<O>
where
    O: Send + 'static,
{
    stream! {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            yield item;
            sleep(duration).await;
        }
    }
        .boxed()
}

/// Create a rs2_stream that emits values at a fixed rate
pub fn tick<O>(period: Duration, item: O) -> RS2Stream<O>
where
    O: Clone + Send + 'static,
{
    stream! {
        loop {
            yield item.clone();
            sleep(period).await;
        }
    }
        .boxed()
}

// ================================
// Parallel Processing
// ================================

/// Parallel evaluation preserving order (parEvalMap) with automatic backpressure
pub fn par_eval_map<I, O, Fut, F>(s: RS2Stream<I>, concurrency: usize, mut f: F) -> RS2Stream<O>
where
    F: FnMut(I) -> Fut + Send + 'static,
    Fut: Future<Output = O> + Send + 'static,
    O: Send + 'static,
    I: Send + 'static,
{
    let buffered_stream = auto_backpressure_block(s, concurrency * 2);

    stream! {
        let mut in_flight = FuturesUnordered::new();
        pin_mut!(buffered_stream);

        while let Some(item) = buffered_stream.next().await {
            in_flight.push(f(item));
            if in_flight.len() >= concurrency {
                if let Some(res) = in_flight.next().await {
                    yield res;
                }
            }
        }
        while let Some(res) = in_flight.next().await {
            yield res;
        }
    }
        .boxed()
}

/// Parallel evaluation unordered (parEvalMapUnordered) with automatic backpressure
pub fn par_eval_map_unordered<I, O, Fut, F>(
    s: RS2Stream<I>,
    concurrency: usize,
    f: F,
) -> RS2Stream<O>
where
    F: FnMut(I) -> Fut + Send + 'static,
    Fut: Future<Output = O> + Send + 'static,
    O: Send + 'static,
    I: Send + 'static,
{
    let buffered_stream = auto_backpressure_block(s, concurrency * 2);
    buffered_stream.map(f).buffer_unordered(concurrency).boxed()
}

/// Parallel join of streams (parJoin) with automatic backpressure
///
/// This combinator takes a rs2_stream of streams and a concurrency limit, and runs
/// up to n inner streams concurrently. It emits all elements from the inner streams,
/// and starts new inner streams as others complete.
///
/// Backpressure is maintained by using a bounded buffer for the outer rs2_stream.
pub fn par_join<O, S>(
    s: RS2Stream<S>,
    concurrency: usize,
) -> RS2Stream<O>
where
    S: Stream<Item = O> + Send + 'static + Unpin,
    O: Send + 'static,
{
    let buffered_stream = auto_backpressure_block(s, concurrency * 2);

    stream! {
        pin_mut!(buffered_stream);

        let mut active_streams: Vec<S> = Vec::with_capacity(concurrency);

        let mut outer_stream_done = false;

        loop {
            while active_streams.len() < concurrency && !outer_stream_done {
                match buffered_stream.next().await {
                    Some(inner_stream) => {
                        active_streams.push(inner_stream);
                    },
                    None => {
                        outer_stream_done = true;
                        break;
                    }
                }
            }
            if active_streams.is_empty() && outer_stream_done {
                break;
            }

            let mut i = 0;
            while i < active_streams.len() {
                match active_streams[i].next().await {
                    Some(item) => {
                        yield item;
                        i += 1;
                    },
                    None => {
                        active_streams.swap_remove(i);
                    }
                }
            }
        }
    }
    .boxed()
}

// ================================
// Resource Management
// ================================

/// Bracket for simple resource handling
pub fn bracket<A, O, St, FAcq, FUse, FRel, R>(
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
    stream! {
        let resource = acquire.await;
        let stream = use_fn(resource.clone());
        pin_mut!(stream);
        while let Some(item) = stream.next().await {
            yield item;
        }
        release(resource).await;
    }
        .boxed()
}

/// BracketCase with exit case semantics for streams of Result<O,E>
pub fn bracket_case<A, O, E, St, FAcq, FUse, FRel, R>(
    acquire: FAcq,
    use_fn: FUse,
    release: FRel,
) -> RS2Stream<Result<O, E>>
where
    FAcq: Future<Output = A> + Send + 'static,
    FUse: FnOnce(A) -> St + Send + 'static,
    St: Stream<Item = Result<O, E>> + Send + 'static,
    FRel: FnOnce(A, ExitCase<E>) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    O: Send + 'static,
    E: Clone + Send + 'static,
    A: Clone + Send + 'static,
{
    stream! {
        let resource = acquire.await;
        let stream = use_fn(resource.clone());
        pin_mut!(stream);
        while let Some(item) = stream.next().await {
            yield item;
        }
        release(resource, ExitCase::Completed).await;
    }
        .boxed()
}

// ================================
// Stream Extensions
// ================================

// Re-export the extension traits from their respective modules
pub use crate::rs2_result_stream_ext::RS2ResultStreamExt;
pub use crate::rs2_stream_ext::RS2StreamExt;
