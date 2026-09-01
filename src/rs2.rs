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
    stream::{self, BoxStream, StreamExt},
    SinkExt,
};
use std::future::Future;
use std::time::Duration;
use std::sync::Arc;
use tokio::{spawn, time::sleep};
use tokio::sync::Mutex;

use crate::error::{StreamError, StreamResult};
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
    /// Resume the producer once the buffer drains to this level.
    ///
    /// Only meaningful with [`BackpressureStrategy::Block`], and only together
    /// with `high_watermark`. See [`BackpressureConfig::watermarks`].
    pub low_watermark: Option<usize>,
    /// Pause the producer once the buffer reaches this level.
    pub high_watermark: Option<usize>,
}

impl BackpressureConfig {
    /// The effective `(low, high)` watermark pair, if usable.
    ///
    /// Returns `None` when either is unset, or when the pair is nonsensical
    /// (`low >= high`, or `high` beyond `buffer_size`) — in which case the
    /// plain bounded-channel behaviour is used instead. These fields were
    /// previously declared, defaulted and never read by anything, so
    /// configuring them had no effect at all.
    pub fn watermarks(&self) -> Option<(usize, usize)> {
        let low = self.low_watermark?;
        let high = self.high_watermark?;
        if low < high && high <= self.buffer_size {
            Some((low, high))
        } else {
            None
        }
    }
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

/// Tags which branch of a two-stream combinator a value came from.
///
/// Rust has no `Either` in std, so RS2 supplies one for [`either`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    pub fn left(self) -> Option<L> {
        match self {
            Either::Left(l) => Some(l),
            Either::Right(_) => None,
        }
    }

    pub fn right(self) -> Option<R> {
        match self {
            Either::Left(_) => None,
            Either::Right(r) => Some(r),
        }
    }
}

/// How a bracketed stream terminated, mirroring FS2's `Resource.ExitCase`.
///
/// FS2 distinguishes three outcomes, and so does this:
///
/// - [`ExitCase::Completed`] — the stream ran to exhaustion.
/// - [`ExitCase::Canceled`] — the consumer stopped early: `take(n)`, a `break`,
///   or simply dropping the stream. FS2 calls this `Canceled`; it is the case
///   that makes `bracket_case` worth having over [`bracket`], because release
///   can tell "finished" from "caller walked away" and commit or roll back
///   accordingly.
/// - [`ExitCase::Errored`] — the stream itself failed.
///
/// # A note on `Errored`
///
/// In FS2 this carries a `Throwable` from the effect's error channel, raised by
/// `Stream.raiseError` or a failing `flatMap`. RS2 streams have no such channel:
/// they are infallible, and errors travel in-band as `Result` items. An
/// in-band `Err` is *data*, exactly as `Left` is in FS2, so it does **not**
/// produce `Errored` — the stream still completed.
///
/// `Errored` is therefore unreachable today. It is retained because it becomes
/// reachable the moment RS2 grows a real error channel, and removing it would
/// force a second breaking change then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitCase<E> {
    /// The stream ran to exhaustion.
    Completed,
    /// The consumer stopped consuming before the stream was exhausted.
    Canceled,
    /// The stream itself failed. Currently unreachable — see the type docs.
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

/// Slice: skip the first n items
///
/// Named `skip` to match Rust convention (`Iterator::skip`, `StreamExt::skip`).
/// FS2 calls this `drop`, but that name shadows `std::mem::drop` from the Rust
/// prelude whenever a caller does `use rs2_stream::rs2::*`, turning an ordinary
/// `drop(guard)` into a confusing type error.
pub fn skip<O>(s: RS2Stream<O>, n: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    s.skip(n).boxed()
}

/// Chunk into `Vec`s of size `n`, optionally dropping a short final chunk
///
/// FS2's `chunkN(n, allowFewer)`. With `allow_fewer = false` a trailing partial
/// chunk is discarded rather than emitted.
pub fn chunk_n<O>(s: RS2Stream<O>, n: usize, allow_fewer: bool) -> RS2Stream<Vec<O>>
where
    O: Send + 'static,
{
    let n = n.max(1);
    stream! {
        let mut buf = Vec::with_capacity(n);
        pin_mut!(s);
        while let Some(item) = s.next().await {
            buf.push(item);
            if buf.len() == n {
                yield std::mem::take(&mut buf);
            }
        }
        if allow_fewer && !buf.is_empty() {
            yield std::mem::take(&mut buf);
        }
    }
        .boxed()
}

/// Chunk the rs2_stream into Vecs of size n
pub fn chunk<O>(s: RS2Stream<O>, size: usize) -> RS2Stream<Vec<O>>
where
    O: Send + 'static,
{
    // A size of 0 would never reach `buf.len() == size`, so the whole stream was
    // buffered and emitted as a single chunk. Treat it as 1.
    let size = size.max(1);

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
/// # Deprecated
///
/// This is a byte-for-byte duplicate of [`group_adjacent_by`] and, despite the
/// name, only groups *adjacent* runs — `[1, 2, 1]` keyed by identity yields
/// three groups, not two. The name suggests a global grouping it has never
/// performed.
///
/// Use [`group_adjacent_by`], which says what it does. If you need genuine
/// key-global grouping, collect into a map yourself: it cannot be streamed
/// without buffering the entire input.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let stream = from_iter(vec![1, 1, 2, 2, 3, 3, 2, 1]);
/// let result = group_adjacent_by(stream, |&x| x % 2).collect::<Vec<_>>().await;
/// assert_eq!(result, vec![(1, vec![1, 1]), (0, vec![2, 2]), (1, vec![3, 3]), (0, vec![2]), (1, vec![1])]);
/// # }
/// ```
#[deprecated(
    since = "0.4.0",
    note = "groups only adjacent runs despite the name; use group_adjacent_by"
)]
pub fn group_by<T, K, F>(s: RS2Stream<T>, key_fn: F) -> RS2Stream<(K, Vec<T>)>
where
    T: Clone + Send + 'static,
    K: Eq + Clone + Send + 'static,
    F: FnMut(&T) -> K + Send + 'static,
{
    group_adjacent_by(s, key_fn)
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
        // `VecDeque`: the previous `Vec::remove(0)` shifted every element on
        // each item, making the operator O(n) per element.
        let mut window: std::collections::VecDeque<T> = std::collections::VecDeque::with_capacity(size);
        pin_mut!(s);

        while let Some(item) = s.next().await {
            window.push_back(item);

            if window.len() > size {
                window.pop_front();
            }

            if window.len() == size {
                yield window.iter().cloned().collect::<Vec<T>>();
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
///
/// Tracks item counts, errors and timing. `bytes_processed` is **not** measured
/// here and stays zero — see [`with_metrics_sized`] if you need it.
///
/// This previously reported `size_of_val(&item)`, which is the *shallow* size of
/// the value: 24 bytes for every `String` regardless of its contents, so
/// `bytes_processed` and every throughput figure derived from it were fiction.
pub fn with_metrics<T>(
    s: RS2Stream<T>,
    name: String,
    thresholds: HealthThresholds
) -> (RS2Stream<T>, Arc<Mutex<StreamMetrics>>)
where
    T: Send + 'static,
{
    with_metrics_sized(s, name, thresholds, |_| 0)
}

/// Collect metrics, sizing each item with the supplied function
///
/// `size_of` is called once per item and should return the item's real byte
/// size — for example `|s: &String| s.len() as u64`. There is no way to derive
/// that generically, which is why it has to be supplied.
pub fn with_metrics_sized<T, F>(
    s: RS2Stream<T>,
    name: String,
    thresholds: HealthThresholds,
    size_of: F,
) -> (RS2Stream<T>, Arc<Mutex<StreamMetrics>>)
where
    T: Send + 'static,
    F: Fn(&T) -> u64 + Send + 'static,
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
            let bytes = size_of(&item);
            {
                let mut m = metrics_clone.lock().await;
                m.record_item(bytes);
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
        // Watermarks only apply to the blocking strategy: they describe when to
        // pause and resume a producer, which the dropping strategies never do.
        BackpressureStrategy::Block => match config.watermarks() {
            Some((low, high)) => auto_backpressure_watermark(s, config.buffer_size, low, high),
            None => auto_backpressure_block(s, config.buffer_size),
        },
        BackpressureStrategy::DropOldest => auto_backpressure_drop_oldest(s, config.buffer_size),
        BackpressureStrategy::DropNewest => auto_backpressure_drop_newest(s, config.buffer_size),
        BackpressureStrategy::Error => auto_backpressure_error(s, config.buffer_size),
    }
}

/// Blocking backpressure with pause/resume hysteresis.
///
/// The producer runs until the buffer reaches `high_watermark`, then pauses
/// until the consumer has drained it to `low_watermark`. That gap is the point:
/// a plain bounded channel unblocks the producer the instant one slot frees, so
/// it wakes on every single item once full. With watermarks it sleeps until
/// there is a meaningful amount of room, trading a little latency for far fewer
/// wakeups.
///
/// `low` must be below `high`; see [`BackpressureConfig::watermarks`].
pub fn auto_backpressure_watermark<O>(
    s: RS2Stream<O>,
    buffer_size: usize,
    low: usize,
    high: usize,
) -> RS2Stream<O>
where
    O: Send + 'static,
{
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Shared<O> {
        items: Mutex<std::collections::VecDeque<O>>,
        /// Consumer has drained to `low`; the producer may resume.
        space: tokio::sync::Notify,
        /// An item is available for the consumer.
        ready: tokio::sync::Notify,
        source_done: AtomicBool,
        consumer_alive: AtomicBool,
    }

    let shared = Arc::new(Shared {
        items: Mutex::new(std::collections::VecDeque::with_capacity(buffer_size)),
        space: tokio::sync::Notify::new(),
        ready: tokio::sync::Notify::new(),
        source_done: AtomicBool::new(false),
        consumer_alive: AtomicBool::new(true),
    });

    struct ConsumerGuard<O>(Arc<Shared<O>>);
    impl<O> Drop for ConsumerGuard<O> {
        fn drop(&mut self) {
            self.0.consumer_alive.store(false, Ordering::Release);
            self.0.space.notify_waiters();
        }
    }

    let producer = Arc::clone(&shared);
    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            if !producer.consumer_alive.load(Ordering::Acquire) {
                break;
            }

            let len = {
                let mut buf = producer.items.lock().await;
                buf.push_back(item);
                buf.len()
            };
            producer.ready.notify_one();

            // At the high watermark, pause until the consumer drains to low.
            if len >= high {
                loop {
                    if !producer.consumer_alive.load(Ordering::Acquire) {
                        return;
                    }
                    let len = producer.items.lock().await.len();
                    if len <= low {
                        break;
                    }
                    producer.space.notified().await;
                }
            }
        }

        producer.source_done.store(true, Ordering::Release);
        producer.ready.notify_waiters();
    });

    stream! {
        let _guard = ConsumerGuard(Arc::clone(&shared));

        loop {
            let (item, len) = {
                let mut buf = shared.items.lock().await;
                let item = buf.pop_front();
                (item, buf.len())
            };

            match item {
                Some(item) => {
                    // Tell a paused producer once we are back under the low mark.
                    if len <= low {
                        shared.space.notify_waiters();
                    }
                    yield item;
                }
                None => {
                    if shared.source_done.load(Ordering::Acquire) {
                        // Re-check under the lock: the producer may have pushed
                        // between our pop and reading the flag.
                        let leftover = { shared.items.lock().await.pop_front() };
                        match leftover {
                            Some(item) => yield item,
                            None => break,
                        }
                    } else {
                        shared.space.notify_waiters();
                        shared.ready.notified().await;
                    }
                }
            }
        }
    }
    .boxed()
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

/// Shared buffer behind the dropping backpressure strategies.
///
/// `Notify` replaces an earlier 1ms polling sleep (which cost up to a
/// millisecond of latency per item), and `consumer_alive` gives the producer
/// task a shutdown signal — without one it ran forever against an infinite
/// source after the consumer went away.
struct DropBuffer<O> {
    items: Mutex<std::collections::VecDeque<O>>,
    ready: tokio::sync::Notify,
    source_done: std::sync::atomic::AtomicBool,
    consumer_alive: std::sync::atomic::AtomicBool,
}

impl<O> DropBuffer<O> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            items: Mutex::new(std::collections::VecDeque::new()),
            ready: tokio::sync::Notify::new(),
            source_done: std::sync::atomic::AtomicBool::new(false),
            consumer_alive: std::sync::atomic::AtomicBool::new(true),
        })
    }
}

/// Marks the buffer as consumer-gone when the output stream is dropped.
struct ConsumerGuard<O>(Arc<DropBuffer<O>>);

impl<O> Drop for ConsumerGuard<O> {
    fn drop(&mut self) {
        self.0
            .consumer_alive
            .store(false, std::sync::atomic::Ordering::Release);
        self.0.ready.notify_waiters();
    }
}

/// Body shared by the drop-oldest and drop-newest strategies.
fn auto_backpressure_dropping<O>(
    s: RS2Stream<O>,
    buffer_size: usize,
    drop_oldest: bool,
) -> RS2Stream<O>
where
    O: Send + 'static,
{
    use std::sync::atomic::Ordering;

    let buffer = DropBuffer::new();
    let producer = Arc::clone(&buffer);

    spawn(async move {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            if !producer.consumer_alive.load(Ordering::Acquire) {
                break; // Consumer went away; stop draining the source.
            }
            {
                let mut buf = producer.items.lock().await;
                if buf.len() >= buffer_size {
                    if drop_oldest {
                        buf.pop_front();
                        buf.push_back(item);
                    }
                    // drop_newest: discard `item` by simply not pushing it.
                } else {
                    buf.push_back(item);
                }
            }
            producer.ready.notify_one();
        }

        producer.source_done.store(true, Ordering::Release);
        producer.ready.notify_waiters();
    });

    stream! {
        let _guard = ConsumerGuard(Arc::clone(&buffer));

        loop {
            let item = {
                let mut buf = buffer.items.lock().await;
                buf.pop_front()
            };

            match item {
                Some(item) => yield item,
                None => {
                    if buffer.source_done.load(Ordering::Acquire) {
                        // Re-check under the lock: the producer may have pushed
                        // between our pop and reading the flag.
                        let leftover = { buffer.items.lock().await.pop_front() };
                        match leftover {
                            Some(item) => yield item,
                            None => break,
                        }
                    } else {
                        buffer.ready.notified().await;
                    }
                }
            }
        }
    }
    .boxed()
}

/// Automatic backpressure that drops oldest items when buffer is full
pub fn auto_backpressure_drop_oldest<O>(s: RS2Stream<O>, buffer_size: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    auto_backpressure_dropping(s, buffer_size, true)
}

/// Automatic backpressure that drops newest items when buffer is full
pub fn auto_backpressure_drop_newest<O>(s: RS2Stream<O>, buffer_size: usize) -> RS2Stream<O>
where
    O: Send + 'static,
{
    auto_backpressure_dropping(s, buffer_size, false)
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
///
/// Both streams are polled concurrently and items are emitted as soon as either
/// side produces one. The merged stream completes once *both* inputs are exhausted.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// // Both sides make progress concurrently rather than one draining before the other.
/// let a = from_iter(vec![1, 2, 3]);
/// let b = from_iter(vec![10, 20, 30]);
/// let merged = merge(a, b).collect::<Vec<_>>().await;
/// assert_eq!(merged.len(), 6);
/// # }
/// ```
pub fn merge<O, S1, S2>(s1: S1, s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    stream::select(s1.boxed(), s2.boxed()).boxed()
}


/// Deterministically interleave two streams, stopping at the shorter
///
/// FS2's `interleave`: alternate starting on the left, terminating as soon as
/// either side is exhausted.
///
/// This previously took a `Vec` and round-robined until every stream was
/// exhausted — neither FS2's `interleave` nor its `interleaveAll`. That
/// behaviour is now [`interleave_many`].
pub fn interleave<O, S1, S2>(s1: S1, s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    stream! {
        pin_mut!(s1);
        pin_mut!(s2);
        loop {
            match s1.next().await {
                Some(a) => yield a,
                None => break,
            }
            match s2.next().await {
                Some(b) => yield b,
                None => break,
            }
        }
    }
    .boxed()
}

/// Deterministically interleave two streams, continuing with whichever is longer
///
/// FS2's `interleaveAll`.
pub fn interleave_all<O, S1, S2>(s1: S1, s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    stream! {
        pin_mut!(s1);
        pin_mut!(s2);
        let mut left_done = false;
        let mut right_done = false;

        while !(left_done && right_done) {
            if !left_done {
                match s1.next().await {
                    Some(a) => yield a,
                    None => left_done = true,
                }
            }
            if !right_done {
                match s2.next().await {
                    Some(b) => yield b,
                    None => right_done = true,
                }
            }
        }
    }
    .boxed()
}

/// Round-robin across many streams, dropping each as it is exhausted
///
/// This is the behaviour `interleave` used to have. It has no direct FS2
/// equivalent; FS2's `interleave`/`interleaveAll` are both binary.
pub fn interleave_many<O, S>(streams: Vec<S>) -> RS2Stream<O>
where
    S: Stream<Item = O> + Send + 'static + Unpin,
    O: Send + 'static,
{
    if streams.is_empty() {
        return empty();
    }

    stream! {
        let mut streams: Vec<_> = streams.into_iter().map(Box::pin).collect();
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

/// Emit from whichever stream produces a value first
///
/// Both streams are polled concurrently and values are emitted as they arrive;
/// when one is exhausted the other continues alone. Ties are broken in favour
/// of `s1`, so the output is deterministic when both are ready.
///
/// # Note on the name
///
/// This used to be called `either`, which collides with FS2: FS2's `either`
/// *tags* each value with its branch and returns `Stream[F, Either[O, O2]]`.
/// That combinator now lives at [`either`]; this racing behaviour is closer to
/// Rx's `amb`.
pub fn race<O, S1, S2>(s1: S1, s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    stream! {
        // `fuse` makes it safe if either side is polled again after completing.
        let s1 = s1.fuse();
        let s2 = s2.fuse();
        pin_mut!(s1);
        pin_mut!(s2);

        let mut s1_done = false;
        let mut s2_done = false;

        while !(s1_done && s2_done) {
            tokio::select! {
                // `biased` makes the tie-break deterministic: when both sides
                // have an item ready, `s1` wins. Without it `select!` chooses at
                // random and the output order is unpredictable. This only
                // decides ties — if `s1` is pending, `s2` is still polled, so a
                // slow `s1` cannot starve a ready `s2`.
                biased;

                maybe_item = s1.next(), if !s1_done => {
                    match maybe_item {
                        Some(item) => yield item,
                        None => s1_done = true,
                    }
                },
                maybe_item = s2.next(), if !s2_done => {
                    match maybe_item {
                        Some(item) => yield item,
                        None => s2_done = true,
                    }
                },
            }
        }
    }
    .boxed()
}

/// Emit from whichever stream produces a value first
///
/// # Deprecated
///
/// Renamed to [`race`]. The name collided with FS2's `either`, which does
/// something different — it *tags* each value with its branch. This alias keeps
/// the original behaviour so existing callers are unaffected; FS2's combinator
/// is [`merge_either`].
#[deprecated(since = "0.4.0", note = "renamed to `race`; FS2's `either` is `merge_either`")]
pub fn either<O, S1, S2>(s1: S1, s2: S2) -> RS2Stream<O>
where
    S1: Stream<Item = O> + Send + 'static,
    S2: Stream<Item = O> + Send + 'static,
    O: Send + 'static,
{
    race(s1, s2)
}

/// Merge two streams, tagging each value with the branch it came from
///
/// This is FS2's `either`: `Stream[F, Either[O, O2]]`. It is *not* named
/// `either` here because that name was already taken by the racing combinator
/// (now [`race`]), and silently changing what an existing name means is worse
/// than picking a new one.
///
/// The two sides may have different item types, and both run concurrently.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let out = merge_either(from_iter(vec![1, 2]), from_iter(vec!["a", "b"]))
///     .collect::<Vec<_>>()
///     .await;
/// assert_eq!(out.len(), 4);
/// # }
/// ```
pub fn merge_either<A, B, S1, S2>(s1: S1, s2: S2) -> RS2Stream<Either<A, B>>
where
    S1: Stream<Item = A> + Send + 'static,
    S2: Stream<Item = B> + Send + 'static,
    A: Send + 'static,
    B: Send + 'static,
{
    race(s1.map(Either::Left).boxed(), s2.map(Either::Right).boxed())
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

/// Emit at most one element per `rate`, without dropping any
///
/// FS2's `metered`. The first element is emitted immediately; each subsequent
/// one waits out the remainder of the period.
pub fn metered<O>(s: RS2Stream<O>, rate: Duration) -> RS2Stream<O>
where
    O: Send + 'static,
{
    throttle(s, rate)
}

/// Throttle rs2_stream to emit one element per `duration`
///
/// FS2 calls this `metered`; [`metered`] is provided under that name.
pub fn throttle<O>(s: RS2Stream<O>, duration: Duration) -> RS2Stream<O>
where
    O: Send + 'static,
{
    stream! {
        pin_mut!(s);
        // Sleep *before* each item except the first, rather than after every
        // item. Sleeping afterwards added a full `duration` to stream
        // completion, after the last item had already been emitted.
        let mut first = true;
        while let Some(item) = s.next().await {
            if !first {
                sleep(duration).await;
            }
            first = false;
            yield item;
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

/// Run an effect on each element for its side effects, passing the element through
///
/// FS2's `evalTap`. The element is emitted unchanged; only the effect's
/// completion is awaited.
pub fn eval_tap<O, F, Fut>(s: RS2Stream<O>, mut f: F) -> RS2Stream<O>
where
    O: Send + 'static,
    F: FnMut(&O) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    stream! {
        pin_mut!(s);
        while let Some(item) = s.next().await {
            f(&item).await;
            yield item;
        }
    }
    .boxed()
}

/// Pair each element with its zero-based index
///
/// FS2's `zipWithIndex`.
pub fn zip_with_index<O>(s: RS2Stream<O>) -> RS2Stream<(O, u64)>
where
    O: Send + 'static,
{
    stream! {
        pin_mut!(s);
        let mut index = 0u64;
        while let Some(item) = s.next().await {
            yield (item, index);
            index += 1;
        }
    }
    .boxed()
}

/// Buffer into chunks of up to `chunk_size`, emitting early when `timeout` elapses
///
/// FS2's `groupWithin`. A chunk is emitted as soon as it is full, or when
/// `timeout` passes with at least one buffered element — so a slow producer
/// still makes progress instead of stalling until the chunk fills.
///
/// Unlike RS2's stateful group-by operators this keeps no per-key state and
/// needs no storage backend.
pub fn group_within<O>(s: RS2Stream<O>, chunk_size: usize, timeout: Duration) -> RS2Stream<Vec<O>>
where
    O: Send + 'static,
{
    let chunk_size = chunk_size.max(1);

    stream! {
        pin_mut!(s);
        let mut buf: Vec<O> = Vec::with_capacity(chunk_size);
        let mut source_done = false;

        while !source_done {
            // With an empty buffer there is nothing to time out on, so wait
            // for the first element rather than spinning on the timer.
            if buf.is_empty() {
                match s.next().await {
                    Some(item) => buf.push(item),
                    None => {
                        source_done = true;
                        continue;
                    }
                }
            }

            let deadline = tokio::time::Instant::now() + timeout;
            while buf.len() < chunk_size {
                match tokio::time::timeout_at(deadline, s.next()).await {
                    Ok(Some(item)) => buf.push(item),
                    Ok(None) => {
                        source_done = true;
                        break;
                    }
                    Err(_) => break, // timeout: emit what we have
                }
            }

            if !buf.is_empty() {
                yield std::mem::take(&mut buf);
                buf.reserve(chunk_size);
            }
        }

        if !buf.is_empty() {
            yield buf;
        }
    }
    .boxed()
}

// ================================
// Parallel Processing
// ================================

/// Parallel evaluation preserving order (parEvalMap) with automatic backpressure
///
/// Up to `concurrency` futures run at once, but results are emitted in the same
/// order as the corresponding inputs arrived. Use [`par_eval_map_unordered`] if
/// you would rather have results as soon as they are ready.
///
/// A `concurrency` of 0 is treated as 1.
///
/// # Examples
/// ```
/// use rs2_stream::rs2::*;
/// use futures_util::stream::StreamExt;
///
/// # async fn example() {
/// let s = from_iter(vec![3u64, 1, 2]);
/// // Slower items do not overtake faster ones.
/// let out = par_eval_map(s, 4, |x| async move { x }).collect::<Vec<_>>().await;
/// assert_eq!(out, vec![3, 1, 2]);
/// # }
/// ```
pub fn par_eval_map<I, O, Fut, F>(s: RS2Stream<I>, concurrency: usize, f: F) -> RS2Stream<O>
where
    F: FnMut(I) -> Fut + Send + 'static,
    Fut: Future<Output = O> + Send + 'static,
    O: Send + 'static,
    I: Send + 'static,
{
    let concurrency = concurrency.max(1);
    let buffered_stream = auto_backpressure_block(s, concurrency * 2);
    buffered_stream.map(f).buffered(concurrency).boxed()
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
    let concurrency = concurrency.max(1);
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
/// Parallel join of streams (parJoin) with automatic backpressure
///
/// Takes a stream of streams and runs up to `concurrency` of them at once,
/// emitting items as they become available and starting new inner streams as
/// others finish.
///
/// An earlier version awaited each inner stream in turn, which was fully
/// sequential — one slow inner stream blocked every other one. Measured on 4
/// inner streams of 3 items at 100ms each: 1.23s before, ~300ms after.
///
/// A `concurrency` of 0 is treated as 1.
pub fn par_join<O, S>(
    s: RS2Stream<S>,
    concurrency: usize,
) -> RS2Stream<O>
where
    S: Stream<Item = O> + Send + 'static + Unpin,
    O: Send + 'static,
{
    let concurrency = concurrency.max(1);
    let buffered_stream = auto_backpressure_block(s, concurrency * 2);
    buffered_stream.flatten_unordered(concurrency).boxed()
}

// ================================
// Resource Management
// ================================

/// Guard that guarantees a bracket's release runs exactly once.
///
/// On normal stream completion the release future is awaited via
/// [`ReleaseGuard::release_now`]. If the stream is instead dropped early —
/// `take(n)`, a `break`, an error, or the consumer simply going away — `Drop`
/// spawns the release future onto the current Tokio runtime.
struct ReleaseGuard<A, R, FRel>
where
    FRel: FnOnce(A) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    A: Send + 'static,
{
    resource: Option<A>,
    release: Option<FRel>,
}

impl<A, R, FRel> ReleaseGuard<A, R, FRel>
where
    FRel: FnOnce(A) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    A: Send + 'static,
{
    fn new(resource: A, release: FRel) -> Self {
        Self {
            resource: Some(resource),
            release: Some(release),
        }
    }

    /// Await the release future now. Disarms the `Drop` fallback.
    async fn release_now(mut self) {
        if let (Some(resource), Some(release)) = (self.resource.take(), self.release.take()) {
            release(resource).await;
        }
    }
}

impl<A, R, FRel> Drop for ReleaseGuard<A, R, FRel>
where
    FRel: FnOnce(A) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    A: Send + 'static,
{
    fn drop(&mut self) {
        if let (Some(resource), Some(release)) = (self.resource.take(), self.release.take()) {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(release(resource));
                }
                Err(_) => {
                    log::warn!(
                        "bracket: stream dropped outside a Tokio runtime; release was not run"
                    );
                }
            }
        }
    }
}

/// Bracket for simple resource handling
///
/// `release` is guaranteed to run whether the stream finishes normally or is
/// terminated early (for example by `take_rs2`, an error, or the consumer
/// dropping the stream).
///
/// Note that on the early-termination path the release future is *spawned*
/// rather than awaited, because `Drop` cannot await. It therefore completes
/// asynchronously, shortly after the stream is dropped.
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
        let guard = ReleaseGuard::new(resource.clone(), release);

        let stream = use_fn(resource);
        pin_mut!(stream);
        while let Some(item) = stream.next().await {
            yield item;
        }

        guard.release_now().await;
    }
        .boxed()
}

/// Guard for [`bracket_case`], carrying the [`ExitCase`] observed so far.
///
/// `error` is updated as the stream runs, so whichever path ends the stream —
/// normal completion, an `Err` item, or an early drop — release sees the
/// correct exit case.
struct ReleaseCaseGuard<A, E, R, FRel>
where
    FRel: FnOnce(A, ExitCase<E>) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    A: Send + 'static,
    E: Clone + Send + 'static,
{
    resource: Option<A>,
    release: Option<FRel>,
    /// The outcome, once known. Still `None` at `Drop` means the consumer
    /// stopped early, which is `ExitCase::Canceled`.
    outcome: Option<ExitCase<E>>,
}

impl<A, E, R, FRel> ReleaseCaseGuard<A, E, R, FRel>
where
    FRel: FnOnce(A, ExitCase<E>) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    A: Send + 'static,
    E: Clone + Send + 'static,
{
    fn new(resource: A, release: FRel) -> Self {
        Self {
            resource: Some(resource),
            release: Some(release),
            outcome: None,
        }
    }

    /// Await the release future now with the given exit case.
    /// Disarms the `Drop` fallback.
    async fn release_with(mut self, case: ExitCase<E>) {
        self.outcome = Some(case.clone());
        if let (Some(resource), Some(release)) = (self.resource.take(), self.release.take()) {
            release(resource, case).await;
        }
    }
}

impl<A, E, R, FRel> Drop for ReleaseCaseGuard<A, E, R, FRel>
where
    FRel: FnOnce(A, ExitCase<E>) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    A: Send + 'static,
    E: Clone + Send + 'static,
{
    fn drop(&mut self) {
        if let (Some(resource), Some(release)) = (self.resource.take(), self.release.take()) {
            // Reaching `Drop` with the resource still armed means the generator
            // was dropped before the stream reached a definite outcome.
            let case = self.outcome.take().unwrap_or(ExitCase::Canceled);
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(release(resource, case));
                }
                Err(_) => {
                    log::warn!(
                        "bracket_case: stream dropped outside a Tokio runtime; release was not run"
                    );
                }
            }
        }
    }
}

/// BracketCase with exit case semantics, mirroring FS2's `Stream.bracketCase`
///
/// Like [`bracket`], release is guaranteed to run on every termination path.
/// The [`ExitCase`] tells it which one:
///
/// - [`ExitCase::Completed`] — the stream was exhausted.
/// - [`ExitCase::Canceled`] — the consumer stopped early (`take_rs2`, a `break`,
///   or dropping the stream).
///
/// In-band `Err` items do **not** produce [`ExitCase::Errored`]; they are data,
/// as `Left` is in FS2. See the [`ExitCase`] docs for why `Errored` is currently
/// unreachable.
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
        let guard = ReleaseCaseGuard::new(resource.clone(), release);

        let stream = use_fn(resource);
        pin_mut!(stream);
        while let Some(item) = stream.next().await {
            // In-band `Err` items are data, not stream failure — the same way a
            // `Left` is in FS2. They do not change the exit case.
            yield item;
        }

        guard.release_with(ExitCase::Completed).await;
    }
        .boxed()
}

/// Like [`bracket_case`], but an `Err` item terminates the stream
///
/// This is the RS2 analogue of an FS2 stream that fails: consumption stops at
/// the first `Err`, that `Err` is emitted as the final item, and release is told
/// [`ExitCase::Errored`] carrying it.
///
/// Use this when an error means "stop"; use [`bracket_case`] when errors are
/// ordinary data that should flow through and the stream should keep going.
/// Together they make all three FS2 exit cases reachable:
///
/// | outcome | `bracket_case` | `try_bracket_case` |
/// |---|---|---|
/// | stream exhausted | `Completed` | `Completed` |
/// | consumer stopped early | `Canceled` | `Canceled` |
/// | an `Err` item | `Completed` | `Errored(e)` |
pub fn try_bracket_case<A, O, E, St, FAcq, FUse, FRel, R>(
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
        let guard = ReleaseCaseGuard::new(resource.clone(), release);

        let stream = use_fn(resource);
        pin_mut!(stream);

        let mut failure: Option<E> = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(value) => yield Ok(value),
                Err(e) => {
                    failure = Some(e.clone());
                    yield Err(e);
                    break;
                }
            }
        }

        match failure {
            Some(e) => guard.release_with(ExitCase::Errored(e)).await,
            None => guard.release_with(ExitCase::Completed).await,
        }
    }
        .boxed()
}

/// Run an action when the stream ends, however it ends
///
/// FS2's `onFinalize`. The action runs on exhaustion and on early termination
/// alike — `take_rs2`, a `break`, or the consumer dropping the stream. It is
/// [`bracket`] without a resource.
///
/// As with [`bracket`], on the early-termination path the action is *spawned*
/// rather than awaited, because `Drop` cannot await.
pub fn on_finalize<O, F, Fut>(s: RS2Stream<O>, f: F) -> RS2Stream<O>
where
    O: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    bracket(async {}, move |()| s, move |()| f())
}

/// Run an action when the stream ends, told how it ended
///
/// FS2's `onFinalizeCase`. The exit case is [`ExitCase::Completed`] if the
/// stream was exhausted and [`ExitCase::Canceled`] if the consumer stopped
/// early. There is no `Errored` here: a plain stream cannot fail — see
/// [`try_bracket_case`] for the erroring variant.
pub fn on_finalize_case<O, F, Fut>(s: RS2Stream<O>, f: F) -> RS2Stream<O>
where
    O: Send + 'static,
    F: FnOnce(ExitCase<()>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    stream! {
        let guard = ReleaseCaseGuard::new((), move |(), case: ExitCase<()>| f(case));
        pin_mut!(s);
        while let Some(item) = s.next().await {
            yield item;
        }
        guard.release_with(ExitCase::Completed).await;
    }
        .boxed()
}

// ================================
// Stream Extensions
// ================================

// Re-export the extension traits from their respective modules
pub use crate::rs2_result_stream_ext::RS2ResultStreamExt;
pub use crate::rs2_stream_ext::RS2StreamExt;
