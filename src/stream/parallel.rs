use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::collections::{VecDeque, HashMap, BTreeMap};
use std::time::{Duration, Instant};

use super::core::Stream;
use crate::stream::core::StreamExt;

/// Configuration for parallel execution
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    pub concurrency: usize,
    pub max_buffer_size: Option<usize>,
    pub timeout: Option<Duration>,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            concurrency: num_cpus::get(),
            max_buffer_size: Some(1000),
            timeout: Some(Duration::from_secs(30)),
        }
    }
}

/// Parallel evaluation map that preserves order using HashMap + VecDeque
pub struct ParEvalMap<S, F, O> {
    source: Pin<Box<S>>,
    f: F,
    config: ParallelConfig,
    // HashMap for O(1) lookup + VecDeque for ordering
    in_flight: HashMap<usize, (Pin<Box<dyn Future<Output = O> + Send>>, Instant)>,
    completed: HashMap<usize, O>,
    emit_queue: VecDeque<usize>, // Track emission order
    next_index: usize,
    next_emit_index: usize,
    source_done: bool,
    total_buffered: usize,
}

impl<S, F, I, O, Fut> ParEvalMap<S, F, O>
where
    S: Stream<Item = I> + Send + 'static,
    F: FnMut(I) -> Fut + Send + 'static + Unpin,
    Fut: Future<Output = O> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static + Unpin,
{
    pub fn new(source: S, f: F, config: ParallelConfig) -> Self {
        let config = if config.concurrency == 0 {
            ParallelConfig { concurrency: 1, ..config }
        } else {
            config
        };

        Self {
            source: Box::pin(source),
            f,
            config,
            in_flight: HashMap::new(),
            completed: HashMap::new(),
            emit_queue: VecDeque::new(),
            next_index: 0,
            next_emit_index: 0,
            source_done: false,
            total_buffered: 0,
        }
    }

    fn handle_timeouts(&mut self) {
        if let Some(timeout_duration) = self.config.timeout {
            let now = Instant::now();
            let expired: Vec<usize> = self.in_flight
                .iter()
                .filter_map(|(index, (_, start_time))| {
                    if now.duration_since(*start_time) > timeout_duration {
                        Some(*index)
                    } else {
                        None
                    }
                })
                .collect();

            for index in expired {
                self.in_flight.remove(&index);
                self.total_buffered = self.total_buffered.saturating_sub(1);
                // Skip this index in emission order
                if index == self.next_emit_index {
                    self.next_emit_index += 1;
                }
            }
        }
    }

    fn is_buffer_full(&self) -> bool {
        if let Some(max_size) = self.config.max_buffer_size {
            self.total_buffered >= max_size
        } else {
            false
        }
    }

    fn try_emit_next(&mut self) -> Option<O> {
        if let Some(result) = self.completed.remove(&self.next_emit_index) {
            self.next_emit_index += 1;
            self.total_buffered = self.total_buffered.saturating_sub(1);
            Some(result)
        } else {
            None
        }
    }
}

impl<S, F, I, O, Fut> Stream for ParEvalMap<S, F, O>
where
    S: Stream<Item = I> + Send + 'static,
    F: FnMut(I) -> Fut + Send + 'static + Unpin,
    Fut: Future<Output = O> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static + Unpin,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.handle_timeouts();

        if let Some(result) = this.try_emit_next() {
            return Poll::Ready(Some(result));
        }

        // Fill up in-flight futures
        while this.in_flight.len() < this.config.concurrency
            && !this.source_done
            && !this.is_buffer_full()
        {
            match this.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let index = this.next_index;
                    this.next_index += 1;

                    let fut = (this.f)(item);
                    this.in_flight.insert(index, (Box::pin(fut), Instant::now()));
                    this.total_buffered += 1;
                }
                Poll::Ready(None) => {
                    this.source_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Poll in-flight futures
        let mut completed_futures = Vec::new();

        for (index, (fut, _start_time)) in &mut this.in_flight {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    completed_futures.push((*index, result));
                }
                Poll::Pending => {}
            }
        }

        // Move completed futures to completed map
        for (index, result) in completed_futures {
            this.in_flight.remove(&index);
            this.completed.insert(index, result);
        }

        // Try to emit after polling
        if let Some(result) = this.try_emit_next() {
            return Poll::Ready(Some(result));
        }

        if this.source_done && this.in_flight.is_empty() && this.completed.is_empty() {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

/// Simple parallel map for CPU-bound operations (non-async)
pub struct ParMap<S, F, O> 
where
    S: Stream,
{
    source: Pin<Box<S>>,
    f: F,
    config: ParallelConfig,
    buffer: VecDeque<O>,
    worker_handle: Option<tokio::task::JoinHandle<Vec<O>>>,
    batch: Vec<S::Item>,
    source_done: bool,
}

impl<S, F, I, O> ParMap<S, F, O>
where
    S: Stream<Item = I> + Send + 'static,
    F: Fn(I) -> O + Send + Sync + Clone + 'static + Unpin,
    I: Send + 'static + Unpin,
    O: Send + 'static + Unpin,
{
    pub fn new(source: S, f: F, config: ParallelConfig) -> Self {
        Self {
            source: Box::pin(source),
            f,
            config,
            buffer: VecDeque::new(),
            worker_handle: None,
            batch: Vec::new(),
            source_done: false,
        }
    }

    fn start_batch_processing(&mut self) {
        if !self.batch.is_empty() && self.worker_handle.is_none() {
            let batch = std::mem::take(&mut self.batch);
            let f = self.f.clone();

            self.worker_handle = Some(tokio::task::spawn_blocking(move || {
                batch.into_iter().map(f).collect()
            }));
        }
    }
}

impl<S, F, I, O> Stream for ParMap<S, F, O>
where
    S: Stream<Item = I> + Send + 'static,
    F: Fn(I) -> O + Send + Sync + Clone + 'static + Unpin,
    I: Send + 'static + Unpin,
    O: Send + 'static + Unpin,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Return buffered items first
        if let Some(item) = this.buffer.pop_front() {
            return Poll::Ready(Some(item));
        }

        // Check if worker is done
        if let Some(mut handle) = this.worker_handle.take() {
            match Pin::new(&mut handle).poll(cx) {
                Poll::Ready(Ok(results)) => {
                    this.buffer.extend(results);
                    if let Some(item) = this.buffer.pop_front() {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(Err(_)) => {
                    // Worker panicked, skip this batch
                }
                Poll::Pending => {
                    this.worker_handle = Some(handle);
                    return Poll::Pending;
                }
            }
        }

        // Collect items for batch processing
        let batch_size = this.config.concurrency * 10;
        while this.batch.len() < batch_size && !this.source_done {
            match this.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.batch.push(item);
                }
                Poll::Ready(None) => {
                    this.source_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        if this.batch.len() >= batch_size || (this.source_done && !this.batch.is_empty()) {
            this.start_batch_processing();
            return Poll::Pending;
        }

        if this.source_done && this.batch.is_empty() && this.worker_handle.is_none() {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

/// Enhanced trait extension for parallel operations
pub trait ParallelStreamExt: Stream + Sized + Send + 'static {
    /// Parallel async map that preserves order
    fn par_eval_map<F, Fut, O>(self, concurrency: usize, f: F) -> ParEvalMap<Self, F, O>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = O> + Send + 'static,
        Self::Item: Send + 'static,
        O: Send + 'static + Unpin,
    {
        let config = ParallelConfig {
            concurrency,
            ..Default::default()
        };
        ParEvalMap::new(self, f, config)
    }

    /// Parallel async map with configuration
    fn par_eval_map_config<F, Fut, O>(
        self,
        config: ParallelConfig,
        f: F
    ) -> ParEvalMap<Self, F, O>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = O> + Send + 'static,
        Self::Item: Send + 'static,
        O: Send + 'static + Unpin,
    {
        ParEvalMap::new(self, f, config)
    }

    /// Parallel CPU-bound map using thread pool
    fn par_map<F, O>(self, concurrency: usize, f: F) -> ParMap<Self, F, O>
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        O: Send + 'static + Unpin,
    {
        let config = ParallelConfig {
            concurrency,
            ..Default::default()
        };
        ParMap::new(self, f, config)
    }

    /// Parallel async map that doesn't preserve order
    fn par_eval_map_unordered<F, Fut, O>(self, concurrency: usize, f: F) -> ParEvalMapUnordered<Self, F, O>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static + Unpin,
        Fut: Future<Output = O> + Send + 'static,
        Self::Item: Send + 'static,
        O: Send + 'static + Unpin,
    {
        let config = ParallelConfig {
            concurrency,
            ..Default::default()
        };
        ParEvalMapUnordered::new(self, f, config)
    }
}

/// Unordered parallel evaluation map using our own implementation
pub struct ParEvalMapUnordered<S, F, O> {
    source: Pin<Box<S>>,
    f: F,
    config: ParallelConfig,
    in_flight: Vec<(Pin<Box<dyn Future<Output = O> + Send>>, Instant)>,
    completed: VecDeque<O>,
    source_done: bool,
    total_buffered: usize,
}

impl<S, F, I, O, Fut> ParEvalMapUnordered<S, F, O>
where
    S: Stream<Item = I> + Send + 'static,
    F: FnMut(I) -> Fut + Send + 'static + Unpin,
    Fut: Future<Output = O> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static + Unpin,
{
    pub fn new(source: S, f: F, config: ParallelConfig) -> Self {
        let config = if config.concurrency == 0 {
            ParallelConfig { concurrency: 1, ..config }
        } else {
            config
        };

        Self {
            source: Box::pin(source),
            f,
            config,
            in_flight: Vec::new(),
            completed: VecDeque::new(),
            source_done: false,
            total_buffered: 0,
        }
    }

    fn handle_timeouts(&mut self) {
        if let Some(timeout_duration) = self.config.timeout {
            let now = Instant::now();
            self.in_flight.retain(|(_, start_time)| {
                now.duration_since(*start_time) <= timeout_duration
            });
            // Recalculate total_buffered after removing timed out futures
            self.total_buffered = self.in_flight.len() + self.completed.len();
        }
    }

    fn is_buffer_full(&self) -> bool {
        if let Some(max_size) = self.config.max_buffer_size {
            self.total_buffered >= max_size
        } else {
            false
        }
    }
}

impl<S, F, I, O, Fut> Stream for ParEvalMapUnordered<S, F, O>
where
    S: Stream<Item = I> + Send + 'static,
    F: FnMut(I) -> Fut + Send + 'static + Unpin,
    Fut: Future<Output = O> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static + Unpin,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.handle_timeouts();

        // Return completed items first
        if let Some(result) = this.completed.pop_front() {
            this.total_buffered = this.total_buffered.saturating_sub(1);
            return Poll::Ready(Some(result));
        }

        // Fill up in-flight futures first
        while this.in_flight.len() < this.config.concurrency
            && !this.source_done
            && !this.is_buffer_full()
        {
            match this.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let fut = (this.f)(item);
                    this.in_flight.push((Box::pin(fut), Instant::now()));
                    this.total_buffered += 1;
                }
                Poll::Ready(None) => {
                    this.source_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Poll in-flight futures
        let mut completed_futures = Vec::new();
        
        for (i, (fut, _start_time)) in this.in_flight.iter_mut().enumerate() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    completed_futures.push(i);
                    this.completed.push_back(result);
                }
                Poll::Pending => {}
            }
        }

        // Remove completed futures (in reverse order to maintain indices)
        for &index in completed_futures.iter().rev() {
            this.in_flight.remove(index);
        }

        // Return completed items if any
        if let Some(result) = this.completed.pop_front() {
            this.total_buffered = this.total_buffered.saturating_sub(1);
            return Poll::Ready(Some(result));
        }

        // Check if we're done
        if this.source_done && this.in_flight.is_empty() && this.completed.is_empty() {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

impl<S> ParallelStreamExt for S where S: Stream + Send + 'static {}
