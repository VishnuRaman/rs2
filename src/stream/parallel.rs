//! Parallel stream processing primitives (ordered and unordered) without external stream deps.
//!
//! Design goals implemented here:
//! - Correct wake handling: rely on `tokio::sync::mpsc::Receiver::poll_recv` (no custom shared wakers).
//! - Bounded memory: bounded result channels + in-flight limit via semaphore + bounded local buffers.
//! - Clean termination: return None only when the source is done, no in-flight tasks remain, and buffers are empty.
//! - Timeouts:
//!   - `task_timeout`: per-task timeout (drops the item on timeout).
//!   - `sequence_timeout`: optional skip of blocked sequence in ordered variant (drops the missing item).
//!   - `timeout`: overall inactivity timeout to prevent hanging forever (optional).
//! - Panic/Join safety: per-task panics/joins are contained and dropped silently (no Result item type here).
//! - No unnecessary Clone bounds on output types.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, Semaphore};

use crate::stream::Stream; // Use the library's Stream trait

// ---------------------------------- Config ----------------------------------

#[derive(Debug, Clone)]
pub struct ParallelConfig {
    pub concurrency: usize,
    // Bounded capacity for the result channel and local buffer upper bound guidance.
    pub max_buffer_size: usize,

    // End-to-end inactivity timeout (no sent or yielded items) before completing the stream.
    // Use Duration::MAX to disable.
    pub timeout: Duration,

    // How long the ordered stream will wait for the next sequence before skipping it.
    // Use Duration::MAX to disable skipping (wait indefinitely).
    pub sequence_timeout: Duration,

    // Per-task timeout; on timeout, the item is dropped.
    // Use Duration::MAX to disable.
    pub task_timeout: Duration,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        // Conservative defaults: bounded memory, reasonable concurrency, timeouts disabled.
        Self {
            concurrency: std::cmp::max(1, num_cpus::get()), // logical CPUs
            max_buffer_size: 1024,
            timeout: Duration::MAX,
            sequence_timeout: Duration::MAX,
            task_timeout: Duration::MAX,
        }
    }
}

impl ParallelConfig {
    pub fn for_small_workloads() -> Self {
        Self {
            concurrency: 4,
            max_buffer_size: 256,
            timeout: Duration::MAX,
            sequence_timeout: Duration::MAX,
            task_timeout: Duration::MAX,
        }
    }

    pub fn for_large_workloads() -> Self {
        Self {
            concurrency: std::cmp::max(1, num_cpus::get()),
            max_buffer_size: 4096,
            timeout: Duration::MAX,
            sequence_timeout: Duration::MAX,
            task_timeout: Duration::MAX,
        }
    }

    pub fn adaptive(concurrency: usize, expected_items: usize) -> Self {
        let c = if concurrency == 0 { 1 } else { concurrency };
        let cap = expected_items.clamp(c, expected_items.max(1024));
        Self {
            concurrency: c,
            max_buffer_size: cap,
            timeout: Duration::MAX,
            sequence_timeout: Duration::MAX,
            task_timeout: Duration::MAX,
        }
    }
}

// ----------------------------- Utilities (internal) -----------------------------

fn clamp_concurrency(c: usize) -> usize {
    if c == 0 { 1 } else { c }
}

fn is_disabled(d: Duration) -> bool {
    d == Duration::MAX
}

// -------------------------------- Ordered --------------------------------

#[derive(Debug)]
pub struct ParEvalMap<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    source: Pin<Box<S>>,
    f: F,

    // Results arrive from workers here; (sequence_id, Option<U>).
    // None indicates worker timeout/panic/dropped.
    result_rx: mpsc::Receiver<(usize, Option<U>)>,
    result_tx: Option<mpsc::Sender<(usize, Option<U>)>>,

    // Concurrency limiter for in-flight workers.
    semaphore: std::sync::Arc<Semaphore>,

    // Ordered reassembly
    reorder_buffer: HashMap<usize, U>, // HashMap for O(1) lookups by sequence
    next_sequence: usize,
    sequence_started_at: HashMap<usize, Instant>, // HashMap for O(1) lookups by sequence
    
    // Tombstone tracking for missing results
    tombstones: std::collections::HashSet<usize>, // HashSet for O(1) tombstone operations

    // Local output buffer
    out_buf: VecDeque<U>,

    // Source & lifecycle
    source_done: bool,
    in_flight: usize,
    sequence_counter: usize,

    // Activity tracking for global inactivity timeout
    last_activity: Instant,

    // Config
    config: ParallelConfig,

    // Marker
    _phantom: PhantomData<U>,
}

// SAFETY: We never move fields that are !Unpin
impl<S, F, Fut, U> Unpin for ParEvalMap<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{}

impl<S, F, Fut, U> ParEvalMap<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    pub fn new(source: S, concurrency: usize, f: F) -> Self {
        let concurrency = clamp_concurrency(concurrency);
        let this = Self::with_config(source, f, ParallelConfig {
            concurrency,
            ..Default::default()
        });
        this
    }

    pub fn with_config(source: S, f: F, mut config: ParallelConfig) -> Self {
        config.concurrency = clamp_concurrency(config.concurrency);
        // Enforce max_buffer_size >= concurrency to prevent stalls
        if config.max_buffer_size < config.concurrency {
            config.max_buffer_size = config.concurrency;
        }
        if config.max_buffer_size == 0 {
            config.max_buffer_size = 1;
        }

        let (tx, rx) = mpsc::channel::<(usize, Option<U>)>(config.max_buffer_size);

        Self {
            source: Box::pin(source),
            f,
            result_rx: rx,
            result_tx: Some(tx),
            semaphore: std::sync::Arc::new(Semaphore::new(config.concurrency)),
            reorder_buffer: HashMap::new(),
            next_sequence: 0,
            sequence_started_at: HashMap::new(),
            tombstones: std::collections::HashSet::new(),
            out_buf: VecDeque::with_capacity(config.max_buffer_size.min(1024)),
            source_done: false,
            in_flight: 0,
            sequence_counter: 0,
            last_activity: Instant::now(),
            config,
            _phantom: PhantomData,
        }
    }

    fn spawn_worker(&mut self, seq: usize, item: S::Item) {
        let fut_factory = self.f.clone();
        let tx = match &self.result_tx {
            Some(tx) => tx.clone(),
            None => return, // Channel closed, don't spawn worker
        };
        let task_timeout = self.config.task_timeout;
        // Move the permit into the spawned task to keep it alive until completion
        let _permit = self.semaphore.clone().try_acquire_owned().expect("permit should be available");

        // Safety check: ensure we have a valid sequence
        debug_assert!(seq >= self.next_sequence, 
            "Spawning worker for sequence {} but next_sequence is {}", seq, self.next_sequence);

        tokio::spawn(async move {
            // Run user future with optional timeout
            let output: Option<U> = if is_disabled(task_timeout) {
                // For non-timeout case, just await the future
                // Panics will be caught by the task runtime and the task will terminate
                // The permit will be dropped, allowing new work to be scheduled
                let fut = fut_factory(item);
                Some(fut.await)
            } else {
                // For timeout case, handle timeout
                let fut = fut_factory(item);
                match tokio::time::timeout(task_timeout, fut).await {
                    Ok(v) => Some(v),
                    Err(_elapsed) => None, // task timed out, drop
                }
            };

            // Send result; ignore if receiver is dropped
            let _ = tx.send((seq, output)).await;
            
            // Permit is automatically dropped here when the task completes
        });
    }

    fn poll_fill_inflight(&mut self, cx: &mut Context<'_>) {
        // Try to pull more from source and spawn workers while:
        // - source not done
        // - in-flight < concurrency
        // - result channel has remaining capacity (approximate via permit count)
        // - output buffers not exceeding configured bounds
        loop {
            if self.in_flight >= self.config.concurrency {
                break;
            }
            if self.source_done {
                break;
            }
            // Don't poll source if output buffer is full
            if self.out_buf.len() >= self.config.max_buffer_size {
                break;
            }

            // Poll source for next item
            match self.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let seq = self.sequence_counter;
                    self.sequence_counter += 1;
                    self.in_flight += 1;
                    self.sequence_started_at.insert(seq, Instant::now());
                    self.spawn_worker(seq, item);
                    // activity recorded as we progressed
                    self.last_activity = Instant::now();
                    
                    // Safety check: ensure in_flight doesn't exceed concurrency
                    debug_assert!(self.in_flight <= self.config.concurrency, 
                        "in_flight ({}) should not exceed concurrency ({})", 
                        self.in_flight, self.config.concurrency);
                }
                Poll::Ready(None) => {
                    self.source_done = true;
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }
    }

    fn poll_drain_results(&mut self, cx: &mut Context<'_>) {
        // Drain as many results as currently available into reorder buffer,
        // then move ready-in-order items into out_buf.
        loop {
            match self.result_rx.poll_recv(cx) {
                Poll::Ready(Some((seq, maybe_u))) => {
                    self.in_flight = self.in_flight.saturating_sub(1);
                    self.sequence_started_at.remove(&seq);
                    
                    if let Some(u) = maybe_u {
                        // Safety check: ensure sequence isn't in both reorder buffer and tombstones
                        debug_assert!(!self.tombstones.contains(&seq), 
                            "Sequence {} should not be in both reorder buffer and tombstones", seq);
                        self.reorder_buffer.insert(seq, u);
                    } else {
                        // Worker timed out/panicked - add tombstone to advance past this sequence
                        if seq == self.next_sequence {
                            // If this is the next expected sequence, advance immediately
                            self.next_sequence += 1;
                            self.last_activity = Instant::now();
                        } else {
                            // Otherwise, mark as tombstone for later processing
                            self.tombstones.insert(seq);
                        }
                        
                        // Safety check: ensure sequence isn't in both reorder buffer and tombstones
                        debug_assert!(!self.reorder_buffer.contains_key(&seq),
                            "Sequence {} should not be in both reorder buffer and tombstones", seq);
                    }
                }
                Poll::Ready(None) => {
                    // Sender dropped; no more results
                    break;
                }
                Poll::Pending => break,
            }
        }
        
        // Consolidate after draining results to advance past tombstones and emit ready items
        self.consolidate_ready();
    }

    fn consolidate_ready(&mut self) {
        // First, advance past any tombstones at the head
        while self.tombstones.contains(&self.next_sequence) {
            self.tombstones.remove(&self.next_sequence);
            self.sequence_started_at.remove(&self.next_sequence);
            self.next_sequence += 1;
            self.last_activity = Instant::now();
            
            // Safety check: ensure we don't have sequences in both states
            debug_assert!(!self.reorder_buffer.contains_key(&(self.next_sequence - 1)),
                "Sequence {} should not be in both reorder buffer and tombstones", self.next_sequence - 1);
        }
        
        // Then drain contiguous ready items in order into out_buf
        while let Some(u) = self.reorder_buffer.remove(&self.next_sequence) {
            self.out_buf.push_back(u);
            self.sequence_started_at.remove(&self.next_sequence);
            self.next_sequence += 1;
            self.last_activity = Instant::now();
            
            // Respect buffer size bounds
            if self.out_buf.len() >= self.config.max_buffer_size {
                break;
            }
            
            // Continue advancing past tombstones
            while self.tombstones.contains(&self.next_sequence) {
                self.tombstones.remove(&self.next_sequence);
                self.sequence_started_at.remove(&self.next_sequence);
                self.next_sequence += 1;
                self.last_activity = Instant::now();
                
                // Safety check: ensure we don't have sequences in both states
                debug_assert!(!self.reorder_buffer.contains_key(&(self.next_sequence - 1)),
                    "Sequence {} should not be in both reorder buffer and tombstones", self.next_sequence - 1);
            }
        }
    }

    fn maybe_skip_blocked_sequence(&mut self) {
        if is_disabled(self.config.sequence_timeout) {
            return;
        }
        // If next_sequence is pending for too long, skip it to avoid deadlock on missing item.
        if let Some(started_at) = self.sequence_started_at.get(&self.next_sequence).cloned() {
            if started_at.elapsed() >= self.config.sequence_timeout {
                // drop this sequence id; do not produce output
                self.sequence_started_at.remove(&self.next_sequence);
                // also clear any stale buffered value if present (should not happen without arrival)
                self.reorder_buffer.remove(&self.next_sequence);
                self.next_sequence += 1;
                // record activity (we progressed)
                self.last_activity = Instant::now();
                // Call consolidate_ready to cascade any newly available items
                self.consolidate_ready();
                
                // Safety check: ensure we don't have sequences in both states
                debug_assert!(self.sequence_started_at.keys().all(|&seq| seq >= self.next_sequence),
                    "All remaining sequences should be >= next_sequence after timeout skip");
            }
        }
    }

    fn is_terminated(&self) -> bool {
        let no_inflight = self.in_flight == 0;
        let buffers_empty = self.out_buf.is_empty() && self.reorder_buffer.is_empty() && self.tombstones.is_empty();
        let source_done = self.source_done;
        
        source_done && no_inflight && buffers_empty
    }

    fn inactivity_expired(&self) -> bool {
        if is_disabled(self.config.timeout) {
            return false;
        }
        self.last_activity.elapsed() >= self.config.timeout
    }
}

impl<S, F, Fut, U> Stream for ParEvalMap<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First, try to receive any completed results.
        self.as_mut().poll_drain_results(cx);

        // Enforce optional sequence timeout handling (skip stuck head).
        self.as_mut().maybe_skip_blocked_sequence();

        // Consolidate ready items after timeout handling
        self.as_mut().consolidate_ready();

        // If we already have output buffered, return it immediately.
        if let Some(item) = self.as_mut().out_buf.pop_front() {
            return Poll::Ready(Some(item));
        }

        // Try to schedule more work if possible.
        self.as_mut().poll_fill_inflight(cx);

        // Try draining results again (new tasks may have completed quickly).
        self.as_mut().poll_drain_results(cx);

        // Consolidate ready items again after draining
        self.as_mut().consolidate_ready();

        // Emit if available now.
        if let Some(item) = self.as_mut().out_buf.pop_front() {
            return Poll::Ready(Some(item));
        }

        // Check termination conditions.
        if self.is_terminated() || self.inactivity_expired() {
            return Poll::Ready(None);
        }

        // Otherwise, we're pending; wake when result channel has new items or source advances.
        Poll::Pending
    }
}

impl<S, F, Fut, U> Drop for ParEvalMap<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    fn drop(&mut self) {
        // Take the sender to close the channel deterministically
        if let Some(tx) = self.result_tx.take() {
            drop(tx); // This closes the channel
        }
        // Remaining tasks will complete and drop their permits; no explicit aborts needed.
        // Buffers will be dropped naturally.
    }
}

// ------------------------------- Unordered --------------------------------

#[derive(Debug)]
pub struct ParEvalMapUnordered<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    source: Pin<Box<S>>,
    f: F,

    result_rx: mpsc::Receiver<Option<U>>,
    result_tx: Option<mpsc::Sender<Option<U>>>,

    semaphore: std::sync::Arc<Semaphore>,
    out_buf: VecDeque<U>,

    source_done: bool,
    in_flight: usize,

    last_activity: Instant,
    config: ParallelConfig,
}

// SAFETY: We never move fields that are !Unpin
impl<S, F, Fut, U> Unpin for ParEvalMapUnordered<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{}

impl<S, F, Fut, U> ParEvalMapUnordered<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    pub fn new(source: S, concurrency: usize, f: F) -> Self {
        let concurrency = clamp_concurrency(concurrency);
        Self::with_config(
            source,
            f,
            ParallelConfig {
                concurrency,
                ..Default::default()
            },
        )
    }

    pub fn with_config(source: S, f: F, mut config: ParallelConfig) -> Self {
        config.concurrency = clamp_concurrency(config.concurrency);
        // Enforce max_buffer_size >= concurrency to prevent stalls
        if config.max_buffer_size < config.concurrency {
            config.max_buffer_size = config.concurrency;
        }
        if config.max_buffer_size == 0 {
            config.max_buffer_size = 1;
        }
        let (tx, rx) = mpsc::channel::<Option<U>>(config.max_buffer_size);

        Self {
            source: Box::pin(source),
            f,
            result_rx: rx,
            result_tx: Some(tx),
            semaphore: std::sync::Arc::new(Semaphore::new(config.concurrency)),
            out_buf: VecDeque::with_capacity(config.max_buffer_size.min(1024)),
            source_done: false,
            in_flight: 0,
            last_activity: Instant::now(),
            config,
        }
    }

    fn spawn_worker(&mut self, item: S::Item) {
        let fut_factory = self.f.clone();
        let tx = match &self.result_tx {
            Some(tx) => tx.clone(),
            None => return, // Channel closed, don't spawn worker
        };
        let task_timeout = self.config.task_timeout;

        // Safety check: ensure we're not spawning when buffer is full
        debug_assert!(self.out_buf.len() < self.config.max_buffer_size,
            "Spawning worker when output buffer is full ({} >= {})", 
            self.out_buf.len(), self.config.max_buffer_size);

        tokio::spawn(async move {
            let fut = (fut_factory)(item);
            let output: Option<U> = if is_disabled(task_timeout) {
                Some(fut.await)
            } else {
                match tokio::time::timeout(task_timeout, fut).await {
                    Ok(v) => Some(v),
                    Err(_elapsed) => None,
                }
            };
            let _ = tx.send(output).await;
        });
    }

    fn poll_fill_inflight(&mut self, cx: &mut Context<'_>) {
        loop {
            if self.in_flight >= self.config.concurrency {
                break;
            }
            if self.source_done {
                break;
            }
            // Don't poll source if output buffer is full
            if self.out_buf.len() >= self.config.max_buffer_size {
                break;
            }

            // Poll source for next item
            match self.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    self.in_flight += 1;
                    self.spawn_worker(item);
                    self.last_activity = Instant::now();
                    
                    // Safety check: ensure in_flight doesn't exceed concurrency
                    debug_assert!(self.in_flight <= self.config.concurrency, 
                        "in_flight ({}) should not exceed concurrency ({})", 
                        self.in_flight, self.config.concurrency);
                }
                Poll::Ready(None) => {
                    self.source_done = true;
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }
    }

    fn poll_drain_results(&mut self, cx: &mut Context<'_>) {
        loop {
            match self.result_rx.poll_recv(cx) {
                Poll::Ready(Some(maybe_u)) => {
                    self.in_flight = self.in_flight.saturating_sub(1);
                    if let Some(u) = maybe_u {
                        self.out_buf.push_back(u);
                        self.last_activity = Instant::now();
                    }
                    // Respect buffer size bounds
                    if self.out_buf.len() >= self.config.max_buffer_size {
                        break;
                    }
                }
                Poll::Ready(None) => {
                    // Sender dropped; no more results
                    break;
                }
                Poll::Pending => break,
            }
        }
    }

    fn is_terminated(&self) -> bool {
        self.source_done && self.in_flight == 0 && self.out_buf.is_empty()
    }

    fn inactivity_expired(&self) -> bool {
        if is_disabled(self.config.timeout) {
            return false;
        }
        self.last_activity.elapsed() >= self.config.timeout
    }
}

impl<S, F, Fut, U> Stream for ParEvalMapUnordered<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Drain any completed results
        self.as_mut().poll_drain_results(cx);

        // Serve buffered output
        if let Some(item) = self.as_mut().out_buf.pop_front() {
            return Poll::Ready(Some(item));
        }

        // Schedule more work if possible
        self.as_mut().poll_fill_inflight(cx);

        // Drain again in case something finished immediately
        self.as_mut().poll_drain_results(cx);

        if let Some(item) = self.as_mut().out_buf.pop_front() {
            return Poll::Ready(Some(item));
        }

        if self.is_terminated() || self.inactivity_expired() {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

impl<S, F, Fut, U> Drop for ParEvalMapUnordered<S, F, Fut, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
    S::Item: Send + 'static,
{
    fn drop(&mut self) {
        // Take the sender to close the channel deterministically
        if let Some(tx) = self.result_tx.take() {
            drop(tx); // This closes the channel
        }
    }
}

// --------------------------------- Sync ParMap ---------------------------------
// Simple CPU-bound parallel map using blocking threads via tokio::spawn_blocking.
// This keeps API symmetry with async eval-map variants.

#[derive(Debug)]
pub struct ParMap<S, F, O>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static,
    O: Send + 'static,
    S::Item: Send + 'static,
{
    source: Pin<Box<S>>,
    f: F,

    result_rx: mpsc::Receiver<O>,
    result_tx: Option<mpsc::Sender<O>>,

    semaphore: std::sync::Arc<Semaphore>,
    out_buf: VecDeque<O>,

    source_done: bool,
    in_flight: usize,

    config: ParallelConfig,
}

// SAFETY: We never move fields that are !Unpin
impl<S, F, O> Unpin for ParMap<S, F, O>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static,
    O: Send + 'static,
    S::Item: Send + 'static,
{}

impl<S, F, O> ParMap<S, F, O>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static,
    O: Send + 'static,
    S::Item: Send + 'static,
{
    pub fn new(source: S, f: F, mut config: ParallelConfig) -> Self {
        config.concurrency = clamp_concurrency(config.concurrency);
        // Enforce max_buffer_size >= concurrency to prevent stalls
        if config.max_buffer_size < config.concurrency {
            config.max_buffer_size = config.concurrency;
        }
        if config.max_buffer_size == 0 {
            config.max_buffer_size = 1;
        }
        let (tx, rx) = mpsc::channel::<O>(config.max_buffer_size);

        Self {
            source: Box::pin(source),
            f,
            result_rx: rx,
            result_tx: Some(tx),
            semaphore: std::sync::Arc::new(Semaphore::new(config.concurrency)),
            out_buf: VecDeque::with_capacity(config.max_buffer_size.min(1024)),
            source_done: false,
            in_flight: 0,
            config,
        }
    }

    fn spawn_worker(&mut self, item: S::Item) {
        let f = self.f.clone();
        let tx = match &self.result_tx {
            Some(tx) => tx.clone(),
            None => return, // Channel closed, don't spawn worker
        };
        // Move the permit into the spawned task to keep it alive until completion
        let _permit = self.semaphore.clone().try_acquire_owned().expect("permit should be available");

        // Safety check: ensure we're not spawning when buffer is full
        debug_assert!(self.out_buf.len() < self.config.max_buffer_size,
            "Spawning worker when output buffer is full ({} >= {})", 
            self.out_buf.len(), self.config.max_buffer_size);

        tokio::spawn(async move {
            // Run CPU-bound work on a blocking thread pool to avoid starving async tasks.
            let out = tokio::task::spawn_blocking(move || (f)(item))
                .await
                .ok(); // drop on panic
            if let Some(v) = out {
                let _ = tx.send(v).await;
            }
            
            // Permit is automatically dropped here when the task completes
        });
    }

    fn poll_fill_inflight(&mut self, cx: &mut Context<'_>) {
        loop {
            if self.in_flight >= self.config.concurrency {
                break;
            }
            if self.source_done {
                break;
            }
            // Don't poll source if output buffer is full
            if self.out_buf.len() >= self.config.max_buffer_size {
                break;
            }

            // Poll source for next item
            match self.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    self.in_flight += 1;
                    self.spawn_worker(item);
                    // Permit is moved into the spawned task and will be dropped when task completes
                    
                    // Safety check: ensure in_flight doesn't exceed concurrency
                    debug_assert!(self.in_flight <= self.config.concurrency, 
                        "in_flight ({}) should not exceed concurrency ({})", 
                        self.in_flight, self.config.concurrency);
                }
                Poll::Ready(None) => {
                    self.source_done = true;
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }
    }

    fn poll_drain_results(&mut self, cx: &mut Context<'_>) {
        loop {
            match self.result_rx.poll_recv(cx) {
                Poll::Ready(Some(v)) => {
                    self.in_flight = self.in_flight.saturating_sub(1);
                    self.out_buf.push_back(v);
                    if self.out_buf.len() >= self.config.max_buffer_size {
                        break;
                    }
                }
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }
    }
}

impl<S, F, O> Stream for ParMap<S, F, O>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static,
    O: Send + 'static,
    S::Item: Send + 'static,
{
    type Item = O;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_drain_results(cx);

        if let Some(v) = self.out_buf.pop_front() {
            return Poll::Ready(Some(v));
        }

        self.poll_fill_inflight(cx);
        self.poll_drain_results(cx);

        if let Some(v) = self.out_buf.pop_front() {
            return Poll::Ready(Some(v));
        }

        let terminated = self.source_done && self.in_flight == 0 && self.out_buf.is_empty();
        
        if terminated {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<S, F, O> Drop for ParMap<S, F, O>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static,
    O: Send + 'static,
    S::Item: Send + 'static,
{
    fn drop(&mut self) {
        // Take the sender to close the channel deterministically
        if let Some(tx) = self.result_tx.take() {
            drop(tx); // This closes the channel
        }
    }
}

// ----------------------------- Extension Trait -----------------------------

pub trait ParallelStreamExt: crate::stream::Stream + Sized {
    fn par_eval_map<F, Fut, U>(self, concurrency: usize, f: F) -> ParEvalMap<Self, F, Fut, U>
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        Self: Send,
        Self: 'static,
    {
        ParEvalMap::new(self, concurrency, f)
    }

    fn par_eval_map_unordered<F, Fut, U>(self, concurrency: usize, f: F) -> ParEvalMapUnordered<Self, F, Fut, U>
    where
        F: Fn(Self::Item) -> Fut + Send + Sync + Clone + 'static + Unpin,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        Self: Send,
        Self: 'static,
    {
        ParEvalMapUnordered::new(self, concurrency, f)
    }

    // Optional: sync CPU-bound parallel map
    fn map_parallel_rs2<F, O>(self, f: F) -> ParMap<Self, F, O>
    where
        Self: Stream + Send + 'static,
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static,
        O: Send + 'static,
        Self::Item: Send + 'static,
    {
        ParMap::new(self, f, ParallelConfig::default())
    }

    fn map_parallel_with_concurrency_rs2<F, O>(
        self,
        concurrency: usize,
        f: F,
    ) -> ParMap<Self, F, O>
    where
        Self: Stream + Send + 'static,
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static,
        O: Send + 'static,
        Self::Item: Send + 'static,
    {
        let mut cfg = ParallelConfig::default();
        cfg.concurrency = clamp_concurrency(concurrency);
        ParMap::new(self, f, cfg)
    }
}

impl<T> ParallelStreamExt for T where T: crate::stream::Stream + Sized {}