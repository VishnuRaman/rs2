use std::collections::{VecDeque, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use futures::Future;
use crate::stream::{Stream, StreamExt};

/// Configuration for parallel processing
#[derive(Clone, Debug)]
pub struct ParallelConfig {
    pub concurrency: usize,
    pub max_buffer_size: usize,
    pub timeout: Duration,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            concurrency: num_cpus::get(),
            max_buffer_size: 1000,
            timeout: Duration::from_secs(30),
        }
    }
}

// --- ParEvalMap (ordered) ---
pub struct ParEvalMap<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    source: Pin<Box<S>>,
    f: F,
    buffer: VecDeque<U>,
    reorder_buffer: HashMap<usize, U>,
    source_done: bool,
    sequence_counter: usize,
    next_sequence: usize,
    items_sent: usize,
    items_yielded: usize,
    waker: Option<Waker>,
    result_receiver: Option<mpsc::Receiver<(usize, U)>>,
    dispatcher_sender: Option<mpsc::Sender<(usize, S::Item)>>,
    dispatcher_handle: Option<JoinHandle<()>>,
    semaphore: Arc<Semaphore>,
    config: ParallelConfig,
}

impl<S, F, U> ParEvalMap<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    pub fn new(source: S, concurrency: usize, f: F) -> Self {
        let config = ParallelConfig {
            concurrency: concurrency.max(1), // Ensure minimum concurrency of 1
            max_buffer_size: 1000,
            timeout: Duration::from_secs(30),
        };
        
        Self {
            source: Box::pin(source),
            f,
            buffer: VecDeque::new(),
            reorder_buffer: HashMap::new(),
            source_done: false,
            sequence_counter: 0,
            next_sequence: 0,
            items_sent: 0,
            items_yielded: 0,
            waker: None,
            result_receiver: None,
            dispatcher_sender: None,
            dispatcher_handle: None,
            semaphore: Arc::new(Semaphore::new(config.concurrency)),
            config,
        }
    }

    fn start_dispatcher(&mut self) {
        let (sender, mut receiver) = mpsc::channel(self.config.max_buffer_size);
        let (result_sender, result_receiver) = mpsc::channel(self.config.max_buffer_size);
        
        self.dispatcher_sender = Some(sender);
        self.result_receiver = Some(result_receiver);
        
        let f = self.f.clone();
        let semaphore = self.semaphore.clone();
        let _concurrency = self.config.concurrency;
        
        let handle = tokio::spawn(async move {
            let mut worker_handles = Vec::new();
            
            while let Some((sequence, item)) = receiver.recv().await {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let f = f.clone();
                let result_sender = result_sender.clone();
                
                let worker_handle = tokio::spawn(async move {
                    let future = f(item);
                    let result = future.await;
                    let _ = result_sender.send((sequence, result)).await;
                    drop(permit);
                });
                
                worker_handles.push(worker_handle);
                
                // Clean up completed workers
                worker_handles.retain(|handle| !handle.is_finished());
            }
            
            // Drop the result sender to signal completion
            drop(result_sender);
            
            // Wait for all workers to complete
            for handle in worker_handles {
                let _ = handle.await;
            }
        });
        
        self.dispatcher_handle = Some(handle);
    }

    fn cleanup_dispatcher(&mut self) {
        // Drop sender to signal dispatcher to stop
        self.dispatcher_sender = None;
        
        // Wait for dispatcher to complete
        if let Some(handle) = self.dispatcher_handle.take() {
            let _ = handle.abort();
        }
    }
}

impl<S, F, U> Stream for ParEvalMap<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The `get_mut()` provides a mutable reference to the inner struct.
        // This is safe because we are not moving out of the `Pin`.
        let this = self.as_mut().get_mut();

        if this.dispatcher_handle.is_none() {
            this.start_dispatcher();
        }

        loop {
            // First, try to drain any results that are waiting in the channel.
            // This is non-blocking.
            if let Some(rx) = this.result_receiver.as_mut() {
                while let Ok((seq, result)) = rx.try_recv() {
                    this.reorder_buffer.insert(seq, result);
                }
            }

            // Try to yield the next item in order from the reorder buffer.
            if let Some(item) = this.reorder_buffer.remove(&this.next_sequence) {
                this.items_yielded += 1;
                this.next_sequence += 1;
                return Poll::Ready(Some(item));
            }

            // Check if the stream is finished.
            if this.source_done && this.items_sent == this.items_yielded {
                this.cleanup_dispatcher();
                return Poll::Ready(None);
            }

            // If we have capacity, poll the source for new items.
            let buffer_full = (this.items_sent - this.items_yielded) >= this.config.max_buffer_size;
            if !this.source_done && !buffer_full {
                match this.source.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        let seq = this.sequence_counter;
                        this.sequence_counter += 1;
                        // If dispatcher is gone, we can't send. Treat as finished.
                        if this.dispatcher_sender.as_ref().unwrap().try_send((seq, item)).is_ok() {
                            this.items_sent += 1;
                            // We made progress, so we loop to check for more.
                            continue;
                        } else {
                            this.source_done = true;
                        }
                    }
                    Poll::Ready(None) => {
                        this.source_done = true;
                        // Drop sender to signal dispatcher to stop
                        this.dispatcher_sender = None;
                    }
                    Poll::Pending => {
                        // Source is not ready. We'll poll results receiver below.
                    }
                }
            }

            // CRITICAL: Before returning Pending, we must poll the result receiver
            // to register the waker. This ensures we are woken up when a result
            // is ready, preventing the hang.
            if let Some(rx) = this.result_receiver.as_mut() {
                match Pin::new(rx).poll_recv(cx) {
                    Poll::Ready(Some((seq, result))) => {
                        this.reorder_buffer.insert(seq, result);
                        // A result arrived. Loop again to try to yield it.
                        continue;
                    }
                    Poll::Ready(None) => {
                        // The dispatcher has shut down. No more results will arrive.
                        this.source_done = true;
                    }
                    Poll::Pending => {
                        // Waker is registered. It's now safe to return Pending.
                    }
                }
            }
            
            // If we are here, we can't make any more progress right now.
            // Check for completion one last time in case states have changed.
            if this.source_done && this.items_sent == this.items_yielded {
                this.cleanup_dispatcher();
                return Poll::Ready(None);
            }

            return Poll::Pending;
        }
    }
}

impl<S, F, U> Drop for ParEvalMap<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    fn drop(&mut self) {
        self.cleanup_dispatcher();
    }
}

// --- ParEvalMapUnordered (unordered) ---
pub struct ParEvalMapUnordered<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    source: Pin<Box<S>>,
    f: F,
    buffer: VecDeque<U>,
    source_done: bool,
    workers_done: bool,
    items_sent: usize,
    items_received: usize,
    waker: Option<Waker>,
    result_receiver: Option<mpsc::Receiver<U>>,
    dispatcher_sender: Option<mpsc::Sender<S::Item>>,
    dispatcher_handle: Option<JoinHandle<()>>,
    semaphore: Arc<Semaphore>,
    config: ParallelConfig,
}

impl<S, F, U> ParEvalMapUnordered<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    pub fn new(source: S, concurrency: usize, f: F) -> Self {
        let mut config = ParallelConfig::default();
        config.concurrency = concurrency;
        Self {
            source: Box::pin(source),
            f,
            buffer: VecDeque::new(),
            source_done: false,
            workers_done: false,
            items_sent: 0,
            items_received: 0,
            waker: None,
            result_receiver: None,
            dispatcher_sender: None,
            dispatcher_handle: None,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            config,
        }
    }

    fn start_dispatcher(&mut self) {
        if self.dispatcher_sender.is_some() {
            return;
        }

        let (result_tx, result_rx) = mpsc::channel(self.config.max_buffer_size);
        let (dispatcher_tx, mut dispatcher_rx) = mpsc::channel(self.config.max_buffer_size);
        
        self.result_receiver = Some(result_rx);
        self.dispatcher_sender = Some(dispatcher_tx);
        
        let f = self.f.clone();
        let semaphore = self.semaphore.clone();
        
        // Spawn dispatcher task
        let handle = tokio::spawn(async move {
            let mut worker_handles = Vec::new();
            
            while let Some(item) = dispatcher_rx.recv().await {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let f = f.clone();
                let result_tx = result_tx.clone();
                
                let worker_handle = tokio::spawn(async move {
                    let _permit = permit;
                    let result = f(item).await;
                    let _ = result_tx.send(result).await;
                });
                
                worker_handles.push(worker_handle);
                
                // Clean up completed workers
                worker_handles.retain(|handle| !handle.is_finished());
            }
            
            // Wait for all workers to complete
            for handle in worker_handles {
                let _ = handle.await;
            }
        });
        
        self.dispatcher_handle = Some(handle);
    }

    fn cleanup_dispatcher(&mut self) {
        // Drop sender to signal dispatcher to stop
        self.dispatcher_sender = None;
        
        // Wait for dispatcher to complete
        if let Some(handle) = self.dispatcher_handle.take() {
            let _ = handle.abort();
        }
    }
}

impl<S, F, U> Stream for ParEvalMapUnordered<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        if this.dispatcher_handle.is_none() {
            this.start_dispatcher();
        }

        loop {
            // First, check if there are any buffered results to yield.
            if let Some(item) = this.buffer.pop_front() {
                return Poll::Ready(Some(item));
            }

            // Check if the stream is fully complete.
            if this.source_done && this.items_sent == this.items_received {
                this.cleanup_dispatcher();
                return Poll::Ready(None);
            }

            // Try to receive more results from workers and buffer them.
            if let Some(rx) = this.result_receiver.as_mut() {
                while let Ok(item) = rx.try_recv() {
                    this.items_received += 1;
                    this.buffer.push_back(item);
                }
                // If we received items, loop again to yield one from the buffer.
                if !this.buffer.is_empty() {
                    continue;
                }
            }

            // If we have capacity, try to poll the source for more items.
            let buffer_full = (this.items_sent - this.items_received) >= this.config.max_buffer_size;
            if !this.source_done && !buffer_full {
                match this.source.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        if this.dispatcher_sender.as_ref().unwrap().try_send(item).is_ok() {
                            this.items_sent += 1;
                            // Progress was made, loop again.
                            continue;
                        } else {
                            // Channel is closed, so we consider the source done.
                            this.source_done = true;
                        }
                    }
                    Poll::Ready(None) => {
                        this.source_done = true;
                    }
                    Poll::Pending => {
                        // Source is not ready, proceed to check results channel.
                    }
                }
            }

            // CRITICAL: Before returning Pending, poll the result receiver to register the waker.
            if let Some(rx) = this.result_receiver.as_mut() {
                match Pin::new(rx).poll_recv(cx) {
                    Poll::Ready(Some(item)) => {
                        this.items_received += 1;
                        this.buffer.push_back(item);
                        // A result arrived, loop again to yield it.
                        continue;
                    }
                    Poll::Ready(None) => {
                        // Dispatcher is done.
                        this.workers_done = true;
                    }
                    Poll::Pending => {
                        // Waker is registered. It's safe to return Pending.
                    }
                }
            }

            // If we're here, no progress can be made.
            // Check for completion one last time.
            if this.source_done && this.items_sent == this.items_received {
                this.cleanup_dispatcher();
                return Poll::Ready(None);
            }

            return Poll::Pending;
        }
    }

}

impl<S, F, U> Drop for ParEvalMapUnordered<S, F, U>
where
    S: Stream + Send + 'static,
    F: Fn(S::Item) -> Pin<Box<dyn Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
    U: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin,
{
    fn drop(&mut self) {
        self.cleanup_dispatcher();
    }
}

/// Extension trait for parallel stream operations
pub trait ParallelStreamExt: Stream + Sized {
    /// Apply a function to each element in parallel with bounded concurrency (ordered)
    fn par_eval_map<F, U>(self, concurrency: usize, f: F) -> ParEvalMap<Self, F, U>
    where
        F: Fn(Self::Item) -> std::pin::Pin<Box<dyn std::future::Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        Self: Send + 'static,
    {
        ParEvalMap::new(self, concurrency, f)
    }

    /// Apply a function to each element in parallel with unordered results
    fn par_eval_map_unordered<F, U>(self, concurrency: usize, f: F) -> ParEvalMapUnordered<Self, F, U>
    where
        F: Fn(Self::Item) -> std::pin::Pin<Box<dyn std::future::Future<Output = U> + Send + 'static>> + Send + Sync + Clone + 'static + Unpin,
        U: Send + 'static + Unpin,
        Self::Item: Send + 'static + Unpin,
        Self: Send + 'static,
    {
        ParEvalMapUnordered::new(self, concurrency, f)
    }

    /// Apply a function to each element in parallel (CPU-bound operations)
    fn par_map<F, O>(self, concurrency: usize, f: F) -> ParMap<Self, F, O>
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static + Unpin + Clone,
        O: Send + 'static + Unpin,
        Self: Send + 'static,
    {
        let mut config = ParallelConfig::default();
        config.concurrency = concurrency;
        ParMap::new(self, f, config)
    }

    /// Apply a function to each element in parallel with custom configuration
    fn par_map_with_config<F, O>(self, config: ParallelConfig, f: F) -> ParMap<Self, F, O>
    where
        F: Fn(Self::Item) -> O + Send + Sync + Clone + 'static + Unpin,
        Self::Item: Send + 'static + Unpin + Clone,
        O: Send + 'static + Unpin,
        Self: Send + 'static,
    {
        ParMap::new(self, f, config)
    }
}

impl<T: StreamExt> ParallelStreamExt for T {}

/// Simple parallel map for CPU-bound operations
pub struct ParMap<S, F, O>
where
    S: Stream,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static + Unpin,
    O: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin + Clone,
{
    source: Pin<Box<S>>,
    f: F,
    buffer: VecDeque<O>,
    source_done: bool,
    waker: Option<Waker>,
    result_receiver: Option<mpsc::Receiver<O>>,
    dispatcher_sender: Option<mpsc::Sender<S::Item>>,
    dispatcher_handle: Option<JoinHandle<()>>,
    semaphore: Arc<Semaphore>,
    config: ParallelConfig,
}

impl<S, F, O> ParMap<S, F, O>
where
    S: Stream,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static + Unpin,
    O: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin + Clone,
{
    pub fn new(source: S, f: F, config: ParallelConfig) -> Self {
        Self {
            source: Box::pin(source),
            f,
            buffer: VecDeque::new(),
            source_done: false,
            waker: None,
            result_receiver: None,
            dispatcher_sender: None,
            dispatcher_handle: None,
            semaphore: Arc::new(Semaphore::new(config.concurrency)),
            config,
        }
    }

    fn start_dispatcher(&mut self) {
        if self.dispatcher_sender.is_some() {
            return;
        }

        let (result_tx, result_rx) = mpsc::channel(self.config.max_buffer_size);
        let (dispatcher_tx, mut dispatcher_rx) = mpsc::channel(self.config.max_buffer_size);
        
        self.result_receiver = Some(result_rx);
        self.dispatcher_sender = Some(dispatcher_tx);
        
        let f = self.f.clone();
        let semaphore = self.semaphore.clone();
        
        // Spawn dispatcher task
        let handle = tokio::spawn(async move {
            let mut worker_handles = Vec::new();
            
            while let Some(item) = dispatcher_rx.recv().await {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let f = f.clone();
                let result_tx = result_tx.clone();
                
                let worker_handle = tokio::spawn(async move {
                    let _permit = permit;
                    let result = tokio::task::spawn_blocking(move || f(item)).await.unwrap();
                    let _ = result_tx.send(result).await;
                });
                
                worker_handles.push(worker_handle);
                
                // Clean up completed workers
                worker_handles.retain(|handle| !handle.is_finished());
            }
            
            // Wait for all workers to complete
            for handle in worker_handles {
                let _ = handle.await;
            }
        });
        
        self.dispatcher_handle = Some(handle);
    }

    fn cleanup_dispatcher(&mut self) {
        // Drop sender to signal dispatcher to stop
        self.dispatcher_sender = None;
        
        // Wait for dispatcher to complete
        if let Some(handle) = self.dispatcher_handle.take() {
            let _ = handle.abort();
        }
    }
}

impl<S, F, O> Stream for ParMap<S, F, O>
where
    S: Stream,
    F: Fn(S::Item) -> O + Send + Sync + Clone + 'static + Unpin,
    O: Send + 'static + Unpin,
    S::Item: Send + 'static + Unpin + Clone,
{
    type Item = O;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        
        // Start dispatcher if not started
        if this.dispatcher_sender.is_none() {
            this.start_dispatcher();
        }
        
        this.waker = Some(cx.waker().clone());
        
        // Check for results from workers
        if let Some(receiver) = &mut this.result_receiver {
            while let Ok(result) = receiver.try_recv() {
                this.buffer.push_back(result);
            }
        }
        
        // Return buffered results if available
        if let Some(result) = this.buffer.pop_front() {
            return Poll::Ready(Some(result));
        }
        
        // Send items to dispatcher
        if let Some(sender) = &this.dispatcher_sender {
            while !this.source_done {
                match this.source.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        if let Err(_) = sender.try_send(item) {
                            // Channel is full, wait for capacity
                            break;
                        }
                    }
                    Poll::Ready(None) => {
                        this.source_done = true;
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }
        
        // Check if we're done
        if this.source_done {
            // Check for any remaining results
            if let Some(receiver) = &mut this.result_receiver {
                while let Ok(result) = receiver.try_recv() {
                    this.buffer.push_back(result);
                }
                
                // Return any buffered results
                if let Some(result) = this.buffer.pop_front() {
                    return Poll::Ready(Some(result));
                }
                
                // Check if channel is closed (all workers done)
                if receiver.try_recv().is_err() {
                    this.cleanup_dispatcher();
                    return Poll::Ready(None);
                }
            }
        }
        
        Poll::Pending
    }
}


