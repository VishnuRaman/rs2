//! Async/parallel combinators: buffered, buffer_unordered, for_each_concurrent, try_for_each_concurrent
use crate::stream::{Stream, StreamExt};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};
use std::future::Future;
use tokio::task::{JoinHandle, spawn};

// Helper function to poll a pinned future
fn poll_pinned_future<F: Future + Unpin>(
    future: &mut F,
    cx: &mut Context<'_>,
) -> Poll<F::Output> {
    // Safety: we know the future is Unpin, so this is safe
    unsafe {
        let mut pinned = Pin::new_unchecked(future);
        pinned.as_mut().poll(cx)
    }
}

// ================================
// Buffered - Ordered Execution (keeps VecDeque for order)
// ================================

pub struct Buffered<S, Fut>
where
    S: Stream,
    Fut: Future,
{
    pub(crate) stream: S,
    pub(crate) n: usize,
    pub(crate) futures: VecDeque<JoinHandle<Fut::Output>>,
    pub(crate) done: bool,
}

impl<S, Fut> Stream for Buffered<S, Fut>
where
    S: Stream<Item = Fut> + Unpin,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    type Item = Result<Fut::Output, tokio::task::JoinError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        // If done and no futures left, we're finished
        if this.done && this.futures.is_empty() {
            return Poll::Ready(None);
        }

        // Try to spawn new futures if we have capacity
        while !this.done && this.futures.len() < this.n {
            let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    let handle = spawn(fut);
                    this.futures.push_back(handle);
                }
                Poll::Ready(None) => {
                    this.done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Poll the front future (maintaining order)
        if let Some(front_fut) = this.futures.front_mut() {
            match poll_pinned_future(front_fut, cx) {
                Poll::Ready(result) => {
                    this.futures.pop_front();
                    return Poll::Ready(Some(result));
                }
                Poll::Pending => {}
            }
        }

        // If we have futures running or might get more, we're pending
        if !this.futures.is_empty() || !this.done {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

// ================================
// BufferUnordered - Unordered Execution (FIXED with Vec + swap_remove)
// ================================

pub struct BufferUnordered<S, Fut>
where
    S: Stream,
    Fut: Future,
{
    pub(crate) stream: S,
    pub(crate) n: usize,
    pub(crate) futures: Vec<JoinHandle<Fut::Output>>,
    pub(crate) done: bool,
}

impl<S, Fut> Stream for BufferUnordered<S, Fut>
where
    S: Stream<Item = Fut> + Unpin,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    type Item = Result<Fut::Output, tokio::task::JoinError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        if this.done && this.futures.is_empty() {
            return Poll::Ready(None);
        }

        // Try to spawn new futures if we have capacity
        while !this.done && this.futures.len() < this.n {
            let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    let handle = spawn(fut);
                    this.futures.push(handle);
                }
                Poll::Ready(None) => {
                    this.done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Poll futures from back to front for O(1) removal
        let mut i = this.futures.len();
        while i > 0 {
            i -= 1;
            if let Some(future) = this.futures.get_mut(i) {
                match poll_pinned_future(future, cx) {
                    Poll::Ready(result) => {
                        // O(1) removal using swap_remove
                        this.futures.swap_remove(i);
                        return Poll::Ready(Some(result));
                    }
                    Poll::Pending => {}
                }
            }
        }

        // If we have futures running or might get more, we're pending
        if !this.futures.is_empty() || !this.done {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

// ================================
// ForEachConcurrent (FIXED with Vec + swap_remove)
// ================================

pub struct ForEachConcurrent<S, F, Fut>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    pub(crate) stream: S,
    pub(crate) n: usize,
    pub(crate) f: F,
    pub(crate) futures: Vec<JoinHandle<()>>,
    pub(crate) done: bool,
}

impl<S, F, Fut> Future for ForEachConcurrent<S, F, Fut>
where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
    S::Item: Send + 'static,
{
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        if this.done && this.futures.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Try to spawn new futures if we have capacity
        while !this.done && this.futures.len() < this.n {
            let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let fut = (this.f)(item);
                    let handle = spawn(fut);
                    this.futures.push(handle);
                }
                Poll::Ready(None) => {
                    this.done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Poll completed futures efficiently (O(1) removal per completed future)
        let mut i = this.futures.len();
        while i > 0 {
            i -= 1;
            if let Some(future) = this.futures.get_mut(i) {
                match poll_pinned_future(future, cx) {
                    Poll::Ready(result) => {
                        // O(1) removal using swap_remove
                        this.futures.swap_remove(i);
                        // If any task failed, return the error
                        if let Err(e) = result {
                            // Cancel remaining tasks to prevent resource leaks
                            this.futures.clear();
                            return Poll::Ready(Err(e));
                        }
                    }
                    Poll::Pending => {}
                }
            }
        }

        // If we have futures running or might get more, we're pending
        if !this.futures.is_empty() || !this.done {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

// ================================
// TryForEachConcurrent (FIXED with Vec + swap_remove)
// ================================

pub struct TryForEachConcurrent<S, F, Fut, E>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    pub(crate) stream: S,
    pub(crate) n: usize,
    pub(crate) f: F,
    pub(crate) futures: Vec<JoinHandle<Result<(), E>>>,
    pub(crate) done: bool,
    pub(crate) error: Option<E>,
}

impl<S, F, Fut, E> Future for TryForEachConcurrent<S, F, Fut, E>
where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    S::Item: Send + 'static,
    E: Send + 'static,
{
    type Output = Result<(), E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        // If we already have an error, return it
        if let Some(error) = this.error.take() {
            // Cancel remaining tasks to prevent resource leaks
            this.futures.clear();
            return Poll::Ready(Err(error));
        }

        if this.done && this.futures.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Try to spawn new futures if we have capacity and no error
        while !this.done && this.futures.len() < this.n {
            let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let fut = (this.f)(item);
                    let handle = spawn(fut);
                    this.futures.push(handle);
                }
                Poll::Ready(None) => {
                    this.done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Poll completed futures efficiently (O(1) removal per completed future)
        let mut i = this.futures.len();
        while i > 0 {
            i -= 1;
            if let Some(future) = this.futures.get_mut(i) {
                match poll_pinned_future(future, cx) {
                    Poll::Ready(result) => {
                        // O(1) removal using swap_remove
                        this.futures.swap_remove(i);
                        match result {
                            Ok(Ok(())) => {
                                // Success, continue
                            }
                            Ok(Err(e)) => {
                                // Application error, cancel remaining and return error
                                this.futures.clear();
                                return Poll::Ready(Err(e));
                            }
                            Err(join_error) => {
                                // Task panic or cancellation - log and continue processing other tasks
                                log::warn!("Task failed with join error: {:?}", join_error);
                                // Could implement retry logic here if needed
                            }
                        }
                    }
                    Poll::Pending => {}
                }
            }
        }

        // If we have futures running or might get more, we're pending
        if !this.futures.is_empty() || !this.done {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

// ================================
// AsyncFilterMap - Async filter_map combinator
// ================================

pub struct AsyncFilterMap<S, F, Fut, U>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = Option<U>>,
{
    pub(crate) stream: S,
    pub(crate) f: F,
    pub(crate) future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Option<U>> + Send + 'static>>>,
}

impl<S, F, Fut, U> Stream for AsyncFilterMap<S, F, Fut, U>
where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = Option<U>> + Send + 'static,
    U: Send + 'static,
{
    type Item = U;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };

        loop {
            // If we have a future, poll it
            if let Some(future) = &mut this.future {
                match poll_pinned_future(future, cx) {
                    Poll::Ready(Some(item)) => {
                        this.future = None;
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        this.future = None;
                        // Continue to get next item from stream
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Get next item from stream and start processing it
            let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let future = (this.f)(item);
                    this.future = Some(Box::pin(future));
                    // Continue the loop to poll the new future immediately
                    // instead of recursively calling self.poll_next(cx)
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Extension Trait
// ================================

pub trait AsyncStreamExt: Stream + Sized {
    /// Buffer futures from this stream with ordered output
    fn buffered<Fut>(self, n: usize) -> Buffered<Self, Fut>
    where
        Self: Stream<Item = Fut> + Unpin,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        assert!(n > 0, "buffered: n must be greater than 0");
        assert!(n <= 1000, "buffered: n must be reasonable (<= 1000)");
        Buffered {
            stream: self,
            n,
            futures: VecDeque::new(),
            done: false
        }
    }

    /// Buffer futures from this stream with unordered output
    fn buffer_unordered<Fut>(self, n: usize) -> BufferUnordered<Self, Fut>
    where
        Self: Stream<Item = Fut> + Unpin,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        assert!(n > 0, "buffer_unordered: n must be greater than 0");
        assert!(n <= 1000, "buffer_unordered: n must be reasonable (<= 1000)");
        BufferUnordered {
            stream: self,
            n,
            futures: Vec::new(),
            done: false
        }
    }

    /// Execute a function concurrently for each item in the stream
    fn for_each_concurrent<F, Fut>(self, n: usize, f: F) -> ForEachConcurrent<Self, F, Fut>
    where
        Self: Unpin,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
        Self::Item: Send + 'static,
    {
        assert!(n > 0, "for_each_concurrent: n must be greater than 0");
        assert!(n <= 1000, "for_each_concurrent: n must be reasonable (<= 1000)");
        ForEachConcurrent {
            stream: self,
            n,
            f,
            futures: Vec::new(),
            done: false
        }
    }

    /// Execute a fallible function concurrently for each item in the stream
    fn try_for_each_concurrent<F, Fut, E>(
        self,
        n: usize,
        f: F
    ) -> TryForEachConcurrent<Self, F, Fut, E>
    where
        Self: Unpin,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        Self::Item: Send + 'static,
        E: Send + 'static,
    {
        assert!(n > 0, "try_for_each_concurrent: n must be greater than 0");
        assert!(n <= 1000, "try_for_each_concurrent: n must be reasonable (<= 1000)");
        TryForEachConcurrent {
            stream: self,
            n,
            f,
            futures: Vec::new(),
            done: false,
            error: None
        }
    }

    /// Filter and map over stream items with an async function
    fn filter_map_async<F, Fut, U>(self, f: F) -> AsyncFilterMap<Self, F, Fut, U>
    where
        Self: Unpin,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = Option<U>> + Send + 'static,
        U: Send + 'static,
    {
        AsyncFilterMap {
            stream: self,
            f,
            future: None,
        }
    }
}

impl<T> AsyncStreamExt for T where T: Stream {}