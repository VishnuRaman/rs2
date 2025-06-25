//! Specialized stream combinators: TryStream, chunks, take_until, skip_until, backpressure
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;
use std::time::Duration;
use std::collections::VecDeque;
use super::core::Stream;
use tokio::time::{Sleep, Instant};

// ================================
// TryStream Trait & Combinators
// ================================

/// A stream that yields `Result<T, E>` items
pub trait TryStream: Stream<Item = Result<Self::Ok, Self::Error>> {
    type Ok;
    type Error;

    fn try_map<U, F>(self, f: F) -> TryMap<Self, F>
    where F: FnMut(Self::Ok) -> U, Self: Sized;

    fn try_filter<F>(self, f: F) -> TryFilter<Self, F>
    where F: FnMut(&Self::Ok) -> bool, Self: Sized;

    fn try_fold<B, F>(self, init: B, f: F) -> TryFold<Self, B, F>
    where F: FnMut(B, Self::Ok) -> B, B: Default, Self: Sized;

    fn try_for_each<F>(self, f: F) -> TryForEach<Self, F>
    where F: FnMut(Self::Ok) -> Result<(), Self::Error>, Self: Sized;
}

impl<S, T, E> TryStream for S
where S: Stream<Item = Result<T, E>>
{
    type Ok = T;
    type Error = E;

    fn try_map<U, F>(self, f: F) -> TryMap<Self, F>
    where F: FnMut(Self::Ok) -> U
    {
        TryMap { stream: self, f }
    }

    fn try_filter<F>(self, f: F) -> TryFilter<Self, F>
    where F: FnMut(&Self::Ok) -> bool
    {
        TryFilter { stream: self, f }
    }

    fn try_fold<B, F>(self, init: B, f: F) -> TryFold<Self, B, F>
    where F: FnMut(B, Self::Ok) -> B
    {
        TryFold { stream: self, acc: init, f }
    }

    fn try_for_each<F>(self, f: F) -> TryForEach<Self, F>
    where F: FnMut(Self::Ok) -> Result<(), Self::Error>
    {
        TryForEach { stream: self, f }
    }
}

pub struct TryMap<S, F>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) f: F,
}

impl<S, F, U, T, E> Stream for TryMap<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(T) -> U,
{
    type Item = Result<U, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };

        match stream.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(f(item)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct TryFilter<S, F>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) f: F,
}

impl<S, F, T, E> Stream for TryFilter<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Result<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    if f(&item) {
                        return Poll::Ready(Some(Ok(item)));
                    }
                    // Continue filtering
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub struct TryFold<S, B, F>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) acc: B,
    pub(crate) f: F,
}

impl<S, B, F, T, E> Future for TryFold<S, B, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(B, T) -> B,
    B: Default,
{
    type Output = Result<B, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, acc, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.acc, &mut this.f)
        };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    *acc = f(std::mem::take(acc), item);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(std::mem::take(acc))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub struct TryForEach<S, F>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) f: F,
}

impl<S, F, T, E> Future for TryForEach<S, F>
where
    S: Stream<Item = Result<T, E>>,
    F: FnMut(T) -> Result<(), E>,
{
    type Output = Result<(), E>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, f) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };
        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    // Early termination on first error
                    if let Err(e) = f(item) {
                        return Poll::Ready(Err(e));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Chunks Combinators
// ================================

pub struct Chunks<S>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) size: usize,
    pub(crate) buffer: Vec<S::Item>,
}

impl<S> Stream for Chunks<S>
where S: Stream
{
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, size, buffer) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), this.size, &mut this.buffer)
        };

        // Fill buffer until we have enough items
        while buffer.len() < size {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    buffer.push(item);
                }
                Poll::Ready(None) => {
                    // Stream ended, return remaining items if any
                    if buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(std::mem::take(buffer)));
                    }
                }
                Poll::Pending => {
                    // If we have some items but not enough, return what we have
                    if !buffer.is_empty() {
                        return Poll::Ready(Some(std::mem::take(buffer)));
                    }
                    return Poll::Pending;
                }
            }
        }

        // We have enough items, return a chunk
        let chunk = std::mem::replace(buffer, Vec::new());
        Poll::Ready(Some(chunk))
    }
}

pub struct ChunksTimeout<S>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) size: usize,
    pub(crate) duration: Duration,
    pub(crate) buffer: Vec<S::Item>,
    pub(crate) timeout: Option<Pin<Box<Sleep>>>,
    pub(crate) last_emit: Option<Instant>,
}

impl<S> Stream for ChunksTimeout<S>
where S: Stream
{
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, size, duration, buffer, timeout, last_emit) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.stream),
                this.size,
                this.duration,
                &mut this.buffer,
                &mut this.timeout,
                &mut this.last_emit
            )
        };

        // Check if timeout has expired
        if let Some(sleep) = timeout {
            match sleep.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    *timeout = None;
                    if !buffer.is_empty() {
                        return Poll::Ready(Some(std::mem::take(buffer)));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Poll stream for new items
        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    buffer.push(item);
                    
                    // Start timeout if this is the first item
                    if last_emit.is_none() {
                        *last_emit = Some(Instant::now());
                        *timeout = Some(Box::pin(tokio::time::sleep(duration)));
                    }

                    // Return chunk if we have enough items
                    if buffer.len() >= size {
                        *timeout = None;
                        *last_emit = None;
                        return Poll::Ready(Some(std::mem::take(buffer)));
                    }
                }
                Poll::Ready(None) => {
                    // Stream ended, return remaining items if any
                    if buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(std::mem::take(buffer)));
                    }
                }
                Poll::Pending => {
                    // If we have some items but not enough, wait for timeout or more items
                    if !buffer.is_empty() {
                        return Poll::Pending;
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

// ================================
// TakeUntil/SkipUntil Combinators
// ================================

pub struct TakeUntil<S, Fut> {
    pub(crate) stream: S,
    pub(crate) future: Option<Pin<Box<Fut>>>,
    pub(crate) done: bool,
}

impl<S, Fut> Stream for TakeUntil<S, Fut>
where
    S: Stream,
    Fut: Future,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, future, done) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.stream),
                &mut this.future,
                &mut this.done
            )
        };

        if *done {
            return Poll::Ready(None);
        }

        // Check if the future has completed
        if let Some(fut) = future {
            match fut.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    *done = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
        }

        // Poll the stream
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct SkipUntil<S, Fut> {
    pub(crate) stream: S,
    pub(crate) future: Option<Pin<Box<Fut>>>,
    pub(crate) triggered: bool,
}

impl<S, Fut> Stream for SkipUntil<S, Fut>
where
    S: Stream,
    Fut: Future,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, future, triggered) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.stream),
                &mut this.future,
                &mut this.triggered
            )
        };

        // Check if the future has completed
        if let Some(fut) = future {
            match fut.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    *triggered = true;
                    *future = None;
                }
                Poll::Pending => {}
            }
        }

        // Skip items until triggered
        if !*triggered {
            // Check future first in each iteration to avoid infinite loops
            if let Some(fut) = future {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(_) => {
                        *triggered = true;
                        *future = None;
                        // Now return items after triggered
                        return stream.as_mut().poll_next(cx);
                    }
                    Poll::Pending => {
                        // Check one item from stream, then yield to avoid busy waiting
                        match stream.as_mut().poll_next(cx) {
                            Poll::Ready(Some(_)) => {
                                // Skip this item and yield to allow future to make progress
                                return Poll::Pending;
                            }
                            Poll::Ready(None) => return Poll::Ready(None),
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                }
            } else {
                // No future, just skip items
                loop {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(_)) => {
                            // Skip this item
                        }
                        Poll::Ready(None) => return Poll::Ready(None),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }

        // Return items after triggered
        stream.as_mut().poll_next(cx)
    }
}

// ================================
// Backpressure Combinators
// ================================

pub struct Backpressure<S>
where S: Stream
{
    pub(crate) stream: S,
    pub(crate) buffer_size: usize,
    pub(crate) buffer: VecDeque<S::Item>,
    pub(crate) paused: bool,
}

impl<S> Stream for Backpressure<S>
where S: Stream
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, buffer_size, buffer, paused) = unsafe {
            let this = self.as_mut().get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.stream),
                this.buffer_size,
                &mut this.buffer,
                &mut this.paused
            )
        };

        // If we have buffered items, return one
        if !buffer.is_empty() {
            return Poll::Ready(Some(buffer.pop_front().unwrap()));
        }

        // If we're paused, don't poll the stream
        if *paused {
            return Poll::Pending;
        }

        // Poll the stream
        match stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Check if buffer is full before adding
                if buffer.len() >= buffer_size {
                    *paused = true;
                    return Poll::Pending; // Don't process the item when paused
                }
                buffer.push_back(item);
                Poll::Ready(Some(buffer.pop_front().unwrap()))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Extension trait for backpressure control
pub trait BackpressureExt: Stream + Sized {
    fn resume(&mut self);
    fn pause(&mut self);
    fn buffer_size(&self) -> usize;
}

impl<S> BackpressureExt for Backpressure<S>
where S: Stream
{
    fn resume(&mut self) {
        self.paused = false;
    }

    fn pause(&mut self) {
        self.paused = true;
    }

    fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

// ================================
// Extension Traits
// ================================

pub trait SpecializedStreamExt: Stream + Sized {
    fn chunks(self, size: usize) -> Chunks<Self> {
        assert!(size > 0, "chunks: size must be greater than 0");
        Chunks {
            stream: self,
            size,
            buffer: Vec::new(),
        }
    }

    fn chunks_timeout(self, size: usize, duration: Duration) -> ChunksTimeout<Self> {
        assert!(size > 0, "chunks_timeout: size must be greater than 0");
        assert!(!duration.is_zero(), "chunks_timeout: duration must be greater than zero");
        ChunksTimeout {
            stream: self,
            size,
            duration,
            buffer: Vec::new(),
            timeout: None,
            last_emit: None,
        }
    }

    fn take_until<Fut>(self, future: Fut) -> TakeUntil<Self, Fut>
    where Fut: Future
    {
        TakeUntil {
            stream: self,
            future: Some(Box::pin(future)),
            done: false,
        }
    }

    fn skip_until<Fut>(self, future: Fut) -> SkipUntil<Self, Fut>
    where Fut: Future
    {
        SkipUntil {
            stream: self,
            future: Some(Box::pin(future)),
            triggered: false,
        }
    }

    fn backpressure(self, buffer_size: usize) -> Backpressure<Self> {
        assert!(buffer_size > 0, "backpressure: buffer_size must be greater than 0");
        Backpressure {
            stream: self,
            buffer_size,
            buffer: VecDeque::new(),
            paused: false,
        }
    }
}

impl<T> SpecializedStreamExt for T where T: Stream {} 