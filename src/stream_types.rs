//! Custom stream types for RS2 - Zero overhead implementation
//! 
//! This module provides our own stream implementation using only std::future::Future,
//! eliminating the need for futures_util and tokio dependencies.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Custom stream trait - zero overhead replacement for futures_util::Stream
pub trait Stream {
    type Item;
    
    /// Poll for the next item in the stream
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
}

/// Extension trait providing stream combinators
pub trait StreamExt: Stream + Sized {
    /// Transform each item in the stream
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U;
    
    /// Filter items based on a predicate
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool;
    
    /// Take the first n items
    fn take(self, n: usize) -> Take<Self>;
    
    /// Skip the first n items
    fn skip(self, n: usize) -> Skip<Self>;
    
    /// Transform each item with an async function
    fn then<U, Fut, F>(self, f: F) -> Then<Self, U, Fut, F>
    where
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = U>;
    
    /// Collect all items into a collection
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>;
}

// ================================
// Stream Combinators
// ================================

/// Map combinator
pub struct Map<S, F> {
    stream: S,
    f: F,
}

impl<S, F, U> Stream for Map<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> U,
{
    type Item = U;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, f) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };
        
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(f(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, F, U> StreamExt for Map<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> U,
{
    fn map<U2, F2>(self, f: F2) -> Map<Self, F2>
    where
        F2: FnMut(Self::Item) -> U2,
    {
        Map { stream: self, f }
    }
    
    fn filter<F2>(self, f: F2) -> Filter<Self, F2>
    where
        F2: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }
    
    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }
    
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }
    
    fn then<U2, Fut, F2>(self, f: F2) -> Then<Self, U2, Fut, F2>
    where
        F2: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = U2>,
    {
        Then { stream: self, f, _phantom: std::marker::PhantomData }
    }
    
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }
}

/// Filter combinator
pub struct Filter<S, F> {
    stream: S,
    f: F,
}

impl<S, F> Stream for Filter<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool,
{
    type Item = S::Item;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, f) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.f)
        };
        
        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if f(&item) {
                        return Poll::Ready(Some(item));
                    }
                    // Continue to next item
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S, F> StreamExt for Filter<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool,
{
    fn map<U, F2>(self, f: F2) -> Map<Self, F2>
    where
        F2: FnMut(Self::Item) -> U,
    {
        Map { stream: self, f }
    }
    
    fn filter<F2>(self, f: F2) -> Filter<Self, F2>
    where
        F2: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }
    
    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }
    
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }
    
    fn then<U, Fut, F2>(self, f: F2) -> Then<Self, U, Fut, F2>
    where
        F2: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = U>,
    {
        Then { stream: self, f, _phantom: std::marker::PhantomData }
    }
    
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }
}

/// Take combinator
pub struct Take<S> {
    stream: S,
    remaining: usize,
}

impl<S> Stream for Take<S>
where
    S: Stream,
{
    type Item = S::Item;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        
        let (mut stream, remaining) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.remaining)
        };
        
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                *remaining -= 1;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> StreamExt for Take<S>
where
    S: Stream,
{
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U,
    {
        Map { stream: self, f }
    }
    
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }
    
    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }
    
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }
    
    fn then<U, Fut, F>(self, f: F) -> Then<Self, U, Fut, F>
    where
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = U>,
    {
        Then { stream: self, f, _phantom: std::marker::PhantomData }
    }
    
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }
}

/// Skip combinator
pub struct Skip<S> {
    stream: S,
    remaining: usize,
}

impl<S> Stream for Skip<S>
where
    S: Stream,
{
    type Item = S::Item;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (mut stream, remaining) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.remaining)
        };
        
        // Skip items until we've skipped enough
        while *remaining > 0 {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(_)) => {
                    *remaining -= 1;
                    // Continue skipping
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        
        // Now return the next item
        stream.poll_next(cx)
    }
}

impl<S> StreamExt for Skip<S>
where
    S: Stream,
{
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U,
    {
        Map { stream: self, f }
    }
    
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }
    
    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }
    
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }
    
    fn then<U, Fut, F>(self, f: F) -> Then<Self, U, Fut, F>
    where
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = U>,
    {
        Then { stream: self, f, _phantom: std::marker::PhantomData }
    }
    
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }
}

/// Then combinator for async transformations
pub struct Then<S, U, Fut, F> {
    stream: S,
    f: F,
    _phantom: std::marker::PhantomData<(U, Fut)>,
}

impl<S, U, Fut, F> Stream for Then<S, U, Fut, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = U>,
{
    type Item = U;
    
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // This is a simplified implementation - would need more complex state management
        // for proper async stream handling
        unimplemented!("Async stream transformations need more complex implementation")
    }
}

impl<S, U, Fut, F> StreamExt for Then<S, U, Fut, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = U>,
{
    fn map<U2, F2>(self, f: F2) -> Map<Self, F2>
    where
        F2: FnMut(Self::Item) -> U2,
    {
        Map { stream: self, f }
    }
    
    fn filter<F2>(self, f: F2) -> Filter<Self, F2>
    where
        F2: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }
    
    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }
    
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }
    
    fn then<U2, Fut2, F2>(self, f: F2) -> Then<Self, U2, Fut2, F2>
    where
        F2: FnMut(Self::Item) -> Fut2,
        Fut2: Future<Output = U2>,
    {
        Then { stream: self, f, _phantom: std::marker::PhantomData }
    }
    
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }
}

/// Collect combinator
pub struct Collect<S, B> {
    stream: S,
    collection: B,
}

impl<S, B> Future for Collect<S, B>
where
    S: Stream,
    B: Default + Extend<S::Item>,
{
    type Output = B;
    
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut stream, collection) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.stream), &mut this.collection)
        };
        
        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    collection.extend(std::iter::once(item));
                }
                Poll::Ready(None) => return Poll::Ready(std::mem::take(collection)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ================================
// Stream Constructors
// ================================

/// Create a stream from an iterator
pub struct Iter<I> {
    iter: I,
}

impl<I> Stream for Iter<I>
where
    I: Iterator,
{
    type Item = I::Item;
    
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let iter = unsafe { &mut self.get_unchecked_mut().iter };
        Poll::Ready(iter.next())
    }
}

impl<I> StreamExt for Iter<I>
where
    I: Iterator,
{
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where
        F: FnMut(Self::Item) -> U,
    {
        Map { stream: self, f }
    }
    
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        F: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }
    
    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }
    
    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }
    
    fn then<U, Fut, F>(self, f: F) -> Then<Self, U, Fut, F>
    where
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = U>,
    {
        Then { stream: self, f, _phantom: std::marker::PhantomData }
    }
    
    fn collect<B>(self) -> Collect<Self, B>
    where
        B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }
}

/// Create a stream from an iterator
pub fn from_iter<I>(iter: I) -> Iter<I::IntoIter>
where
    I: IntoIterator,
{
    Iter { iter: iter.into_iter() }
} 