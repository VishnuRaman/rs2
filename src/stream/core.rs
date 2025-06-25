//! Core stream combinators and trait definitions for RS2
//! This file is moved from stream_types.rs for modularization.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Custom stream trait - zero overhead replacement for futures_util::Stream
pub trait Stream {
    type Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
}

/// Extension trait providing stream combinators
pub trait StreamExt: Stream + Sized {
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where F: FnMut(Self::Item) -> U;
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where F: FnMut(&Self::Item) -> bool;
    fn take(self, n: usize) -> Take<Self>;
    fn skip(self, n: usize) -> Skip<Self>;
    fn then<U, Fut, F>(self, f: F) -> Then<Self, U, Fut, F>
    where F: FnMut(Self::Item) -> Fut, Fut: Future<Output = U>;
    fn collect<B>(self) -> Collect<Self, B>
    where B: Default + Extend<Self::Item>;
    fn enumerate(self) -> Enumerate<Self>;
    fn chain<U>(self, other: U) -> Chain<Self, U>
    where U: Stream<Item = Self::Item>;
    fn next(self) -> Next<Self>;
    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where F: FnMut(Self::Item) -> Fut, Fut: Future<Output = ()>;
    fn fold<B, F, Fut>(self, init: B, f: F) -> Fold<Self, B, F, Fut>
    where B: Default, F: FnMut(B, Self::Item) -> Fut, Fut: Future<Output = B>;
}

// ================================
// Stream Combinators
// ================================

pub struct Map<S, F> {
    stream: S,
    f: F
}

impl<S, F, U> Stream for Map<S, F>
where S: Stream, F: FnMut(S::Item) -> U {
    type Item = U;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some((this.f)(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Filter<S, F> {
    stream: S,
    f: F
}

impl<S, F> Stream for Filter<S, F>
where S: Stream, F: FnMut(&S::Item) -> bool {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if (this.f)(&item) {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub struct Take<S> {
    stream: S,
    remaining: usize
}

impl<S> Stream for Take<S>
where S: Stream {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.remaining -= 1;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Skip<S> {
    stream: S,
    remaining: usize
}

impl<S> Stream for Skip<S>
where S: Stream {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };

        while this.remaining > 0 {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(_)) => {
                    this.remaining -= 1;
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        stream.poll_next(cx)
    }
}

enum ThenState<Fut> {
    Streaming,
    Processing(Pin<Box<Fut>>),
}

pub struct Then<S, U, Fut, F> {
    stream: S,
    f: F,
    state: ThenState<Fut>,
    _phantom: std::marker::PhantomData<U>,
}

impl<S, U, Fut, F> Stream for Then<S, U, Fut, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = U>,
{
    type Item = U;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };

        loop {
            match &mut this.state {
                ThenState::Streaming => {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let fut = (this.f)(item);
                            this.state = ThenState::Processing(Box::pin(fut));
                        }
                        Poll::Ready(None) => return Poll::Ready(None),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ThenState::Processing(fut) => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(val) => {
                            this.state = ThenState::Streaming;
                            return Poll::Ready(Some(val));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

pub struct Collect<S, B> {
    stream: S,
    collection: B
}

impl<S, B> Future for Collect<S, B>
where S: Stream, B: Default + Extend<S::Item> {
    type Output = B;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.collection.extend(std::iter::once(item));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(std::mem::take(&mut this.collection));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub struct Enumerate<S> {
    stream: S,
    index: usize
}

impl<S> Stream for Enumerate<S>
where S: Stream {
    type Item = (usize, S::Item);
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let current_index = this.index;
                this.index += 1;
                Poll::Ready(Some((current_index, item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Chain<S1, S2> {
    first: Option<S1>,
    second: S2
}

impl<S1, S2> Stream for Chain<S1, S2>
where S1: Stream, S2: Stream<Item = S1::Item> {
    type Item = S1::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let second = unsafe { Pin::new_unchecked(&mut this.second) };

        if let Some(first_stream) = this.first.as_mut() {
            let first_pinned = unsafe { Pin::new_unchecked(first_stream) };
            match first_pinned.poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    this.first = None;
                    second.poll_next(cx)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            second.poll_next(cx)
        }
    }
}

pub struct Next<S> { stream: S }

impl<S> Future for Next<S>
where S: Stream {
    type Output = Option<S::Item>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let stream = unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().stream) };
        stream.poll_next(cx)
    }
}

enum ForEachState<Fut> {
    Streaming,
    Processing(Pin<Box<Fut>>),
}

pub struct ForEach<S, F, Fut> {
    stream: S,
    f: F,
    state: ForEachState<Fut>,
    _phantom: std::marker::PhantomData<Fut>,
}

impl<S, F, Fut> Future for ForEach<S, F, Fut>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };

        loop {
            match &mut this.state {
                ForEachState::Streaming => {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let fut = (this.f)(item);
                            this.state = ForEachState::Processing(Box::pin(fut));
                        }
                        Poll::Ready(None) => return Poll::Ready(()),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ForEachState::Processing(fut) => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(()) => {
                            this.state = ForEachState::Streaming;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

enum FoldState<B, Fut> {
    Streaming(B),
    Processing(Pin<Box<Fut>>),
}

pub struct Fold<S, B, F, Fut> {
    stream: S,
    f: F,
    state: FoldState<B, Fut>,
    _phantom: std::marker::PhantomData<Fut>,
}

impl<S, B, F, Fut> Future for Fold<S, B, F, Fut>
where
    S: Stream,
    B: Default,
    F: FnMut(B, S::Item) -> Fut,
    Fut: Future<Output = B>,
{
    type Output = B;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut stream = unsafe { Pin::new_unchecked(&mut this.stream) };

        loop {
            match &mut this.state {
                FoldState::Streaming(acc) => {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let acc_value = std::mem::take(acc);
                            let fut = (this.f)(acc_value, item);
                            this.state = FoldState::Processing(Box::pin(fut));
                        }
                        Poll::Ready(None) => {
                            return Poll::Ready(std::mem::take(acc));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                FoldState::Processing(fut) => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(new_acc) => {
                            this.state = FoldState::Streaming(new_acc);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

// ================================
// StreamExt Implementation - Generic for all Stream types
// ================================

impl<T> StreamExt for T
where T: Stream
{
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where F: FnMut(Self::Item) -> U
    {
        Map { stream: self, f }
    }

    fn filter<F>(self, f: F) -> Filter<Self, F>
    where F: FnMut(&Self::Item) -> bool
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
    where F: FnMut(Self::Item) -> Fut, Fut: Future<Output = U>
    {
        Then {
            stream: self,
            f,
            state: ThenState::Streaming,
            _phantom: std::marker::PhantomData
        }
    }

    fn collect<B>(self) -> Collect<Self, B>
    where B: Default + Extend<Self::Item>
    {
        Collect { stream: self, collection: B::default() }
    }

    fn enumerate(self) -> Enumerate<Self> {
        Enumerate { stream: self, index: 0 }
    }

    fn chain<U>(self, other: U) -> Chain<Self, U>
    where U: Stream<Item = Self::Item>
    {
        Chain { first: Some(self), second: other }
    }

    fn next(self) -> Next<Self> {
        Next { stream: self }
    }

    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where F: FnMut(Self::Item) -> Fut, Fut: Future<Output = ()>
    {
        ForEach {
            stream: self,
            f,
            state: ForEachState::Streaming,
            _phantom: std::marker::PhantomData
        }
    }

    fn fold<B, F, Fut>(self, init: B, f: F) -> Fold<Self, B, F, Fut>
    where B: Default, F: FnMut(B, Self::Item) -> Fut, Fut: Future<Output = B>
    {
        Fold {
            stream: self,
            f,
            state: FoldState::Streaming(init),
            _phantom: std::marker::PhantomData
        }
    }
}