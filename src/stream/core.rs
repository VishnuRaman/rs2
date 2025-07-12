//! Core stream combinators and trait definitions for RS2
//! This file is moved from stream_types.rs for modularization.

use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::marker::PhantomData;

/// Custom stream trait - zero overhead replacement for futures_util::Stream
pub trait Stream {
    type Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
}

// Blanket impl for trait objects so combinators like par_join work with boxed streams
impl<T: Send + 'static> Stream for Box<dyn Stream<Item = T> + Send> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: Box<dyn Stream> is always Unpin, so this is safe
        unsafe { self.map_unchecked_mut(|b| &mut **b) }.poll_next(cx)
    }
}

/// Extension trait providing stream combinators
pub trait StreamExt: Stream + Sized {
    /// Get the next item from the stream
    fn next(&mut self) -> Next<'_, Self>;
    fn map<U, F>(self, f: F) -> Map<Self, U, F>
    where F: FnMut(Self::Item) -> U;
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where F: FnMut(&Self::Item) -> bool;
    fn filter_map<U, F>(self, f: F) -> FilterMap<Self, U, F>
    where F: FnMut(Self::Item) -> Option<U>;
    fn take(self, n: usize) -> Take<Self>;
    fn skip(self, n: usize) -> Skip<Self>;
    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where F: FnMut(Self::Item) -> Fut, Fut: Future;
    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where F: FnMut(Self::Item) -> Fut, Fut: Future<Output = ()>;
    fn fold<B, F, Fut>(self, init: B, f: F) -> Fold<Self, B, F, Fut>
    where F: FnMut(B, Self::Item) -> Fut, Fut: Future<Output = B>;
    fn flatten(self) -> Flatten<Self>
    where Self::Item: Stream;
    fn collect<B>(self) -> Collect<Self, B>
    where B: Default + Extend<Self::Item>;
    fn collect_stream<B>(self) -> Collect<Self, B>
    where B: Default + Extend<Self::Item>;
}

impl<S: Stream + Sized> StreamExt for S {
    fn next(&mut self) -> Next<'_, Self> {
        Next { stream: self }
    }

    fn map<U, F>(self, f: F) -> Map<Self, U, F>
    where F: FnMut(Self::Item) -> U,
    {
        Map { stream: self, f, _phantom: PhantomData }
    }

    fn filter<F>(self, f: F) -> Filter<Self, F>
    where F: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }

    fn filter_map<U, F>(self, f: F) -> FilterMap<Self, U, F>
    where F: FnMut(Self::Item) -> Option<U>,
    {
        FilterMap { stream: self, f, _phantom: PhantomData }
    }

    fn take(self, n: usize) -> Take<Self> {
        Take { stream: self, remaining: n }
    }

    fn skip(self, n: usize) -> Skip<Self> {
        Skip { stream: self, remaining: n }
    }

    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where F: FnMut(Self::Item) -> Fut, Fut: Future,
    {
        Then { stream: self, f, future: None, _phantom: PhantomData }
    }

    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where F: FnMut(Self::Item) -> Fut, Fut: Future<Output = ()>,
    {
        ForEach { stream: self, f, state: ForEachState::Streaming, _phantom: PhantomData }
    }

    fn fold<B, F, Fut>(self, init: B, f: F) -> Fold<Self, B, F, Fut>
    where F: FnMut(B, Self::Item) -> Fut, Fut: Future<Output = B>,
    {
        Fold { stream: self, acc: init, f, state: FoldState::Streaming, _phantom: PhantomData }
    }

    fn flatten(self) -> Flatten<Self>
    where Self::Item: Stream,
    {
        Flatten { stream: self, inner: None }
    }

    fn collect<B>(self) -> Collect<Self, B>
    where B: Default + Extend<Self::Item>,
    {
        Collect { stream: self, collection: B::default() }
    }

    fn collect_stream<B>(self) -> Collect<Self, B>
    where B: Default + Extend<Self::Item>,
    {
        self.collect()
    }
}

// Combinator structs
pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Next<'a, S> {
        stream: &'a mut S,
    }
}

impl<'a, S: Stream> Future for Next<'a, S> {
    type Output = Option<S::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We know that the stream is pinned because we're inside a pinned context
        unsafe { Pin::new_unchecked(&mut *self.stream).poll_next(cx) }
    }
}

pin_project! {
    pub struct Map<S, U, F> {
        #[pin]
        stream: S,
        f: F,
        _phantom: PhantomData<U>,
    }
}

impl<S, U, F> Stream for Map<S, U, F>
where
    S: Stream,
    F: FnMut(S::Item) -> U,
{
    type Item = U;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some((this.f)(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    pub struct Filter<S, F> {
        #[pin]
        stream: S,
        f: F,
    }
}

impl<S, F> Stream for Filter<S, F>
where
    S: Stream,
    F: FnMut(&S::Item) -> bool,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
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

pin_project! {
    pub struct FilterMap<S, U, F> {
        #[pin]
        stream: S,
        f: F,
        _phantom: PhantomData<U>,
    }
}

impl<S, U, F> Stream for FilterMap<S, U, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Option<U>,
{
    type Item = U;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if let Some(mapped) = (this.f)(item) {
                        return Poll::Ready(Some(mapped));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    pub struct Take<S> {
        #[pin]
        stream: S,
        remaining: usize,
    }
}

impl<S: Stream> Stream for Take<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.remaining == 0 {
            return Poll::Ready(None);
        }
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                *this.remaining -= 1;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    pub struct Skip<S> {
        #[pin]
        stream: S,
        remaining: usize,
    }
}

impl<S: Stream> Stream for Skip<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        while *this.remaining > 0 {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(_)) => *this.remaining -= 1,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        this.stream.poll_next(cx)
    }
}

pin_project! {
    pub struct Then<S, F, Fut> {
        #[pin]
        stream: S,
        f: F,
        #[pin]
        future: Option<Fut>,
        _phantom: PhantomData<Fut>,
    }
}

impl<S, F, Fut> Stream for Then<S, F, Fut>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            
            if let Some(fut) = this.future.as_mut().as_pin_mut() {
                if let Poll::Ready(item) = fut.poll(cx) {
                    this.future.set(None);
                    return Poll::Ready(Some(item));
                } else {
                    return Poll::Pending;
                }
            }

            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let fut = (this.f)(item);
                    this.future.set(Some(fut));
                    // Continue loop to poll the newly set future
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ForEach<S, F, Fut> {
        #[pin]
        stream: S,
        f: F,
        #[pin]
        state: ForEachState<Fut>,
        _phantom: PhantomData<Fut>,
    }
}
pin_project! {
    #[project = ForEachStateProj]
    enum ForEachState<Fut> {
        Streaming,
        Processing { #[pin] future: Fut },
    }
}

impl<S, F, Fut> Future for ForEach<S, F, Fut>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                ForEachStateProj::Streaming => {
                    match this.stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let future = (this.f)(item);
                            this.state.set(ForEachState::Processing { future });
                        }
                        Poll::Ready(None) => return Poll::Ready(()),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ForEachStateProj::Processing { future } => {
                    match future.poll(cx) {
                        Poll::Ready(_) => {
                            this.state.set(ForEachState::Streaming);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Fold<S, B, F, Fut> {
        #[pin]
        stream: S,
        acc: B,
        f: F,
        #[pin]
        state: FoldState<B, Fut>,
        _phantom: PhantomData<Fut>,
    }
}
pin_project! {
    #[project = FoldStateProj]
    enum FoldState<B, Fut> {
        Streaming,
        Processing { #[pin] future: Fut },
        Done { val: B },
    }
}

impl<S, B, F, Fut> Future for Fold<S, B, F, Fut>
where
    S: Stream,
    B: Clone,
    F: FnMut(B, S::Item) -> Fut,
    Fut: Future<Output = B>,
{
    type Output = B;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                FoldStateProj::Streaming => {
                    match this.stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let future = (this.f)(this.acc.clone(), item);
                            this.state.set(FoldState::Processing { future });
                        }
                        Poll::Ready(None) => {
                            let result = this.acc.clone();
                            this.state.set(FoldState::Done { val: result.clone() });
                            return Poll::Ready(result);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                FoldStateProj::Processing { future } => {
                    match future.poll(cx) {
                        Poll::Ready(new_acc) => {
                            *this.acc = new_acc;
                            this.state.set(FoldState::Streaming);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                FoldStateProj::Done { val } => {
                    return Poll::Ready(val.clone());
                }
            }
        }
    }
}

pin_project! {
    pub struct Flatten<S>
    where S: Stream,
          S::Item: Stream,
    {
        #[pin]
        stream: S,
        #[pin]
        inner: Option<S::Item>,
    }
}

impl<S> Stream for Flatten<S>
where S: Stream,
      S::Item: Stream,
{
    type Item = <S::Item as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if let Some(inner_stream) = this.inner.as_mut().as_pin_mut() {
                match inner_stream.poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        this.inner.set(None);
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }

            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(inner_stream)) => {
                    this.inner.set(Some(inner_stream));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Collect<S, B> {
        #[pin]
        stream: S,
        collection: B,
    }
}

impl<S, B> Future for Collect<S, B>
where
    S: Stream,
    B: Default + Extend<S::Item>,
{
    type Output = B;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.collection.extend(std::iter::once(item));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(std::mem::take(this.collection));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Bracket combinator for resource management
pub struct BracketStream<A, O, St, FAcq, FUse, FRel, R>
where
    FAcq: Future<Output = A> + Send + 'static,
    FUse: FnOnce(A) -> St + Send + 'static,
    St: Stream<Item = O> + Send + 'static,
    FRel: FnOnce(A) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    O: Send + 'static,
    A: Clone + Send + 'static,
{
    pub state: BracketState<A, O, St, R>,
    pub acquire: Option<FAcq>,
    pub use_fn: Option<FUse>,
    pub release: Option<FRel>,
}

pub enum BracketState<A, O, St, R> {
    Start,
    Acquiring(Pin<Box<dyn Future<Output = A> + Send>>),
    Streaming {
        resource: A,
        stream: St,
    },
    Releasing(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
    _Phantom(PhantomData<(O, R)>), // Use both O and R type parameters
}

impl<A, O, St, FAcq, FUse, FRel, R> Stream for BracketStream<A, O, St, FAcq, FUse, FRel, R>
where
    FAcq: Future<Output = A> + Send + 'static,
    FUse: FnOnce(A) -> St + Send + 'static,
    St: Stream<Item = O> + Send + 'static,
    FRel: FnOnce(A) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    O: Send + 'static,
    A: Clone + Send + 'static,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match &mut this.state {
                BracketState::Start => {
                    if let Some(acquire) = this.acquire.take() {
                        this.state = BracketState::Acquiring(Box::pin(acquire));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                BracketState::Acquiring(acquire_future) => {
                    match acquire_future.as_mut().poll(cx) {
                        Poll::Ready(resource) => {
                            if let Some(use_fn) = this.use_fn.take() {
                                let stream = use_fn(resource.clone());
                                this.state = BracketState::Streaming { resource, stream };
                            } else {
                                this.state = BracketState::Done;
                                return Poll::Ready(None);
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                BracketState::Streaming { resource, stream } => {
                    let pinned_stream = unsafe { Pin::new_unchecked(stream) };
                    match pinned_stream.poll_next(cx) {
                        Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                        Poll::Ready(None) => {
                            // Stream finished, start releasing
                            if let Some(release_fn) = this.release.take() {
                                let release_future = release_fn(resource.clone());
                                this.state = BracketState::Releasing(Box::pin(release_future));
                            } else {
                                this.state = BracketState::Done;
                                return Poll::Ready(None);
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                BracketState::Releasing(release_future) => {
                    match release_future.as_mut().poll(cx) {
                        Poll::Ready(()) => {
                            this.state = BracketState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                BracketState::Done => return Poll::Ready(None),
                BracketState::_Phantom(_) => unreachable!(),
            }
        }
    }
}

// Implement Stream for Pin<Box<BracketStream<...>>>
impl<A, O, St, FAcq, FUse, FRel, R> Stream for Pin<Box<BracketStream<A, O, St, FAcq, FUse, FRel, R>>>
where
    FAcq: Future<Output = A> + Send + 'static,
    FUse: FnOnce(A) -> St + Send + 'static,
    St: Stream<Item = O> + Send + 'static,
    FRel: FnOnce(A) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
    O: Send + 'static,
    A: Clone + Send + 'static,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: Box<BracketStream> is always Unpin, so this is safe
        unsafe { Pin::get_unchecked_mut(self).as_mut().poll_next(cx) }
    }
}