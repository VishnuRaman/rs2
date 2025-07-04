//! Stream constructors: empty, once, repeat, iter, unfold, pending
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::marker::PhantomData;
use super::core::Stream;

// ================================
// Basic Constructors
// ================================

pub struct Empty<T> {
    pub(crate) _phantom: PhantomData<T>
}

impl<T> Stream for Empty<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

pin_project! {
    pub struct Once<T> {
        pub(crate) value: Option<T>
    }
}

impl<T> Stream for Once<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        Poll::Ready(this.value.take())
    }
}

pin_project! {
    pub struct Repeat<T> {
        pub(crate) value: T
    }
}

impl<T: Clone> Stream for Repeat<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        Poll::Ready(Some(this.value.clone()))
    }
}

pin_project! {
    pub struct Iter<I> {
        pub(crate) iter: I
    }
}

impl<I> Stream for Iter<I>
where I: Iterator {
    type Item = I::Item;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Direct access without pin_project overhead for better performance
        unsafe {
            let this = self.get_unchecked_mut();
            Poll::Ready(this.iter.next())
        }
    }
}

pub struct Pending<T> {
    pub(crate) _phantom: PhantomData<T>
}

impl<T> Stream for Pending<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

// ================================
// Function-based Constructors
// ================================

pin_project! {
    pub struct RepeatWith<F> {
        pub(crate) f: F
    }
}

impl<T, F> Stream for RepeatWith<F>
where F: FnMut() -> T {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        Poll::Ready(Some((this.f)()))
    }
}

pin_project! {
    pub struct OnceWith<F> {
        pub(crate) f: Option<F>
    }
}

impl<T, F> Stream for OnceWith<F>
where F: FnOnce() -> T {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        Poll::Ready(this.f.take().map(|f| f()))
    }
}

// ================================
// Conditional Stream Adapters
// ================================

pin_project! {
    pub struct SkipWhile<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
        pub(crate) skipping: bool
    }
}

impl<S, F> Stream for SkipWhile<S, F>
where S: Stream, F: FnMut(&S::Item) -> bool {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.skipping {
            loop {
                match this.stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        if !(this.f)(&item) {
                            *this.skipping = false;
                            return Poll::Ready(Some(item));
                        }
                        // Continue skipping
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        } else {
            this.stream.poll_next(cx)
        }
    }
}

pin_project! {
    pub struct TakeWhile<S, F> {
        #[pin]
        pub(crate) stream: S,
        pub(crate) f: F,
        pub(crate) done: bool
    }
}

impl<S, F> Stream for TakeWhile<S, F>
where S: Stream, F: FnMut(&S::Item) -> bool {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                if (this.f)(&item) {
                    Poll::Ready(Some(item))
                } else {
                    *this.done = true;
                    Poll::Ready(None)
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ================================
// Unfold - Complex Stateful Stream
// ================================

pin_project! {
    pub struct Unfold<S, F, Fut> {
        pub(crate) state: Option<S>,
        pub(crate) f: F,
        #[pin]
        pub(crate) future: Option<Pin<Box<Fut>>>,
    }
}

impl<S, O, F, Fut> Stream for Unfold<S, F, Fut>
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Option<(O, S)>>,
{
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // If we have a future in progress, poll it
            if let Some(fut) = this.future.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Poll::Ready(Some((item, new_state))) => {
                        this.future.set(None);
                        *this.state = Some(new_state);
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        this.future.set(None);
                        *this.state = None;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // If we have state but no future, create a new future
            if let Some(state) = this.state.take() {
                let fut = (this.f)(state);
                this.future.set(Some(Box::pin(fut)));
                // Continue the loop to poll the new future
            } else {
                // No state and no future, we're done
                return Poll::Ready(None);
            }
        }
    }
}

// ================================
// Extension Trait for Additional Constructors
// ================================

pub trait ConstructorStreamExt: Stream + Sized {
    fn skip_while<F>(self, f: F) -> SkipWhile<Self, F>
    where F: FnMut(&Self::Item) -> bool
    {
        SkipWhile {
            stream: self,
            f,
            skipping: true
        }
    }

    fn take_while<F>(self, f: F) -> TakeWhile<Self, F>
    where F: FnMut(&Self::Item) -> bool
    {
        TakeWhile {
            stream: self,
            f,
            done: false
        }
    }
}

impl<T> ConstructorStreamExt for T where T: Stream {}

// Note: StreamExt is automatically implemented for all Stream types via blanket impl
// in core.rs, so we don't need to manually implement it for each constructor type.

// ================================
// Constructor Functions
// ================================

/// Create an empty stream
pub fn empty<T>() -> Empty<T> {
    Empty { _phantom: PhantomData }
}

/// Create a stream that emits a single value
pub fn once<T>(value: T) -> Once<T> {
    Once { value: Some(value) }
}

/// Create a stream that repeats a value indefinitely
pub fn repeat<T>(value: T) -> Repeat<T> {
    Repeat { value }
}

/// Create a stream from an iterator
pub fn from_iter<I>(iter: I) -> Iter<I::IntoIter>
where I: IntoIterator {
    Iter { iter: iter.into_iter() }
}

/// Create a stream that never produces values
pub fn pending<T>() -> Pending<T> {
    Pending { _phantom: PhantomData }
}

/// Create a stream that repeats values from a closure
pub fn repeat_with<T, F>(f: F) -> RepeatWith<F>
where F: FnMut() -> T {
    RepeatWith { f }
}

/// Create a stream that emits a single value from a closure
pub fn once_with<T, F>(f: F) -> OnceWith<F>
where F: FnOnce() -> T {
    OnceWith { f: Some(f) }
}

/// Create a stream from a seed value and a function
pub fn unfold<S, O, F, Fut>(init: S, f: F) -> Unfold<S, F, Fut>
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Option<(O, S)>>
{
    Unfold {
        state: Some(init),
        f,
        future: None,
    }
}