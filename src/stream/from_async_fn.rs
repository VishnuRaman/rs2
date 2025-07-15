use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};
use crate::stream::Stream;

/// A stream that yields items from an async function until it returns None.
pub struct FromAsyncFn<Fut, F>
where
    F: FnMut() -> Fut,
    Fut: Future,
{
    f: F,
    fut: Option<Fut>,
}

impl<Fut, F> FromAsyncFn<Fut, F>
where
    F: FnMut() -> Fut,
    Fut: Future,
{
    pub fn new(f: F) -> Self {
        Self { f, fut: None }
    }
}

impl<Fut, F, T> Stream for FromAsyncFn<Fut, F>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: We never move the struct after being pinned
        let this = unsafe { self.get_unchecked_mut() };
        
        loop {
            if let Some(fut) = this.fut.as_mut() {
                // SAFETY: We never move the future after being pinned
                let fut = unsafe { Pin::new_unchecked(fut) };
                match fut.poll(cx) {
                    Poll::Ready(item) => {
                        this.fut = None;
                        return Poll::Ready(item);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                this.fut = Some((this.f)());
            }
        }
    }
}

/// Convenience function to create a FromAsyncFn stream
pub fn from_async_fn<Fut, F, T>(f: F) -> FromAsyncFn<Fut, F>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    FromAsyncFn::new(f)
}
