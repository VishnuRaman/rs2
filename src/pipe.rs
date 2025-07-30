use crate::stream::Stream;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use pin_project_lite::pin_project;

/// A Pipe represents a stream transformation from one type to another.
/// It's a function from Stream[I] to Stream[O].
pub struct Pipe<I, O> {
    f: Box<dyn Fn(Box<dyn Stream<Item = I> + Send + Unpin>) -> Box<dyn Stream<Item = O> + Send + Unpin> + Send + Sync + 'static>,
}

impl<I, O> Pipe<I, O> 
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Create a new pipe from a function
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Box<dyn Stream<Item = I> + Send + Unpin>) -> Box<dyn Stream<Item = O> + Send + Unpin> + Send + Sync + 'static,
    {
        Pipe { f: Box::new(f) }
    }

    /// Apply this pipe to a stream
    pub fn apply<S>(&self, input: S) -> impl Stream<Item = O> + Send + Unpin + 'static
    where
        S: Stream<Item = I> + Send + Unpin + 'static,
    {
        // Convert to Box<dyn Stream>, apply transformation, then convert back
        let boxed_input: Box<dyn Stream<Item = I> + Send + Unpin> = Box::new(input);
        let boxed_output = (self.f)(boxed_input);
        
        // Create a concrete stream that delegates to the Box<dyn Stream>
        BoxStreamWrapper::new(boxed_output)
    }
}

// Note: Clone is not implemented for Pipe because Box<dyn Fn...> doesn't implement Clone
// If cloning is needed, we would need to use Arc<dyn Fn...> and adjust the design

/// Wrapper to convert Box<dyn Stream> to impl Stream
pin_project! {
    struct BoxStreamWrapper<T> {
        #[pin]
        inner: Box<dyn Stream<Item = T> + Send + Unpin>,
    }
}

impl<T> BoxStreamWrapper<T> {
    fn new(inner: Box<dyn Stream<Item = T> + Send + Unpin>) -> Self {
        Self { inner }
    }
}

impl<T> Stream for BoxStreamWrapper<T>
where
    T: Send + 'static,
{
    type Item = T;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        // Since the inner Box<dyn Stream + Unpin> implements Unpin,
        // we can safely create a new Pin from the dereferenced Box
        Pin::new(&mut **this.inner).poll_next(cx)
    }
}

/// Create a pipe that applies the given function to each element
pub fn map<I, O, F>(f: F) -> Pipe<I, O>
where
    F: Fn(I) -> O + Send + Sync + Clone + Unpin + 'static,
    I: Send + 'static,
    O: Send + Unpin + 'static,
{
    Pipe::new(move |input| {
        let f = f.clone();
        Box::new(MapStream {
            inner: input,
            f,
            _phantom: PhantomData,
        })
    })
}

/// Create a pipe that filters elements based on the predicate
pub fn filter<I, F>(predicate: F) -> Pipe<I, I>
where
    F: Fn(&I) -> bool + Send + Sync + Clone + Unpin + 'static,
    I: Send + Unpin + 'static,
{
    Pipe::new(move |input| {
        let predicate = predicate.clone();
        Box::new(FilterStream {
            inner: input,
            predicate,
        })
    })
}

/// Compose two pipes together
pub fn compose<I, M, O>(p1: Pipe<I, M>, p2: Pipe<M, O>) -> Pipe<I, O>
where
    I: Send + 'static,
    M: Send + 'static,
    O: Send + 'static,
{
    Pipe::new(move |input| {
        let intermediate = (p1.f)(input);
        (p2.f)(intermediate)
    })
}

/// Identity pipe that doesn't transform the stream
pub fn identity<I>() -> Pipe<I, I>
where
    I: Send + 'static,
{
    Pipe::new(|input| input)
}

/// Extension trait for pipes
pub trait PipeExt<I, O> {
    /// Compose this pipe with another pipe
    fn compose<P>(self, other: Pipe<O, P>) -> Pipe<I, P>
    where
        P: Send + 'static;
}

impl<I, O> PipeExt<I, O> for Pipe<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    fn compose<P>(self, other: Pipe<O, P>) -> Pipe<I, P>
    where
        P: Send + 'static,
    {
        compose::<I, O, P>(self, other)
    }
}

// Stream wrapper implementations

pin_project! {
    struct MapStream<I, O, F> {
        #[pin]
        inner: Box<dyn Stream<Item = I> + Send + Unpin>,
        f: F,
        _phantom: PhantomData<O>,
    }
}

impl<I, O, F> Stream for MapStream<I, O, F>
where
    F: Fn(I) -> O + Send + Unpin,
    I: Send + 'static,
    O: Send + 'static,
{
    type Item = O;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        // Since the inner Box<dyn Stream + Unpin> implements Unpin,
        // we can safely create a new Pin from the dereferenced Box
        match Pin::new(&mut **this.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some((this.f)(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    struct FilterStream<I, F> {
        #[pin]
        inner: Box<dyn Stream<Item = I> + Send + Unpin>,
        predicate: F,
    }
}

impl<I, F> Stream for FilterStream<I, F>
where
    F: Fn(&I) -> bool + Send + Unpin,
    I: Send + 'static,
{
    type Item = I;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            // Since the inner Box<dyn Stream + Unpin> implements Unpin,
            // we can safely create a new Pin from the dereferenced Box
            match Pin::new(&mut **this.inner).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if (this.predicate)(&item) {
                        return Poll::Ready(Some(item));
                    }
                    // Continue polling if predicate doesn't match
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
