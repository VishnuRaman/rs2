use crate::rs2::RS2Stream;
use futures_util::StreamExt;
use std::sync::Arc;
use async_stream::stream;

/// A Pipe represents a rs2_stream transformation from one type to another.
/// It's a function from Stream[I] to Stream[O].
pub struct Pipe<I, O> {
    f: Arc<dyn Fn(RS2Stream<I>) -> RS2Stream<O> + Send + Sync + 'static>,
}

impl<I, O> Clone for Pipe<I, O> {
    fn clone(&self) -> Self {
        Pipe {
            f: Arc::clone(&self.f),
        }
    }
}

impl<I, O> Pipe<I, O> {
    /// Create a new pipe from a function
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(RS2Stream<I>) -> RS2Stream<O> + Send + Sync + 'static,
    {
        Pipe {
            f: Arc::new(f),
        }
    }

    /// Apply this pipe to a rs2_stream
    pub fn apply(&self, input: RS2Stream<I>) -> RS2Stream<O> {
        (self.f)(input)
    }
}

/// Create a pipe that applies the given function to each element
pub fn map<I, O, F>(f: F) -> Pipe<I, O>
where
    F: Fn(I) -> O + Send + Sync + Clone + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    Pipe::new(move |input| {
        let f = f.clone();
        input.map(move |i| f(i)).boxed()
    })
}

/// Create a pipe that filters elements based on the predicate
pub fn filter<I, F>(predicate: F) -> Pipe<I, I>
where
    F: Fn(&I) -> bool + Send + Sync + Clone + 'static,
    I: Send + 'static,
{
    Pipe::new(move |input| {
        let predicate = predicate.clone();
        stream! {
            let mut s = input;
            while let Some(item) = s.next().await {
                if predicate(&item) {
                    yield item;
                }
            }
        }.boxed()
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
        let p1 = p1.clone();
        let p2 = p2.clone();
        p2.apply(p1.apply(input))
    })
}

/// Identity pipe that doesn't transform the rs2_stream
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
        compose(self, other)
    }
}
