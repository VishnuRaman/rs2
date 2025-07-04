//! Custom stream implementation with zero overhead
//! 
//! This module provides a custom Stream trait and combinators that avoid the overhead
//! of `.boxed()` calls and external dependencies like `tokio-stream` and `futures_util`.

pub mod core;
pub mod constructors;
pub mod advanced;
pub mod utility;
pub mod select;
pub mod rate;
pub mod async_combinators;
pub mod specialized;
pub mod parallel;

// Re-export core types
pub use core::{Stream, StreamExt};

// Re-export constructors
pub use constructors::{
    empty, once, repeat, from_iter, pending, repeat_with, once_with, unfold,
    Empty, Once, Repeat, Iter, Pending, RepeatWith, OnceWith, Unfold,
    SkipWhile, TakeWhile, ConstructorStreamExt
};

// Re-export advanced combinators
pub use advanced::{
    FlatMap, FilterMap, Scan, Zip, Flatten, AdvancedStreamExt
};

// Re-export utility combinators
pub use utility::{
    Nth, Last, All, Any, Find, Position, Count, StepBy, Inspect,
    Enumerate, Chain, UtilityStreamExt,
};

// Re-export select/merge combinators
pub use select::{
    Select, Merge, Fuse, Peekable, SelectStreamExt
};

// Re-export rate limiting combinators
pub use rate::{
    Throttle, Debounce, RateStreamExt
};

// Re-export async/parallel combinators
pub use async_combinators::{
    BufferUnordered, ForEachConcurrent, TryForEachConcurrent, AsyncStreamExt
};

// Re-export specialized combinators
pub use specialized::{
    TryStream, TryMap, TryFilter, TryFold, TryForEach,
    Chunks, ChunksTimeout, TakeUntil, SkipUntil, Backpressure,
    SpecializedStreamExt, BackpressureExt
};

// Re-export parallel combinators
pub use parallel::{
    ParEvalMap, ParEvalMapUnordered, ParallelStreamExt
};

// Implement Stream for Box<dyn Stream> to support trait objects
impl<T> Stream for Box<dyn Stream<Item = T> + Send + 'static> {
    type Item = T;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        use std::pin::Pin;
        // Safety: We're just delegating to the inner stream's poll_next
        let inner = unsafe { Pin::new_unchecked(&mut **self) };
        inner.poll_next(cx)
    }
} 