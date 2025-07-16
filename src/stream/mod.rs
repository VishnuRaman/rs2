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
pub mod round_robin;
pub mod metrics;
pub mod timeout;
pub mod from_async_fn;
pub mod either;
pub mod group_by;
pub mod deduplicate;
pub mod stateful_throttle;
pub mod join;
pub mod window;

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

// Re-export metrics stream
pub use metrics::WithMetricsStream;

// Re-export timeout stream
pub use timeout::TimeoutStream;

// Re-export either enum
pub use either::Either;

// Re-export from_async_fn
pub use from_async_fn::{from_async_fn, FromAsyncFn}; 