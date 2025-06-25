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
    Enumerate, UtilityStreamExt,
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