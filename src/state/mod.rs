pub mod config;
pub mod storage;
pub mod stream_ext;
pub mod traits;

// `StateConfigBuilder` and `StateConfigs` are public in `config` but were
// not re-exported, so `rs2_stream::state::*` could not reach them.
pub use config::{StateConfig, StateConfigBuilder, StateConfigs};
pub use storage::InMemoryState;
pub use stream_ext::StatefulStreamExt;
pub use traits::{
    CustomKeyExtractor, FieldKeyExtractor, KeyExtractor, StateError, StateResult, StateStorage,
    StateStorageType, TryKeyExtractor,
};
