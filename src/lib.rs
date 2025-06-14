pub mod queue;
pub mod rs2;
pub mod error;

pub mod pipe;

pub mod stream_configuration;
pub mod stream_performance_metrics;
pub mod connectors;
pub mod work_stealing;
pub mod rs2_stream_ext;
pub mod rs2_result_stream_ext;

// Re-export all items from rs2 module at the crate root
pub use rs2::*;
pub use work_stealing::{
    WorkStealingConfig,
    WorkStealingExt,
    par_eval_map_work_stealing,
};
