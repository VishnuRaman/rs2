use crate::stream::Stream;
use crate::stream::StreamExt;
use std::pin::Pin;
use std::future::Future;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::any::Any;

#[derive(Debug)]
pub enum PipelineError {
    NoSource,
    NoSink,
    InvalidPipeline(String),
    RuntimeError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::NoSource => write!(f, "Pipeline has no source"),
            PipelineError::NoSink => write!(f, "Pipeline has no sink"),
            PipelineError::InvalidPipeline(msg) => write!(f, "Invalid pipeline: {}", msg),
            PipelineError::RuntimeError(e) => write!(f, "Runtime error: {}", e),
        }
    }
}

impl std::error::Error for PipelineError {}

pub type PipelineResult<T> = Result<T, PipelineError>;

/// A pipeline node that can be executed
pub trait PipelineNode<T>: Send + Sync {
    fn execute(&self, input: Option<Arc<dyn Stream<Item = T> + Send + Sync>>) -> PipelineResult<Option<Arc<dyn Stream<Item = T> + Send + Sync>>>;
    fn as_any(&self) -> &dyn Any;
}

/// Source node that creates a stream
pub struct SourceNode<T> {
    name: String,
    func: Arc<dyn Fn() -> Arc<dyn Stream<Item = T> + Send + Sync> + Send + Sync>,
}

impl<T> PipelineNode<T> for SourceNode<T>
where
    T: Send + 'static,
{
    fn execute(&self, _input: Option<Arc<dyn Stream<Item = T> + Send + Sync>>) -> PipelineResult<Option<Arc<dyn Stream<Item = T> + Send + Sync>>> {
        Ok(Some((self.func)()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Transform node that modifies a stream
pub struct TransformNode<T> {
    name: String,
    func: Arc<dyn Fn(Arc<dyn Stream<Item = T> + Send + Sync>) -> Arc<dyn Stream<Item = T> + Send + Sync> + Send + Sync>,
}

impl<T> PipelineNode<T> for TransformNode<T>
where
    T: Send + 'static,
{
    fn execute(&self, input: Option<Arc<dyn Stream<Item = T> + Send + Sync>>) -> PipelineResult<Option<Arc<dyn Stream<Item = T> + Send + Sync>>> {
        if let Some(stream) = input {
            Ok(Some((self.func)(stream)))
        } else {
            Err(PipelineError::InvalidPipeline("Transform node requires input".to_string()))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Sink node that consumes a stream
pub struct SinkNode<T> {
    name: String,
    func: Arc<dyn Fn(Arc<dyn Stream<Item = T> + Send + Sync>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
}

impl<T> PipelineNode<T> for SinkNode<T>
where
    T: Send + 'static,
{
    fn execute(&self, input: Option<Arc<dyn Stream<Item = T> + Send + Sync>>) -> PipelineResult<Option<Arc<dyn Stream<Item = T> + Send + Sync>>> {
        if let Some(stream) = input {
            // Spawn the sink execution
            let future = (self.func)(stream);
            tokio::spawn(async move {
                future.await;
            });
            Ok(None) // Sink consumes the stream
        } else {
            Err(PipelineError::InvalidPipeline("Sink node requires input".to_string()))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct PipelineConfig {
    pub name: String,
    pub buffer_size: usize,
    pub enable_metrics: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "unnamed-pipeline".to_string(),
            buffer_size: 1000,
            enable_metrics: false,
        }
    }
}

pub struct Pipeline<T> {
    config: PipelineConfig,
    nodes: Vec<Box<dyn PipelineNode<T>>>,
}

impl<T: Send + Clone + 'static> Pipeline<T> {
    pub fn new() -> Self {
        Self {
            config: PipelineConfig::default(),
            nodes: vec![],
        }
    }

    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    pub fn named_source<F>(mut self, name: &str, f: F) -> Self
    where
        F: Fn() -> Arc<dyn Stream<Item = T> + Send + Sync> + Send + Sync + 'static,
    {
        self.nodes.push(Box::new(SourceNode {
            name: name.to_string(),
            func: Arc::new(f),
        }));
        self
    }

    pub fn source<F>(self, f: F) -> Self
    where
        F: Fn() -> Arc<dyn Stream<Item = T> + Send + Sync> + Send + Sync + 'static,
    {
        self.named_source("source", f)
    }

    pub fn named_transform<F>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(Arc<dyn Stream<Item = T> + Send + Sync>) -> Arc<dyn Stream<Item = T> + Send + Sync> + Send + Sync + 'static,
    {
        self.nodes.push(Box::new(TransformNode {
            name: name.to_string(),
            func: Arc::new(f),
        }));
        self
    }

    pub fn transform<F>(self, f: F) -> Self
    where
        F: Fn(Arc<dyn Stream<Item = T> + Send + Sync>) -> Arc<dyn Stream<Item = T> + Send + Sync> + Send + Sync + 'static,
    {
        self.named_transform("transform", f)
    }

    pub fn named_sink<F>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(Arc<dyn Stream<Item = T> + Send + Sync>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        self.nodes.push(Box::new(SinkNode {
            name: name.to_string(),
            func: Arc::new(f),
        }));
        self
    }

    pub fn sink<F>(self, f: F) -> Self
    where
        F: Fn(Arc<dyn Stream<Item = T> + Send + Sync>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        self.named_sink("sink", f)
    }

    pub fn validate(&self) -> PipelineResult<()> {
        if self.nodes.is_empty() {
            return Err(PipelineError::InvalidPipeline("Empty pipeline".to_string()));
        }

        let mut has_source = false;
        let mut has_sink = false;

        for node in &self.nodes {
            if node.as_any().downcast_ref::<SourceNode<T>>().is_some() {
                has_source = true;
            }
            if node.as_any().downcast_ref::<SinkNode<T>>().is_some() {
                has_sink = true;
            }
        }

        if !has_source {
            return Err(PipelineError::NoSource);
        }

        if !has_sink {
            return Err(PipelineError::NoSink);
        }

        Ok(())
    }

    pub async fn run(self) -> PipelineResult<()> {
        self.validate()?;

        let mut current_stream: Option<Arc<dyn Stream<Item = T> + Send + Sync>> = None;

        for node in self.nodes {
            current_stream = node.execute(current_stream)?;
        }

        Ok(())
    }
}

// Fix the Default implementation to match the trait bounds
impl<T: Send + Clone + 'static> Default for Pipeline<T> {
    fn default() -> Self {
        Self::new()
    }
}
