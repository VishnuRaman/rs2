use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use async_stream::stream;
use crate::RS2Stream;

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

pub enum PipelineNode<T> {
    Source {
        name: String,
        func: Box<dyn Fn() -> RS2Stream<T> + Send + Sync>,
    },
    Transform {
        name: String,
        func: Box<dyn Fn(RS2Stream<T>) -> RS2Stream<T> + Send + Sync>,
    },
    Sink {
        name: String,
        func: Box<dyn Fn(RS2Stream<T>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    },
    Branch {
        name: String,
        sinks: Vec<Box<dyn Fn(RS2Stream<T>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>>,
    },
}

#[derive(Debug)]
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
    nodes: Vec<PipelineNode<T>>,
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
        F: Fn() -> RS2Stream<T> + Send + Sync + 'static,
    {
        self.nodes.push(PipelineNode::Source {
            name: name.to_string(),
            func: Box::new(f),
        });
        self
    }

    pub fn source<F>(self, f: F) -> Self
    where
        F: Fn() -> RS2Stream<T> + Send + Sync + 'static,
    {
        self.named_source("source", f)
    }

    pub fn named_transform<F>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(RS2Stream<T>) -> RS2Stream<T> + Send + Sync + 'static,
    {
        self.nodes.push(PipelineNode::Transform {
            name: name.to_string(),
            func: Box::new(f),
        });
        self
    }

    pub fn transform<F>(self, f: F) -> Self
    where
        F: Fn(RS2Stream<T>) -> RS2Stream<T> + Send + Sync + 'static,
    {
        self.named_transform("transform", f)
    }

    pub fn named_sink<F>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(RS2Stream<T>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        self.nodes.push(PipelineNode::Sink {
            name: name.to_string(),
            func: Box::new(f),
        });
        self
    }

    pub fn sink<F>(self, f: F) -> Self
    where
        F: Fn(RS2Stream<T>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        self.named_sink("sink", f)
    }

    pub fn branch<F1, F2>(mut self, name: &str, f1: F1, f2: F2) -> Self
    where
        F1: Fn(RS2Stream<T>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
        F2: Fn(RS2Stream<T>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        self.nodes.push(PipelineNode::Branch {
            name: name.to_string(),
            sinks: vec![Box::new(f1), Box::new(f2)],
        });
        self
    }

    pub fn validate(&self) -> PipelineResult<()> {
        if self.nodes.is_empty() {
            return Err(PipelineError::InvalidPipeline("Empty pipeline".to_string()));
        }

        let mut has_source = false;
        let mut has_sink_or_branch = false;

        for node in &self.nodes {
            match node {
                PipelineNode::Source { .. } => has_source = true,
                PipelineNode::Sink { .. } | PipelineNode::Branch { .. } => has_sink_or_branch = true,
                _ => {}
            }
        }

        if !has_source {
            return Err(PipelineError::NoSource);
        }

        if !has_sink_or_branch {
            return Err(PipelineError::NoSink);
        }

        Ok(())
    }

    pub async fn run(self) -> PipelineResult<()> {
        self.validate()?;

        let mut stream = None;

        for node in self.nodes {
            match node {
                PipelineNode::Source { name, func } => {
                    if self.config.enable_metrics {
                        println!("Pipeline '{}': Starting source '{}'", self.config.name, name);
                    }
                    stream = Some(func());
                }
                PipelineNode::Transform { name, func } => {
                    if let Some(s) = stream.take() {
                        if self.config.enable_metrics {
                            println!("Pipeline '{}': Applying transform '{}'", self.config.name, name);
                        }
                        stream = Some(func(s));
                    }
                }
                PipelineNode::Sink { name, func } => {
                    if let Some(s) = stream.take() {
                        if self.config.enable_metrics {
                            println!("Pipeline '{}': Running sink '{}'", self.config.name, name);
                        }
                        func(s).await;
                    }
                }
                PipelineNode::Branch { name, sinks } => {
                    if let Some(s) = stream.take() {
                        if self.config.enable_metrics {
                            println!("Pipeline '{}': Branching to {} sinks at '{}'",
                                     self.config.name, sinks.len(), name);
                        }

                        // Use broadcast to fan out to multiple sinks
                        let (tx, _) = broadcast::channel(self.config.buffer_size);

                        // Spawn task to feed the broadcast channel
                        let tx_clone = tx.clone();
                        tokio::spawn(async move {
                            let mut stream = s;
                            while let Some(item) = stream.next().await {
                                if tx_clone.send(item).is_err() {
                                    break; // All receivers dropped
                                }
                            }
                        });

                        // Run all sinks concurrently
                        let mut handles = Vec::new();
                        for sink_func in sinks {
                            let mut rx = tx.subscribe();

                            // Create a stream from the broadcast receiver
                            let sink_stream = stream! {
                                while let Ok(item) = rx.recv().await {
                                    yield item;
                                }
                            }.boxed();

                            handles.push(tokio::spawn(async move {
                                sink_func(sink_stream).await;
                            }));
                        }

                        // Wait for all sinks to complete
                        for handle in handles {
                            if let Err(e) = handle.await {
                                return Err(PipelineError::RuntimeError(Box::new(e)));
                            }
                        }
                    }
                }
            }
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