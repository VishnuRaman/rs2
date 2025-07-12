//! Metrics stream implementation
//! 
//! This module provides a stream wrapper that tracks metrics while processing items.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Mutex;

use crate::stream_performance_metrics::StreamMetrics;
use crate::stream::Stream;

/// A stream wrapper that tracks metrics while processing items
pub struct WithMetricsStream<S> {
    inner: Pin<Box<S>>,
    metrics: Arc<Mutex<StreamMetrics>>,
}

impl<S> WithMetricsStream<S> {
    /// Create a new metrics stream wrapper
    pub fn new(inner: S, metrics: Arc<Mutex<StreamMetrics>>) -> Self {
        Self { inner: Box::pin(inner), metrics }
    }
}

impl<S> Stream for WithMetricsStream<S>
where
    S: Stream,
    S::Item: Send + 'static,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let inner = this.inner.as_mut();
        
        match inner.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Update metrics synchronously
                let size = std::mem::size_of_val(&item) as u64;
                let metrics = this.metrics.clone();
                
                // Spawn a task to update metrics asynchronously
                tokio::spawn(async move {
                    let mut m = metrics.lock().await;
                    m.record_item(size);
                });
                
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                // Stream ended, finalize metrics
                let metrics = this.metrics.clone();
                tokio::spawn(async move {
                    let mut m = metrics.lock().await;
                    m.finalize();
                });
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
} 