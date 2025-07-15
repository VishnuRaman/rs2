//! Priority queue implementation for media streaming
//!
//! Extends the existing Queue with priority-based ordering

use super::types::{MediaChunk, MediaPriority};
use crate::queue::{Queue, QueueError};
use crate::stream::{Stream, StreamExt};
use crate::rs2_stream_ext::RS2StreamExt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::resource_manager::get_global_resource_manager;
use std::pin::Pin;
use std::task::{Context, Poll};
use pin_project_lite::pin_project;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PriorityItem {
    chunk: MediaChunk,
    priority: MediaPriority,
    sequence: u64,
}

impl PartialOrd for PriorityItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first, then lower sequence number (older chunks first)
        self.priority
            .cmp(&other.priority)
            .then_with(|| Reverse(self.sequence).cmp(&Reverse(other.sequence)))
    }
}

// Custom stream for priority queue dequeue
pin_project! {
    struct PriorityQueueStream {
        priority_buffer: Arc<Mutex<BinaryHeap<PriorityItem>>>,
        queue_stream: Pin<Box<dyn Stream<Item = PriorityItem> + Send>>,
        resource_manager: Arc<crate::resource_manager::ResourceManager>,
    }
}

impl Stream for PriorityQueueStream {
    type Item = MediaChunk;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        // First check priority buffer
        if let Ok(mut buffer) = this.priority_buffer.try_lock() {
            if let Some(item) = buffer.pop() {
                // Track memory deallocation
                let resource_manager = this.resource_manager.clone();
                tokio::spawn(async move {
                    resource_manager.track_memory_deallocation(1).await;
                });
                return Poll::Ready(Some(item.chunk));
            }
        }
        
        // No high priority items, get from main queue
        match this.queue_stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Track memory deallocation
                let resource_manager = this.resource_manager.clone();
                tokio::spawn(async move {
                    resource_manager.track_memory_deallocation(1).await;
                });
                Poll::Ready(Some(item.chunk))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Priority queue for media chunks
/// Uses your existing Queue internally but adds priority ordering
pub struct MediaPriorityQueue {
    internal_queue: Queue<PriorityItem>,
    priority_buffer: Arc<Mutex<BinaryHeap<PriorityItem>>>,
    buffer_size: usize,
}

impl MediaPriorityQueue {
    pub fn new(capacity: usize, priority_buffer_size: usize) -> Self {
        Self {
            internal_queue: Queue::bounded(capacity),
            priority_buffer: Arc::new(Mutex::new(BinaryHeap::new())),
            buffer_size: priority_buffer_size,
        }
    }

    pub async fn enqueue(&self, chunk: MediaChunk) -> Result<(), QueueError> {
        let resource_manager = get_global_resource_manager();
        let priority = chunk.priority.clone();
        let sequence = chunk.sequence_number;
        let item = PriorityItem {
            chunk,
            priority,
            sequence,
        };
        // Try to add to priority buffer first
        {
            let mut buffer = self.priority_buffer.lock().await;
            if buffer.len() < self.buffer_size {
                buffer.push(item);
                resource_manager.track_memory_allocation(1).await.ok();
                return Ok(());
            } else {
                resource_manager.track_buffer_overflow().await.ok();
            }
        }
        // Buffer full, push to main queue
        self.internal_queue.enqueue(item).await
    }

    pub fn dequeue(&self) -> impl Stream<Item = MediaChunk> + Send + 'static {
        PriorityQueueStream {
            priority_buffer: Arc::clone(&self.priority_buffer),
            queue_stream: Box::pin(self.internal_queue.stream()),
            resource_manager: get_global_resource_manager(),
        }
    }

    /// Try to enqueue without blocking - useful for live streaming
    pub async fn try_enqueue(&self, chunk: MediaChunk) -> Result<(), QueueError> {
        let resource_manager = get_global_resource_manager();
        let priority = chunk.priority.clone();
        let sequence = chunk.sequence_number;
        let item = PriorityItem {
            chunk,
            priority,
            sequence,
        };
        // Try to add to priority buffer first
        {
            let mut buffer = self.priority_buffer.lock().await;
            if buffer.len() < self.buffer_size {
                buffer.push(item);
                resource_manager.track_memory_allocation(1).await.ok();
                return Ok(());
            } else {
                resource_manager.track_buffer_overflow().await.ok();
            }
        }
        // Buffer full, try to push to main queue without blocking
        self.internal_queue.try_enqueue(item).await
    }

    pub async fn close(&self) {
        // Note: We can't close the internal queue from a shared reference
        // This is a limitation of the current design
        // In a real implementation, you might want to use a different approach
        // such as a separate close channel or atomic flags
    }

    pub async fn len(&self) -> usize {
        let buffer_len = {
            let buffer = self.priority_buffer.lock().await;
            buffer.len()
        };
        buffer_len + self.internal_queue.len().await
    }

    /// Get a stream of priority items
    pub fn get_stream(&self) -> impl Stream<Item = PriorityItem> + Send + 'static {
        self.internal_queue.stream()
    }
}
