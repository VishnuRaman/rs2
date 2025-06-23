//! Priority queue implementation for media streaming
//!
//! Extends the existing Queue with priority-based ordering

use super::types::{MediaChunk, MediaPriority};
use crate::queue::{Queue, QueueError};
use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::resource_manager::get_global_resource_manager;

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
        let priority = chunk.priority;
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
        let priority_buffer = Arc::clone(&self.priority_buffer);
        let queue_stream = self.internal_queue.dequeue();
        let resource_manager = get_global_resource_manager();
        stream! {
            let mut queue_stream = std::pin::pin!(queue_stream);
            loop {
                // First check priority buffer
                let high_priority_item = {
                    let mut buffer = priority_buffer.lock().await;
                    let popped = buffer.pop();
                    if popped.is_some() {
                        resource_manager.track_memory_deallocation(1).await;
                    }
                    popped
                };
                if let Some(item) = high_priority_item {
                    yield item.chunk;
                } else {
                    // No high priority items, get from main queue
                    match queue_stream.next().await {
                        Some(item) => {
                            resource_manager.track_memory_deallocation(1).await;
                            yield item.chunk
                        },
                        None => break,
                    }
                }
            }
        }
    }

    /// Try to enqueue without blocking - useful for live streaming
    pub async fn try_enqueue(&self, chunk: MediaChunk) -> Result<(), QueueError> {
        let resource_manager = get_global_resource_manager();
        let priority = chunk.priority;
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
        self.internal_queue.close().await;
    }

    pub async fn len(&self) -> usize {
        let buffer_len = {
            let buffer = self.priority_buffer.lock().await;
            buffer.len()
        };
        buffer_len + self.internal_queue.len().await
    }
}
