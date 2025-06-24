//! Priority queue implementation for media streaming
//!
//! Extends the existing Queue with priority-based ordering

use super::types::{MediaChunk, MediaPriority};
use crate::queue::{Queue, QueueError, QueueStats};
use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
#[derive(Clone)]
pub struct MediaPriorityQueue {
    internal_queue: Queue<PriorityItem>,
    priority_buffer: Arc<Mutex<BinaryHeap<PriorityItem>>>,
    buffer_size: usize,
}

impl MediaPriorityQueue {
    /// Create a new priority queue with specified capacities
    pub fn new(capacity: usize, priority_buffer_size: usize) -> Self {
        Self {
            internal_queue: Queue::bounded(capacity),
            priority_buffer: Arc::new(Mutex::new(BinaryHeap::new())),
            buffer_size: priority_buffer_size,
        }
    }

    /// Create an unbounded priority queue with specified priority buffer size
    pub fn unbounded(priority_buffer_size: usize) -> Self {
        Self {
            internal_queue: Queue::unbounded(),
            priority_buffer: Arc::new(Mutex::new(BinaryHeap::new())),
            buffer_size: priority_buffer_size,
        }
    }

    /// Enqueue a media chunk with priority handling
    pub async fn enqueue(&self, chunk: MediaChunk) -> Result<(), QueueError> {
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
                return Ok(());
            }

            // Priority buffer is full - check if we should evict lower priority item
            if let Some(lowest) = buffer.peek() {
                if item > *lowest {
                    // New item has higher priority, evict lowest and insert new one
                    let evicted = buffer.pop().unwrap();
                    buffer.push(item);

                    // Put evicted item in main queue
                    drop(buffer); // Release lock before async call
                    return self.internal_queue.enqueue(evicted).await;
                }
            }
        }

        // Buffer full and new item doesn't have higher priority, push to main queue
        self.internal_queue.enqueue(item).await
    }

    /// Create a stream for dequeuing chunks in priority order
    pub fn dequeue(&self) -> impl Stream<Item = MediaChunk> + Send + 'static {
        let priority_buffer = Arc::clone(&self.priority_buffer);
        let queue_stream = self.internal_queue.dequeue();

        stream! {
            let mut queue_stream = std::pin::pin!(queue_stream);
            loop {
                // First check priority buffer
                let high_priority_item = {
                    let mut buffer = priority_buffer.lock().await;
                    buffer.pop()
                };
                
                if let Some(item) = high_priority_item {
                    yield item.chunk;
                } else {
                    // No high priority items, get from main queue
                    match queue_stream.next().await {
                        Some(item) => yield item.chunk,
                        None => break,
                    }
                }
            }
        }
    }

    /// Try to enqueue without blocking - useful for live streaming
    pub async fn try_enqueue(&self, chunk: MediaChunk) -> Result<(), QueueError> {
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
                return Ok(());
            }

            // Priority buffer is full - check if we should evict lower priority item
            if let Some(lowest) = buffer.peek() {
                if item > *lowest {
                    // New item has higher priority, evict lowest and insert new one
                    let evicted = buffer.pop().unwrap();
                    buffer.push(item);

                    // Try to put evicted item in main queue without blocking
                    drop(buffer); // Release lock before async call
                    return self.internal_queue.try_enqueue(evicted).await;
                }
            }
        }

        // Buffer full and new item doesn't have higher priority, try main queue
        self.internal_queue.try_enqueue(item).await
    }

    /// Close the priority queue
    pub async fn close(&self) {
        self.internal_queue.close().await;
    }

    /// Check if the queue is closed
    pub fn is_closed(&self) -> bool {
        self.internal_queue.is_closed()
    }

    /// Get the capacity of the main queue (None for unbounded)
    pub fn capacity(&self) -> Option<usize> {
        self.internal_queue.capacity()
    }

    /// Get the priority buffer capacity
    pub fn priority_buffer_capacity(&self) -> usize {
        self.buffer_size
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Get the total number of items in both buffer and main queue
    pub async fn len(&self) -> usize {
        let buffer_len = {
            let buffer = self.priority_buffer.lock().await;
            buffer.len()
        };
        buffer_len + self.internal_queue.len().await
    }

    /// Get fast length estimate (may be slightly inaccurate)
    pub fn len_fast(&self) -> usize {
        // Note: This only counts main queue as priority buffer lock would block
        self.internal_queue.len_fast()
    }

    /// Get the number of items in the priority buffer
    pub async fn priority_buffer_len(&self) -> usize {
        let buffer = self.priority_buffer.lock().await;
        buffer.len()
    }

    /// Get the number of items in the main queue
    pub async fn main_queue_len(&self) -> usize {
        self.internal_queue.len().await
    }

    /// Drain all items from both buffer and main queue
    pub async fn drain(&self) -> Vec<MediaChunk> {
        let mut all_items = Vec::new();

        // Drain priority buffer first (these should come out in priority order)
        {
            let mut buffer = self.priority_buffer.lock().await;
            while let Some(item) = buffer.pop() {
                all_items.push(item.chunk);
            }
        }

        // Then drain main queue
        let main_items = self.internal_queue.drain().await;
        all_items.extend(main_items.into_iter().map(|item| item.chunk));

        all_items
    }

    /// Get combined queue statistics
    pub async fn stats(&self) -> MediaPriorityQueueStats {
        let main_stats = self.internal_queue.stats();
        let buffer_len = self.priority_buffer_len().await;
        let total_len = buffer_len + main_stats.length;

        MediaPriorityQueueStats {
            total_length: total_len,
            priority_buffer_length: buffer_len,
            priority_buffer_capacity: self.buffer_size,
            priority_buffer_utilization: buffer_len as f64 / self.buffer_size as f64,
            main_queue_length: main_stats.length,
            main_queue_capacity: main_stats.capacity,
            main_queue_utilization: main_stats.utilization,
            is_closed: main_stats.is_closed,
        }
    }

    /// Check if the priority buffer is nearly full
    pub async fn is_priority_buffer_nearly_full(&self, threshold: f64) -> bool {
        let buffer_len = self.priority_buffer_len().await;
        (buffer_len as f64 / self.buffer_size as f64) >= threshold
    }

    /// Check if the main queue is nearly full
    pub fn is_main_queue_nearly_full(&self, threshold: f64) -> bool {
        self.internal_queue.is_nearly_full(threshold)
    }

    /// Get available capacity in the main queue
    pub fn available_main_queue_capacity(&self) -> Option<usize> {
        self.internal_queue.available_capacity()
    }

    /// Get available capacity in the priority buffer
    pub async fn available_priority_buffer_capacity(&self) -> usize {
        let buffer_len = self.priority_buffer_len().await;
        self.buffer_size.saturating_sub(buffer_len)
    }
}

/// Extended statistics for MediaPriorityQueue
#[derive(Debug, Clone)]
pub struct MediaPriorityQueueStats {
    pub total_length: usize,
    pub priority_buffer_length: usize,
    pub priority_buffer_capacity: usize,
    pub priority_buffer_utilization: f64,
    pub main_queue_length: usize,
    pub main_queue_capacity: Option<usize>,
    pub main_queue_utilization: f64,
    pub is_closed: bool,
}

impl std::fmt::Display for MediaPriorityQueueStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MediaPriorityQueue(total: {}, priority_buf: {}/{} ({:.1}%), main: {}/{} ({:.1}%){})",
            self.total_length,
            self.priority_buffer_length,
            self.priority_buffer_capacity,
            self.priority_buffer_utilization * 100.0,
            self.main_queue_length,
            self.main_queue_capacity.map_or("unbounded".to_string(), |c| c.to_string()),
            self.main_queue_utilization * 100.0,
            if self.is_closed { ", closed" } else { "" }
        )
    }
}

impl std::fmt::Debug for MediaPriorityQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaPriorityQueue")
            .field("buffer_size", &self.buffer_size)
            .field("internal_queue", &self.internal_queue)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::types::ChunkType;
    use futures_util::StreamExt;
    use std::time::Duration;

    fn create_test_chunk(stream_id: &str, sequence: u64, chunk_type: ChunkType) -> MediaChunk {
        MediaChunk {
            stream_id: stream_id.to_string(),
            sequence_number: sequence,
            data: vec![0u8; 1024],
            chunk_type,
            priority: match chunk_type {
                ChunkType::VideoIFrame => MediaPriority::High,
                ChunkType::VideoBFrame => MediaPriority::Low,
                _ => MediaPriority::Normal,
            },
            timestamp: Duration::from_millis(sequence * 33),
            is_final: false,
            checksum: None,
        }
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let queue = MediaPriorityQueue::new(10, 5);

        // Enqueue items with different priorities
        let low_chunk = create_test_chunk("stream1", 1, ChunkType::VideoBFrame);
        let normal_chunk = create_test_chunk("stream1", 2, ChunkType::VideoPFrame);
        let high_chunk = create_test_chunk("stream1", 3, ChunkType::VideoIFrame);

        queue.enqueue(low_chunk).await.unwrap();
        queue.enqueue(normal_chunk).await.unwrap();
        queue.enqueue(high_chunk).await.unwrap();

        // Pin the stream before using it
        let mut stream = std::pin::pin!(queue.dequeue());

        // Should get high priority first
        let first = stream.next().await.unwrap();
        assert_eq!(first.priority, MediaPriority::High);
        assert_eq!(first.sequence_number, 3);

        // Then normal priority
        let second = stream.next().await.unwrap();
        assert_eq!(second.priority, MediaPriority::Normal);
        assert_eq!(second.sequence_number, 2);

        // Finally low priority
        let third = stream.next().await.unwrap();
        assert_eq!(third.priority, MediaPriority::Low);
        assert_eq!(third.sequence_number, 1);
    }

    #[tokio::test]
    async fn test_sequence_ordering_within_priority() {
        let queue = MediaPriorityQueue::new(10, 5);

        // Enqueue multiple items with same priority but different sequences
        let chunk1 = create_test_chunk("stream1", 3, ChunkType::VideoPFrame);
        let chunk2 = create_test_chunk("stream1", 1, ChunkType::VideoPFrame);
        let chunk3 = create_test_chunk("stream1", 2, ChunkType::VideoPFrame);

        queue.enqueue(chunk1).await.unwrap();
        queue.enqueue(chunk2).await.unwrap();
        queue.enqueue(chunk3).await.unwrap();

        let items: Vec<_> = queue.dequeue().take(3).collect().await;

        // Should be ordered by sequence number (oldest first)
        assert_eq!(items[0].sequence_number, 1);
        assert_eq!(items[1].sequence_number, 2);
        assert_eq!(items[2].sequence_number, 3);
    }

    #[tokio::test]
    async fn test_priority_buffer_eviction() {
        let queue = MediaPriorityQueue::new(10, 2); // Small priority buffer

        // Fill priority buffer with normal priority items
        let normal1 = create_test_chunk("stream1", 1, ChunkType::VideoPFrame);
        let normal2 = create_test_chunk("stream1", 2, ChunkType::VideoPFrame);
        queue.enqueue(normal1).await.unwrap();
        queue.enqueue(normal2).await.unwrap();

        // Buffer should be full
        assert_eq!(queue.priority_buffer_len().await, 2);

        // Add high priority item - should evict lowest priority item
        let high_chunk = create_test_chunk("stream1", 3, ChunkType::VideoIFrame);
        queue.enqueue(high_chunk).await.unwrap();

        // Priority buffer should still have 2 items, but now includes the high priority item
        assert_eq!(queue.priority_buffer_len().await, 2);

        // Main queue should have 1 item (the evicted normal priority item)
        assert_eq!(queue.main_queue_len().await, 1);
    }

    #[tokio::test]
    async fn test_try_enqueue_when_full() {
        let queue = MediaPriorityQueue::new(1, 1); // Capacity=1, buffer=1

        // Fill buffer
        let chunk1 = create_test_chunk("stream1", 1, ChunkType::VideoPFrame);
        queue.try_enqueue(chunk1).await.unwrap();

        // Fill main queue
        let chunk2 = create_test_chunk("stream1", 2, ChunkType::VideoPFrame);
        queue.try_enqueue(chunk2).await.unwrap();

        // Try to enqueue another chunk (should fail)
        let chunk3 = create_test_chunk("stream1", 3, ChunkType::VideoPFrame);
        let result = queue.try_enqueue(chunk3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_queue_stats() {
        let queue = MediaPriorityQueue::new(10, 5);

        let chunk1 = create_test_chunk("stream1", 1, ChunkType::VideoIFrame);
        let chunk2 = create_test_chunk("stream1", 2, ChunkType::VideoPFrame);
        queue.enqueue(chunk1).await.unwrap();
        queue.enqueue(chunk2).await.unwrap();

        let stats = queue.stats().await;
        assert_eq!(stats.total_length, 2);
        assert_eq!(stats.priority_buffer_capacity, 5);
        assert!(!stats.is_closed);

        println!("Priority queue stats: {}", stats);
    }

    #[tokio::test]
    async fn test_close_and_drain() {
        let queue = MediaPriorityQueue::new(10, 5);

        let chunk1 = create_test_chunk("stream1", 1, ChunkType::VideoIFrame);
        let chunk2 = create_test_chunk("stream1", 2, ChunkType::VideoPFrame);
        queue.enqueue(chunk1).await.unwrap();
        queue.enqueue(chunk2).await.unwrap();

        queue.close().await;
        assert!(queue.is_closed());

        let drained = queue.drain().await;
        assert_eq!(drained.len(), 2);
        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn test_unbounded_priority_queue() {
        let queue = MediaPriorityQueue::unbounded(5);

        assert_eq!(queue.capacity(), None);

        // Should be able to enqueue many items
        for i in 0..100 {
            let chunk = create_test_chunk("stream1", i, ChunkType::VideoPFrame);
            queue.enqueue(chunk).await.unwrap();
        }

        let total_len = queue.len().await;
        assert_eq!(total_len, 100);
    }
}