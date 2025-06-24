
//! High-performance Queue implementation for RS2
//!
//! Provides efficient concurrent queue with Stream interface and minimal locking

use async_stream::stream;
use futures_core::Stream;
use futures_util::{stream::BoxStream, StreamExt};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

/// Error types for Queue operations
#[derive(Debug, Clone, PartialEq)]
pub enum QueueError {
    /// Queue has been closed
    QueueClosed,
    /// Queue is full (for bounded queues)
    QueueFull,
    /// Channel disconnected
    ChannelDisconnected,
    /// Invalid operation
    InvalidOperation,

    // Legacy compatibility variants
    /// Legacy: Queue has been closed
    Closed,
    /// Legacy: Queue is full
    Full,
    /// Legacy: Channel disconnected
    Disconnected,
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::QueueClosed | QueueError::Closed => write!(f, "Queue is closed"),
            QueueError::QueueFull | QueueError::Full => write!(f, "Queue is full"),
            QueueError::ChannelDisconnected | QueueError::Disconnected => {
                write!(f, "Queue channel disconnected")
            }
            QueueError::InvalidOperation => write!(f, "Invalid queue operation"),
        }
    }
}

impl std::error::Error for QueueError {}

/// Internal queue state for efficient operations
struct QueueState<T> {
    sender: mpsc::Sender<T>,
    capacity: Option<usize>,
    closed: AtomicBool,
    item_count: AtomicUsize,
    close_notify: Notify,
}

/// High-performance concurrent queue with Stream interface
#[derive(Clone)]
pub struct Queue<T> {
    state: Arc<QueueState<T>>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<T>>>,
}

impl<T> Queue<T>
where
    T: Send + 'static,
{
    /// Create a new bounded queue with the given capacity
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            state: Arc::new(QueueState {
                sender,
                capacity: Some(capacity),
                closed: AtomicBool::new(false),
                item_count: AtomicUsize::new(0),
                close_notify: Notify::new(),
            }),
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
        }
    }

    /// Create a new unbounded queue
    pub fn unbounded() -> Self {
        // Use a very large bounded queue to maintain unified interface
        let capacity = 1_000_000; // Effectively unbounded for most use cases
        let (sender, receiver) = mpsc::channel(capacity);

        Self {
            state: Arc::new(QueueState {
                sender,
                capacity: None, // Indicate this is unbounded
                closed: AtomicBool::new(false),
                item_count: AtomicUsize::new(0),
                close_notify: Notify::new(),
            }),
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
        }
    }

    /// Enqueue an item into the queue (blocking if full)
    pub async fn enqueue(&self, item: T) -> Result<(), QueueError> {
        if self.state.closed.load(Ordering::Acquire) {
            return Err(QueueError::QueueClosed);
        }

        match self.state.sender.send(item).await {
            Ok(_) => {
                self.state.item_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => Err(QueueError::ChannelDisconnected),
        }
    }

    /// Try to enqueue an item without blocking
    pub async fn try_enqueue(&self, item: T) -> Result<(), QueueError> {
        if self.state.closed.load(Ordering::Acquire) {
            return Err(QueueError::QueueClosed);
        }

        match self.state.sender.try_send(item) {
            Ok(_) => {
                self.state.item_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::ChannelDisconnected),
        }
    }

    /// Get a stream for dequeuing items - returns BoxStream to avoid pinning issues
    pub fn dequeue(&self) -> BoxStream<'static, T> {
        let receiver = Arc::clone(&self.receiver);
        let state = Arc::clone(&self.state);

        let stream = stream! {
            loop {
                let item = {
                    let mut rx = receiver.lock().await;
                    tokio::select! {
                        item = rx.recv() => item,
                        _ = state.close_notify.notified() => {
                            // Check if there are still items to process
                            rx.recv().await
                        }
                    }
                };

                match item {
                    Some(item) => {
                        state.item_count.fetch_sub(1, Ordering::Relaxed);
                        yield item;
                    }
                    None => {
                        // Channel is closed and no more items
                        break;
                    }
                }
            }
        };

        Box::pin(stream)
    }

    /// Close the queue, preventing further enqueues but allowing existing items to be consumed
    pub async fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.close_notify.notify_waiters();
    }

    /// Check if the queue is closed
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    /// Get the capacity of the queue (None for unbounded)
    pub fn capacity(&self) -> Option<usize> {
        self.state.capacity
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Get the current number of items in the queue
    pub async fn len(&self) -> usize {
        // Try to get accurate count from receiver if possible
        if let Ok(rx) = self.receiver.try_lock() {
            rx.len()
        } else {
            // Fallback to atomic counter (may have slight race conditions)
            self.state.item_count.load(Ordering::Relaxed)
        }
    }

    /// Get the current number of items in the queue (non-blocking, may be slightly inaccurate)
    pub fn len_fast(&self) -> usize {
        self.state.item_count.load(Ordering::Relaxed)
    }

    /// Drain all remaining items from the queue
    pub async fn drain(&self) -> Vec<T> {
        let mut items = Vec::new();
        let mut rx = self.receiver.lock().await;

        while let Ok(item) = rx.try_recv() {
            self.state.item_count.fetch_sub(1, Ordering::Relaxed);
            items.push(item);
        }

        items
    }

    /// Get queue statistics for monitoring
    pub fn stats(&self) -> QueueStats {
        let current_len = self.len_fast();
        let capacity = self.capacity();
        let utilization = if let Some(cap) = capacity {
            if cap > 0 {
                current_len as f64 / cap as f64
            } else {
                0.0
            }
        } else {
            0.0 // Unbounded queues have 0 utilization by definition
        };

        QueueStats {
            length: current_len,
            capacity,
            utilization,
            is_closed: self.is_closed(),
        }
    }

    /// Check if the queue is nearly full (useful for backpressure)
    pub fn is_nearly_full(&self, threshold: f64) -> bool {
        if let Some(cap) = self.capacity() {
            let current = self.len_fast();
            (current as f64 / cap as f64) >= threshold
        } else {
            false // Unbounded queues are never "full"
        }
    }

    /// Get available space in the queue
    pub fn available_capacity(&self) -> Option<usize> {
        if let Some(cap) = self.capacity() {
            let current = self.len_fast();
            Some(cap.saturating_sub(current))
        } else {
            None // Unbounded
        }
    }
}

/// Queue statistics for monitoring and debugging
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub length: usize,
    pub capacity: Option<usize>,
    pub utilization: f64, // 0.0 to 1.0 for bounded queues
    pub is_closed: bool,
}

impl fmt::Display for QueueStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.capacity {
            Some(cap) => write!(
                f,
                "Queue({}/{}, {:.1}%{})",
                self.length,
                cap,
                self.utilization * 100.0,
                if self.is_closed { ", closed" } else { "" }
            ),
            None => write!(
                f,
                "Queue({}, unbounded{})",
                self.length,
                if self.is_closed { ", closed" } else { "" }
            ),
        }
    }
}



impl<T> fmt::Debug for Queue<T>
where
    T: Send + 'static,  // Add the same bounds as the methods
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Access fields directly instead of calling methods to avoid further issues
        let capacity = self.state.capacity;
        let is_closed = self.state.closed.load(Ordering::Acquire);
        let current_len = self.state.item_count.load(Ordering::Relaxed);

        match capacity {
            Some(cap) => {
                let utilization = if cap > 0 {
                    current_len as f64 / cap as f64
                } else {
                    0.0
                };
                f.debug_struct("Queue")
                    .field("capacity", &cap)
                    .field("length", &current_len)
                    .field("utilization", &format!("{:.1}%", utilization * 100.0))
                    .field("is_closed", &is_closed)
                    .finish()
            }
            None => {
                f.debug_struct("Queue")
                    .field("capacity", &"unbounded")
                    .field("length", &current_len)
                    .field("is_closed", &is_closed)
                    .finish()
            }
        }
    }
}
