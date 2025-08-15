//! Queue implementation for buffering streams

use crate::stream::Stream;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::VecDeque;

/// Queue error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum QueueError {
    #[error("Queue is full")]
    Full,
    #[error("Queue is closed")]
    Closed,
    #[error("Queue is empty")]
    Empty,
    #[error("Timeout waiting for operation")]
    Timeout,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Queue implementation for buffering streams
pub struct Queue<T> {
    buffer: Arc<Mutex<VecDeque<T>>>,
    capacity: usize,
    closed: Arc<Mutex<bool>>,
}

impl<T> Queue<T>
where
    T: Send + 'static,
{
    /// Create a new unbounded queue
    pub fn unbounded() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            capacity: usize::MAX,
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a new bounded queue
    pub fn bounded(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            capacity,
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Enqueue an item
    pub async fn enqueue(&self, item: T) -> Result<(), QueueError> {
        if *self.closed.lock().await {
            return Err(QueueError::Closed);
        }

        let mut buffer = self.buffer.lock().await;
        if buffer.len() >= self.capacity {
            return Err(QueueError::Full);
        }

        buffer.push_back(item);
        Ok(())
    }

    /// Try to enqueue an item without blocking
    pub async fn try_enqueue(&self, item: T) -> Result<(), QueueError> {
        if *self.closed.lock().await {
            return Err(QueueError::Closed);
        }

        let mut buffer = self.buffer.lock().await;
        if buffer.len() >= self.capacity {
            return Err(QueueError::Full);
        }

        buffer.push_back(item);
        Ok(())
    }

    /// Dequeue an item
    pub async fn dequeue(&self) -> Result<T, QueueError> {
        loop {
            // First try to get from buffer
            {
                let mut buffer = self.buffer.lock().await;
                if let Some(item) = buffer.pop_front() {
                    return Ok(item);
                }
            }

            // If buffer is empty and queue is closed, we're done
            if *self.closed.lock().await {
                return Err(QueueError::Closed);
            }

            // Wait a bit before trying again - use a longer sleep for proper blocking
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Try to dequeue an item without blocking
    pub async fn try_dequeue(&self) -> Result<T, QueueError> {
        let mut buffer = self.buffer.lock().await;
        if let Some(item) = buffer.pop_front() {
            Ok(item)
        } else if *self.closed.lock().await {
            Err(QueueError::Closed)
        } else {
            Err(QueueError::Empty)
        }
    }

    /// Get the current length of the queue
    pub async fn len(&self) -> usize {
        self.buffer.lock().await.len()
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.buffer.lock().await.is_empty()
    }

    /// Check if the queue is full
    pub async fn is_full(&self) -> bool {
        self.buffer.lock().await.len() >= self.capacity
    }

    /// Get the capacity of the queue
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Close the queue
    pub async fn close(&mut self) {
        *self.closed.lock().await = true;
    }

    /// Close the queue (immutable version for Arc usage)
    pub async fn close_immutable(&self) {
        *self.closed.lock().await = true;
    }

    /// Check if the queue is closed
    pub async fn is_closed(&self) -> bool {
        *self.closed.lock().await
    }

    /// Clear all items from the queue
    pub async fn clear(&self) {
        self.buffer.lock().await.clear();
    }

    /// Get a peek at the next item without removing it
    pub async fn peek(&self) -> Option<T> where T: Clone {
        let buffer = self.buffer.lock().await;
        buffer.front().cloned()
    }

    /// Get a mutable peek at the next item without removing it
    pub async fn peek_mut(&self) -> Option<T> where T: Clone {
        let buffer = self.buffer.lock().await;
        buffer.front().cloned()
    }

    /// Reserve space in the queue
    pub async fn reserve(&self, additional: usize) -> Result<(), QueueError> {
        if *self.closed.lock().await {
            return Err(QueueError::Closed);
        }

        let current_len = self.len().await;
        if current_len + additional > self.capacity {
            return Err(QueueError::Full);
        }

        Ok(())
    }

    /// Get the remaining capacity
    pub async fn remaining_capacity(&self) -> usize {
        let current_len = self.len().await;
        if current_len >= self.capacity {
            0
        } else {
            self.capacity - current_len
        }
    }

    /// Drain all items from the queue
    pub async fn drain(&self) -> Vec<T> {
        let mut buffer = self.buffer.lock().await;
        buffer.drain(..).collect()
    }

    /// Get a stream that yields items in batches
    pub fn batch_stream(&self, batch_size: usize) -> impl Stream<Item = Vec<T>> + Send + 'static {
        let buffer = Arc::clone(&self.buffer);
        let closed = Arc::clone(&self.closed);

        crate::stream::constructors::unfold((), move |_| {
            let buffer = Arc::clone(&buffer);
            let closed = Arc::clone(&closed);

            async move {
                let mut batch = Vec::with_capacity(batch_size);

                // First try to get from buffer (even if closed)
                {
                    let mut buffer_guard = buffer.lock().await;
                    while batch.len() < batch_size {
                        if let Some(item) = buffer_guard.pop_front() {
                            batch.push(item);
                        } else {
                            break;
                        }
                    }
                }

                // If we got items from buffer, return them
                if !batch.is_empty() {
                    return Some((batch, ()));
                }

                // If buffer is empty and queue is closed, we're done
                if *closed.lock().await {
                    return None;
                }

                // Wait a bit before trying again
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

                // Then try to get from buffer again
                let mut buffer_guard = buffer.lock().await;
                while batch.len() < batch_size {
                    if let Some(item) = buffer_guard.pop_front() {
                        batch.push(item);
                    } else {
                        break;
                    }
                }

                if batch.is_empty() {
                    None
                } else {
                    Some((batch, ()))
                }
            }
        })
    }

    /// Get a stream that yields items with a timeout
    pub fn timeout_stream(&self, timeout: std::time::Duration) -> impl Stream<Item = Result<T, QueueError>> + Send + 'static {
        let buffer = Arc::clone(&self.buffer);
        let closed = Arc::clone(&self.closed);

        crate::stream::constructors::unfold((), move |_| {
            let buffer = Arc::clone(&buffer);
            let closed = Arc::clone(&closed);

            async move {
                // First try to get from buffer (even if closed)
                {
                    let mut buffer_guard = buffer.lock().await;
                    if let Some(item) = buffer_guard.pop_front() {
                        return Some((Ok(item), ())); 
                    }
                }

                // If buffer is empty and queue is closed, we're done
                if *closed.lock().await {
                    return None;
                }

                // Wait for notification that an item is available with timeout
                match tokio::time::timeout(timeout, tokio::time::sleep(tokio::time::Duration::from_millis(1))).await {
                    Ok(_) => {
                        let mut buffer_guard = buffer.lock().await;
                        if let Some(item) = buffer_guard.pop_front() {
                            Some((Ok(item), ()))
                        } else {
                            Some((Err(QueueError::Closed), ()))
                        }
                    },
                    Err(_) => Some((Err(QueueError::Timeout), ())),
                }
            }
        })
    }

    /// Get a stream that yields items from the queue
    pub fn stream(&self) -> impl Stream<Item = T> + Send + 'static {
        let buffer = Arc::clone(&self.buffer);
        let closed = Arc::clone(&self.closed);

        crate::stream::constructors::unfold((), move |_| {
            let buffer = Arc::clone(&buffer);
            let closed = Arc::clone(&closed);

            async move {
                loop {
                    // First try to get from buffer (even if closed)
                    {
                        let mut buffer_guard = buffer.lock().await;
                        if let Some(item) = buffer_guard.pop_front() {
                            return Some((item, ()));
                        }
                    }

                    // If buffer is empty and queue is closed, we're done
                    if *closed.lock().await {
                        return None;
                    }

                    // Wait a bit before trying again - use a longer sleep for proper blocking
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            }
        })
    }
}

impl<T> Clone for Queue<T>
where
    T: Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        // Only the buffer, capacity, and closed state are cloned.
        Self {
            buffer: Arc::clone(&self.buffer),
            capacity: self.capacity,
            closed: Arc::clone(&self.closed),
        }
    }
}

impl<T> std::fmt::Debug for Queue<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("capacity", &self.capacity)
            .field("closed", &self.closed.try_lock().map(|guard| *guard).unwrap_or(false))
            .finish()
    }
}

impl<T> Default for Queue<T>
where
    T: Send + 'static,
{
    fn default() -> Self {
        Self::unbounded()
    }
}