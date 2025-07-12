//! Queue implementation for buffering streams

use crate::stream::Stream;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
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

#[derive(Clone)]
enum QueueSender<T> {
    Bounded(mpsc::Sender<T>),
    Unbounded(mpsc::UnboundedSender<T>),
}

enum QueueReceiver<T> {
    Bounded(mpsc::Receiver<T>),
    Unbounded(mpsc::UnboundedReceiver<T>),
}

/// Queue implementation for buffering streams
pub struct Queue<T> {
    sender: Option<QueueSender<T>>,
    receiver: Arc<Mutex<Option<QueueReceiver<T>>>>,
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
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender: Some(QueueSender::Unbounded(sender)),
            receiver: Arc::new(Mutex::new(Some(QueueReceiver::Unbounded(receiver)))),
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            capacity: usize::MAX,
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Create a new bounded queue
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender: Some(QueueSender::Bounded(sender)),
            receiver: Arc::new(Mutex::new(Some(QueueReceiver::Bounded(receiver)))),
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

        if let Some(sender) = &self.sender {
            match sender {
                QueueSender::Bounded(sender) => sender.send(item).await.map_err(|_| QueueError::Closed),
                QueueSender::Unbounded(sender) => sender.send(item).map_err(|_| QueueError::Closed),
            }
        } else {
            Err(QueueError::Closed)
        }
    }

    /// Try to enqueue an item without blocking
    pub async fn try_enqueue(&self, item: T) -> Result<(), QueueError> {
        if *self.closed.lock().await {
            return Err(QueueError::Closed);
        }

        if let Some(sender) = &self.sender {
            match sender {
                QueueSender::Bounded(sender) => sender.try_send(item).map_err(|_| QueueError::Full),
                QueueSender::Unbounded(sender) => sender.send(item).map_err(|_| QueueError::Closed),
            }
        } else {
            Err(QueueError::Closed)
        }
    }

    /// Dequeue an item
    pub async fn dequeue(&self) -> Result<T, QueueError> {
        if *self.closed.lock().await {
            return Err(QueueError::Closed);
        }

        // First try to get from buffer
        {
            let mut buffer = self.buffer.lock().await;
            if let Some(item) = buffer.pop_front() {
                return Ok(item);
            }
        }

        // Then try to get from receiver
        let mut receiver_guard = self.receiver.lock().await;
        if let Some(receiver) = receiver_guard.as_mut() {
            match receiver {
                QueueReceiver::Bounded(receiver) => receiver.recv().await.ok_or(QueueError::Closed),
                QueueReceiver::Unbounded(receiver) => receiver.recv().await.ok_or(QueueError::Closed),
            }
        } else {
            Err(QueueError::Closed)
        }
    }

    /// Try to dequeue an item without blocking
    pub async fn try_dequeue(&self) -> Result<T, QueueError> {
        if *self.closed.lock().await {
            return Err(QueueError::Closed);
        }

        // First try to get from buffer
        {
            let mut buffer = self.buffer.lock().await;
            if let Some(item) = buffer.pop_front() {
                return Ok(item);
            }
        }

        // Then try to get from receiver
        let mut receiver_guard = self.receiver.lock().await;
        if let Some(receiver) = receiver_guard.as_mut() {
            match receiver {
                QueueReceiver::Bounded(receiver) => receiver.try_recv().map_err(|e| match e {
                    mpsc::error::TryRecvError::Empty => QueueError::Empty,
                    mpsc::error::TryRecvError::Disconnected => QueueError::Closed,
                }),
                QueueReceiver::Unbounded(receiver) => receiver.try_recv().map_err(|e| match e {
                    mpsc::error::TryRecvError::Empty => QueueError::Empty,
                    mpsc::error::TryRecvError::Disconnected => QueueError::Closed,
                }),
            }
        } else {
            Err(QueueError::Closed)
        }
    }

    /// Get the current length of the queue
    pub async fn len(&self) -> usize {
        let buffer_len = self.buffer.lock().await.len();
        let receiver_len = if let Some(receiver) = &*self.receiver.lock().await {
            match receiver {
                QueueReceiver::Bounded(receiver) => receiver.len(),
                QueueReceiver::Unbounded(receiver) => 0,
            }
        } else {
            0
        };
        buffer_len + receiver_len
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Check if the queue is full
    pub async fn is_full(&self) -> bool {
        self.len().await >= self.capacity
    }

    /// Get the capacity of the queue
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Close the queue
    pub async fn close(&mut self) {
        *self.closed.lock().await = true;
        self.sender = None;
        *self.receiver.lock().await = None;
    }

    /// Check if the queue is closed
    pub async fn is_closed(&self) -> bool {
        *self.closed.lock().await
    }

    /// Clear all items from the queue
    pub async fn clear(&self) {
        self.buffer.lock().await.clear();
        // Note: We can't clear the receiver without consuming it
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
        let mut items = Vec::new();
        
        // Drain buffer
        {
            let mut buffer = self.buffer.lock().await;
            items.extend(buffer.drain(..));
        }

        // Drain receiver
        let mut receiver_guard = self.receiver.lock().await;
        if let Some(receiver) = receiver_guard.as_mut() {
            match receiver {
                QueueReceiver::Bounded(receiver) => {
                    while let Ok(item) = receiver.try_recv() {
                        items.push(item);
                    }
                },
                QueueReceiver::Unbounded(receiver) => {
                    while let Ok(item) = receiver.try_recv() {
                        items.push(item);
                    }
                },
            }
        }

        items
    }

    /// Get a stream that yields items in batches
    pub fn batch_stream(&self, batch_size: usize) -> impl Stream<Item = Vec<T>> + Send + 'static {
        let receiver = Arc::clone(&self.receiver);
        let buffer = Arc::clone(&self.buffer);
        let closed = Arc::clone(&self.closed);

        crate::stream::constructors::unfold((), move |_| {
            let receiver = Arc::clone(&receiver);
            let buffer = Arc::clone(&buffer);
            let closed = Arc::clone(&closed);

            async move {
                // Check if closed
                if *closed.lock().await {
                    return None;
                }

                let mut batch = Vec::with_capacity(batch_size);

                // First try to get from buffer
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

                // Then try to get from receiver
                let mut receiver_guard = receiver.lock().await;
                if let Some(receiver) = receiver_guard.as_mut() {
                    match receiver {
                        QueueReceiver::Bounded(receiver) => {
                            while batch.len() < batch_size {
                                match receiver.try_recv() {
                                    Ok(item) => batch.push(item),
                                    Err(_) => break,
                                }
                            }
                        },
                        QueueReceiver::Unbounded(receiver) => {
                            while batch.len() < batch_size {
                                match receiver.try_recv() {
                                    Ok(item) => batch.push(item),
                                    Err(_) => break,
                                }
                            }
                        },
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
        let receiver = Arc::clone(&self.receiver);
        let buffer = Arc::clone(&self.buffer);
        let closed = Arc::clone(&self.closed);

        crate::stream::constructors::unfold((), move |_| {
            let receiver = Arc::clone(&receiver);
            let buffer = Arc::clone(&buffer);
            let closed = Arc::clone(&closed);

            async move {
                // Check if closed
                if *closed.lock().await {
                    return None;
                }

                // First try to get from buffer
                {
                    let mut buffer_guard = buffer.lock().await;
                    if let Some(item) = buffer_guard.pop_front() {
                        return Some((Ok(item), ())); 
                    }
                }

                // Then try to get from receiver with timeout
                let mut receiver_guard = receiver.lock().await;
                if let Some(receiver) = receiver_guard.as_mut() {
                    match receiver {
                        QueueReceiver::Bounded(receiver) => {
                            match tokio::time::timeout(timeout, receiver.recv()).await {
                                Ok(Some(item)) => Some((Ok(item), ())),
                                Ok(None) => Some((Err(QueueError::Closed), ())),
                                Err(_) => Some((Err(QueueError::Timeout), ())),
                            }
                        },
                        QueueReceiver::Unbounded(receiver) => {
                            match tokio::time::timeout(timeout, receiver.recv()).await {
                                Ok(Some(item)) => Some((Ok(item), ())),
                                Ok(None) => Some((Err(QueueError::Closed), ())),
                                Err(_) => Some((Err(QueueError::Timeout), ())),
                            }
                        },
                    }
                } else {
                    Some((Err(QueueError::Closed), ()))
                }
            }
        })
    }

    /// Get a stream that yields items from the queue
    pub fn stream(&self) -> impl Stream<Item = T> + Send + 'static {
        let receiver = Arc::clone(&self.receiver);
        let buffer = Arc::clone(&self.buffer);
        let closed = Arc::clone(&self.closed);

        crate::stream::constructors::unfold((), move |_| {
            let receiver = Arc::clone(&receiver);
            let buffer = Arc::clone(&buffer);
            let closed = Arc::clone(&closed);

            async move {
                // Check if closed
                if *closed.lock().await {
                    return None;
                }

                // First try to get from buffer
                {
                    let mut buffer_guard = buffer.lock().await;
                    if let Some(item) = buffer_guard.pop_front() {
                        return Some((item, ()));
                    }
                }

                // Then try to get from receiver
                let mut receiver_guard = receiver.lock().await;
                if let Some(receiver) = receiver_guard.as_mut() {
                    match receiver {
                        QueueReceiver::Bounded(receiver) => {
                            match receiver.recv().await {
                                Some(item) => Some((item, ())),
                                None => None,
                            }
                        },
                        QueueReceiver::Unbounded(receiver) => {
                            match receiver.recv().await {
                                Some(item) => Some((item, ())),
                                None => None,
                            }
                        },
                    }
                } else {
                    None
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
        // Only the sender, buffer, and closed state are cloned. Receiver is not cloned.
        Self {
            sender: self.sender.clone(),
            receiver: Arc::new(Mutex::new(None)), // Receiver is not cloned
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