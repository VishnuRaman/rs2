
//! Queue implementation for RStream
//!
//! This module provides a concurrent queue with Stream interface for dequeuing
//! and async methods for enqueuing.

use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Error types for Queue operations
#[derive(Debug, Clone, PartialEq)]
pub enum QueueError {
    /// Queue has been closed
    Closed,
    /// Queue is full (for bounded queues with try_enqueue)
    Full,
    /// Channel disconnected
    Disconnected,
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::Closed => write!(f, "Queue is closed"),
            QueueError::Full => write!(f, "Queue is full"),
            QueueError::Disconnected => write!(f, "Queue channel disconnected"),
        }
    }
}

impl std::error::Error for QueueError {}

/// A Queue represents a concurrent queue with a Stream interface for dequeuing
/// and async methods for enqueuing.
#[derive(Clone)]
pub enum Queue<T> {
    Bounded {
        sender: Arc<Mutex<Option<mpsc::Sender<T>>>>,
        receiver: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
        capacity: usize,
    },
    Unbounded {
        sender: Arc<Mutex<Option<mpsc::UnboundedSender<T>>>>,
        receiver: Arc<Mutex<Option<mpsc::UnboundedReceiver<T>>>>,
    },
}

impl<T> Queue<T>
where
    T: Send + 'static,
{
    /// Create a new bounded queue with the given capacity
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Queue::Bounded {
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver: Arc::new(Mutex::new(Some(receiver))),
            capacity,
        }
    }

    /// Create a new unbounded queue
    pub fn unbounded() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Queue::Unbounded {
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }

    /// Enqueue an item into the queue
    pub async fn enqueue(&self, item: T) -> Result<(), QueueError> {
        match self {
            Queue::Bounded { sender, .. } => {
                let guard = sender.lock().await;
                match &*guard {
                    Some(sender) => {
                        match sender.send(item).await {
                            Ok(_) => Ok(()),
                            Err(_) => Err(QueueError::Disconnected),
                        }
                    }
                    None => Err(QueueError::Closed),
                }
            }
            Queue::Unbounded { sender, .. } => {
                let guard = sender.lock().await;
                match &*guard {
                    Some(sender) => {
                        match sender.send(item) {
                            Ok(_) => Ok(()),
                            Err(_) => Err(QueueError::Disconnected),
                        }
                    }
                    None => Err(QueueError::Closed),
                }
            }
        }
    }

    /// Try to enqueue an item without blocking
    pub async fn try_enqueue(&self, item: T) -> Result<(), QueueError> {
        match self {
            Queue::Bounded { sender, .. } => {
                let guard = sender.lock().await;
                match &*guard {
                    Some(sender) => {
                        match sender.try_send(item) {
                            Ok(_) => Ok(()),
                            Err(mpsc::error::TrySendError::Full(_)) => Err(QueueError::Full),
                            Err(mpsc::error::TrySendError::Closed(_)) => Err(QueueError::Disconnected),
                        }
                    }
                    None => Err(QueueError::Closed),
                }
            }
            Queue::Unbounded { sender, .. } => {
                let guard = sender.lock().await;
                match &*guard {
                    Some(sender) => {
                        match sender.send(item) {
                            Ok(_) => Ok(()),
                            Err(_) => Err(QueueError::Disconnected),
                        }
                    }
                    None => Err(QueueError::Closed),
                }
            }
        }
    }

    /// Get a rs2_stream for dequeuing items
    pub fn dequeue(&self) -> impl Stream<Item = T> + Send + 'static {
        match self {
            Queue::Bounded { receiver, .. } => {
                let receiver = Arc::clone(receiver);

                stream! {
                    loop {
                        let item = {
                            let mut guard = receiver.lock().await;
                            if let Some(rx) = &mut *guard {
                                rx.recv().await
                            } else {
                                None
                            }
                        };
                        
                        match item {
                            Some(item) => yield item,
                            None => break,
                        }
                    }
                }.boxed()
            }
            Queue::Unbounded { receiver, .. } => {
                let receiver = Arc::clone(receiver);

                stream! {
                    loop {
                        let item = {
                            let mut guard = receiver.lock().await;
                            if let Some(rx) = &mut *guard {
                                rx.recv().await
                            } else {
                                None
                            }
                        };
                        
                        match item {
                            Some(item) => yield item,
                            None => break,
                        }
                    }
                }.boxed()
            }
        }
    }

    /// Close the queue, preventing further enqueues
    pub async fn close(&self) {
        match self {
            Queue::Bounded { sender, .. } => {
                let mut guard = sender.lock().await;
                *guard = None;
            }
            Queue::Unbounded { sender, .. } => {
                let mut guard = sender.lock().await;
                *guard = None;
            }
        }
    }

    /// Get the capacity of the queue (None for unbounded)
    pub fn capacity(&self) -> Option<usize> {
        match self {
            Queue::Bounded { capacity, .. } => Some(*capacity),
            Queue::Unbounded { .. } => None,
        }
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Get the current number of items in the queue
    pub async fn len(&self) -> usize {
        match self {
            Queue::Bounded { receiver, .. } => {
                let guard = receiver.lock().await;
                if let Some(rx) = &*guard {
                    rx.len()
                } else {
                    0
                }
            }
            Queue::Unbounded { receiver, .. } => {
                let guard = receiver.lock().await;
                if let Some(rx) = &*guard {
                    rx.len()
                } else {
                    0
                }
            }
        }
    }
}

impl<T> fmt::Debug for Queue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Queue::Bounded { capacity, .. } => {
                f.debug_struct("Queue::Bounded")
                    .field("capacity", capacity)
                    .finish()
            }
            Queue::Unbounded { .. } => {
                f.debug_struct("Queue::Unbounded").finish()
            }
        }
    }
}