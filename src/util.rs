use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::stream::core::Stream;

/// Join multiple futures into a single future that completes when all futures complete
pub async fn join_all<F>(futures: Vec<F>) -> Vec<F::Output>
where
    F: Future + Unpin + Send + 'static,
    F::Output: Send + 'static,
{
    // Simple implementation using tokio::task::spawn_blocking for each future
    let mut handles = Vec::new();
    
    for future in futures {
        let handle = tokio::task::spawn(async move {
            future.await
        });
        handles.push(handle);
    }
    
    let mut results = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(result) => results.push(result),
            Err(_) => {
                // Handle join error - in a real implementation you might want to handle this differently
                panic!("Future join failed");
            }
        }
    }
    
    results
}

/// Join multiple streams into a single stream that yields items from all streams
pub fn join_streams<S>(streams: Vec<S>) -> JoinStreams<S>
where
    S: Stream + Unpin,
{
    JoinStreams {
        streams: streams.into_iter().map(|s| Box::pin(s)).collect(),
        current_index: 0,
    }
}

pin_project_lite::pin_project! {
    pub struct JoinStreams<S> {
        #[pin]
        streams: Vec<Pin<Box<S>>>,
        current_index: usize,
    }
}

impl<S> Stream for JoinStreams<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        
        if this.streams.is_empty() {
            return Poll::Ready(None);
        }
        
        // Round-robin through streams
        let start_index = *this.current_index;
        let mut index = start_index;
        
        loop {
            if index < this.streams.len() {
                let stream = &mut this.streams[index];
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        *this.current_index = (index + 1) % this.streams.len();
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        // Remove completed stream
                        this.streams.remove(index);
                        if this.streams.is_empty() {
                            return Poll::Ready(None);
                        }
                        // Adjust current_index if needed
                        if index < *this.current_index {
                            *this.current_index = this.current_index.saturating_sub(1);
                        }
                        // Don't increment index since we removed an element
                        continue;
                    }
                    Poll::Pending => {
                        // Try next stream
                        index = (index + 1) % this.streams.len();
                        if index == start_index {
                            return Poll::Pending;
                        }
                    }
                }
            } else {
                index = (index + 1) % this.streams.len();
                if index == start_index {
                    return Poll::Ready(None);
                }
            }
        }
    }
}
