use crate::stream::Stream;
use crate::state::{StateError, StateStorage};
use crate::state::stream_ext::StateAccess;
use crate::state::traits::KeyExtractor;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct StatefulWindowStream<S, F, T, R>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
    T: Send + Sync + Clone + 'static + Unpin,
    R: Send + Sync + 'static + Unpin,
{
    stream: S,
    window_buffers: HashMap<String, Vec<T>>,
    storage: Arc<dyn StateStorage + Send + Sync>,
    key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
    f: F,
    window_size: usize,
    slide_size: usize,
    emit_partial: bool,
    pending_results: Vec<Result<R, StateError>>,
    stream_done: bool,
    pending_future: Option<Pin<Box<dyn std::future::Future<Output = Option<Result<R, StateError>>> + Send>>>,
    finalize_keys: Option<Vec<(String, Vec<T>)>>,
}

impl<S, F, T, R> StatefulWindowStream<S, F, T, R>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
    T: Send + Sync + Clone + 'static + Unpin,
    R: Send + Sync + 'static + Unpin,
{
    pub fn new(
        stream: S,
        storage: Arc<dyn StateStorage + Send + Sync>,
        key_extractor: Arc<dyn KeyExtractor<T> + Send + Sync>,
        f: F,
        window_size: usize,
        slide_size: usize,
        emit_partial: bool,
    ) -> Self {
        Self {
            stream,
            window_buffers: HashMap::new(),
            storage,
            key_extractor,
            f,
            window_size,
            slide_size,
            emit_partial,
            pending_results: Vec::new(),
            stream_done: false,
            pending_future: None,
            finalize_keys: None,
        }
    }

    fn poll_emit_future(
        self_: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<R, StateError>>> {
        if let Some(fut) = &mut self_.pending_future {
            match fut.as_mut().poll(cx) {
                Poll::Ready(res) => {
                    self_.pending_future = None;
                    Poll::Ready(res)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

impl<S, F, T, R> Stream for StatefulWindowStream<S, F, T, R>
where
    S: Stream<Item = T> + Send + Unpin,
    F: FnMut(Vec<T>, StateAccess) -> Pin<Box<dyn std::future::Future<Output = Result<R, StateError>> + Send>> + Send + Sync + 'static + Unpin,
    T: Send + Sync + Clone + 'static + Unpin,
    R: Send + Sync + 'static + Unpin,
{
    type Item = Result<R, StateError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 1. If we have a pending emission future, poll it
        if self.pending_future.is_some() {
            return Self::poll_emit_future(&mut self, cx);
        }

        // 2. If we have pending results, return them
        if let Some(result) = self.pending_results.pop() {
            return Poll::Ready(Some(result));
        }

        // 3. If stream is done, finalize remaining windows
        if self.stream_done {
            // If we are already finalizing, continue
            if let Some(keys) = &mut self.finalize_keys {
                if let Some((key, buffer)) = keys.pop() {
                    if !buffer.is_empty() && (self.emit_partial || buffer.len() >= self.window_size) {
                        let state_access = StateAccess::new(self.storage.clone(), key.clone());
                        let fut = (self.f)(buffer, state_access);
                        self.pending_future = Some(Box::pin(async move { Some(fut.await) }));
                        return Self::poll_emit_future(&mut self, cx);
                    } else {
                        // Skip empty or non-emit buffers
                        return self.poll_next(cx);
                    }
                } else {
                    // All done
                    return Poll::Ready(None);
                }
            } else {
                // Start finalization
                let keys: Vec<_> = self.window_buffers.drain().collect();
                self.finalize_keys = Some(keys);
                return self.poll_next(cx);
            }
        }

        // 4. Poll the source stream
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let emit_partial = self.emit_partial;
                let window_size = self.window_size;
                let slide_size = self.slide_size;
                let key = self.key_extractor.extract_key(&item);
                let buffer = self.window_buffers.entry(key.clone()).or_insert_with(Vec::new);
                buffer.push(item);

                let should_emit = if emit_partial {
                    buffer.len() >= slide_size
                } else {
                    buffer.len() >= window_size
                };

                if should_emit {
                    let items_to_emit = if buffer.len() >= window_size {
                        buffer.drain(..window_size).collect()
                    } else {
                        buffer.drain(..).collect()
                    };
                    let state_access = StateAccess::new(self.storage.clone(), key.clone());
                    let fut = (self.f)(items_to_emit, state_access);
                    self.pending_future = Some(Box::pin(async move { Some(fut.await) }));
                    return Self::poll_emit_future(&mut self, cx);
                } else {
                    // No emission, poll again
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(None) => {
                self.stream_done = true;
                return self.poll_next(cx);
            }
            Poll::Pending => Poll::Pending,
        }
    }
} 