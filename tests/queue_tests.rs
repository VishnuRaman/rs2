use futures_util::StreamExt;
use rs2_stream::queue::{Queue, QueueError};
use tokio::runtime::Runtime;

#[tokio::test]
async fn test_bounded_queue_basic() {
    let queue = Queue::bounded(2);

    // Test enqueue
    assert!(queue.enqueue(1).await.is_ok());
    assert!(queue.enqueue(2).await.is_ok());

    // Test try_enqueue when full
    assert!(matches!(
            queue.try_enqueue(3).await,
            Err(QueueError::QueueFull)
        ));

    // Test dequeue - now this will work!
    let mut stream = queue.dequeue();
    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, Some(2));

    // Should be able to enqueue again
    assert!(queue.enqueue(3).await.is_ok());
    assert_eq!(stream.next().await, Some(3));
}

#[tokio::test]
async fn test_queue_close() {
    let queue = Queue::bounded(5);

    // Enqueue some items
    queue.enqueue(1).await.unwrap();
    queue.enqueue(2).await.unwrap();

    // Close the queue
    queue.close().await;
    assert!(queue.is_closed());

    // Should not be able to enqueue
    assert!(matches!(
            queue.enqueue(3).await,
            Err(QueueError::QueueClosed)
        ));

    // Should still be able to dequeue existing items
    let mut stream = queue.dequeue();
    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, Some(2));

    // Stream should end after existing items
    assert_eq!(stream.next().await, None);
}

#[tokio::test]
async fn test_queue_stats() {
    let queue = Queue::bounded(10);

    queue.enqueue(1).await.unwrap();
    queue.enqueue(2).await.unwrap();

    let stats = queue.stats();
    assert_eq!(stats.length, 2);
    assert_eq!(stats.capacity, Some(10));
    assert_eq!(stats.utilization, 0.2);
    assert!(!stats.is_closed);

    println!("Queue stats: {}", stats);
}

#[tokio::test]
async fn test_unbounded_queue() {
    let queue = Queue::unbounded();

    // Should have no capacity limit
    assert_eq!(queue.capacity(), None);

    // Should be able to enqueue many items
    for i in 0..1000 {
        queue.enqueue(i).await.unwrap();
    }

    // Should be able to dequeue all items
    let items: Vec<_> = queue.dequeue().take(1000).collect().await;
    assert_eq!(items.len(), 1000);
    assert_eq!(items, (0..1000).collect::<Vec<_>>());
}

#[tokio::test]
async fn test_concurrent_access() {
    let queue = Queue::bounded(100);
    let queue_clone = queue.clone();

    // Producer task
    let producer = tokio::spawn(async move {
        for i in 0..50 {
            queue_clone.enqueue(i).await.unwrap();
        }
    });

    // Consumer task
    let consumer = tokio::spawn(async move {
        let stream = queue.dequeue();
        let items: Vec<_> = stream.take(50).collect().await;
        items
    });

    let (_, items) = tokio::join!(producer, consumer);
    let items = items.unwrap();
    assert_eq!(items.len(), 50);

    // Verify all items are present (may be out of order due to concurrency)
    for i in 0..50 {
        assert!(items.contains(&i));
    }
}

#[tokio::test]
async fn test_queue_utilities() {
    let queue = Queue::bounded(10);

    // Test is_nearly_full
    assert!(!queue.is_nearly_full(0.8));

    for i in 0..8 {
        queue.enqueue(i).await.unwrap();
    }

    assert!(queue.is_nearly_full(0.7));
    assert_eq!(queue.available_capacity(), Some(2));

    // Test drain
    let drained = queue.drain().await;
    assert_eq!(drained.len(), 8);
    assert!(queue.is_empty().await);
}

#[tokio::test]
async fn test_stream_pinning() {
    let queue = Queue::bounded(3);

    // Add some items
    queue.enqueue("hello").await.unwrap();
    queue.enqueue("world").await.unwrap();
    queue.close().await;

    // This should now work without pinning issues
    let mut stream = queue.dequeue();
    assert_eq!(stream.next().await, Some("hello"));
    assert_eq!(stream.next().await, Some("world"));
    assert_eq!(stream.next().await, None);
}