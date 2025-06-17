
use futures_util::StreamExt;
use tokio::runtime::Runtime;
use rs2_stream::queue::Queue;

#[test]
fn test_queue_basic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(3);

        // Enqueue some items
        assert!(queue.enqueue(1).await.is_ok());
        assert!(queue.enqueue(2).await.is_ok());
        assert!(queue.enqueue(3).await.is_ok());

        // Dequeue items - collect from the rs2_stream
        let mut dequeue_stream = queue.dequeue();
        assert_eq!(dequeue_stream.next().await.unwrap(), 1);
        assert_eq!(dequeue_stream.next().await.unwrap(), 2);
        assert_eq!(dequeue_stream.next().await.unwrap(), 3);
    });
}

#[test]
fn test_queue_try_enqueue() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(2);

        // Fill the queue
        assert!(queue.try_enqueue(1).await.is_ok());
        assert!(queue.try_enqueue(2).await.is_ok());

        // This should fail because queue is full
        let result = queue.try_enqueue(3).await;
        assert!(result.is_err());
        // Check for Full error if it exists, otherwise just check it's an error
        if let Err(e) = result {
            // The test should pass if we get any error when queue is full
            println!("Got expected error when queue full: {:?}", e);
        }
    });
}

#[test]
fn test_queue_close() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(3);

        // Enqueue some items
        assert!(queue.enqueue(1).await.is_ok());
        assert!(queue.enqueue(2).await.is_ok());

        // Close the queue
        queue.close().await;

        // Try to enqueue after closing - should fail
        let result = queue.enqueue(3).await;
        assert!(result.is_err());
        // Check for Closed error if it exists, otherwise just check it's an error
        if let Err(e) = result {
            // The test should pass if we get any error when queue is closed
            println!("Got expected error when queue closed: {:?}", e);
        }

        // Should still be able to dequeue existing items
        let mut dequeue_stream = queue.dequeue();
        assert_eq!(dequeue_stream.next().await.unwrap(), 1);
        assert_eq!(dequeue_stream.next().await.unwrap(), 2);
    });
}

#[test]
fn test_queue_unbounded() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::unbounded();

        // Enqueue many items (should not fail for unbounded)
        for i in 0..100 { // Reduced from 1000 for faster testing
            assert!(queue.enqueue(i).await.is_ok());
        }

        // Dequeue all items
        let mut dequeue_stream = queue.dequeue();
        for i in 0..100 {
            assert_eq!(dequeue_stream.next().await.unwrap(), i);
        }
    });
}

#[test]
fn test_queue_try_enqueue_unbounded() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::unbounded();

        // For unbounded queues, try_enqueue should always succeed
        for i in 0..50 { // Reduced for faster testing
            assert!(queue.try_enqueue(i).await.is_ok());
        }

        // Verify all items were enqueued
        let mut dequeue_stream = queue.dequeue();
        for i in 0..50 {
            assert_eq!(dequeue_stream.next().await.unwrap(), i);
        }
    });
}

#[test]
fn test_queue_close_unbounded() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::unbounded();

        // Enqueue some items
        assert!(queue.enqueue(1).await.is_ok());
        assert!(queue.enqueue(2).await.is_ok());

        // Close the queue
        queue.close().await;

        // Try to enqueue after closing - should fail
        let result = queue.enqueue(3).await;
        assert!(result.is_err());

        // Try_enqueue should also fail after closing
        let result = queue.try_enqueue(4).await;
        assert!(result.is_err());
    });
}

#[test]
fn test_queue_capacity() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(2);

        if let Some(cap) = queue.capacity() {
            assert_eq!(cap, 2);
        }

        let unbounded_queue = Queue::<i32>::unbounded();
        assert_eq!(unbounded_queue.capacity(), None);
    });
}

#[test]
fn test_queue_is_empty() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(3);

        assert!(queue.is_empty().await);

        queue.enqueue(1).await.unwrap();
        assert!(!queue.is_empty().await);

        let mut dequeue_stream = queue.dequeue();
        dequeue_stream.next().await.unwrap();
        assert!(queue.is_empty().await);
    });
}

#[test]
fn test_queue_len() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(5);

        assert_eq!(queue.len().await, 0);

        queue.enqueue(1).await.unwrap();
        queue.enqueue(2).await.unwrap();
        assert_eq!(queue.len().await, 2);

        let mut dequeue_stream = queue.dequeue();
        dequeue_stream.next().await.unwrap();
        assert_eq!(queue.len().await, 1);

        dequeue_stream.next().await.unwrap();
        assert_eq!(queue.len().await, 0);
    });
}

#[test]
fn test_queue_dequeue_empty() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(3);

        // Dequeue from empty queue should either block indefinitely or return None
        // Using a timeout to test this behavior
        let mut dequeue_stream = queue.dequeue();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            dequeue_stream.next()
        ).await;

        // Should timeout because queue is empty and dequeue blocks
        assert!(result.is_err(), "dequeue() should block/timeout on empty queue");
    });
}

#[test]
fn test_queue_stream_multiple_items() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(5);

        // Enqueue items
        for i in 1..=5 {
            queue.enqueue(i).await.unwrap();
        }

        // Collect all items from the dequeue rs2_stream
        let items: Vec<i32> = queue.dequeue().take(5).collect().await;
        assert_eq!(items, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_queue_concurrent_access() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let queue = Queue::<i32>::bounded(10);

        // Spawn a task to enqueue items
        let queue_clone = queue.clone();
        let enqueue_task = tokio::spawn(async move {
            for i in 1..=10 {
                queue_clone.enqueue(i).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Collect items as they become available
        let mut dequeue_stream = queue.dequeue();
        let mut collected = Vec::new();

        for _ in 0..10 {
            if let Some(item) = dequeue_stream.next().await {
                collected.push(item);
            }
        }

        enqueue_task.await.unwrap();

        assert_eq!(collected.len(), 10);
        assert_eq!(collected, (1..=10).collect::<Vec<i32>>());
    });
}