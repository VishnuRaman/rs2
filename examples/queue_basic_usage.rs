use futures_util::stream::StreamExt;
use rs2_stream::queue::*;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a bounded queue with capacity 5
        let queue = Queue::bounded(5);

        // Enqueue some items
        for i in 1..=3 {
            queue.enqueue(i).await.unwrap();
            println!("Enqueued: {}", i);
        }

        // Get the current queue length
        let len = queue.len().await;
        println!("Queue length: {}", len);
        assert_eq!(len, 3);

        // Close the queue before draining it.
        //
        // `dequeue()` ends only when the queue is closed — otherwise `recv()`
        // waits for a producer that may never send again, and the loop below
        // never terminates. Close first when you know no more items are coming.
        queue.close().await;

        // Get a stream for dequeuing
        let mut dequeue_stream = queue.dequeue();

        // Dequeue and process items
        while let Some(item) = dequeue_stream.next().await {
            println!("Dequeued: {}", item);
        }

        // --- unbounded queues -------------------------------------------
        // `Queue::bounded(n)` applies backpressure once n items are in
        // flight. `Queue::unbounded()` never blocks the producer —
        // convenient, but it can grow without limit if the consumer lags.
        let unbounded: Queue<i32> = Queue::unbounded();
        for i in 0..5 {
            unbounded.enqueue(i).await.expect("enqueue");
        }
        println!(
            "\nUnbounded queue: len={} capacity={:?}",
            unbounded.len().await,
            unbounded.capacity()
        );

        // Here we take exactly as many as we enqueued, so closing is optional —
        // but draining with `while let` would need it, as above.
        let mut rx = Box::pin(unbounded.dequeue());
        let mut drained = Vec::new();
        for _ in 0..5 {
            if let Some(v) = rx.next().await {
                drained.push(v);
            }
        }
        println!("Drained: {:?}, len now {}", drained, unbounded.len().await);
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);
        assert_eq!(unbounded.len().await, 0, "len must drop as items are dequeued");
        println!("(capacity() is None for unbounded queues)");
    });
}
