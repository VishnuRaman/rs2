use rs2::stream::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

struct TestStream {
    values: Vec<i32>,
    index: usize,
}

impl TestStream {
    fn new(values: Vec<i32>) -> Self {
        Self { values, index: 0 }
    }
}

impl Stream for TestStream {
    type Item = i32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.values.len() {
            let value = self.values[self.index];
            self.index += 1;
            Poll::Ready(Some(value))
        } else {
            Poll::Ready(None)
        }
    }
}

#[tokio::test]
async fn test_map() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let mapped: Vec<i32> = stream.map(|x| x * 2).collect().await;
    assert_eq!(mapped, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn test_filter() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let filtered: Vec<i32> = stream.filter(|x| x % 2 == 0).collect().await;
    assert_eq!(filtered, vec![2, 4, 6]);
}

#[tokio::test]
async fn test_take() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let taken: Vec<i32> = stream.take(3).collect().await;
    assert_eq!(taken, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_skip() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let skipped: Vec<i32> = stream.skip(2).collect().await;
    assert_eq!(skipped, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_chain() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let chained: Vec<i32> = stream1.chain(stream2).collect().await;
    assert_eq!(chained, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_zip() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect().await;
    assert_eq!(zipped, vec![(1, 4), (2, 5), (3, 6)]);
}

#[tokio::test]
async fn test_scan() {
    let stream = TestStream::new(vec![1, 2, 3, 4]);
    let scanned: Vec<i32> = stream.scan(0, |acc, x| {
        *acc += x;
        Some(*acc)
    }).collect().await;
    assert_eq!(scanned, vec![1, 3, 6, 10]);
}

#[tokio::test]
async fn test_fuse() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let fused: Vec<i32> = stream.fuse().collect().await;
    assert_eq!(fused, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_peekable() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.peek().await, Some(&1));
    let collected: Vec<i32> = peekable.collect().await;
    assert_eq!(collected, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_cycle() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let cycled: Vec<i32> = stream.cycle().take(7).collect().await;
    assert_eq!(cycled, vec![1, 2, 3, 1, 2, 3, 1]);
}

#[tokio::test]
async fn test_enumerate() {
    let stream = TestStream::new(vec![10, 20, 30]);
    let enumerated: Vec<(usize, i32)> = stream.enumerate().collect().await;
    assert_eq!(enumerated, vec![(0, 10), (1, 20), (2, 30)]);
}

#[tokio::test]
async fn test_inspect() {
    let mut inspected = Vec::new();
    let stream = TestStream::new(vec![1, 2, 3]);
    let result: Vec<i32> = stream.inspect(|x| inspected.push(*x)).collect().await;
    assert_eq!(result, vec![1, 2, 3]);
    assert_eq!(inspected, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_flat_map() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let flat_mapped: Vec<i32> = stream.flat_map(|x| TestStream::new(vec![x, x * 2])).collect().await;
    assert_eq!(flat_mapped, vec![1, 2, 2, 4, 3, 6]);
}

#[tokio::test]
async fn test_flatten() {
    let stream = TestStream::new(vec![
        TestStream::new(vec![1, 2]),
        TestStream::new(vec![3, 4]),
    ]);
    let flattened: Vec<i32> = stream.flatten().collect().await;
    assert_eq!(flattened, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_collect() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let collected: Vec<i32> = stream.collect().await;
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_fold() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let sum = stream.fold(0, |acc, x| acc + x).await;
    assert_eq!(sum, 15);
}

#[tokio::test]
async fn test_any() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert!(stream.clone().any(|x| x > 3).await);
    assert!(!stream.any(|x| x > 10).await);
}

#[tokio::test]
async fn test_all() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert!(stream.clone().all(|x| x > 0).await);
    assert!(!stream.all(|x| x > 3).await);
}

#[tokio::test]
async fn test_find() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.clone().find(|x| *x > 3).await, Some(4));
    assert_eq!(stream.find(|x| *x > 10).await, None);
}

#[tokio::test]
async fn test_position() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.clone().position(|x| x > 3).await, Some(3));
    assert_eq!(stream.position(|x| x > 10).await, None);
}

#[tokio::test]
async fn test_count() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.count().await, 5);
}

#[tokio::test]
async fn test_sum() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.sum::<i32>().await, 15);
}

#[tokio::test]
async fn test_product() {
    let stream = TestStream::new(vec![1, 2, 3, 4]);
    assert_eq!(stream.product::<i32>().await, 24);
}

#[tokio::test]
async fn test_max() {
    let stream = TestStream::new(vec![1, 3, 5, 2, 4]);
    assert_eq!(stream.max().await, Some(5));
}

#[tokio::test]
async fn test_min() {
    let stream = TestStream::new(vec![3, 1, 5, 2, 4]);
    assert_eq!(stream.min().await, Some(1));
}

#[tokio::test]
async fn test_max_by() {
    let stream = TestStream::new(vec![1, 3, 5, 2, 4]);
    assert_eq!(stream.max_by(|a, b| a.cmp(b)).await, Some(5));
}

#[tokio::test]
async fn test_min_by() {
    let stream = TestStream::new(vec![3, 1, 5, 2, 4]);
    assert_eq!(stream.min_by(|a, b| a.cmp(b)).await, Some(1));
}

#[tokio::test]
async fn test_max_by_key() {
    let stream = TestStream::new(vec![1, 3, 5, 2, 4]);
    assert_eq!(stream.max_by_key(|x| x.abs()).await, Some(5));
}

#[tokio::test]
async fn test_min_by_key() {
    let stream = TestStream::new(vec![3, 1, 5, 2, 4]);
    assert_eq!(stream.min_by_key(|x| x.abs()).await, Some(1));
}

#[tokio::test]
async fn test_nth() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.clone().nth(2).await, Some(3));
    assert_eq!(stream.nth(10).await, None);
}

#[tokio::test]
async fn test_last() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.last().await, Some(5));
}

#[tokio::test]
async fn test_partition() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let (even, odd): (Vec<i32>, Vec<i32>) = stream.partition(|x| x % 2 == 0).collect().await;
    assert_eq!(even, vec![2, 4, 6]);
    assert_eq!(odd, vec![1, 3, 5]);
}

#[tokio::test]
async fn test_for_each() {
    let mut sum = 0;
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    stream.for_each(|x| sum += x).await;
    assert_eq!(sum, 15);
}

#[tokio::test]
async fn test_try_fold() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<i32, &str> = stream.try_fold(0, |acc, x| {
        if x > 10 {
            Err("too large")
        } else {
            Ok(acc + x)
        }
    }).await;
    assert_eq!(result, Ok(15));
}

#[tokio::test]
async fn test_try_for_each() {
    let mut sum = 0;
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<(), &str> = stream.try_for_each(|x| {
        if x > 10 {
            Err("too large")
        } else {
            sum += x;
            Ok(())
        }
    }).await;
    assert_eq!(result, Ok(()));
    assert_eq!(sum, 15);
}

#[tokio::test]
async fn test_unzip() {
    let stream = TestStream::new(vec![(1, 'a'), (2, 'b'), (3, 'c')]);
    let (numbers, letters): (Vec<i32>, Vec<char>) = stream.unzip().collect().await;
    assert_eq!(numbers, vec![1, 2, 3]);
    assert_eq!(letters, vec!['a', 'b', 'c']);
}

#[tokio::test]
async fn test_copied() {
    let stream = TestStream::new(vec![&1, &2, &3]);
    let copied: Vec<i32> = stream.copied().collect().await;
    assert_eq!(copied, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_cloned() {
    let stream = TestStream::new(vec![&1, &2, &3]);
    let cloned: Vec<i32> = stream.cloned().collect().await;
    assert_eq!(cloned, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_rev() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let reversed: Vec<i32> = stream.rev().collect().await;
    assert_eq!(reversed, vec![5, 4, 3, 2, 1]);
}

#[tokio::test]
async fn test_unzip3() {
    let stream = TestStream::new(vec![(1, 'a', true), (2, 'b', false), (3, 'c', true)]);
    let (numbers, letters, bools): (Vec<i32>, Vec<char>, Vec<bool>) = stream.unzip3().collect().await;
    assert_eq!(numbers, vec![1, 2, 3]);
    assert_eq!(letters, vec!['a', 'b', 'c']);
    assert_eq!(bools, vec![true, false, true]);
}

#[tokio::test]
async fn test_chain_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let chained: Vec<i32> = stream1.chain(stream2).collect().await;
    assert_eq!(chained, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_zip_different_lengths() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4]);
    let stream2 = TestStream::new(vec![5, 6]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect().await;
    assert_eq!(zipped, vec![(1, 5), (2, 6)]);
}

#[tokio::test]
async fn test_scan_early_termination() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let scanned: Vec<i32> = stream.scan(0, |acc, x| {
        if *acc > 5 {
            None
        } else {
            *acc += x;
            Some(*acc)
        }
    }).collect().await;
    assert_eq!(scanned, vec![1, 3, 6]);
}

#[tokio::test]
async fn test_cycle_empty() {
    let stream = TestStream::new(vec![]);
    let cycled: Vec<i32> = stream.cycle().take(5).collect().await;
    assert_eq!(cycled, vec![]);
}

#[tokio::test]
async fn test_peekable_multiple_peeks() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.peek().await, Some(&1));
    assert_eq!(peekable.peek().await, Some(&1));
    assert_eq!(peekable.next().await, Some(1));
    assert_eq!(peekable.peek().await, Some(&2));
}

#[tokio::test]
async fn test_inspect_modification() {
    let mut values = Vec::new();
    let stream = TestStream::new(vec![1, 2, 3]);
    let result: Vec<i32> = stream.inspect(|x| values.push(*x * 2)).collect().await;
    assert_eq!(result, vec![1, 2, 3]);
    assert_eq!(values, vec![2, 4, 6]);
}

#[tokio::test]
async fn test_flat_map_empty() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let flat_mapped: Vec<i32> = stream.flat_map(|_| TestStream::new(vec![])).collect().await;
    assert_eq!(flat_mapped, vec![]);
}

#[tokio::test]
async fn test_flatten_empty() {
    let stream = TestStream::new(vec![
        TestStream::new(vec![]),
        TestStream::new(vec![]),
    ]);
    let flattened: Vec<i32> = stream.flatten().collect().await;
    assert_eq!(flattened, vec![]);
}

#[tokio::test]
async fn test_fold_empty() {
    let stream = TestStream::new(vec![]);
    let result = stream.fold(42, |acc, _x| acc).await;
    assert_eq!(result, 42);
}

#[tokio::test]
async fn test_any_empty() {
    let stream = TestStream::new(vec![]);
    assert!(!stream.any(|_| true).await);
}

#[tokio::test]
async fn test_all_empty() {
    let stream = TestStream::new(vec![]);
    assert!(stream.all(|_| false).await);
}

#[tokio::test]
async fn test_find_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.find(|_| true).await, None);
}

#[tokio::test]
async fn test_count_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.count().await, 0);
}

#[tokio::test]
async fn test_sum_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.sum::<i32>().await, 0);
}

#[tokio::test]
async fn test_product_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.product::<i32>().await, 1);
}

#[tokio::test]
async fn test_max_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.max().await, None);
}

#[tokio::test]
async fn test_min_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.min().await, None);
}

#[tokio::test]
async fn test_last_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.last().await, None);
}

#[tokio::test]
async fn test_nth_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.nth(0).await, None);
}

#[tokio::test]
async fn test_partition_empty() {
    let stream = TestStream::new(vec![]);
    let (even, odd): (Vec<i32>, Vec<i32>) = stream.partition(|x| x % 2 == 0).collect().await;
    assert_eq!(even, vec![]);
    assert_eq!(odd, vec![]);
}

#[tokio::test]
async fn test_for_each_empty() {
    let mut count = 0;
    let stream = TestStream::new(vec![]);
    stream.for_each(|_| count += 1).await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_try_fold_early_error() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<i32, &str> = stream.try_fold(0, |acc, x| {
        if x == 3 {
            Err("found 3")
        } else {
            Ok(acc + x)
        }
    }).await;
    assert_eq!(result, Err("found 3"));
}

#[tokio::test]
async fn test_unzip_empty() {
    let stream = TestStream::new(vec![]);
    let (numbers, letters): (Vec<i32>, Vec<char>) = stream.unzip().collect().await;
    assert_eq!(numbers, vec![]);
    assert_eq!(letters, vec![]);
}

#[tokio::test]
async fn test_rev_empty() {
    let stream = TestStream::new(vec![]);
    let reversed: Vec<i32> = stream.rev().collect().await;
    assert_eq!(reversed, vec![]);
}

#[tokio::test]
async fn test_unzip3_empty() {
    let stream = TestStream::new(vec![]);
    let (numbers, letters, bools): (Vec<i32>, Vec<char>, Vec<bool>) = stream.unzip3().collect().await;
    assert_eq!(numbers, vec![]);
    assert_eq!(letters, vec![]);
    assert_eq!(bools, vec![]);
} 