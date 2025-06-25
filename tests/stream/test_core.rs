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

#[test]
fn test_map() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let mapped: Vec<i32> = stream.map(|x| x * 2).collect();
    assert_eq!(mapped, vec![2, 4, 6, 8, 10]);
}

#[test]
fn test_filter() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let filtered: Vec<i32> = stream.filter(|x| x % 2 == 0).collect();
    assert_eq!(filtered, vec![2, 4, 6]);
}

#[test]
fn test_take() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let taken: Vec<i32> = stream.take(3).collect();
    assert_eq!(taken, vec![1, 2, 3]);
}

#[test]
fn test_skip() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let skipped: Vec<i32> = stream.skip(2).collect();
    assert_eq!(skipped, vec![3, 4, 5]);
}

#[test]
fn test_chain() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let chained: Vec<i32> = stream1.chain(stream2).collect();
    assert_eq!(chained, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_zip() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect();
    assert_eq!(zipped, vec![(1, 4), (2, 5), (3, 6)]);
}

#[test]
fn test_scan() {
    let stream = TestStream::new(vec![1, 2, 3, 4]);
    let scanned: Vec<i32> = stream.scan(0, |acc, x| {
        *acc += x;
        Some(*acc)
    }).collect();
    assert_eq!(scanned, vec![1, 3, 6, 10]);
}

#[test]
fn test_fuse() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let fused: Vec<i32> = stream.fuse().collect();
    assert_eq!(fused, vec![1, 2, 3]);
}

#[test]
fn test_peekable() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.peek(), Some(&1));
    let collected: Vec<i32> = peekable.collect();
    assert_eq!(collected, vec![1, 2, 3]);
}

#[test]
fn test_cycle() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let cycled: Vec<i32> = stream.cycle().take(7).collect();
    assert_eq!(cycled, vec![1, 2, 3, 1, 2, 3, 1]);
}

#[test]
fn test_enumerate() {
    let stream = TestStream::new(vec![10, 20, 30]);
    let enumerated: Vec<(usize, i32)> = stream.enumerate().collect();
    assert_eq!(enumerated, vec![(0, 10), (1, 20), (2, 30)]);
}

#[test]
fn test_inspect() {
    let mut inspected = Vec::new();
    let stream = TestStream::new(vec![1, 2, 3]);
    let result: Vec<i32> = stream.inspect(|x| inspected.push(*x)).collect();
    assert_eq!(result, vec![1, 2, 3]);
    assert_eq!(inspected, vec![1, 2, 3]);
}

#[test]
fn test_flat_map() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let flat_mapped: Vec<i32> = stream.flat_map(|x| TestStream::new(vec![x, x * 2])).collect();
    assert_eq!(flat_mapped, vec![1, 2, 2, 4, 3, 6]);
}

#[test]
fn test_flatten() {
    let stream = TestStream::new(vec![
        TestStream::new(vec![1, 2]),
        TestStream::new(vec![3, 4]),
    ]);
    let flattened: Vec<i32> = stream.flatten().collect();
    assert_eq!(flattened, vec![1, 2, 3, 4]);
}

#[test]
fn test_collect() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let collected: Vec<i32> = stream.collect();
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_fold() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let sum = stream.fold(0, |acc, x| acc + x);
    assert_eq!(sum, 15);
}

#[test]
fn test_any() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert!(stream.clone().any(|x| x > 3));
    assert!(!stream.any(|x| x > 10));
}

#[test]
fn test_all() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert!(stream.clone().all(|x| x > 0));
    assert!(!stream.all(|x| x > 3));
}

#[test]
fn test_find() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.clone().find(|x| *x > 3), Some(4));
    assert_eq!(stream.find(|x| *x > 10), None);
}

#[test]
fn test_position() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.clone().position(|x| x > 3), Some(3));
    assert_eq!(stream.position(|x| x > 10), None);
}

#[test]
fn test_count() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.count(), 5);
}

#[test]
fn test_sum() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.sum::<i32>(), 15);
}

#[test]
fn test_product() {
    let stream = TestStream::new(vec![1, 2, 3, 4]);
    assert_eq!(stream.product::<i32>(), 24);
}

#[test]
fn test_max() {
    let stream = TestStream::new(vec![1, 3, 5, 2, 4]);
    assert_eq!(stream.max(), Some(5));
}

#[test]
fn test_min() {
    let stream = TestStream::new(vec![3, 1, 5, 2, 4]);
    assert_eq!(stream.min(), Some(1));
}

#[test]
fn test_max_by() {
    let stream = TestStream::new(vec![1, 3, 5, 2, 4]);
    assert_eq!(stream.max_by(|a, b| a.cmp(b)), Some(5));
}

#[test]
fn test_min_by() {
    let stream = TestStream::new(vec![3, 1, 5, 2, 4]);
    assert_eq!(stream.min_by(|a, b| a.cmp(b)), Some(1));
}

#[test]
fn test_max_by_key() {
    let stream = TestStream::new(vec![1, 3, 5, 2, 4]);
    assert_eq!(stream.max_by_key(|x| x.abs()), Some(5));
}

#[test]
fn test_min_by_key() {
    let stream = TestStream::new(vec![3, 1, 5, 2, 4]);
    assert_eq!(stream.min_by_key(|x| x.abs()), Some(1));
}

#[test]
fn test_nth() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.clone().nth(2), Some(3));
    assert_eq!(stream.nth(10), None);
}

#[test]
fn test_last() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    assert_eq!(stream.last(), Some(5));
}

#[test]
fn test_partition() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let (even, odd): (Vec<i32>, Vec<i32>) = stream.partition(|x| x % 2 == 0);
    assert_eq!(even, vec![2, 4, 6]);
    assert_eq!(odd, vec![1, 3, 5]);
}

#[test]
fn test_for_each() {
    let mut sum = 0;
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    stream.for_each(|x| sum += x);
    assert_eq!(sum, 15);
}

#[test]
fn test_try_fold() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<i32, &str> = stream.try_fold(0, |acc, x| {
        if x > 10 {
            Err("too large")
        } else {
            Ok(acc + x)
        }
    });
    assert_eq!(result, Ok(15));
}

#[test]
fn test_try_for_each() {
    let mut sum = 0;
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<(), &str> = stream.try_for_each(|x| {
        if x > 10 {
            Err("too large")
        } else {
            sum += x;
            Ok(())
        }
    });
    assert_eq!(result, Ok(()));
    assert_eq!(sum, 15);
}

#[test]
fn test_unzip() {
    let stream = TestStream::new(vec![(1, 'a'), (2, 'b'), (3, 'c')]);
    let (numbers, letters): (Vec<i32>, Vec<char>) = stream.unzip();
    assert_eq!(numbers, vec![1, 2, 3]);
    assert_eq!(letters, vec!['a', 'b', 'c']);
}

#[test]
fn test_copied() {
    let stream = TestStream::new(vec![&1, &2, &3]);
    let copied: Vec<i32> = stream.copied().collect();
    assert_eq!(copied, vec![1, 2, 3]);
}

#[test]
fn test_cloned() {
    let stream = TestStream::new(vec![&1, &2, &3]);
    let cloned: Vec<i32> = stream.cloned().collect();
    assert_eq!(cloned, vec![1, 2, 3]);
}

#[test]
fn test_rev() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let reversed: Vec<i32> = stream.rev().collect();
    assert_eq!(reversed, vec![5, 4, 3, 2, 1]);
}

#[test]
fn test_unzip3() {
    let stream = TestStream::new(vec![(1, 'a', true), (2, 'b', false), (3, 'c', true)]);
    let (numbers, letters, bools): (Vec<i32>, Vec<char>, Vec<bool>) = stream.unzip3();
    assert_eq!(numbers, vec![1, 2, 3]);
    assert_eq!(letters, vec!['a', 'b', 'c']);
    assert_eq!(bools, vec![true, false, true]);
}

#[test]
fn test_chain_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let chained: Vec<i32> = stream1.chain(stream2).collect();
    assert_eq!(chained, vec![1, 2, 3]);
}

#[test]
fn test_zip_different_lengths() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4]);
    let stream2 = TestStream::new(vec![5, 6]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect();
    assert_eq!(zipped, vec![(1, 5), (2, 6)]);
}

#[test]
fn test_scan_early_termination() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let scanned: Vec<i32> = stream.scan(0, |acc, x| {
        if *acc > 5 {
            None
        } else {
            *acc += x;
            Some(*acc)
        }
    }).collect();
    assert_eq!(scanned, vec![1, 3, 6]);
}

#[test]
fn test_cycle_empty() {
    let stream = TestStream::new(vec![]);
    let cycled: Vec<i32> = stream.cycle().take(5).collect();
    assert_eq!(cycled, vec![]);
}

#[test]
fn test_peekable_multiple_peeks() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.peek(), Some(&1));
    assert_eq!(peekable.peek(), Some(&1));
    assert_eq!(peekable.next(), Some(1));
    assert_eq!(peekable.peek(), Some(&2));
}

#[test]
fn test_inspect_modification() {
    let mut values = Vec::new();
    let stream = TestStream::new(vec![1, 2, 3]);
    let result: Vec<i32> = stream.inspect(|x| values.push(*x * 2)).collect();
    assert_eq!(result, vec![1, 2, 3]);
    assert_eq!(values, vec![2, 4, 6]);
}

#[test]
fn test_flat_map_empty() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let flat_mapped: Vec<i32> = stream.flat_map(|_| TestStream::new(vec![])).collect();
    assert_eq!(flat_mapped, vec![]);
}

#[test]
fn test_flatten_empty() {
    let stream = TestStream::new(vec![
        TestStream::new(vec![]),
        TestStream::new(vec![]),
    ]);
    let flattened: Vec<i32> = stream.flatten().collect();
    assert_eq!(flattened, vec![]);
}

#[test]
fn test_fold_empty() {
    let stream = TestStream::new(vec![]);
    let result = stream.fold(42, |acc, _x| acc);
    assert_eq!(result, 42);
}

#[test]
fn test_any_empty() {
    let stream = TestStream::new(vec![]);
    assert!(!stream.any(|_| true));
}

#[test]
fn test_all_empty() {
    let stream = TestStream::new(vec![]);
    assert!(stream.all(|_| false));
}

#[test]
fn test_find_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.find(|_| true), None);
}

#[test]
fn test_count_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.count(), 0);
}

#[test]
fn test_sum_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.sum::<i32>(), 0);
}

#[test]
fn test_product_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.product::<i32>(), 1);
}

#[test]
fn test_max_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.max(), None);
}

#[test]
fn test_min_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.min(), None);
}

#[test]
fn test_last_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.last(), None);
}

#[test]
fn test_nth_empty() {
    let stream = TestStream::new(vec![]);
    assert_eq!(stream.nth(0), None);
}

#[test]
fn test_partition_empty() {
    let stream = TestStream::new(vec![]);
    let (even, odd): (Vec<i32>, Vec<i32>) = stream.partition(|x| x % 2 == 0);
    assert_eq!(even, vec![]);
    assert_eq!(odd, vec![]);
}

#[test]
fn test_for_each_empty() {
    let mut count = 0;
    let stream = TestStream::new(vec![]);
    stream.for_each(|_| count += 1);
    assert_eq!(count, 0);
}

#[test]
fn test_try_fold_early_error() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<i32, &str> = stream.try_fold(0, |acc, x| {
        if x == 3 {
            Err("found 3")
        } else {
            Ok(acc + x)
        }
    });
    assert_eq!(result, Err("found 3"));
}

#[test]
fn test_unzip_empty() {
    let stream = TestStream::new(vec![]);
    let (numbers, letters): (Vec<i32>, Vec<char>) = stream.unzip();
    assert_eq!(numbers, vec![]);
    assert_eq!(letters, vec![]);
}

#[test]
fn test_rev_empty() {
    let stream = TestStream::new(vec![]);
    let reversed: Vec<i32> = stream.rev().collect();
    assert_eq!(reversed, vec![]);
}

#[test]
fn test_unzip3_empty() {
    let stream = TestStream::new(vec![]);
    let (numbers, letters, bools): (Vec<i32>, Vec<char>, Vec<bool>) = stream.unzip3();
    assert_eq!(numbers, vec![]);
    assert_eq!(letters, vec![]);
    assert_eq!(bools, vec![]);
} 