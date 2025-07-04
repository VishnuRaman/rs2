use rs2::stream::{Stream, StreamExt, utility::*};
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

impl Clone for TestStream {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            index: 0,
        }
    }
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
fn test_inspect_empty() {
    let mut inspected = Vec::new();
    let stream = TestStream::new(vec![]);
    let result: Vec<i32> = stream.inspect(|x| inspected.push(*x)).collect();
    assert_eq!(result, vec![]);
    assert_eq!(inspected, vec![]);
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
fn test_peekable() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.peek(), Some(&1));
    let collected: Vec<i32> = peekable.collect();
    assert_eq!(collected, vec![1, 2, 3]);
}

#[test]
fn test_peekable_empty() {
    let stream = TestStream::new(vec![]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.peek(), None);
    let collected: Vec<i32> = peekable.collect();
    assert_eq!(collected, vec![]);
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
fn test_peekable_after_next() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut peekable = stream.peekable();
    assert_eq!(peekable.next(), Some(1));
    assert_eq!(peekable.peek(), Some(&2));
    assert_eq!(peekable.next(), Some(2));
    assert_eq!(peekable.peek(), Some(&3));
    assert_eq!(peekable.next(), Some(3));
    assert_eq!(peekable.peek(), None);
}

#[test]
fn test_fuse() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let fused: Vec<i32> = stream.fuse().collect();
    assert_eq!(fused, vec![1, 2, 3]);
}

#[test]
fn test_fuse_empty() {
    let stream = TestStream::new(vec![]);
    let fused: Vec<i32> = stream.fuse().collect();
    assert_eq!(fused, vec![]);
}

#[test]
fn test_fuse_after_none() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let mut fused = stream.fuse();
    assert_eq!(fused.next(), Some(1));
    assert_eq!(fused.next(), Some(2));
    assert_eq!(fused.next(), Some(3));
    assert_eq!(fused.next(), None);
    assert_eq!(fused.next(), None);
    assert_eq!(fused.next(), None);
}

#[test]
fn test_enumerate() {
    let stream = TestStream::new(vec![10, 20, 30]);
    let enumerated: Vec<(usize, i32)> = stream.enumerate().collect();
    assert_eq!(enumerated, vec![(0, 10), (1, 20), (2, 30)]);
}

#[test]
fn test_enumerate_empty() {
    let stream = TestStream::new(vec![]);
    let enumerated: Vec<(usize, i32)> = stream.enumerate().collect();
    assert_eq!(enumerated, vec![]);
}

#[test]
fn test_enumerate_single() {
    let stream = TestStream::new(vec![42]);
    let enumerated: Vec<(usize, i32)> = stream.enumerate().collect();
    assert_eq!(enumerated, vec![(0, 42)]);
}

#[test]
fn test_rev() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let reversed: Vec<i32> = stream.rev().collect();
    assert_eq!(reversed, vec![5, 4, 3, 2, 1]);
}

#[test]
fn test_rev_empty() {
    let stream = TestStream::new(vec![]);
    let reversed: Vec<i32> = stream.rev().collect();
    assert_eq!(reversed, vec![]);
}

#[test]
fn test_rev_single() {
    let stream = TestStream::new(vec![42]);
    let reversed: Vec<i32> = stream.rev().collect();
    assert_eq!(reversed, vec![42]);
}

#[test]
fn test_copied() {
    let stream = TestStream::new(vec![&1, &2, &3]);
    let copied: Vec<i32> = stream.copied().collect();
    assert_eq!(copied, vec![1, 2, 3]);
}

#[test]
fn test_copied_empty() {
    let stream = TestStream::new(vec![]);
    let copied: Vec<i32> = stream.copied().collect();
    assert_eq!(copied, vec![]);
}

#[test]
fn test_cloned() {
    let stream = TestStream::new(vec![&1, &2, &3]);
    let cloned: Vec<i32> = stream.cloned().collect();
    assert_eq!(cloned, vec![1, 2, 3]);
}

#[test]
fn test_cloned_empty() {
    let stream = TestStream::new(vec![]);
    let cloned: Vec<i32> = stream.cloned().collect();
    assert_eq!(cloned, vec![]);
}

#[test]
fn test_chain() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let chained: Vec<i32> = stream1.chain(stream2).collect();
    assert_eq!(chained, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_chain_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let chained: Vec<i32> = stream1.chain(stream2).collect();
    assert_eq!(chained, vec![1, 2, 3]);
}

#[test]
fn test_chain_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let chained: Vec<i32> = stream1.chain(stream2).collect();
    assert_eq!(chained, vec![]);
}

#[test]
fn test_zip() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect();
    assert_eq!(zipped, vec![(1, 4), (2, 5), (3, 6)]);
}

#[test]
fn test_zip_different_lengths() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4]);
    let stream2 = TestStream::new(vec![5, 6]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect();
    assert_eq!(zipped, vec![(1, 5), (2, 6)]);
}

#[test]
fn test_zip_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect();
    assert_eq!(zipped, vec![]);
}

#[test]
fn test_zip_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let zipped: Vec<(i32, i32)> = stream1.zip(stream2).collect();
    assert_eq!(zipped, vec![]);
}

#[test]
fn test_unzip() {
    let stream = TestStream::new(vec![(1, 'a'), (2, 'b'), (3, 'c')]);
    let (numbers, letters): (Vec<i32>, Vec<char>) = stream.unzip();
    assert_eq!(numbers, vec![1, 2, 3]);
    assert_eq!(letters, vec!['a', 'b', 'c']);
}

#[test]
fn test_unzip_empty() {
    let stream = TestStream::new(vec![]);
    let (numbers, letters): (Vec<i32>, Vec<char>) = stream.unzip();
    assert_eq!(numbers, vec![]);
    assert_eq!(letters, vec![]);
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
fn test_unzip3_empty() {
    let stream = TestStream::new(vec![]);
    let (numbers, letters, bools): (Vec<i32>, Vec<char>, Vec<bool>) = stream.unzip3();
    assert_eq!(numbers, vec![]);
    assert_eq!(letters, vec![]);
    assert_eq!(bools, vec![]);
}

#[test]
fn test_partition() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let (even, odd): (Vec<i32>, Vec<i32>) = stream.partition(|x| x % 2 == 0);
    assert_eq!(even, vec![2, 4, 6]);
    assert_eq!(odd, vec![1, 3, 5]);
}

#[test]
fn test_partition_empty() {
    let stream = TestStream::new(vec![]);
    let (even, odd): (Vec<i32>, Vec<i32>) = stream.partition(|x| x % 2 == 0);
    assert_eq!(even, vec![]);
    assert_eq!(odd, vec![]);
}

#[test]
fn test_partition_all_true() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let (true_vals, false_vals): (Vec<i32>, Vec<i32>) = stream.partition(|_| true);
    assert_eq!(true_vals, vec![1, 2, 3]);
    assert_eq!(false_vals, vec![]);
}

#[test]
fn test_partition_all_false() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let (true_vals, false_vals): (Vec<i32>, Vec<i32>) = stream.partition(|_| false);
    assert_eq!(true_vals, vec![]);
    assert_eq!(false_vals, vec![1, 2, 3]);
}

#[test]
fn test_for_each() {
    let mut sum = 0;
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    stream.for_each(|x| sum += x);
    assert_eq!(sum, 15);
}

#[test]
fn test_for_each_empty() {
    let mut count = 0;
    let stream = TestStream::new(vec![]);
    stream.for_each(|_| count += 1);
    assert_eq!(count, 0);
}

#[test]
fn test_for_each_single() {
    let mut value = 0;
    let stream = TestStream::new(vec![42]);
    stream.for_each(|x| value = x);
    assert_eq!(value, 42);
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
fn test_try_fold_empty() {
    let stream = TestStream::new(vec![]);
    let result: Result<i32, &str> = stream.try_fold(42, |acc, _x| Ok(acc));
    assert_eq!(result, Ok(42));
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
fn test_try_for_each_early_error() {
    let mut sum = 0;
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let result: Result<(), &str> = stream.try_for_each(|x| {
        if x == 3 {
            Err("found 3")
        } else {
            sum += x;
            Ok(())
        }
    });
    assert_eq!(result, Err("found 3"));
    assert_eq!(sum, 3);
}

#[test]
fn test_try_for_each_empty() {
    let mut count = 0;
    let stream = TestStream::new(vec![]);
    let result: Result<(), &str> = stream.try_for_each(|_| {
        count += 1;
        Ok(())
    });
    assert_eq!(result, Ok(()));
    assert_eq!(count, 0);
} 