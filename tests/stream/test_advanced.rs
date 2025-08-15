use rs2::stream::{Stream, StreamExt, advanced::*};
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

#[tokio::test]
async fn test_flat_map() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let flat_mapped: Vec<i32> = stream.flat_map(|x| TestStream::new(vec![x, x * 2])).collect().await;
    assert_eq!(flat_mapped, vec![1, 2, 2, 4, 3, 6]);
}

#[tokio::test]
async fn test_flat_map_empty() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let flat_mapped: Vec<i32> = stream.flat_map(|_| TestStream::new(vec![])).collect().await;
    assert_eq!(flat_mapped, vec![]);
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
async fn test_flatten_empty() {
    let stream = TestStream::new(vec![
        TestStream::new(vec![]),
        TestStream::new(vec![]),
    ]);
    let flattened: Vec<i32> = stream.flatten().collect().await;
    assert_eq!(flattened, vec![]);
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
async fn test_scan_empty() {
    let stream = TestStream::new(vec![]);
    let scanned: Vec<i32> = stream.scan(42, |acc, _x| Some(*acc)).collect().await;
    assert_eq!(scanned, vec![]);
}

#[tokio::test]
async fn test_cycle() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let cycled: Vec<i32> = stream.cycle().take(7).collect().await;
    assert_eq!(cycled, vec![1, 2, 3, 1, 2, 3, 1]);
}

#[tokio::test]
async fn test_cycle_empty() {
    let stream = TestStream::new(vec![]);
    let cycled: Vec<i32> = stream.cycle().take(5).collect().await;
    assert_eq!(cycled, vec![]);
}

#[tokio::test]
async fn test_cycle_single() {
    let stream = TestStream::new(vec![42]);
    let cycled: Vec<i32> = stream.cycle().take(3).collect().await;
    assert_eq!(cycled, vec![42, 42, 42]);
}

#[tokio::test]
async fn test_zip_longest() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect().await;
    assert_eq!(zipped, vec![
        (Some(1), Some(4)),
        (Some(2), Some(5)),
        (Some(3), None),
    ]);
}

#[tokio::test]
async fn test_zip_longest_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![1, 2]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect().await;
    assert_eq!(zipped, vec![
        (None, Some(1)),
        (None, Some(2)),
    ]);
}

#[tokio::test]
async fn test_zip_longest_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect().await;
    assert_eq!(zipped, vec![]);
}

#[tokio::test]
async fn test_interleave() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect().await;
    assert_eq!(interleaved, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_interleave_different_lengths() {
    let stream1 = TestStream::new(vec![1, 3, 5, 7]);
    let stream2 = TestStream::new(vec![2, 4]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect().await;
    assert_eq!(interleaved, vec![1, 2, 3, 4, 5, 7]);
}

#[tokio::test]
async fn test_interleave_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect().await;
    assert_eq!(interleaved, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_interleave_shortest() {
    let stream1 = TestStream::new(vec![1, 3, 5, 7]);
    let stream2 = TestStream::new(vec![2, 4]);
    let interleaved: Vec<i32> = stream1.interleave_shortest(stream2).collect().await;
    assert_eq!(interleaved, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_interleave_shortest_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let interleaved: Vec<i32> = stream1.interleave_shortest(stream2).collect().await;
    assert_eq!(interleaved, vec![]);
}

#[tokio::test]
async fn test_merge() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    assert_eq!(merged.len(), 6);
    assert!(merged.contains(&1));
    assert!(merged.contains(&2));
    assert!(merged.contains(&3));
    assert!(merged.contains(&4));
    assert!(merged.contains(&5));
    assert!(merged.contains(&6));
}

#[tokio::test]
async fn test_merge_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge(stream2).collect().await;
    assert_eq!(merged, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_merge_by() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge_by(stream2, |a, b| a < b).collect().await;
    assert_eq!(merged, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_merge_by_reverse() {
    let stream1 = TestStream::new(vec![5, 3, 1]);
    let stream2 = TestStream::new(vec![6, 4, 2]);
    let merged: Vec<i32> = stream1.merge_by(stream2, |a, b| a > b).collect().await;
    assert_eq!(merged, vec![6, 5, 4, 3, 2, 1]);
}

#[tokio::test]
async fn test_merge_by_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge_by(stream2, |a, b| a < b).collect().await;
    assert_eq!(merged, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_merge_by_key() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge_by_key(stream2, |x| *x).collect().await;
    assert_eq!(merged, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn test_merge_by_key_reverse() {
    let stream1 = TestStream::new(vec![5, 3, 1]);
    let stream2 = TestStream::new(vec![6, 4, 2]);
    let merged: Vec<i32> = stream1.merge_by_key(stream2, |x| -*x).collect().await;
    assert_eq!(merged, vec![6, 5, 4, 3, 2, 1]);
}

#[tokio::test]
async fn test_merge_by_key_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge_by_key(stream2, |x| *x).collect().await;
    assert_eq!(merged, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_group_by() {
    let stream = TestStream::new(vec![1, 1, 2, 2, 2, 3, 3]);
    let grouped: Vec<Vec<i32>> = stream.group_by(|a, b| a == b).collect().await;
    assert_eq!(grouped, vec![
        vec![1, 1],
        vec![2, 2, 2],
        vec![3, 3],
    ]);
}

#[tokio::test]
async fn test_group_by_empty() {
    let stream = TestStream::new(vec![]);
    let grouped: Vec<Vec<i32>> = stream.group_by(|a, b| a == b).collect().await;
    assert_eq!(grouped, vec![]);
}

#[tokio::test]
async fn test_group_by_single() {
    let stream = TestStream::new(vec![42]);
    let grouped: Vec<Vec<i32>> = stream.group_by(|a, b| a == b).collect().await;
    assert_eq!(grouped, vec![vec![42]]);
}

#[tokio::test]
async fn test_group_by_key() {
    let stream = TestStream::new(vec![1, 1, 2, 2, 2, 3, 3]);
    let grouped: Vec<Vec<i32>> = stream.group_by_key(|x| x % 2).collect().await;
    assert_eq!(grouped, vec![
        vec![1, 1],
        vec![2, 2, 2],
        vec![3, 3],
    ]);
}

#[tokio::test]
async fn test_group_by_key_empty() {
    let stream = TestStream::new(vec![]);
    let grouped: Vec<Vec<i32>> = stream.group_by_key(|x| x % 2).collect().await;
    assert_eq!(grouped, vec![]);
}

#[tokio::test]
async fn test_chunks() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5, 6]);
    let chunks: Vec<Vec<i32>> = stream.chunks(2).collect().await;
    assert_eq!(chunks, vec![
        vec![1, 2],
        vec![3, 4],
        vec![5, 6],
    ]);
}

#[tokio::test]
async fn test_chunks_remainder() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let chunks: Vec<Vec<i32>> = stream.chunks(2).collect().await;
    assert_eq!(chunks, vec![
        vec![1, 2],
        vec![3, 4],
        vec![5],
    ]);
}

#[tokio::test]
async fn test_chunks_empty() {
    let stream = TestStream::new(vec![]);
    let chunks: Vec<Vec<i32>> = stream.chunks(2).collect().await;
    assert_eq!(chunks, vec![]);
}

#[tokio::test]
async fn test_chunks_size_one() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let chunks: Vec<Vec<i32>> = stream.chunks(1).collect().await;
    assert_eq!(chunks, vec![
        vec![1],
        vec![2],
        vec![3],
    ]);
}

#[tokio::test]
async fn test_windows() {
    let stream = TestStream::new(vec![1, 2, 3, 4, 5]);
    let windows: Vec<Vec<i32>> = stream.windows(3).collect().await;
    assert_eq!(windows, vec![
        vec![1, 2, 3],
        vec![2, 3, 4],
        vec![3, 4, 5],
    ]);
}

#[tokio::test]
async fn test_windows_small() {
    let stream = TestStream::new(vec![1, 2]);
    let windows: Vec<Vec<i32>> = stream.windows(3).collect().await;
    assert_eq!(windows, vec![]);
}

#[tokio::test]
async fn test_windows_empty() {
    let stream = TestStream::new(vec![]);
    let windows: Vec<Vec<i32>> = stream.windows(2).collect().await;
    assert_eq!(windows, vec![]);
}

#[tokio::test]
async fn test_windows_size_one() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let windows: Vec<Vec<i32>> = stream.windows(1).collect().await;
    assert_eq!(windows, vec![
        vec![1],
        vec![2],
        vec![3],
    ]);
}

#[tokio::test]
async fn test_tee() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let (mut stream1, stream2) = stream.tee();
    let result1: Vec<i32> = stream1.collect().await;
    let result2: Vec<i32> = stream2.collect().await;
    assert_eq!(result1, vec![1, 2, 3]);
    assert_eq!(result2, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_tee_empty() {
    let stream = TestStream::new(vec![]);
    let (stream1, stream2) = stream.tee();
    let result1: Vec<i32> = stream1.collect().await;
    let result2: Vec<i32> = stream2.collect().await;
    assert_eq!(result1, vec![]);
    assert_eq!(result2, vec![]);
}

#[tokio::test]
async fn test_tee_single() {
    let stream = TestStream::new(vec![42]);
    let (stream1, stream2) = stream.tee();
    let result1: Vec<i32> = stream1.collect().await;
    let result2: Vec<i32> = stream2.collect().await;
    assert_eq!(result1, vec![42]);
    assert_eq!(result2, vec![42]);
}

#[tokio::test]
async fn test_combinations() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let combinations: Vec<Vec<i32>> = stream.combinations(2).collect().await;
    assert_eq!(combinations, vec![
        vec![1, 2],
        vec![1, 3],
        vec![2, 3],
    ]);
}

#[tokio::test]
async fn test_combinations_empty() {
    let stream = TestStream::new(vec![]);
    let combinations: Vec<Vec<i32>> = stream.combinations(2).collect().await;
    assert_eq!(combinations, vec![]);
}

#[tokio::test]
async fn test_combinations_size_one() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let combinations: Vec<Vec<i32>> = stream.combinations(1).collect().await;
    assert_eq!(combinations, vec![
        vec![1],
        vec![2],
        vec![3],
    ]);
}

#[tokio::test]
async fn test_combinations_large_size() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let combinations: Vec<Vec<i32>> = stream.combinations(4).collect().await;
    assert_eq!(combinations, vec![]);
}

#[tokio::test]
async fn test_permutations() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let permutations: Vec<Vec<i32>> = stream.permutations(2).collect().await;
    assert_eq!(permutations, vec![
        vec![1, 2],
        vec![1, 3],
        vec![2, 1],
        vec![2, 3],
        vec![3, 1],
        vec![3, 2],
    ]);
}

#[tokio::test]
async fn test_permutations_empty() {
    let stream = TestStream::new(vec![]);
    let permutations: Vec<Vec<i32>> = stream.permutations(2).collect().await;
    assert_eq!(permutations, vec![]);
}

#[tokio::test]
async fn test_permutations_size_one() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let permutations: Vec<Vec<i32>> = stream.permutations(1).collect().await;
    assert_eq!(permutations, vec![
        vec![1],
        vec![2],
        vec![3],
    ]);
}

#[tokio::test]
async fn test_permutations_large_size() {
    let stream = TestStream::new(vec![1, 2, 3]);
    let permutations: Vec<Vec<i32>> = stream.permutations(4).collect().await;
    assert_eq!(permutations, vec![]);
}

#[tokio::test]
async fn test_cartesian_product() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![3, 4]);
    let product: Vec<(i32, i32)> = stream1.cartesian_product(stream2).collect().await;
    assert_eq!(product, vec![
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
    ]);
}

#[tokio::test]
async fn test_cartesian_product_empty() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![]);
    let product: Vec<(i32, i32)> = stream1.cartesian_product(stream2).collect().await;
    assert_eq!(product, vec![]);
}

#[tokio::test]
async fn test_cartesian_product_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let product: Vec<(i32, i32)> = stream1.cartesian_product(stream2).collect().await;
    assert_eq!(product, vec![]);
}

#[tokio::test]
async fn test_cartesian_product_single() {
    let stream1 = TestStream::new(vec![1]);
    let stream2 = TestStream::new(vec![2]);
    let product: Vec<(i32, i32)> = stream1.cartesian_product(stream2).collect().await;
    assert_eq!(product, vec![(1, 2)]);
} 