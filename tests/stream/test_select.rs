use rs2::stream::{Stream, StreamExt, select::*};
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
fn test_select() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let selected: Vec<i32> = stream1.select(stream2).collect();
    assert_eq!(selected.len(), 6);
    assert!(selected.contains(&1));
    assert!(selected.contains(&2));
    assert!(selected.contains(&3));
    assert!(selected.contains(&4));
    assert!(selected.contains(&5));
    assert!(selected.contains(&6));
}

#[test]
fn test_select_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let selected: Vec<i32> = stream1.select(stream2).collect();
    assert_eq!(selected, vec![1, 2, 3]);
}

#[test]
fn test_select_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let selected: Vec<i32> = stream1.select(stream2).collect();
    assert_eq!(selected, vec![]);
}

#[test]
fn test_select_by() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let selected: Vec<i32> = stream1.select_by(stream2, |a, b| a < b).collect();
    assert_eq!(selected, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_select_by_reverse() {
    let stream1 = TestStream::new(vec![5, 3, 1]);
    let stream2 = TestStream::new(vec![6, 4, 2]);
    let selected: Vec<i32> = stream1.select_by(stream2, |a, b| a > b).collect();
    assert_eq!(selected, vec![6, 5, 4, 3, 2, 1]);
}

#[test]
fn test_select_by_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let selected: Vec<i32> = stream1.select_by(stream2, |a, b| a < b).collect();
    assert_eq!(selected, vec![1, 2, 3]);
}

#[test]
fn test_select_by_key() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let selected: Vec<i32> = stream1.select_by_key(stream2, |x| *x).collect();
    assert_eq!(selected, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_select_by_key_reverse() {
    let stream1 = TestStream::new(vec![5, 3, 1]);
    let stream2 = TestStream::new(vec![6, 4, 2]);
    let selected: Vec<i32> = stream1.select_by_key(stream2, |x| -*x).collect();
    assert_eq!(selected, vec![6, 5, 4, 3, 2, 1]);
}

#[test]
fn test_select_by_key_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let selected: Vec<i32> = stream1.select_by_key(stream2, |x| *x).collect();
    assert_eq!(selected, vec![1, 2, 3]);
}

#[test]
fn test_merge() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge(stream2).collect();
    assert_eq!(merged.len(), 6);
    assert!(merged.contains(&1));
    assert!(merged.contains(&2));
    assert!(merged.contains(&3));
    assert!(merged.contains(&4));
    assert!(merged.contains(&5));
    assert!(merged.contains(&6));
}

#[test]
fn test_merge_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge(stream2).collect();
    assert_eq!(merged, vec![1, 2, 3]);
}

#[test]
fn test_merge_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge(stream2).collect();
    assert_eq!(merged, vec![]);
}

#[test]
fn test_merge_by() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge_by(stream2, |a, b| a < b).collect();
    assert_eq!(merged, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_merge_by_reverse() {
    let stream1 = TestStream::new(vec![5, 3, 1]);
    let stream2 = TestStream::new(vec![6, 4, 2]);
    let merged: Vec<i32> = stream1.merge_by(stream2, |a, b| a > b).collect();
    assert_eq!(merged, vec![6, 5, 4, 3, 2, 1]);
}

#[test]
fn test_merge_by_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge_by(stream2, |a, b| a < b).collect();
    assert_eq!(merged, vec![1, 2, 3]);
}

#[test]
fn test_merge_by_key() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let merged: Vec<i32> = stream1.merge_by_key(stream2, |x| *x).collect();
    assert_eq!(merged, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_merge_by_key_reverse() {
    let stream1 = TestStream::new(vec![5, 3, 1]);
    let stream2 = TestStream::new(vec![6, 4, 2]);
    let merged: Vec<i32> = stream1.merge_by_key(stream2, |x| -*x).collect();
    assert_eq!(merged, vec![6, 5, 4, 3, 2, 1]);
}

#[test]
fn test_merge_by_key_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let merged: Vec<i32> = stream1.merge_by_key(stream2, |x| *x).collect();
    assert_eq!(merged, vec![1, 2, 3]);
}

#[test]
fn test_interleave() {
    let stream1 = TestStream::new(vec![1, 3, 5]);
    let stream2 = TestStream::new(vec![2, 4, 6]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect();
    assert_eq!(interleaved, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_interleave_different_lengths() {
    let stream1 = TestStream::new(vec![1, 3, 5, 7]);
    let stream2 = TestStream::new(vec![2, 4]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect();
    assert_eq!(interleaved, vec![1, 2, 3, 4, 5, 7]);
}

#[test]
fn test_interleave_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect();
    assert_eq!(interleaved, vec![1, 2, 3]);
}

#[test]
fn test_interleave_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let interleaved: Vec<i32> = stream1.interleave(stream2).collect();
    assert_eq!(interleaved, vec![]);
}

#[test]
fn test_interleave_shortest() {
    let stream1 = TestStream::new(vec![1, 3, 5, 7]);
    let stream2 = TestStream::new(vec![2, 4]);
    let interleaved: Vec<i32> = stream1.interleave_shortest(stream2).collect();
    assert_eq!(interleaved, vec![1, 2, 3, 4]);
}

#[test]
fn test_interleave_shortest_empty() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![]);
    let interleaved: Vec<i32> = stream1.interleave_shortest(stream2).collect();
    assert_eq!(interleaved, vec![]);
}

#[test]
fn test_interleave_shortest_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let interleaved: Vec<i32> = stream1.interleave_shortest(stream2).collect();
    assert_eq!(interleaved, vec![]);
}

#[test]
fn test_zip_longest() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (Some(1), Some(4)),
        (Some(2), Some(5)),
        (Some(3), None),
    ]);
}

#[test]
fn test_zip_longest_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![1, 2]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (None, Some(1)),
        (None, Some(2)),
    ]);
}

#[test]
fn test_zip_longest_both_empty() {
    let stream1 = TestStream::new(vec![]);
    let stream2 = TestStream::new(vec![]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![]);
}

#[test]
fn test_zip_longest_equal_lengths() {
    let stream1 = TestStream::new(vec![1, 2, 3]);
    let stream2 = TestStream::new(vec![4, 5, 6]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (Some(1), Some(4)),
        (Some(2), Some(5)),
        (Some(3), Some(6)),
    ]);
}

#[test]
fn test_zip_longest_first_longer() {
    let stream1 = TestStream::new(vec![1, 2, 3, 4]);
    let stream2 = TestStream::new(vec![5, 6]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (Some(1), Some(5)),
        (Some(2), Some(6)),
        (Some(3), None),
        (Some(4), None),
    ]);
}

#[test]
fn test_zip_longest_second_longer() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![3, 4, 5, 6]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (Some(1), Some(3)),
        (Some(2), Some(4)),
        (None, Some(5)),
        (None, Some(6)),
    ]);
}

#[test]
fn test_zip_longest_single() {
    let stream1 = TestStream::new(vec![1]);
    let stream2 = TestStream::new(vec![2]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![(Some(1), Some(2))]);
}

#[test]
fn test_zip_longest_first_single() {
    let stream1 = TestStream::new(vec![1]);
    let stream2 = TestStream::new(vec![2, 3]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (Some(1), Some(2)),
        (None, Some(3)),
    ]);
}

#[test]
fn test_zip_longest_second_single() {
    let stream1 = TestStream::new(vec![1, 2]);
    let stream2 = TestStream::new(vec![3]);
    let zipped: Vec<(Option<i32>, Option<i32>)> = stream1.zip_longest(stream2).collect();
    assert_eq!(zipped, vec![
        (Some(1), Some(3)),
        (Some(2), None),
    ]);
} 