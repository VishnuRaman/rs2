use rs2_stream::stream::round_robin::RoundRobinInterleave;
use rs2_stream::stream::{from_iter, StreamExt};

#[tokio::test]
async fn test_round_robin_three_streams() {
    let s1 = from_iter(vec![1, 4, 7]);
    let s2 = from_iter(vec![2, 5, 8]);
    let s3 = from_iter(vec![3, 6, 9]);
    let rr = RoundRobinInterleave::new(vec![s1, s2, s3]);
    let result: Vec<_> = rr.collect().await;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[tokio::test]
async fn test_round_robin_different_lengths() {
    let s1 = from_iter(vec![1, 4]);
    let s2 = from_iter(vec![2, 5, 8]);
    let s3 = from_iter(vec![3]);
    let rr = RoundRobinInterleave::new(vec![s1, s2, s3]);
    let result: Vec<_> = rr.collect().await;
    // Order: 1,2,3,4,5,8
    assert_eq!(result, vec![1, 2, 3, 4, 5, 8]);
}

#[tokio::test]
async fn test_round_robin_empty_list() {
    let rr: RoundRobinInterleave<rs2_stream::stream::Iter<std::vec::IntoIter<i32>>> = RoundRobinInterleave::new(vec![]);
    let result: Vec<i32> = rr.collect().await;
    assert_eq!(result, Vec::<i32>::new());
}

#[tokio::test]
async fn test_round_robin_one_stream() {
    let s1 = from_iter(vec![10, 20, 30]);
    let rr = RoundRobinInterleave::new(vec![s1]);
    let result: Vec<_> = rr.collect().await;
    assert_eq!(result, vec![10, 20, 30]);
} 