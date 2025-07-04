use rs2_stream::rs2_new;
use rs2_stream::rs2_new_result_stream_ext::RS2ResultStreamExt;
use rs2_stream::rs2_new_stream_ext::RS2StreamExt;
use rs2_stream::error::StreamError;

#[tokio::test]
async fn test_successes_rs2() {
    let stream = rs2_new::from_iter_rs2(vec![Ok(1), Err(StreamError::Custom("error".to_string())), Ok(3)]);
    let successes = stream.successes_rs2();
    let result: Vec<_> = successes.collect_rs2().await;
    assert_eq!(result, vec![1, 3]);
}

#[tokio::test]
async fn test_map_errors_rs2() {
    let stream = rs2_new::from_iter_rs2(vec![Ok(1), Err(StreamError::Custom("error".to_string())), Ok(3)]);
    let mapped = stream.map_errors_rs2(|e| format!("mapped_{}", e));
    let result: Vec<_> = mapped.collect_rs2().await;
    // Should have the mapped error
    assert_eq!(result.len(), 3);
}

#[tokio::test]
async fn test_or_else_rs2() {
    let stream = rs2_new::from_iter_rs2(vec![Ok(1), Err(StreamError::Custom("error".to_string())), Ok(3)]);
    let or_else = stream.or_else_rs2(|_| async move { 99 });
    let result: Vec<_> = or_else.collect_rs2().await;
    // Should have replacement for error
    assert_eq!(result.len(), 3);
} 