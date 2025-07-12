use rs2_stream::rs2::*;
use tokio::runtime::Runtime;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::{empty, from_iter, repeat, StreamExt};

#[test]
fn test_emit() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = emit(42);
        let result: Vec<i32> = stream.collect_rs2().await;
        assert_eq!(result, vec![42]);
    });
}

#[test]
fn test_empty() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = empty::<i32>();
        let result = stream.collect_rs2().await;
        assert_eq!(result, Vec::<i32>::new());
    });
}

#[test]
fn test_from_iter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let result = stream.collect_rs2().await;
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_eval() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = eval(async { 42 });
        let result: Vec<i32> = stream.collect().await;
        assert_eq!(result, vec![42]);
    });
}

#[test]
fn test_repeat() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = repeat(42);
        let result = stream.take_rs2(5).collect_rs2().await;
        assert_eq!(result, vec![42, 42, 42, 42, 42]);
    });
}

#[test]
fn test_emit_after() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let start = std::time::Instant::now();
        let stream = emit_after(42, std::time::Duration::from_millis(100));
        let result: Vec<i32> = stream.collect().await;
        let elapsed = start.elapsed();

        assert_eq!(result, vec![42]);
        assert!(
            elapsed.as_millis() >= 100,
            "Should have waited at least 100ms"
        );
    });
}

#[test]
fn test_take() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let result = stream.take_rs2(3).collect_rs2().await;
        assert_eq!(result, vec![1, 2, 3]);
    });
}

#[test]
fn test_drop() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let result = stream.drop_rs2(2).collect_rs2().await;
        assert_eq!(result, vec![3, 4, 5]);
    });
}
