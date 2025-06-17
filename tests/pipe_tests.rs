use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use rs2_stream::pipe;
use rs2_stream::pipe::*;

#[test]
fn test_pipe_map() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let pipe = map(|x: i32| x * 2);

        let result = pipe.apply(stream).collect::<Vec<_>>().await;
        assert_eq!(result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_pipe_filter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let pipe = pipe::filter(|x: &i32| x % 2 == 0);

        let result = pipe.apply(stream).collect::<Vec<_>>().await;
        assert_eq!(result, vec![2, 4]);
    });
}

#[test]
fn test_pipe_compose() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Create pipes
        let double = pipe::map(|x: i32| x * 2);
        let even_only = pipe::filter(|x: &i32| x % 2 == 0);

        // Compose pipes: first double, then filter for even numbers
        let pipe = pipe::compose(double, even_only);

        let result = pipe.apply(stream).collect::<Vec<_>>().await;
        // After doubling, all numbers are even, so all should pass the filter
        assert_eq!(result, vec![2, 4, 6, 8, 10]);
    });
}

#[test]
fn test_pipe_identity() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);
        let pipe = pipe::identity();

        let result = pipe.apply(stream).collect::<Vec<_>>().await;
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn test_pipe_ext_compose() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Create pipes
        let double = pipe::map(|x: i32| x * 2);
        let to_string = pipe::map(|x: i32| x.to_string());

        // Use PipeExt to compose
        let pipe = double.compose(to_string);

        let result = pipe.apply(stream).collect::<Vec<_>>().await;
        assert_eq!(result, vec!["2".to_string(), "4".to_string(), "6".to_string(), "8".to_string(), "10".to_string()]);
    });
}
