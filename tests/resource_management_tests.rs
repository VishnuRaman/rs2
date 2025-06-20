use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

#[test]
fn test_bracket() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Track resource acquisition and release
        let acquired = Arc::new(Mutex::new(false));
        let released = Arc::new(Mutex::new(false));

        let acquired_clone = Arc::clone(&acquired);
        let released_clone = Arc::clone(&released);

        // Create a rs2_stream using bracket
        let stream = bracket(
            async move {
                *acquired_clone.lock().unwrap() = true;
                "resource"
            },
            |resource| {
                assert_eq!(resource, "resource");
                from_iter(vec![1, 2, 3])
            },
            move |resource| async move {
                assert_eq!(resource, "resource");
                *released_clone.lock().unwrap() = true;
            },
        );

        // Collect the rs2_stream
        let result = stream.collect::<Vec<_>>().await;

        // Verify the rs2_stream produced the expected values
        assert_eq!(result, vec![1, 2, 3]);

        // Verify resource was acquired and released
        assert!(*acquired.lock().unwrap());
        assert!(*released.lock().unwrap());
    });
}

#[test]
fn test_bracket_case_success() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Track resource acquisition and release
        let acquired = Arc::new(Mutex::new(false));
        let released = Arc::new(Mutex::new(false));
        let exit_case = Arc::new(Mutex::new(None));

        let acquired_clone = Arc::clone(&acquired);
        let released_clone = Arc::clone(&released);
        let exit_case_clone = Arc::clone(&exit_case);

        // Create a rs2_stream using bracket_case with successful results
        let stream = bracket_case(
            async move {
                *acquired_clone.lock().unwrap() = true;
                "resource"
            },
            |resource| {
                assert_eq!(resource, "resource");
                from_iter(vec![Ok(1), Ok(2), Ok(3)])
            },
            move |resource, case: ExitCase<&str>| async move {
                assert_eq!(resource, "resource");
                *released_clone.lock().unwrap() = true;
                *exit_case_clone.lock().unwrap() = Some(format!("{:?}", case));
            },
        );

        // Collect the rs2_stream
        let result = stream.collect::<Vec<_>>().await;

        // Verify the rs2_stream produced the expected values
        assert_eq!(result, vec![Ok(1), Ok(2), Ok(3)]);

        // Verify resource was acquired and released
        assert!(*acquired.lock().unwrap());
        assert!(*released.lock().unwrap());

        // Verify exit case was Completed
        let case = exit_case.lock().unwrap().clone().unwrap();
        assert!(case.contains("Completed"));
    });
}

#[test]
fn test_bracket_case_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Track resource acquisition and release
        let acquired = Arc::new(Mutex::new(false));
        let released = Arc::new(Mutex::new(false));
        let exit_case = Arc::new(Mutex::new(None));

        let acquired_clone = Arc::clone(&acquired);
        let released_clone = Arc::clone(&released);
        let exit_case_clone = Arc::clone(&exit_case);

        // Create a rs2_stream using bracket_case with an error
        let stream = bracket_case(
            async move {
                *acquired_clone.lock().unwrap() = true;
                "resource"
            },
            |resource| {
                assert_eq!(resource, "resource");
                from_iter(vec![Ok(1), Err("error"), Ok(3)])
            },
            move |resource, case: ExitCase<&str>| async move {
                assert_eq!(resource, "resource");
                *released_clone.lock().unwrap() = true;
                *exit_case_clone.lock().unwrap() = Some(format!("{:?}", case));
            },
        );

        // Collect the rs2_stream
        let result = stream.collect::<Vec<_>>().await;

        // Verify the rs2_stream produced the expected values (including the error)
        assert_eq!(result, vec![Ok(1), Err("error"), Ok(3)]);

        // Verify resource was acquired and released
        assert!(*acquired.lock().unwrap());
        assert!(*released.lock().unwrap());

        // Verify exit case was Completed (even with an error in the rs2_stream)
        let case = exit_case.lock().unwrap().clone().unwrap();
        assert!(case.contains("Completed"));
    });
}
