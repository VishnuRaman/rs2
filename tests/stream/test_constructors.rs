use rs2::stream::{Stream, StreamExt, constructors::*};

#[test]
fn test_once() {
    let stream = once(42);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![42]);
}

#[test]
fn test_once_with() {
    let stream = once_with(|| 42);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![42]);
}

#[test]
fn test_repeat() {
    let stream = repeat(42);
    let result: Vec<i32> = stream.take(3).collect();
    assert_eq!(result, vec![42, 42, 42]);
}

#[test]
fn test_repeat_with() {
    let mut counter = 0;
    let stream = repeat_with(|| {
        counter += 1;
        counter
    });
    let result: Vec<i32> = stream.take(3).collect();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_empty() {
    let stream: Empty<i32> = empty();
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_pending() {
    let stream: Pending<i32> = pending();
    let result: Vec<i32> = stream.take(3).collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_iter() {
    let iter = vec![1, 2, 3].into_iter();
    let stream = iter.into_stream();
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_from_fn() {
    let mut counter = 0;
    let stream = from_fn(move || {
        if counter < 3 {
            counter += 1;
            Some(counter)
        } else {
            None
        }
    });
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_from_fn_empty() {
    let stream = from_fn(|| None::<i32>);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_successors() {
    let stream = successors(Some(1), |&n| {
        if n < 5 {
            Some(n + 1)
        } else {
            None
        }
    });
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_successors_empty() {
    let stream = successors(None::<i32>, |_| Some(1));
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_unfold() {
    let stream = unfold(0, |state| {
        if *state < 3 {
            let value = *state;
            *state += 1;
            Some(value)
        } else {
            None
        }
    });
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![0, 1, 2]);
}

#[test]
fn test_unfold_empty() {
    let stream = unfold(0, |_| None::<i32>);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_range() {
    let stream = range(1, 4);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_range_empty() {
    let stream = range(5, 1);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_range_inclusive() {
    let stream = range_inclusive(1, 3);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_range_inclusive_empty() {
    let stream = range_inclusive(5, 1);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_chain_constructors() {
    let stream1 = once(1);
    let stream2 = once(2);
    let chained = stream1.chain(stream2);
    let result: Vec<i32> = chained.collect();
    assert_eq!(result, vec![1, 2]);
}

#[test]
fn test_zip_constructors() {
    let stream1 = once(1);
    let stream2 = once(2);
    let zipped = stream1.zip(stream2);
    let result: Vec<(i32, i32)> = zipped.collect();
    assert_eq!(result, vec![(1, 2)]);
}

#[test]
fn test_map_constructor() {
    let stream = once(1);
    let mapped = stream.map(|x| x * 2);
    let result: Vec<i32> = mapped.collect();
    assert_eq!(result, vec![2]);
}

#[test]
fn test_filter_constructor() {
    let stream = range(1, 4);
    let filtered = stream.filter(|x| x % 2 == 0);
    let result: Vec<i32> = filtered.collect();
    assert_eq!(result, vec![2]);
}

#[test]
fn test_take_constructor() {
    let stream = range(1, 5);
    let taken = stream.take(2);
    let result: Vec<i32> = taken.collect();
    assert_eq!(result, vec![1, 2]);
}

#[test]
fn test_skip_constructor() {
    let stream = range(1, 5);
    let skipped = stream.skip(2);
    let result: Vec<i32> = skipped.collect();
    assert_eq!(result, vec![3, 4]);
}

#[test]
fn test_repeat_take() {
    let stream = repeat(42);
    let result: Vec<i32> = stream.take(0).collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_repeat_with_side_effects() {
    let mut calls = 0;
    let stream = repeat_with(|| {
        calls += 1;
        calls
    });
    let result: Vec<i32> = stream.take(3).collect();
    assert_eq!(result, vec![1, 2, 3]);
    assert_eq!(calls, 3);
}

#[test]
fn test_from_fn_side_effects() {
    let mut calls = 0;
    let stream = from_fn(move || {
        calls += 1;
        if calls <= 2 {
            Some(calls)
        } else {
            None
        }
    });
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![1, 2]);
    assert_eq!(calls, 3);
}

#[test]
fn test_successors_fibonacci() {
    let stream = successors(Some((0, 1)), |&(a, b)| {
        let next = a + b;
        if next <= 10 {
            Some((b, next))
        } else {
            None
        }
    }).map(|(a, _)| a);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8]);
}

#[test]
fn test_unfold_fibonacci() {
    let stream = unfold((0, 1), |state| {
        let (a, b) = *state;
        if a <= 10 {
            let next = (b, a + b);
            *state = next;
            Some(a)
        } else {
            None
        }
    });
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![0, 1, 1, 2, 3, 5, 8]);
}

#[test]
fn test_range_step() {
    let stream = range(0, 10);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn test_range_inclusive_step() {
    let stream = range_inclusive(0, 5);
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn test_empty_collect() {
    let stream: Empty<i32> = empty();
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_pending_collect() {
    let stream: Pending<i32> = pending();
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_iter_empty() {
    let iter = Vec::<i32>::new().into_iter();
    let stream = iter.into_stream();
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![]);
}

#[test]
fn test_iter_single() {
    let iter = vec![42].into_iter();
    let stream = iter.into_stream();
    let result: Vec<i32> = stream.collect();
    assert_eq!(result, vec![42]);
} 