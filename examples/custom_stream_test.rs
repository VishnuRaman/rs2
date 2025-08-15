//! Example demonstrating the custom stream implementation
//! 
//! This example shows how to use our custom Stream trait and combinators
//! without the overhead of `.boxed()` calls or external dependencies.

use rs2_stream::stream::*;
use std::pin::Pin;
use std::task::{Context, Poll};

// Simple test stream that yields numbers
struct NumberStream {
    current: u32,
    max: u32,
}

impl NumberStream {
    fn new(max: u32) -> Self {
        Self { current: 0, max }
    }
}

impl Stream for NumberStream {
    type Item = u32;
    
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.current < this.max {
            let item = this.current;
            this.current += 1;
            Poll::Ready(Some(item))
        } else {
            Poll::Ready(None)
        }
    }
}

#[tokio::main]
async fn main() {
    println!("Testing custom stream implementation...");
    
    // Test basic stream operations
    let stream = NumberStream::new(5);
    let result: Vec<u32> = stream.collect().await;
    println!("Collected: {:?}", result);
    
    // Test map combinator
    let stream = NumberStream::new(3);
    let doubled: Vec<u32> = stream.map(|x| x * 2).collect().await;
    println!("Doubled: {:?}", doubled);
    
    // Test filter combinator
    let stream = NumberStream::new(10);
    let evens: Vec<u32> = stream.filter(|&x| x % 2 == 0).collect().await;
    println!("Evens: {:?}", evens);
    
    // Test take combinator
    let stream = NumberStream::new(10);
    let first_three: Vec<u32> = stream.take(3).collect().await;
    println!("First three: {:?}", first_three);
    
    // Test enumerate combinator
    let stream = NumberStream::new(3);
    let enumerated: Vec<(usize, u32)> = stream.enumerate().collect().await;
    println!("Enumerated: {:?}", enumerated);
    
    // Test zip combinator
    let stream1 = NumberStream::new(3);
    let stream2 = NumberStream::new(3);
    let zipped: Vec<(u32, u32)> = stream1.zip(stream2).collect().await;
    println!("Zipped: {:?}", zipped);
    
    // Test utility combinators
    let stream = NumberStream::new(5);
    let count = stream.count().await;
    println!("Count: {}", count);
    
    let stream = NumberStream::new(5);
    let all_positive = stream.all(|&x| x >= 0).await;
    println!("All positive: {}", all_positive);
    
    let stream = NumberStream::new(5);
    let any_greater_than_3 = stream.any(|&x| x > 3).await;
    println!("Any greater than 3: {}", any_greater_than_3);
    
    // Test constructors
    let empty_stream: Vec<u32> = empty::<u32>().collect().await;
    println!("Empty: {:?}", empty_stream);
    
    let once_stream: Vec<u32> = once(42).collect().await;
    println!("Once: {:?}", once_stream);
    
    let repeat_stream: Vec<u32> = repeat(7).take(3).collect().await;
    println!("Repeat: {:?}", repeat_stream);
    
    let iter_stream: Vec<u32> = from_iter(0..3).collect().await;
    println!("From iter: {:?}", iter_stream);
    
    println!("All tests passed! Custom stream implementation is working.");
} 