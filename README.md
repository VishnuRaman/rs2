# RS2: Rust Streaming Library

RS2 is a powerful, functional streaming library for Rust inspired by FS2 (Functional Streams for Scala). It provides a comprehensive set of tools for working with asynchronous streams in a functional programming style, with built-in support for backpressure handling, resource management, and error handling.

## Features

- **Functional API**: Chain operations together in a fluent, functional style
- **Backpressure Handling**: Built-in support for handling backpressure with configurable strategies
- **Resource Management**: Safe resource acquisition and release with bracket patterns
- **Error Handling**: Comprehensive error handling with retry policies
- **Parallel Processing**: Process stream elements in parallel with bounded concurrency
- **Time-based Operations**: Throttling, debouncing, sampling, and timeouts
- **Transformations**: Rich set of stream transformation operations

## Installation

Add RS2 to your `Cargo.toml`:

```toml
[dependencies]
rs2 = "0.1.0"
```

## Basic Usage

For basic usage examples, see [examples/basic_usage.rs](examples/basic_usage.rs).

```rust
// This example demonstrates basic stream creation and transformation
// See the full code at examples/basic_usage.rs
```

## Real-World Example: Processing a Stream of Users

For a more complex example that processes a stream of users, demonstrating several RS2 features, see [examples/processing_stream_of_users.rs](examples/processing_stream_of_users.rs).

```rust
// This example demonstrates:
// - Creating streams from async functions
// - Applying backpressure
// - Filtering and transforming streams
// - Grouping elements by key
// - Parallel processing with bounded concurrency
// - Timeout handling
// See the full code at examples/processing_stream_of_users.rs
```

This example demonstrates:
- Creating a stream of users
- Applying backpressure to avoid overwhelming downstream systems
- Filtering for active users only
- Grouping users by role
- Processing users in parallel with bounded concurrency
- Adding timeouts to operations
- Collecting results

## API Overview

### Stream Creation

- `emit(item)` - Create a stream that emits a single element
- `empty()` - Create an empty stream
- `from_iter(iter)` - Create a stream from an iterator
- `eval(future)` - Evaluate a Future and emit its output
- `repeat(item)` - Create a stream that repeats a value
- `emit_after(item, duration)` - Create a stream that emits a value after a delay
- `unfold(init, f)` - Create a stream by repeatedly applying a function

#### Examples

##### Stream Creation with `emit`, `empty`, and `from_iter`

For examples of basic stream creation, see [examples/stream_creation_basic.rs](examples/stream_creation_basic.rs).

```rust
// This example demonstrates:
// - Creating a stream with a single element using emit()
// - Creating an empty stream using empty()
// - Creating a stream from an iterator using from_iter()
// See the full code at examples/stream_creation_basic.rs
```

##### Async Stream Creation with `eval` and `emit_after`

For examples of async stream creation, see [examples/stream_creation_async.rs](examples/stream_creation_async.rs).

```rust
// This example demonstrates:
// - Creating a stream by evaluating a future using eval()
// - Creating a stream that emits a value after a delay using emit_after()
// See the full code at examples/stream_creation_async.rs
```

##### Infinite Stream Creation with `repeat` and `unfold`

For examples of creating infinite streams, see [examples/stream_creation_infinite.rs](examples/stream_creation_infinite.rs).

```rust
// This example demonstrates:
// - Creating an infinite stream that repeats a value using repeat()
// - Creating an infinite stream by repeatedly applying a function using unfold()
// See the full code at examples/stream_creation_infinite.rs
```

### Transformations

- `map_rs2(f)` - Apply a function to each element
- `filter_rs2(predicate)` - Keep only elements that satisfy the predicate
- `flat_map_rs2(f)` - Apply a function that returns a stream to each element and flatten the results
- `take_rs2(n)` - Take the first n elements
- `skip_rs2(n)` - Skip the first n elements
- `distinct_rs2()` - Remove duplicate elements
- `distinct_until_changed_rs2()` - Remove consecutive duplicate elements
- `distinct_by_rs2(f)` - Remove duplicate elements based on a key function
- `distinct_until_changed_by_rs2(f)` - Remove consecutive duplicate elements based on a key function

#### Examples

##### Basic Transformations

For examples of basic transformations, see [examples/transformations_basic.rs](examples/transformations_basic.rs).

```rust
// This example demonstrates:
// - Mapping elements using map_rs2()
// - Filtering elements using filter_rs2()
// - Flattening nested streams using flat_map_rs2()
// See the full code at examples/transformations_basic.rs
```

##### Async Transformations

For examples of async transformations, see [examples/transformations_async.rs](examples/transformations_async.rs).

```rust
// This example demonstrates:
// - Mapping elements with async functions using eval_map_rs2()
// - Filtering elements with async predicates using eval_filter_rs2()
// See the full code at examples/transformations_async.rs
```

##### Combining Streams

For examples of combining streams, see [examples/transformations_combining.rs](examples/transformations_combining.rs).

```rust
// This example demonstrates:
// - Concatenating streams using concat_rs2()
// - Merging streams using merge_rs2()
// - Zipping streams using zip_rs2()
// See the full code at examples/transformations_combining.rs
```

##### Grouping Elements

For examples of grouping elements, see [examples/transformations_grouping.rs](examples/transformations_grouping.rs).

```rust
// This example demonstrates:
// - Grouping elements by key using group_by_rs2()
// - Grouping elements into chunks using chunks_rs2()
// See the full code at examples/transformations_grouping.rs
```

##### Slicing and Windowing

For examples of slicing and windowing, see [examples/transformations_slicing.rs](examples/transformations_slicing.rs).

```rust
// This example demonstrates:
// - Taking elements using take_rs2()
// - Skipping elements using skip_rs2()
// - Creating sliding windows using sliding_window_rs2()
// See the full code at examples/transformations_slicing.rs
```

### Accumulation

- `fold_rs2(init, f)` - Accumulate a value over a stream
- `scan_rs2(init, f)` - Apply a function to each element and emit intermediate accumulated values
- `for_each_rs2(f)` - Apply a function to each element without accumulating a result
- `collect_rs2::<B>()` - Collect all items into a collection

#### Examples

##### Accumulating Values with `fold_rs2` and `scan_rs2`

For examples of accumulating values, see [examples/accumulating_values.rs](examples/accumulating_values.rs).

```rust
// This example demonstrates:
// - Accumulating values using fold_rs2()
// - Emitting intermediate accumulated values using scan_rs2()
// - Applying a function to each element using for_each_rs2()
// - Collecting elements into different collections using collect_rs2()
// See the full code at examples/accumulating_values.rs
```

### Parallel Processing

- `par_eval_map_rs2(concurrency, f)` - Process elements in parallel with bounded concurrency, preserving order
- `par_eval_map_unordered_rs2(concurrency, f)` - Process elements in parallel without preserving order
- `par_join_rs2(concurrency)` - Run multiple streams concurrently and combine their outputs

### Time-based Operations

- `throttle_rs2(duration)` - Emit at most one element per duration
- `debounce_rs2(duration)` - Emit an element after a quiet period
- `sample_rs2(interval)` - Sample at regular intervals
- `timeout_rs2(duration)` - Add timeout to operations

#### Examples

##### Time-based Operations

For examples of time-based operations, see [examples/timeout_operations.rs](examples/timeout_operations.rs).

```rust
// This example demonstrates:
// - Adding timeouts to operations using timeout_rs2()
// - Throttling a stream using throttle_rs2()
// - Debouncing a stream using debounce_rs2()
// - Sampling a stream at regular intervals using sample_rs2()
// - Creating a delayed stream using emit_after()
// See the full code at examples/timeout_operations.rs
```

##### Processing Elements in Parallel

For examples of processing elements in parallel, see [examples/processing_elements.rs](examples/processing_elements.rs).

```rust
// This example demonstrates:
// - Processing elements in parallel with bounded concurrency using par_eval_map_rs2()
// - Processing elements in parallel without preserving order using par_eval_map_unordered_rs2()
// - Running multiple streams concurrently using par_join_rs2()
// See the full code at examples/processing_elements.rs
```

### Error Handling

- `recover_rs2(f)` - Recover from errors by applying a function
- `retry_with_policy_rs2(policy, f)` - Retry failed operations with a retry policy
- `on_error_resume_next_rs2()` - Continue processing after errors

### Resource Management

- `bracket_rs2(acquire, use_fn, release)` - Safely acquire and release resources

#### Examples

##### Resource Management with `bracket_rs2`

For examples of resource management, see [examples/resource_management_bracket.rs](examples/resource_management_bracket.rs).

```rust
// This example demonstrates:
// - Safely acquiring and releasing resources using bracket_rs2()
// - Ensuring resources are released even if an error occurs
// See the full code at examples/resource_management_bracket.rs
```

### Backpressure

- `auto_backpressure_rs2()` - Apply automatic backpressure
- `auto_backpressure_with_rs2(config)` - Apply automatic backpressure with custom configuration
- `rate_limit_backpressure_rs2(rate)` - Apply rate-limited backpressure

#### Examples

##### Custom Backpressure

For examples of custom backpressure, see [examples/custom_backpressure.rs](examples/custom_backpressure.rs).

```rust
// This example demonstrates:
// - Applying automatic backpressure using auto_backpressure_rs2()
// - Configuring custom backpressure strategies using auto_backpressure_with_rs2()
// - Applying rate-limited backpressure using rate_limit_backpressure_rs2()
// See the full code at examples/custom_backpressure.rs
```

## Connectors: External System Integration

RS2 provides connectors for integrating with external systems like Kafka, databases, and more. Connectors implement the `StreamConnector` trait:

```rust
#[async_trait]
pub trait StreamConnector<T>: Send + Sync
where
    T: Send + 'static,
{
    type Config: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    type Metadata: Send + Sync;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<T>, Self::Error>;
    async fn to_sink(&self, stream: RS2Stream<T>, config: Self::Config) -> Result<Self::Metadata, Self::Error>;
    async fn health_check(&self) -> Result<bool, Self::Error>;
    async fn metadata(&self) -> Result<Self::Metadata, Self::Error>;
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
}
```

#### Kafka Connector Example

RS2 includes a Kafka connector that allows you to create streams from Kafka topics and send streams to Kafka topics:

```rust
// This example demonstrates how to use the Kafka connector to:
// - Create a stream from a Kafka topic
// - Process the stream with RS2 transformations
// - Send the processed stream back to a different Kafka topic
// See the full code at examples/connector_kafka.rs
```

For the complete example, see [examples/connector_kafka.rs](examples/connector_kafka.rs).

### Creating Custom Connectors

You can create your own connectors by implementing the `StreamConnector` trait. For a complete example of creating a custom connector, see [examples/connector_custom.rs](examples/connector_custom.rs).

```rust
// This example demonstrates how to:
// - Create a custom connector for a hypothetical message queue
// - Implement the StreamConnector trait
// - Create a stream from the connector
// - Process the stream with RS2 transformations
// - Send the processed stream back to the connector
// See the full code at examples/connector_custom.rs
```

## Pipe: Stream Transformation Functions

A Pipe represents a stream transformation from one type to another. It's a function from Stream[I] to Stream[O] that can be composed with other pipes to create complex stream processing pipelines.

### Pipe Methods

- `Pipe::new(f)` - Create a new pipe from a function
- `apply(input)` - Apply this pipe to a stream
- `compose(other)` - Compose this pipe with another pipe

### Utility Functions

- `map(f)` - Create a pipe that applies the given function to each element
- `filter(predicate)` - Create a pipe that filters elements based on the predicate
- `compose(p1, p2)` - Compose two pipes together
- `identity()` - Identity pipe that doesn't transform the stream

### Examples

#### Basic Pipe Usage

For examples of basic pipe usage, see [examples/pipe_basic_usage.rs](examples/pipe_basic_usage.rs).

```rust
// This example demonstrates:
// - Creating a pipe that doubles each number
// - Applying the pipe to a stream
// See the full code at examples/pipe_basic_usage.rs
```

#### Composing Pipes

For examples of composing pipes, see [examples/pipe_composing.rs](examples/pipe_composing.rs).

```rust
// This example demonstrates:
// - Creating pipes for different transformations
// - Composing pipes using the compose function
// - Composing pipes using the compose method
// See the full code at examples/pipe_composing.rs
```

#### Real-World Example: User Data Processing Pipeline

For a more complex example of using pipes to process user data, see [examples/pipe_user_data_processing.rs](examples/pipe_user_data_processing.rs).

```rust
// This example demonstrates:
// - Creating pipes for filtering active users
// - Creating pipes for transforming User to UserStats
// - Composing pipes to create a processing pipeline
// - Grouping users by login frequency
// See the full code at examples/pipe_user_data_processing.rs
```

## Queue: Concurrent Queue with Stream Interface

A Queue represents a concurrent queue with a Stream interface for dequeuing and async methods for enqueuing. It supports both bounded and unbounded queues.

### Queue Types

- `Queue::bounded(capacity)` - Create a new bounded queue with the given capacity
- `Queue::unbounded()` - Create a new unbounded queue

### Queue Methods

- `enqueue(item)` - Enqueue an item into the queue
- `try_enqueue(item)` - Try to enqueue an item without blocking
- `dequeue()` - Get a stream for dequeuing items
- `close()` - Close the queue, preventing further enqueues
- `capacity()` - Get the capacity of the queue (None for unbounded)
- `is_empty()` - Check if the queue is empty
- `len()` - Get the current number of items in the queue

### Examples

#### Basic Queue Usage

For examples of basic queue usage, see [examples/queue_basic_usage.rs](examples/queue_basic_usage.rs).

```rust
// This example demonstrates:
// - Creating a bounded queue
// - Enqueuing items
// - Dequeuing items as a stream
// See the full code at examples/queue_basic_usage.rs
```

#### Producer-Consumer Pattern

For examples of using queues in a producer-consumer pattern, see [examples/queue_producer_consumer.rs](examples/queue_producer_consumer.rs).

```rust
// This example demonstrates:
// - Creating a shared queue
// - Spawning producer and consumer tasks
// - Handling backpressure with bounded queues
// See the full code at examples/queue_producer_consumer.rs
```

#### Real-World Example: Message Processing System

For a more complex example of using queues to build a message processing system, see [examples/queue_message_processing.rs](examples/queue_message_processing.rs).

```rust
// This example demonstrates:
// - Creating a message processing system with priority queues
// - Processing messages based on priority
// - Handling different message types
// See the full code at examples/queue_message_processing.rs
```

## **Feature Comparison with Other Rust Streaming Libraries**

| **Feature** | **futures-rs** | **tokio-stream** | **async-stream** | **async-std** | **RS2** |
|-------------|----------------|------------------|------------------|---------------|---------|
| **Backpressure Strategies** | ❌ None | ⚠️ Basic buffering | ❌ None | ❌ None | ✅ **4 strategies**: Block, DropOldest, DropNewest, Error |
| **Parallel Processing** | ⚠️ `buffer_unordered` only | ⚠️ Limited buffering | ❌ None | ⚠️ Basic | ✅ **Advanced**: `par_eval_map`, `par_join`, ordered/unordered |
| **Error Recovery** | ⚠️ Manual `Result` handling | ⚠️ Manual | ⚠️ Manual | ⚠️ Manual | ✅ **Automatic**: `recover`, `retry_with_policy`, `on_error_resume_next` |
| **Time Operations** | ❌ None | ⚠️ `throttle`, `timeout` | ❌ None | ⚠️ Basic intervals | ✅ **Rich set**: `debounce`, `sample`, `sliding_window`, `emit_after` |
| **Resource Management** | ❌ Manual | ❌ Manual | ❌ Manual | ❌ Manual | ✅ **Bracket patterns**: Guaranteed cleanup on success/failure |
| **Stream Combinators** | ✅ Standard set | ✅ Extended set | ⚠️ Manual creation | ✅ Standard set | ✅ **Enhanced**: All standard + advanced grouping |
| **Prefetching & Buffering** | ⚠️ Basic `buffered` | ⚠️ Basic buffering | ❌ None | ⚠️ Basic | ✅ **Intelligent**: `prefetch`, `rate_limit_backpressure` |
| **Stream Creation** | ✅ `iter`, `once`, `empty` | ✅ Extended creation | ✅ `stream!` macro | ✅ Standard | ✅ **Rich**: `eval`, `unfold`, `emit_after`, `repeat` |
| **Metrics & Monitoring** | ❌ None | ❌ None | ❌ None | ❌ None | ✅ **Built-in**: `with_metrics`, throughput tracking |
| **Cancellation Safety** | ⚠️ Manual | ⚠️ Manual | ⚠️ Manual | ⚠️ Manual | ✅ **Interrupt-aware**: `interrupt_when` |
| **Memory Efficiency** | ✅ Good | ✅ Good | ✅ Good | ✅ Good | ✅ **Optimized**: Constant memory with backpressure |
| **Functional Style** | ⚠️ Partial | ⚠️ Partial | ❌ Imperative | ⚠️ Partial | ✅ **Pure functional**: Inspired by FS2 |
| **External Connectors** | ❌ None | ❌ None | ❌ None | ❌ None | ✅ **Built-in**: Kafka, custom connectors |
