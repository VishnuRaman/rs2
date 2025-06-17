# RS2: Rust Streaming Library

**RS2** is a high-performance, production-ready async streaming library for Rust that combines the ergonomics of reactive streams with enterprise-grade reliability features. Built for real-world applications that demand both developer productivity and operational excellence.

## 🚀 Why RS2?

**Superior Scaling Performance**: While RS2 has modest sequential overhead (1.6x vs futures-rs, comparable to tokio-stream), it delivers exceptional parallel performance with **near-linear scaling up to 16+ cores** and **7.8-8.5x speedup** for I/O-bound workloads.

**Production-Grade Reliability**: Unlike basic streaming libraries, RS2 includes built-in **automatic backpressure**, **retry policies with exponential backoff**, **circuit breakers**, **timeout handling**, and **resource management** - eliminating the need to manually implement these critical production patterns.

**Effortless Parallelization**: Transform any sequential stream into parallel processing with a single method call. RS2's `par_eval_map_rs2()` automatically handles concurrency, ordering, and error propagation.

**Enterprise Integration**: First-class connector system for Kafka, and custom systems with health checks, metrics, and automatic retry logic built-in.

# RS2 Performance Benchmarks

## Throughput Performance

| **Workload Type** | **Sequential** | **Parallel (8 cores)** | **Parallel (16 cores)** |
|-------------------|----------------|------------------------|--------------------------|
| **Pure CPU Operations** | 1.1M/sec | 6.6-8.8M/sec | 11-13.2M/sec |
| **Light Async I/O** | 110K-550K/sec | 550K-1.1M/sec | 880K-1.65M/sec |
| **Heavy I/O (Network/DB)** | 11K-55K/sec | 55K-110K/sec | 88K-165K/sec |
| **Message Queue Processing** | 5.5K-22K/sec | 22K-88K/sec | 44K-176K/sec |
| **JSON/Data Transformation** | 110K-330K/sec | 440K-880K/sec | 660K-1.32M/sec |
| **Real-time Analytics** | 220K-550K/sec | 880K-1.65M/sec | 1.32M-2.2M/sec |

## Benchmark-Based Performance

| **Operation** | **RS2 Performance** | **vs Baseline** | **Scaling Factor** |
|---------------|---------------------|------------------|-------------------|
| **Map + Filter** | ~1.54M records/sec | 3.2x vs tokio-stream | 7.8x parallel speedup |
| **Chunking + Fold** | ~880K records/sec | Competitive with tokio-stream | 8.5x parallel speedup |
| **Async Transform** | ~330K records/sec | Near-linear scaling | Up to 16 cores |
| **Backpressure Handling** | ~220K records/sec | Built-in reliability | Automatic throttling |

## Key Performance Highlights

- ✅ **CPU-bound**: Up to 13.2M records/sec with 16 cores
- ✅ **I/O-bound**: 110K-1.1M records/sec typical range
- ✅ **Production**: 55K-550K records/sec for most real-world scenarios
- ✅ **Scaling**: Near-linear performance gains with core count
- ✅ **Parallel Speedup**: 7.8-8.5x performance improvement
- ✅ **Built-in Reliability**: Automatic backpressure and error handling
- ✅ **Optimized Memory**: 10% throughput improvement from BufferConfig optimization

### **Perfect For:**
- **High-throughput data pipelines** processing millions of events per second
- **Microservices** requiring resilient inter-service communication
- **ETL workloads** that need automatic parallelization and error recovery
- **Real-time analytics** with backpressure-aware stream processing

### **Get Started**

```rust
use rs2::prelude::*;

// Transform any iterator into a resilient, parallel stream
let results = from_iter(data)
.par_eval_map_rs2(8, |item| async { process(item).await })
.auto_retry_rs2(RetryPolicy::exponential_backoff())
.with_timeout_rs2(Duration::from_secs(30))
.collect_rs2::<Vec<_>>()
.await?;
```

### **I/O Scaling Performance**

| **Concurrency** | **Time** | **Speedup** |
|-----------------|----------|-------------|
| Sequential | 4.22s | 1x |
| 8 concurrent | 537ms | **7.8x** |
| 16 concurrent | 284ms | **14.8x** |
| 32 concurrent | 161ms | **26x** |
| 64 concurrent | 99ms | **42x** |

## ⚡ Performance Optimized

RS2 delivers **20-50% faster** stream processing compared to previous versions:

- **Map/Filter chains**: Up to 50% faster
- **Chunked processing**: Up to 45% faster
- **Async operations**: Up to 29% faster
- **Fold operations**: Up to 22% faster

*Performance improvements scale consistently from 1K to 1M+ items*

### **Consistent Scaling Performance**
The improvements hold steady across different data sizes:

| **Operation** | **1K items** | **10K items** | **100K items** | **1M items** |
|---------------|--------------|---------------|----------------|--------------|
| **Map/Filter** | 46% faster | 50% faster | 49% faster | 49% faster |
| **Chunk Process** | 43% faster | 45% faster | 45% faster | 45% faster |

### **Consistent Scaling Performance**
The improvements hold steady across different data sizes:

| **Operation** | **1K items** | **10K items** | **100K items** | **1M items** |
|---------------|--------------|---------------|----------------|--------------|
| **Map/Filter** | 3.66µs vs 6.78µs | 32.6µs vs 65.2µs | 326µs vs 647µs | 3.30ms vs 6.60ms |
| **Chunk Process** | 3.59µs vs 6.30µs | 34.9µs vs 63.5µs | 346µs vs 631µs | 3.45ms vs 6.33ms |

*Times shown as: **RS2 time vs Previous time***

**No performance degradation at scale** - RS2 maintains its 46-50% speed advantage from 1,000 to 1,000,000 items.

**Key Metrics:**
- **Sequential Operations**: Comparable to tokio-stream (43µs vs 43µs for 10K items)
- **Parallel I/O Scaling**: Linear scaling from 2.26s (1 core) → 134ms (16 cores)
- **CPU-bound Tasks**: Optimal scaling up to physical core count
- **Real-world Workloads**: 2.2-2.5s for complex data processing pipelines
- **Memory Efficiency**: Chunked processing for large datasets (2.9ms for 100K items)


RS2 is optimized for the 95% of use cases where **developer productivity**, **operational reliability**, and **parallel performance** matter more than raw sequential speed. Perfect for microservices, data pipelines, API gateways, and any application requiring robust stream processing.

## Features

- **Functional API**: Chain operations together in a fluent, functional style
- **Backpressure Handling**: Built-in support for handling backpressure with configurable strategies
- **Resource Management**: Safe resource acquisition and release with bracket patterns
- **Error Handling**: Comprehensive error handling with retry policies
- **Parallel Processing**: Process stream elements in parallel with bounded concurrency
- **Time-based Operations**: Throttling, debouncing, sampling, and timeouts
- **Transformations**: Rich set of stream transformation operations
- **Media Streaming**: Robust media streaming with codec, chunk processing, and priority-based delivery ([documentation](MEDIA_STREAMING.md))

## Installation

Add RS2 to your `Cargo.toml`:

```toml
[dependencies]
rs2-stream = "1.0.1"
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
- `eval_map_rs2(f)` - Map elements with an async function
- `chunk_rs2(size)` - Collect elements into chunks of the specified size
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

##### Interleaving Streams

For examples of interleaving streams, see [examples/interleave_example.rs](examples/interleave_example.rs).

```rust
// This example demonstrates:
// - Interleaving multiple streams in round-robin fashion using interleave_rs2()
// - Interleaving streams with different lengths
// - Interleaving streams that emit items at different rates
// - Using interleaving for multiplexing data sources
// See the full code at examples/interleave_example.rs
```

##### Grouping Elements

For examples of grouping elements, see [examples/transformations_grouping.rs](examples/transformations_grouping.rs) and [examples/chunk_rs2_example.rs](examples/chunk_rs2_example.rs).

```rust
// This example demonstrates:
// - Grouping elements by key using group_by_rs2()
// - Grouping elements into chunks using chunks_rs2()
// - Collecting elements into chunks of specified size using chunk_rs2()
// See the full code at examples/transformations_grouping.rs and examples/chunk_rs2_example.rs
```

##### Slicing and Windowing

For examples of slicing operations, see [examples/transformations_slicing.rs](examples/transformations_slicing.rs).

```rust
// This example demonstrates:
// - Taking elements using take_rs2()
// - Skipping elements using skip_rs2()
// See the full code at examples/transformations_slicing.rs
```

##### Sliding Windows

For examples of sliding windows, see [examples/sliding_window_example.rs](examples/sliding_window_example.rs).

```rust
// This example demonstrates:
// - Creating sliding windows of elements using sliding_window_rs2()
// - Using sliding windows for time series analysis
// - Creating phrases from sliding windows of words
// See the full code at examples/sliding_window_example.rs
```

##### Batch Processing

For examples of batch processing, see [examples/batch_process_example.rs](examples/batch_process_example.rs).

```rust
// This example demonstrates:
// - Processing elements in batches using batch_process_rs2()
// - Transforming batches of elements
// - Using batch processing for database operations
// - Combining batch processing with async operations
// See the full code at examples/batch_process_example.rs
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

- `map_parallel_rs2(f)` - Transform elements in parallel using all available CPU cores (automatic concurrency)
- `map_parallel_with_concurrency_rs2(concurrency, f)` - Transform elements in parallel with custom concurrency control
- `par_eval_map_rs2(concurrency, f)` - Process elements in parallel with bounded concurrency, preserving order
- `par_eval_map_unordered_rs2(concurrency, f)` - Process elements in parallel without preserving order
- `par_join_rs2(concurrency)` - Run multiple streams concurrently and combine their outputs
#### When to Use Each Parallel Processing Method

| Method | Best For | When to Use | Avoid When |
|--------|----------|-------------|------------|
| **map_parallel_rs2** | CPU-bound work | • Simple parallelization needs<br>• Balanced workloads (similar processing time)<br>• When optimal concurrency = CPU cores<br>• Mathematical calculations, data parsing | • I/O-bound operations<br>• Memory-intensive tasks<br>• Uneven workloads<br>• When you need fine-tuned concurrency |
| **map_parallel_with_concurrency_rs2** | I/O-bound work with sync functions | • Resource-constrained environments<br>• Custom concurrency needs<br>• Network requests, file operations<br>• Mixed workloads (varying processing times) | • Simple CPU-bound work<br>• When you already have async functions<br>• When automatic concurrency is sufficient |
| **par_eval_map_rs2** | Async operations | • Already have async functions<br>• Need custom concurrency control<br>• Want maximum control/performance<br>• API calls, database operations | • Simple synchronous operations<br>• When order doesn't matter<br>• When simpler methods would suffice |

## **Quick Decision Guide:**

**Start here:** Do you have async functions?
- ✅ **Yes** → Use `par_eval_map_rs2`
- ❌ **No** → Continue below

**Is your work CPU-bound?**
- ✅ **Yes** → Use `map_parallel_rs2`
- ❌ **No (I/O-bound)** → Use `map_parallel_with_concurrency_rs2`

**Need custom concurrency?**
- ✅ **Yes** → Use `map_parallel_with_concurrency_rs2` or `par_eval_map_rs2`
- ❌ **No** → Use `map_parallel_rs2`

### **Concurrency Recommendations:**

| **Workload Type** | **Recommended Concurrency** |
|-------------------|------------------------------|
| **CPU-bound** | `num_cpus::get()` (automatic in `map_parallel_rs2`) |
| **Network I/O** | `50-200` |
| **File I/O** | `4-16` |
| **Database** | `10-50` (respect connection pool) |
| **Memory-heavy** | `1-4` |

**Concurrency Guidelines:**
- **CPU-bound**: Set concurrency to number of CPU cores (`num_cpus::get()`)
- **I/O-bound**: Use higher concurrency (10-100x CPU cores) to maximize throughput
- **Database**: Match your connection pool size (typically 10-50)
- **Network**: Balance between throughput and rate limits (typically 20-200)

### Time-based Operations

- `throttle_rs2(duration)` - Emit at most one element per duration
- `debounce_rs2(duration)` - Emit an element after a quiet period
- `sample_rs2(interval)` - Sample at regular intervals
- `timeout_rs2(duration)` - Add timeout to operations
- `tick_rs(period, item)` - Create a stream that emits a value at a fixed rate

#### Examples

##### Time-based Operations

For examples of time-based operations, see [examples/timeout_operations.rs](examples/timeout_operations.rs) and [examples/tick_rs_example.rs](examples/tick_rs_example.rs).

```rust
// This example demonstrates:
// - Adding timeouts to operations using timeout_rs2()
// - Throttling a stream using throttle_rs2()
// - Debouncing a stream using debounce_rs2()
// - Sampling a stream at regular intervals using sample_rs2()
// - Creating a delayed stream using emit_after()
// - Creating a stream that emits values at a fixed rate using tick_rs()
// See the full code at examples/timeout_operations.rs and examples/tick_rs_example.rs
```

##### Processing Elements in Parallel

For examples of processing elements in parallel, see [examples/processing_elements.rs](examples/processing_elements.rs), and [examples/parallel_mapping.rs](examples/parallel_mapping.rs).

```rust
// This example demonstrates:
// - Processing elements in parallel with bounded concurrency using par_eval_map_rs2()
// - Processing elements in parallel without preserving order using par_eval_map_unordered_rs2()
// - Running multiple streams concurrently using par_join_rs2()
// - Transforming elements in parallel using all available CPU cores with map_parallel_rs2()
// - Transforming elements in parallel with custom concurrency using map_parallel_with_concurrency_rs2()
// See the full code at examples/processing_elements.rs and examples/parallel_mapping.rs
```

### Error Handling

- `recover_rs2(f)` - Recover from errors by applying a function
- `retry_with_policy_rs2(policy, f)` - Retry failed operations with a retry policy
- `on_error_resume_next_rs2()` - Continue processing after errors

### Resource Management

- `bracket_rs2(acquire, use_fn, release)` - Safely acquire and release resources
- `bracket_case(acquire, use_fn, release)` - Safely acquire and release resources with exit case semantics for streams of Result

#### Examples

##### Resource Management with `bracket_rs2` and `bracket_case`

For examples of resource management, see [examples/resource_management_bracket.rs](examples/resource_management_bracket.rs), [examples/bracket_rs_example.rs](examples/bracket_rs_example.rs), and [examples/bracket_case_example.rs](examples/bracket_case_example.rs).

```rust
// This example demonstrates:
// - Safely acquiring and releasing resources using bracket() function
// - Safely acquiring and releasing resources using bracket_rs() extension method
// - Safely acquiring and releasing resources with exit case semantics using bracket_case() extension method
// - Ensuring resources are released even if an error occurs
// See the full code at examples/resource_management_bracket.rs, examples/bracket_rs_example.rs, and examples/bracket_case_example.rs
```

### Backpressure

- `auto_backpressure_rs2()` - Apply automatic backpressure
- `auto_backpressure_with_rs2(config)` - Apply automatic backpressure with custom configuration
- `rate_limit_backpressure_rs2(rate)` - Apply rate-limited backpressure
- `rate_limit_backpressure(capacity)` - Apply back-pressure-aware rate limiting via bounded channel for streams of Result

#### BackpressureConfig

The `BackpressureConfig` struct allows you to customize how backpressure is handled in your streams:

```rust
pub struct BackpressureConfig {
    pub strategy: BackpressureStrategy,
    pub buffer_size: usize,
    pub low_watermark: Option<usize>,  // Resume at this level
    pub high_watermark: Option<usize>, // Pause at this level
}
```

##### Parameters

- **strategy**: Defines the behavior when the buffer reaches capacity:
  - `BackpressureStrategy::DropOldest` - Discards the oldest items in the buffer when it's full
  - `BackpressureStrategy::DropNewest` - Discards the newest incoming items when the buffer is full
  - `BackpressureStrategy::Block` - Blocks the producer until the consumer catches up (default strategy)
  - `BackpressureStrategy::Error` - Fails immediately when the buffer is full

- **buffer_size**: The maximum number of items that can be held in the buffer. Default is 100 items.

- **low_watermark**: The buffer level at which to resume processing after being paused. When the buffer level drops below this threshold, a paused producer can resume sending data. Optional, with a default value of 25 (25% of the default buffer size).

- **high_watermark**: The buffer level at which to pause processing. When the buffer level exceeds this threshold, the producer may be paused to allow the consumer to catch up. Optional, with a default value of 75 (75% of the default buffer size).

##### Default Configuration

The default configuration uses:
- `Block` strategy
- Buffer size of 100 items
- Low watermark of 25 items
- High watermark of 75 items

This creates a system that blocks producers when the buffer is full, pauses when it reaches 75% capacity, and resumes when it drops to 25% capacity.

#### Examples

##### Custom Backpressure

For examples of custom backpressure, see [examples/custom_backpressure.rs](examples/custom_backpressure.rs) and [examples/rate_limit_backpressure_example.rs](examples/rate_limit_backpressure_example.rs).

```rust
// This example demonstrates:
// - Applying automatic backpressure using auto_backpressure_rs2()
// - Configuring custom backpressure strategies using auto_backpressure_with_rs2()
// - Applying rate-limited backpressure using rate_limit_backpressure_rs2()
// - Applying back-pressure-aware rate limiting to streams of Result using rate_limit_backpressure()
// See the full code at examples/custom_backpressure.rs and examples/rate_limit_backpressure_example.rs
```

### Metrics and Monitoring

RS2 provides built-in support for collecting metrics while processing streams, allowing you to monitor throughput, processing time, and other performance metrics.

- `with_metrics_rs2(name)` - Collect metrics while processing the stream

#### Examples

##### Stream Metrics Collection

For examples of collecting metrics from streams, see [examples/with_metrics_example.rs](examples/with_metrics_example.rs).

```rust
// This example demonstrates:
// - Collecting metrics from streams using with_metrics_rs2()
// - Monitoring throughput and processing time
// - Comparing metrics for different stream transformations
// - Collecting metrics for async operations
// See the full code at examples/with_metrics_example.rs
```

### Media Streaming

RS2 includes a comprehensive media streaming system with support for file and live streaming, codec operations, chunk processing, and priority-based delivery.

- **MediaStreamingService**: High-level API for media streaming
- **MediaCodec**: Encoding and decoding of media data
- **ChunkProcessor**: Processing pipeline for media chunks
- **MediaPriorityQueue**: Priority-based delivery of media chunks

#### Examples

##### Basic File Streaming

For examples of streaming media from a file, see [examples/media_streaming/basic_file_streaming.rs](examples/basic_file_streaming.rs).

```rust
// This example demonstrates:
// - Creating a MediaStreamingService
// - Configuring a media stream
// - Starting streaming from a file
// - Processing and displaying the media chunks
// See the full code at examples/media_streaming/basic_file_streaming.rs
```

##### Live Streaming

For examples of setting up a live stream, see [examples/media_streaming/live_streaming.rs](examples/live_streaming.rs).

```rust
// This example demonstrates:
// - Creating a MediaStreamingService for live streaming
// - Configuring a live media stream
// - Starting a live stream
// - Processing and displaying the media chunks
// - Monitoring stream metrics in real-time
// See the full code at examples/media_streaming/live_streaming.rs
```

##### Custom Codec Configuration

For examples of configuring a custom codec, see [examples/media_streaming/custom_codec.rs](examples/custom_codec.rs).

```rust
// This example demonstrates:
// - Creating a custom codec configuration
// - Creating a MediaCodec with the custom configuration
// - Using the codec to encode and decode media data
// - Monitoring codec performance
// See the full code at examples/media_streaming/custom_codec.rs
```

##### Handling Stream Events

For examples of handling media stream events, see [examples/media_streaming/stream_events.rs](examples/stream_events.rs).

```rust
// This example demonstrates:
// - Creating and handling MediaStreamEvent objects
// - Converting events to UserActivity for analytics
// - Processing events in a stream
// - Implementing a simple event handler
// See the full code at examples/media_streaming/stream_events.rs
```

For comprehensive documentation on the media streaming components, see the [Media Streaming README](docs/media_streaming_readme.md).

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

#### Kafka Data Streaming Pipeline Example

For a more complex example that demonstrates a complete data streaming pipeline using Kafka and rs2, see [examples/kafka_data_pipeline.rs](examples/kafka_data_pipeline.rs).

```rust
// This example demonstrates a complex data streaming pipeline using Kafka and rs2:
// - Data Production: Generate sample user activity data and send it to a Kafka topic
// - Data Consumption: Consume the data from Kafka using rs2 streams
// - Data Processing: Process the data using various rs2 transformations
//   - Parsing and validation
//   - Enrichment with additional data
//   - Aggregation and analytics
//   - Filtering and transformation
// - Result Publishing: Send the processed results back to different Kafka topics
// - Parallel Processing: Using par_eval_map_rs2 for efficient processing
// - Backpressure Handling: Automatic backpressure to handle fast producers
// - Error Recovery: Fallback mechanisms for when Kafka is not available
// See the full code at examples/kafka_data_pipeline.rs
```

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

## Pipelines and Schema Validation

RS2 makes it easy to build robust, production-grade streaming pipelines with ergonomic composition and strong data validation guarantees.

### Pipeline Builder

The pipeline builder lets you compose sources, transforms, and sinks in a clear, modular way:

```rust
let pipeline = Pipeline::new()
    .source(my_source)
    .transform(my_transform)
    .sink(my_sink)
    .build();
```

You can branch, window, aggregate, and combine streams with ergonomic combinators. See [examples/kafka_data_pipeline.rs](examples/kafka_data_pipeline.rs) for a real-world, multi-branch pipeline.

### Schema Validation

**Production-grade schema validation** is built in. RS2 provides:
- The `SchemaValidator` trait for pluggable validation (JSON Schema, Avro, Protobuf, custom)
- A `JsonSchemaValidator` for validating JSON data using [JSON Schema](https://json-schema.org/)
- The `.with_schema_validation_rs2(validator)` combinator to filter out invalid items and log errors
- Clear error types: `SchemaError::ValidationFailed`, `SchemaError::ParseError`, etc.

#### Example: Validating JSON in a Pipeline

```rust
use rs2::schema_validation::JsonSchemaValidator;
use serde_json::json;

let schema = json!({
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "value": {"type": "integer"}
    },
    "required": ["id", "value"]
});
let validator = JsonSchemaValidator::new("my-schema", schema);

let validated_stream = raw_stream
    .with_schema_validation_rs2(validator)
    .filter_map(|json| async move { serde_json::from_str::<MyType>(&json).ok() })
    .boxed();
```

See [examples/kafka_data_pipeline.rs](examples/kafka_data_pipeline.rs) for a full production pipeline with schema validation, branching, analytics, and error handling.

**Extensibility:** You can implement your own `SchemaValidator` for Avro, Protobuf, or custom formats. The system is async-friendly and ready for integration with schema registries.

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

### Parallel Performance
RS2 excels at parallel processing with near-linear scaling:

| Concurrency | I/O Scaling | Speedup | CPU Scaling | Speedup |
|-------------|-------------|---------|-------------|---------|
| **1 core** | 2.26s | 1.0x | 478µs | 1.0x |
| **2 cores** | 1.11s | 2.0x | 219µs | 2.2x |
| **4 cores** | 530ms | 4.3x | 209µs | 2.3x |
| **8 cores** | 265ms | 8.5x | 210µs | 2.3x |
| **16 cores** | 134ms | 16.9x | 204µs | 2.3x |

**Scaling Characteristics:**
- **I/O bound**: Near-perfect linear scaling up to 16+ cores
- **CPU bound**: Scales well up to physical core count
- **Mixed workloads**: Automatic optimization based on workload type

## Advanced Analytics (Production-Ready)

RS2 provides robust, production-grade advanced analytics features:

- **Time-based windowed aggregations**: Tumbling and sliding windows with custom time semantics, for real-time stats, metrics, and summaries.
- **Keyed, time-windowed joins**: Join two streams on a key (e.g., user_id) within a time window, for enrichment and correlation.

### Available Methods

- `window_by_time_rs2(config, timestamp_fn)` - Apply time-based windowing to the stream, grouping elements into windows based on their timestamps
- `join_with_time_window_rs2(other, config, timestamp_fn1, timestamp_fn2, join_fn, key_selector)` - Join with another stream using time windows, optionally matching on keys

**Caveat:**
> In time-windowed joins, deduplication is performed by timestamp pairs. If your events have identical timestamps and you require deduplication by other keys, you may need to extend the join logic. Most users will not need to change this, but advanced users can open an issue or PR for more control.

#### Examples

##### Time-based Windowed Aggregations

For examples of time-based windowed aggregations, see [examples/advanced_analytics_example.rs](examples/advanced_analytics_example.rs).

```rust
// This example demonstrates:
// - Creating time-based windows of user events
// - Calculating statistics for each window (event count, unique users, event types)
// - Configuring window size, slide interval, and watermark delay
// See the full code at examples/advanced_analytics_example.rs
```

##### Stream Joins with Time Windows

For examples of joining streams with time windows, see [examples/advanced_analytics_example.rs](examples/advanced_analytics_example.rs).

```rust
// This example demonstrates:
// - Joining user events with user profiles using time windows
// - Enriching events with profile information
// - Configuring time join parameters
// - Optional key-based matching
// See the full code at examples/advanced_analytics_example.rs
```

## Performance Optimization Guide

This section provides guidance on configuring RS2 for optimal performance based on your specific workload characteristics.

### Buffer Configuration

Buffer configurations significantly impact throughput and memory usage. Key parameters include:

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `initial_capacity` | Initial buffer size | Higher values reduce allocations but increase memory usage. Default: 1024 (general) or 8192 (performance) |
| `max_capacity` | Maximum buffer size | Limits memory usage. Default: 1MB |
| `growth_strategy` | How buffers grow | Exponential growth (default 1.5-2.0x) balances allocation frequency and memory usage |

### Backpressure Configuration

Configure backpressure to balance throughput and resource usage:

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `strategy` | How to handle buffer overflow | `Block` (default) for lossless processing; `DropOldest`/`DropNewest` for higher throughput with data loss |
| `buffer_size` | Maximum items in buffer | Larger values increase throughput but use more memory. Default: 100 |
| `low_watermark` | When to resume processing | Lower values reduce stop/start frequency. Default: 25% of buffer_size |
| `high_watermark` | When to pause processing | Higher values increase throughput but risk overflow. Default: 75% of buffer_size |

### File I/O Configuration

Optimize file operations for different workloads:

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `buffer_size` | Size of I/O buffers | Larger values (32KB-128KB) improve throughput for sequential access. Default: 8KB |
| `read_ahead` | Whether to prefetch data | Enable for sequential access; disable for random access |
| `sync_on_write` | Whether to sync after writes | Disable for maximum throughput; enable for durability |
| `compression` | Optional compression | Disable for maximum throughput; enable to reduce I/O at CPU cost |

### Advanced Throughput Techniques

Additional methods to optimize throughput:

| Technique | Description | Performance Impact |
|-----------|-------------|-------------------|
| `prefetch_rs2(n)` | Eagerly evaluate n elements ahead | Improves throughput by 10-30% for I/O-bound workloads |
| `batch_process_rs2(size, fn)` | Process items in batches | Can improve throughput 2-5x for database or network operations |
| `chunk_rs2(size)` | Group items into chunks | Reduces per-item overhead; optimal sizes typically 32-128 |

### Metrics Collection

Enable metrics to identify bottlenecks:

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `enabled` | Whether metrics are collected | Minimal overhead (1-2%) when enabled |
| `sample_rate` | Fraction of operations to measure | Lower values reduce overhead; 0.1 (10%) provides good balance |


## Roadmap / Planned Features

The following features are planned for future releases. If you need them, please open an issue or contribute!

- **Complex Event Processing (CEP)**: Sequence-aware, stateful pattern matching (e.g., fraud detection, sessionization).
- **Work stealing scheduler**: Dynamic, adaptive parallelism for maximum throughput and resource utilization.
- **Deduplicated/sequence-aware joins**: SQL-like, deduped, or "first match" joins for advanced analytics.

---
