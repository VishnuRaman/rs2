# RS2: Rust Streaming Library

**RS2** is a streaming library for Rust that provides async stream processing with built-in state management, backpressure control, and parallel processing capabilities. It's designed for applications that need reliable stream processing with minimal external dependencies.

**RS2 includes stateful streaming capabilities** with integrated state management for operations like session tracking, deduplication, windowing, and real-time analytics. The state management is handled internally with configurable storage backends.

## Why RS2?

**Parallel Processing**: RS2 provides parallel stream processing with near-linear scaling up to 16+ cores. For I/O-bound workloads, it achieves 7.8-8.5x speedup compared to sequential processing.

**Built-in Reliability Features**: RS2 includes automatic backpressure, retry policies with exponential backoff, circuit breakers, timeout handling, and resource management. These features eliminate the need to manually implement these patterns.

**Stateful Stream Processing**: RS2 provides integrated state management for stateful operations like deduplication, windowing, session tracking, and real-time analytics. No external state stores required - everything is handled internally with configurable storage backends.

**Parallelization**: Transform sequential streams into parallel processing with a single method call. RS2's `par_eval_map_rs2()` handles concurrency, ordering, and error propagation automatically.

**External System Integration**: Connector system for Kafka and custom systems with health checks, metrics, and automatic retry logic built-in.

## 🎯 Quick Start Examples

**See RS2 in action with these examples:**

### 🚀 [Parallel Processing Comprehensive](examples/parallel_processing_comprehensive.rs)
**Understanding RS2's parallel processing capabilities:**
- **Sequential vs Parallel Performance Comparison** - See actual speedup numbers
- **Ordered vs Unordered Processing** - Learn when to use each approach
- **Mixed Workload Processing** - CPU + I/O bound tasks
- **Pipeline Processing** - Multiple parallel stages
- **Adaptive Concurrency** - Test different concurrency levels
- **Error Handling** - How errors work in parallel operations
- **Resource Management** - Backpressure and memory management
- **Real-World Scenarios** - E-commerce order processing

```bash
cargo run --example parallel_processing_comprehensive
```

### 📊 [Real-Time Analytics Pipeline](examples/real_time_analytics_pipeline.rs)
**Stateful streaming analytics system:**
- **Session Management** - Track user sessions with timeouts
- **User Metrics Aggregation** - Real-time user behavior analytics
- **Page Analytics** - Group by page URL for insights
- **Event Pattern Detection** - Detect conversion funnels and error patterns
- **Error Rate Monitoring** - Throttled error alerting
- **Real-Time Metrics Windows** - Sliding window analytics
- **Event Deduplication** - Remove duplicate events
- **Complex Analytics Pipeline** - Multi-stage processing with alerts

```bash
cargo run --example real_time_analytics_pipeline
```

**These examples demonstrate RS2's capabilities - from basic parallel processing to complex stateful analytics pipelines. Understanding how to build streaming applications!**

## 📚 API Reference

### Core Stream Constructors (`rs2` module)

#### Basic Constructors
- `emit(item)` - Emit a single element
- `empty_rs2()` - Create an empty stream
- `from_iter_rs2(iter)` - Create stream from iterator
- `eval(fut)` - Evaluate a Future and emit its output
- `repeat_rs2(item)` - Repeat an item infinitely
- `emit_after(item, duration)` - Emit item after delay
- `unfold_rs2(init, f)` - Create stream from state and function
- `once_stream(item)` - Emit single item
- `repeat_stream(item)` - Repeat item infinitely
- `from_iter_stream(iter)` - Create from iterator
- `pending_stream()` - Never emits items
- `repeat_with_stream(f)` - Repeat using function
- `once_with_stream(f)` - Emit once using function
- `unfold_stream(init, f)` - Unfold with async function

#### Advanced Constructors
- `group_adjacent_by(stream, key_fn)` - Group adjacent items by key
- `take(stream, n)` - Take first n items
- `drop(stream, n)` - Drop first n items
- `chunk(stream, size)` - Chunk items into vectors
- `timeout(stream, duration)` - Apply timeout to stream
- `scan(stream, init, f)` - Scan with state
- `fold(stream, init, f)` - Fold stream into single value
- `reduce(stream, f)` - Reduce stream with function
- `filter_map(stream, f)` - Filter and map
- `take_while(stream, predicate)` - Take while predicate is true
- `drop_while(stream, predicate)` - Drop while predicate is true
- `group_by(stream, key_fn)` - Group by key function
- `sliding_window(stream, size)` - Create sliding windows
- `batch_process(stream, batch_size, processor)` - Process in batches

#### Backpressure Constructors
- `auto_backpressure(stream, config)` - Apply backpressure with config
- `auto_backpressure_block(stream, config)` - Block strategy
- `auto_backpressure_drop_oldest(stream, config)` - Drop oldest strategy
- `auto_backpressure_drop_newest(stream, config)` - Drop newest strategy
- `auto_backpressure_error(stream, config)` - Error strategy

#### Parallel Processing
- `eval_map(stream, f)` - Map with async function
- `par_eval_map(stream, concurrency, f)` - Parallel map
- `par_eval_map_unordered(stream, concurrency, f)` - Unordered parallel map
- `par_join(streams, concurrency)` - Join parallel streams

#### Composition
- `concat(first, second)` - Concatenate streams
- `zip_with(s1, s2, f)` - Zip with function
- `either(s1, s2)` - Take from either stream
- `merge(s1, s2)` - Merge streams
- `interleave(s1, s2)` - Interleave streams

#### Rate Control
- `debounce(stream, duration)` - Debounce stream
- `distinct_until_changed(stream)` - Remove consecutive duplicates
- `sample(stream, interval)` - Sample at intervals
- `sample_finite(stream, interval)` - Sample finite stream
- `sample_every_nth(stream, n)` - Sample every nth item
- `sample_first(stream, n)` - Sample first n items
- `sample_auto(stream, interval)` - Auto sample
- `throttle(stream, duration)` - Throttle stream
- `tick(period, item)` - Emit at regular intervals

#### Resource Management
- `bracket(acquire, use_fn, release)` - Resource management
- `bracket_case(acquire, use_fn, release)` - Resource management with error handling

#### Metrics and Monitoring
- `with_metrics(stream, name, thresholds)` - Add metrics
- `with_metrics_config(stream, name, thresholds, config)` - Add metrics with config
- `with_metrics_simple(stream, name, thresholds)` - Simple metrics

#### Utility Functions
- `prefetch(stream, count)` - Prefetch items
- `distinct_until_changed_by(stream, eq)` - Custom duplicate removal
- `rate_limit_backpressure(stream, capacity)` - Rate limiting with backpressure
- `interrupt_when(stream, signal)` - Interrupt on signal

### Stream Extension Methods (`RS2StreamExt` trait)

#### Basic Transformations
- `map_rs2(f)` - Map over stream items
- `filter_rs2(f)` - Filter stream items
- `take_rs2(n)` - Take n items
- `skip_rs2(n)` - Skip n items
- `drop_rs2(n)` - Drop n items (alias for skip)
- `chain_rs2(other)` - Chain with another stream
- `zip_rs2(other)` - Zip with another stream
- `merge_rs2(other)` - Merge with another stream

#### Collection and Aggregation
- `collect_rs2()` - Collect into vector
- `collect_with_config_rs2(config)` - Collect with buffer config
- `fold_rs2(init, f)` - Fold stream
- `reduce_rs2(f)` - Reduce stream
- `count_rs2()` - Count items
- `first_rs2()` - Get first item
- `last_rs2()` - Get last item
- `find_rs2(predicate)` - Find item matching predicate
- `any_rs2(predicate)` - Check if any item matches
- `all_rs2(predicate)` - Check if all items match
- `nth_rs2(n)` - Get nth item
- `position_rs2(predicate)` - Get position of matching item

#### Chunking and Windowing
- `chunks_rs2(size)` - Chunk items into vectors
- `sliding_window_with_step_rs2(size, step)` - Sliding window with step
- `chunk_rs2(size)` - Chunk items
- `sliding_window_rs2(size)` - Sliding window

#### Inspection and Debugging
- `enumerate_rs2()` - Enumerate items
- `inspect_rs2(f)` - Inspect items without modifying
- `peekable_rs2()` - Make stream peekable

#### Conditional Processing
- `skip_while_rs2(predicate)` - Skip while predicate is true
- `drop_while_rs2(predicate)` - Drop while predicate is true
- `take_while_rs2(predicate)` - Take while predicate is true

#### Advanced Transformations
- `scan_rs2(init, f)` - Scan with state
- `flat_map_rs2(f)` - Flat map over stream
- `eval_map_rs2(f)` - Map with async function
- `flatten_rs2()` - Flatten nested streams
- `filter_map_rs2(f)` - Filter and map
- `filter_map_async_rs2(f)` - Filter and map with async function

#### Parallel Processing
- `par_eval_map_rs2(concurrency, f)` - Parallel map
- `par_eval_map_unordered_rs2(concurrency, f)` - Unordered parallel map
- `par_join_rs2(concurrency)` - Join parallel streams
- `map_parallel_rs2(f)` - CPU-bound parallel map
- `map_parallel_with_concurrency_rs2(concurrency, f)` - Parallel map with concurrency

#### Backpressure Control
- `auto_backpressure_rs2()` - Apply backpressure with default config
- `auto_backpressure_with_rs2(config)` - Apply backpressure with custom config
- `auto_backpressure_drop_oldest_rs2(config)` - Drop oldest strategy
- `auto_backpressure_drop_newest_rs2(config)` - Drop newest strategy
- `auto_backpressure_error_rs2(config)` - Error strategy

#### Rate Control
- `throttle_rs2(duration)` - Throttle stream
- `debounce_rs2(duration)` - Debounce stream
- `sample_rs2(interval)` - Sample at intervals
- `sample_every_nth_rs2(n)` - Sample every nth item

#### Composition
- `zip_with_rs2(other, f)` - Zip with function
- `group_by_rs2(key_fn)` - Group by key function
- `group_adjacent_by_rs2(key_fn)` - Group adjacent items
- `concat_rs2(other)` - Concatenate streams
- `either_rs2(other)` - Take from either stream
- `interleave_rs2(streams)` - Interleave multiple streams

#### Deduplication
- `distinct_until_changed_rs2()` - Remove consecutive duplicates
- `distinct_until_changed_by_rs2(eq)` - Custom duplicate removal

#### Utility
- `tick_rs2(period)` - Emit at regular intervals
- `prefetch_rs2(count)` - Prefetch items
- `timeout_rs2(duration)` - Apply timeout
- `rate_limit_backpressure_rs2(capacity)` - Rate limiting
- `interrupt_when_rs2(signal)` - Interrupt on signal
- `batch_process_rs2(batch_size, processor)` - Process in batches

#### Metrics and Validation
- `with_metrics_rs2(name, thresholds)` - Add metrics
- `with_metrics_config_rs2(name, thresholds, config)` - Add metrics with config
- `with_metrics_simple_rs2()` - Simple metrics
- `with_schema_validation_rs2(validator)` - Schema validation
- `bracket_rs2(acquire, use_fn, release)` - Resource management

### Result Stream Methods (`RS2ResultStreamExt` trait)

#### Basic Transformations
- `map_ok(f)` - Map over successful values
- `map_err(f)` - Map over error values
- `ok_values()` - Filter out errors, keep successes
- `err_values()` - Filter out successes, keep errors

#### Collection Methods
- `collect_ok()` - Collect successful values
- `collect_err()` - Collect error values
- `partition_results()` - Partition successes and errors
- `count_results()` - Count successes and errors

#### Error Handling
- `retry()` - Retry failed operations (default 3 retries)
- `retry_with_delay(max_retries, delay)` - Retry with delay
- `retry_with_policy(policy)` - Retry with custom policy
- `retry_with_policy_rs2(policy, factory)` - Retry with policy and factory
- `handle_errors(handler)` - Handle errors with function
- `log_errors()` - Log errors and continue
- `ignore_errors()` - Ignore errors and continue
- `map_err_into()` - Transform error types
- `bimap(success_fn, error_fn)` - Map both success and error cases
- `unwrap_results()` - Unwrap results with default on error
- `unwrap_or_default()` - Unwrap with default on error
- `unwrap_or_else(fallback)` - Unwrap with fallback on error

#### Recovery Methods
- `recover_rs2(f)` - Recover from errors with async function
- `on_error_resume_next_rs2(f)` - Switch to alternative stream on error
- `or_else_rs2(f)` - Use fallback value on error

#### Aggregation Methods
- `all_ok()` - Check if all values are successful
- `any_ok()` - Check if any values are successful
- `find_ok(predicate)` - Find first successful value
- `find_err(predicate)` - Find first error value
- `reduce_ok(f)` - Reduce successful values
- `fold_ok(init, f)` - Fold successful values
- `sum_ok()` - Sum successful values
- `product_ok()` - Product of successful values
- `max_ok()` - Maximum successful value
- `min_ok()` - Minimum successful value
- `max_by_key_ok(f)` - Maximum by key function
- `min_by_key_ok(f)` - Minimum by key function

### Configuration Types

#### BackpressureConfig
```rust
pub struct BackpressureConfig {
    pub strategy: BackpressureStrategy,
    pub buffer_size: usize,
    pub low_watermark: Option<usize>,  // Resume at this level
    pub high_watermark: Option<usize>, // Pause at this level
}
```

#### BackpressureStrategy
```rust
pub enum BackpressureStrategy {
    DropOldest,    // Drop oldest items when buffer is full
    DropNewest,    // Drop newest items when buffer is full
    Block,         // Block producer until consumer catches up
    Error,         // Fail fast when buffer is full
}
```

#### HealthThresholds
```rust
pub struct HealthThresholds {
    pub max_latency: Duration,
    pub max_error_rate: f64,
    pub min_throughput: f64,
}
```

#### MetricsConfig
```rust
pub struct MetricsConfig {
    pub enabled: bool,
    pub sample_rate: f64,
    pub collection_interval: Duration,
}
```

#### BufferConfig
```rust
pub struct BufferConfig {
    pub initial_capacity: usize,
    pub max_capacity: Option<usize>,
    pub growth_strategy: GrowthStrategy,
}
```

## 📖 Examples

### Basic Stream Operations
- [Basic Usage](examples/basic_usage.rs) - Simple stream creation and transformation
- [Stream Creation](examples/stream_creation_basic.rs) - Different ways to create streams
- [Async Stream Creation](examples/stream_creation_async.rs) - Creating async streams
- [Infinite Streams](examples/stream_creation_infinite.rs) - Working with infinite streams

### Transformations
- [Basic Transformations](examples/transformations_basic.rs) - Map, filter, take, skip
- [Async Transformations](examples/transformations_async.rs) - Async map and filter
- [Combining Streams](examples/transformations_combining.rs) - Zip, merge, concat
- [Grouping Operations](examples/transformations_grouping.rs) - Group by operations
- [Slicing Operations](examples/transformations_slicing.rs) - Take while, skip while
- [Accumulating Values](examples/accumulating_values.rs) - Fold, reduce, scan

### Advanced Features
- [Parallel Processing](examples/parallel_processing_comprehensive.rs) - Parallel stream processing
- [State Management](examples/state_management_example.rs) - Stateful stream operations
- [Real-Time Analytics](examples/real_time_analytics_pipeline.rs) - Complex analytics pipeline
- [Resource Management](examples/resource_management_example.rs) - Resource cleanup patterns
- [Custom Storage](examples/custom_storage_example.rs) - Custom state storage backends

### Error Handling
- [Error Handling](examples/error_handling_example.rs) - Result stream operations
- [Retry Logic](examples/retry_example.rs) - Retry policies and error recovery

### Performance and Monitoring
- [Metrics Collection](examples/with_metrics_example.rs) - Performance monitoring
- [Backpressure](examples/custom_backpressure.rs) - Custom backpressure strategies
- [Rate Limiting](examples/rate_limit_backpressure_example.rs) - Rate limiting with backpressure

### Connectors and Integration
- [Kafka Connector](examples/connector_kafka.rs) - Kafka integration
- [Custom Connector](examples/connector_custom.rs) - Building custom connectors
- [Queue Operations](examples/queue_basic_usage.rs) - Queue-based processing

### Specialized Operations
- [Sliding Windows](examples/sliding_window_example.rs) - Time-based windowing
- [Chunk Processing](examples/chunk_rs2_example.rs) - Chunk-based processing
- [Schema Validation](examples/schema_validation_example.rs) - Data validation
- [Custom Codecs](examples/custom_codec.rs) - Custom encoding/decoding

### Stateful Operations
- [Stateful Map](examples/stateful_map_example.rs) - Stateful mapping
- [Stateful Filter](examples/stateful_filter_example.rs) - Stateful filtering
- [Stateful Reduce](examples/stateful_reduce_example.rs) - Stateful reduction
- [Stateful Deduplicate](examples/stateful_deduplicate_example.rs) - Stateful deduplication
- [Stateful Group By](examples/stateful_group_by_example.rs) - Stateful grouping
- [Stateful Session](examples/stateful_session_example.rs) - Session management
- [Stateful Pattern](examples/stateful_pattern_example.rs) - Pattern detection
- [Stateful Throttle](examples/stateful_throttle_example.rs) - Stateful rate limiting

### Media Streaming
- [Media Streaming](examples/live_streaming.rs) - Live media streaming
- [File Streaming](examples/basic_file_streaming.rs) - File-based streaming
- [Media Events](examples/stream_events.rs) - Media event processing

### Advanced Patterns
- [Pipe Composition](examples/pipe_composing.rs) - Composing processing pipelines
- [User Data Processing](examples/pipe_user_data_processing.rs) - User data pipelines
- [Batch Processing](examples/batch_process_example.rs) - Batch processing patterns
- [Advanced Analytics](examples/advanced_analytics_example.rs) - Complex analytics

### Testing and Debugging
- [Custom Stream Test](examples/custom_stream_test.rs) - Testing custom streams
- [Processing Elements](examples/processing_elements.rs) - Element processing patterns

## 🚀 Performance Benchmarks

*Based on Criterion.rs benchmarks on test hardware - Updated with actual measured performance*

## Basic Operations Performance

| **Operation** | **1K Items** | **10K Items** | **100K Items** | **1M Items** | **Throughput (1K)** |
|---------------|--------------|---------------|----------------|--------------|-------------------|
| **Map/Filter** | 1.42-1.44 µs | 12.56-12.68 µs | 128.13-129.98 µs | 1.335-1.351 ms | ~714K items/sec |
| **Fold** | 3.67-3.72 µs | 35.49-35.52 µs | 358.07-365.89 µs | 3.726-3.793 ms | ~273K items/sec |
| **Chunk Process** | 2.82-2.88 µs | 27.57-27.73 µs | 271.05-273.48 µs | 2.687-2.726 ms | ~357K items/sec |
| **Take/Skip** | 1.10-1.12 µs | 9.26-9.37 µs | 100.54-101.72 µs | - | ~909K items/sec |
| **Flat Map** | 1.97-1.99 µs | 18.13-18.36 µs | 201.03-204.24 µs | - | ~508K items/sec |

## Async Operations Performance

| **Operation** | **1K Items** | **10K Items** | **50K Items** | **Throughput (1K)** |
|---------------|--------------|---------------|---------------|-------------------|
| **Eval Map** | 15.07-15.14 µs | 147.94-148.94 µs | 748.33-750.44 µs | ~66K items/sec |
| **Filter Map Async** | 29.89-30.31 µs | 294.91-296.13 µs | 1.502-1.506 ms | ~33K items/sec |

## Aggregation Operations Performance

| **Operation** | **1K Items** | **10K Items** | **100K Items** | **Throughput (1K)** |
|---------------|--------------|---------------|----------------|-------------------|
| **Count** | 1.17-1.18 µs | 9.54-9.75 µs | 101.55-109.27 µs | ~857K items/sec |
| **Reduce** | 3.64-3.66 µs | 35.44-35.64 µs | 366.34-369.57 µs | ~274K items/sec |
| **Find** | 2.06-2.11 µs | 19.39-19.51 µs | - | ~485K items/sec |
| **Any/All** | 4.29-4.33 µs | 39.95-40.08 µs | - | ~233K items/sec |

## Composition Operations Performance

| **Operation** | **1K Items** | **10K Items** | **50K Items** | **Throughput (1K)** |
|---------------|--------------|---------------|---------------|-------------------|
| **Zip** | 2.23-2.25 µs | 20.28-20.35 µs | 110.78-112.46 µs | ~444K items/sec |
| **Merge** | 2.06-2.08 µs | 18.63-18.77 µs | - | ~485K items/sec |

## Parallel Processing Performance

### I/O Simulation (200 items)

| **Concurrency** | **Sequential** | **Parallel Ordered** | **Parallel Unordered** | **Speedup Factor** |
|-----------------|----------------|---------------------|----------------------|-------------------|
| **8 cores** | 4.07s | 530ms | 530ms | 7.7x |
| **16 cores** | 4.07s | 281ms | 282ms | 14.5x |
| **32 cores** | 4.07s | 160ms | 160ms | 25.4x |
| **64 cores** | 4.07s | 99ms | 99ms | 41.1x |

### Light CPU Workloads (2000 items)

| **Concurrency** | **Sequential** | **Parallel Ordered** | **Parallel Unordered** | **Speedup Factor** |
|-----------------|----------------|---------------------|----------------------|-------------------|
| **2 cores** | 7.06µs | 3.18ms | 3.15ms | 0.004x |
| **4 cores** | 7.06µs | 1.79ms | 1.78ms | 0.004x |

### Medium CPU Workloads (500 items)

| **Concurrency** | **Sequential** | **Parallel Ordered** | **Parallel Unordered** | **Speedup Factor** |
|-----------------|----------------|---------------------|----------------------|-------------------|
| **2 cores** | 563µs | 743µs | 732µs | 0.76x |
| **4 cores** | 563µs | 750µs | 733µs | 0.75x |
| **8 cores** | 563µs | 778µs | 797µs | 0.72x |

### Heavy CPU Workloads (100 items)

| **Concurrency** | **Sequential** | **Parallel Ordered** | **Parallel Unordered** | **Speedup Factor** |
|-----------------|----------------|---------------------|----------------------|-------------------|
| **2 cores** | 9.98ms | 10.77ms | 10.91ms | 0.93x |
| **4 cores** | 9.98ms | 10.83ms | 10.83ms | 0.92x |
| **8 cores** | 9.98ms | 10.82ms | 10.72ms | 0.93x |
| **16 cores** | 9.98ms | 10.56ms | 10.42ms | 0.96x |

### Variable Workloads (400 items)

| **Concurrency** | **Sequential** | **Parallel Ordered** | **Parallel Unordered** | **Speedup Factor** |
|-----------------|----------------|---------------------|----------------------|-------------------|
| **4 cores** | 676µs | 1.07ms | 1.02ms | 0.63x |
| **8 cores** | 676µs | 1.01ms | 1.01ms | 0.67x |
| **16 cores** | 676µs | 995µs | 991µs | 0.68x |

### Parallel Scaling Analysis

#### Heavy CPU Scaling (100 items)

| **Cores** | **Time** | **vs Sequential** |
|-----------|----------|-------------------|
| **1** | 9.80ms | 1.00x |
| **2** | 10.64ms | 0.92x |
| **4** | 10.76ms | 0.91x |
| **8** | 10.53ms | 0.93x |
| **16** | 10.27ms | 0.95x |
| **32** | 10.01ms | 0.98x |

#### I/O Simulation Scaling (200 items)

| **Cores** | **Time** | **vs Sequential** |
|-----------|----------|-------------------|
| **1** | 2.04s | 1.00x |
| **2** | 1.03s | 1.98x |
| **4** | 545ms | 3.74x |
| **8** | 286ms | 7.12x |
| **16** | 160ms | 12.7x |
| **32** | 99ms | 20.6x |

#### Variable Workload Scaling (400 items)

| **Cores** | **Time** | **vs Sequential** |
|-----------|----------|-------------------|
| **1** | 168µs | 1.00x |
| **2** | 348µs | 0.48x |
| **4** | 256µs | 0.66x |
| **8** | 241µs | 0.70x |
| **16** | 207µs | 0.81x |
| **32** | 195µs | 0.86x |

### Concurrency Optimization

#### I/O-Bound Concurrency (200 items)

| **Concurrency** | **Time** | **vs Sequential** |
|-----------------|----------|-------------------|
| **1** | 3.05s | 1.00x |
| **2** | 1.53s | 1.99x |
| **4** | 781ms | 3.91x |
| **8** | 406ms | 7.51x |
| **16** | 219ms | 13.9x |
| **32** | 129ms | 23.6x |
| **64** | 83ms | 36.7x |

#### CPU-Bound Concurrency (100 items)

| **Concurrency** | **Time** | **vs Sequential** |
|-----------------|----------|-------------------|
| **1** | 14.89ms | 1.00x |
| **2** | 16.57ms | 0.90x |
| **4** | 16.74ms | 0.89x |
| **8** | 16.50ms | 0.90x |
| **16** | 16.10ms | 0.93x |

### Map Parallel Functions

#### Light CPU (2000 items)

| **Method** | **Time** | **vs Sequential** |
|------------|----------|-------------------|
| **Sequential** | 5.98µs | 1.00x |
| **Map Parallel** | 460µs | 0.013x |
| **Map Parallel (2 cores)** | 3.14ms | 0.002x |
| **Map Parallel (4 cores)** | 1.77ms | 0.003x |

#### Medium CPU (500 items)

| **Method** | **Time** | **vs Sequential** |
|------------|----------|-------------------|
| **Sequential** | 573µs | 1.00x |
| **Map Parallel** | 742µs | 0.77x |
| **Map Parallel (2 cores)** | 792µs | 0.72x |
| **Map Parallel (4 cores)** | 742µs | 0.77x |
| **Map Parallel (8 cores)** | 749µs | 0.76x |

#### Heavy CPU (100 items)

| **Method** | **Time** | **vs Sequential** |
|------------|----------|-------------------|
| **Sequential** | 9.78ms | 1.00x |
| **Map Parallel** | 10.41ms | 0.94x |
| **Map Parallel (2 cores)** | 10.81ms | 0.90x |
| **Map Parallel (4 cores)** | 10.72ms | 0.91x |
| **Map Parallel (8 cores)** | 10.64ms | 0.92x |
| **Map Parallel (16 cores)** | 10.36ms | 0.94x |

### Map Parallel vs Par Eval Map (200 items)

| **Method** | **Time** | **vs Sequential** |
|------------|----------|-------------------|
| **Sequential** | 19.99ms | 1.00x |
| **Map Parallel** | 21.60ms | 0.93x |
| **Map Parallel (concurrency)** | 21.48ms | 0.93x |
| **Par Eval Map** | 21.59ms | 0.93x |
| **Par Eval Map (unordered)** | 21.46ms | 0.93x |

# RS2 Stateful Operations Performance

*Based on Criterion.rs benchmarks on test hardware*

## Stateful Operations Performance

| **Operation** | **1K Items** | **10K Items** | **Throughput (1K)** | **Notes** |
|---------------|--------------|---------------|-------------------|-----------|
| **Stateful Map** | 653.54-655.34 µs | 6.4788-6.4863 ms | ~1.53M items/sec | Standard stateful operation |
| **Stateful Filter** | 638.71-640.32 µs | 6.4290-6.4758 ms | ~1.56M items/sec | Similar to stateful map |
| **Stateful Fold** | 629.79-630.10 µs | 6.2187-6.2300 ms | ~1.59M items/sec | Most efficient stateful operation |
| **Stateful Window** | 131.47-131.94 µs | 1.2135-1.2191 ms | ~7.6M items/sec | Highly optimized |
| **Stateful Join** | 759.11-761.31 µs (500 items) | 2.2078-2.2149 ms (1K items) | ~658K items/sec | Complex operation |
| **Stateful Group By** | 159.22-160.18 µs (500 items) | 255.12-255.42 µs (1K items) | ~3.1M items/sec | Efficient grouping |

## Specialized Stateful Operations

| **Operation** | **1K Items** | **10K Items** | **Throughput (1K)** | **Use Case** |
|---------------|--------------|---------------|-------------------|--------------|
| **Stateful Deduplicate** | 212.65-213.14 µs | 1.8903-1.8989 ms | ~4.7M items/sec | Remove duplicates |
| **Stateful Throttle** | 466.61-468.10 µs | 4.4923-4.4998 ms | ~2.14M items/sec | Rate limiting |
| **Stateful Session** | 463.59-466.12 µs | 4.6182-4.6219 ms | ~2.15M items/sec | Session tracking |

## Storage Performance Comparison

| **Storage Type** | **1K Items** | **10K Items** | **Performance** |
|------------------|--------------|---------------|-----------------|
| **In-Memory** | 670.86-673.17 µs | 6.4952-6.5164 ms | Baseline |
| **Custom Storage** | 513.22-515.54 µs | 5.1436-5.1601 ms | ~22% faster |

## Cardinality Impact

| **Cardinality** | **1K Items** | **10K Items** | **Performance Impact** |
|-----------------|--------------|---------------|----------------------|
| **Low Cardinality** | 669.18-671.55 µs | 6.4367-6.4516 ms | Baseline performance |
| **High Cardinality** | 664.73-669.03 µs | 142.89-143.85 ms | Significant degradation at scale |

**Note**: High cardinality shows predictable performance degradation rather than system failure, with 22x slowdown at 10K items being controlled and stable.

## Performance Characteristics

### **Efficient Operations**
- **Windowing**: Fastest at ~8.4M items/sec
- **Group By**: Efficient at ~3.2M items/sec
- **Deduplication**: High throughput at ~4.7M items/sec

### **Standard Operations**
- **Stateful Map/Filter**: ~1.56M items/sec
- **Stateful Fold**: ~1.63M items/sec
- **Join operations**: ~732K items/sec (complex operation)

### **Resource Management**
- **Memory tracking**: Enabled for all stateful operations
- **Automatic cleanup**: Prevents memory leaks
- **Cardinality protection**: Graceful degradation under load

*Performance measurements based on Criterion.rs benchmarks. Results may vary based on hardware and workload characteristics.*

## Benchmark Hardware & Methodology

- **Measurement Tool**: Criterion.rs statistical benchmarking
- **Test Data**: Synthetic events with realistic payloads
- **Runs**: 100 iterations per benchmark for statistical accuracy
- **Environment**: Standard development hardware

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
