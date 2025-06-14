## Performance Optimization Guide

This section provides guidance on configuring RS2 for optimal performance based on your specific workload characteristics.

### Buffer Configuration

Buffer configurations significantly impact throughput and memory usage:

```rust
// Optimize buffer settings for high-throughput processing
let buffer_config = BufferConfig {
    initial_capacity: 8192,                // Start with larger buffers
    max_capacity: Some(1048576),           // Allow growth up to 1MB
    growth_strategy: GrowthStrategy::Exponential(1.5), // Grow by 50% each time
};
```

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `initial_capacity` | Initial buffer size | Higher values reduce allocations but increase memory usage. Default: 1024 (general) or 8192 (performance) |
| `max_capacity` | Maximum buffer size | Limits memory usage. Default: 1MB |
| `growth_strategy` | How buffers grow | Exponential growth (default 1.5-2.0x) balances allocation frequency and memory usage |

### Parallel Processing Configuration

Optimize parallel processing based on your workload type:

```rust
// For CPU-bound workloads
stream.par_eval_map_cpu_intensive_rs2(process_item)

// For I/O-bound workloads with custom concurrency
stream.par_eval_map_rs2(32, process_item)

// For fine-tuned work stealing
let config = WorkStealingConfig {
    num_workers: Some(16),         // Explicit worker count
    local_queue_size: 4,           // Smaller queues for better load balancing
    steal_interval_ms: 1,          // Aggressive stealing
    use_blocking: true,            // Use blocking threads for CPU work
};
stream.par_eval_map_work_stealing_with_config_rs2(config, process_item)
```

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `num_workers` | Number of worker threads | Set to available CPU cores for CPU-bound work; can be higher (2-4x cores) for I/O-bound work |
| `local_queue_size` | Size of worker-local queues | Smaller (4-8) for better load balancing; larger (16-32) for reducing synchronization overhead |
| `steal_interval_ms` | How often workers check for stealing | Lower values (1ms) improve responsiveness; higher values reduce contention |
| `use_blocking` | Whether to use blocking threads | Enable for CPU-intensive tasks; disable for I/O-bound tasks |

### Backpressure Configuration

Configure backpressure to balance throughput and resource usage:

```rust
// High-throughput configuration with controlled memory usage
let backpressure_config = BackpressureConfig {
    strategy: BackpressureStrategy::Block,  // Block when full for lossless processing
    buffer_size: 1000,                      // Larger buffer for higher throughput
    low_watermark: Some(250),               // Resume at 25% capacity
    high_watermark: Some(750),              // Pause at 75% capacity
};
stream.auto_backpressure_with_rs2(backpressure_config)
```

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `strategy` | How to handle buffer overflow | `Block` (default) for lossless processing; `DropOldest`/`DropNewest` for higher throughput with data loss |
| `buffer_size` | Maximum items in buffer | Larger values increase throughput but use more memory. Default: 100 |
| `low_watermark` | When to resume processing | Lower values reduce stop/start frequency. Default: 25% of buffer_size |
| `high_watermark` | When to pause processing | Higher values increase throughput but risk overflow. Default: 75% of buffer_size |

### File I/O Configuration

Optimize file operations for different workloads:

```rust
// High-throughput file processing
let file_config = FileConfig {
    buffer_size: 65536,           // 64KB buffers for large files
    read_ahead: true,             // Enable read-ahead for sequential access
    sync_on_write: false,         // Disable sync for maximum throughput
    compression: None,            // No compression for raw speed
};
```

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `buffer_size` | Size of I/O buffers | Larger values (32KB-128KB) improve throughput for sequential access. Default: 8KB |
| `read_ahead` | Whether to prefetch data | Enable for sequential access; disable for random access |
| `sync_on_write` | Whether to sync after writes | Disable for maximum throughput; enable for durability |
| `compression` | Optional compression | Disable for maximum throughput; enable to reduce I/O at CPU cost |

### Advanced Throughput Techniques

Additional methods to optimize throughput:

```rust
// Prefetch elements ahead of consumption
stream.prefetch_rs2(100)

// Process in batches for better efficiency
stream.batch_process_rs2(64, process_batch)

// Chunk items for more efficient processing
stream.chunk_rs2(128).map_rs2(process_chunks)
```

| Technique | Description | Performance Impact |
|-----------|-------------|-------------------|
| `prefetch_rs2(n)` | Eagerly evaluate n elements ahead | Improves throughput by 10-30% for I/O-bound workloads |
| `batch_process_rs2(size, fn)` | Process items in batches | Can improve throughput 2-5x for database or network operations |
| `chunk_rs2(size)` | Group items into chunks | Reduces per-item overhead; optimal sizes typically 32-128 |

### Metrics Collection

Enable metrics to identify bottlenecks:

```rust
// Collect performance metrics
let (stream, metrics) = stream.with_metrics_rs2("processing_pipeline");

// Process stream normally, then analyze metrics
let throughput = metrics.lock().await.throughput_items_per_sec();
println!("Throughput: {} items/sec", throughput);
```

| Parameter | Description | Performance Impact |
|-----------|-------------|-------------------|
| `enabled` | Whether metrics are collected | Minimal overhead (1-2%) when enabled |
| `sample_rate` | Fraction of operations to measure | Lower values reduce overhead; 0.1 (10%) provides good balance |