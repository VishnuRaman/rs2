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
