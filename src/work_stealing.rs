
use crate::rs2::RS2Stream;
use async_stream::stream;
use std::future::Future;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use tokio::sync::{mpsc, Mutex};
use futures_util::stream::StreamExt;

/// Configuration for work-stealing parallel processing
#[derive(Debug, Clone)]
pub struct WorkStealingConfig {
    /// Number of worker threads. If None, uses available_parallelism()
    pub num_workers: Option<usize>,
    /// Maximum size of each worker's local queue before sharing with global queue
    pub local_queue_size: usize,
    /// Interval in milliseconds for checking for work to steal
    pub steal_interval_ms: u64,
    /// Whether to use spawn_blocking for CPU-intensive tasks
    pub use_blocking: bool,
}

impl Default for WorkStealingConfig {
    fn default() -> Self {
        Self {
            num_workers: None,
            local_queue_size: 16,
            steal_interval_ms: 1,
            use_blocking: true,
        }
    }
}

// Helper struct to avoid Self in async context
struct WorkStealingProcessor;

impl WorkStealingProcessor {
    async fn process_task<I, O, F, Fut>(
        item: I,
        processor: F,
        use_blocking: bool,
    ) -> O
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        if use_blocking {
            // Use spawn_blocking for CPU-intensive tasks
            let result = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(processor(item))
            }).await;

            result.unwrap_or_else(|e| panic!("Task panicked: {:?}", e))
        } else {
            // Regular async processing
            processor(item).await
        }
    }
}

/// Work-stealing parallel evaluation function
pub fn par_eval_map_work_stealing<I, O, F, Fut>(
    stream: RS2Stream<I>,
    config: WorkStealingConfig,
    processor: F,
) -> RS2Stream<O>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    let num_workers = config.num_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    let output_stream = stream! {
        // Task structure to maintain order
        #[derive(Debug)]
        struct Task<T> {
            item: T,
            index: usize,
        }

        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Task<I>>();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<(usize, O)>();

        // Track completion state - FIXED: Use atomic for total count
        let total_tasks = Arc::new(AtomicUsize::new(0));
        let input_done = Arc::new(AtomicBool::new(false));

        // Spawn input feeder task
        let input_feeder = {
            let input_tx = input_tx.clone();
            let total_tasks = total_tasks.clone();
            let input_done = input_done.clone();
            tokio::spawn(async move {
                let mut stream = stream;
                let mut index = 0;

                while let Some(item) = stream.next().await {
                    let task = Task { item, index };
                    if input_tx.send(task).is_err() {
                        break;
                    }
                    index += 1;
                }

                // FIXED: Set total count AFTER all tasks are sent
                total_tasks.store(index, Ordering::SeqCst);
                input_done.store(true, Ordering::SeqCst);
                drop(input_tx); // Signal end of input
            })
        };

        // Create work-stealing queues
        let global_queue: Arc<Mutex<VecDeque<Task<I>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let worker_queues: Vec<Arc<Mutex<VecDeque<Task<I>>>>> = (0..num_workers)
            .map(|_| Arc::new(Mutex::new(VecDeque::new())))
            .collect();

        // Spawn distributor task
        let distributor = {
            let global_queue = global_queue.clone();
            let worker_queues = worker_queues.clone();
            let config = config.clone();
            tokio::spawn(async move {
                let mut worker_index = 0;

                while let Some(task) = input_rx.recv().await {
                    // Try to put in worker queue first, fallback to global
                    if !worker_queues.is_empty() {
                        let target_queue = &worker_queues[worker_index % worker_queues.len()];
                        let mut queue = target_queue.lock().await;
                        if queue.len() < config.local_queue_size {
                            queue.push_back(task);
                            worker_index += 1;
                            continue;
                        }
                    }

                    // Fallback to global queue
                    let mut global = global_queue.lock().await;
                    global.push_back(task);
                }
            })
        };

        // Track active workers
        let active_workers = Arc::new(AtomicUsize::new(num_workers));

        // Spawn worker tasks
        let workers: Vec<_> = (0..num_workers).map(|worker_id| {
            let global_queue = global_queue.clone();
            let worker_queues = worker_queues.clone();
            let output_tx = output_tx.clone();
            let processor = processor.clone();
            let use_blocking = config.use_blocking;
            let steal_interval = std::time::Duration::from_millis(config.steal_interval_ms);
            let input_done = input_done.clone();
            let active_workers = active_workers.clone();

            tokio::spawn(async move {
                loop {
                    let mut found_work = false;

                    // 1. Try own queue first
                    if let Some(task) = {
                        if worker_id < worker_queues.len() {
                            let mut queue = worker_queues[worker_id].lock().await;
                            queue.pop_front()
                        } else {
                            None
                        }
                    } {
                        let result = WorkStealingProcessor::process_task(task.item, processor.clone(), use_blocking).await;
                        if output_tx.send((task.index, result)).is_err() {
                            break;
                        }
                        found_work = true;
                        continue;
                    }

                    // 2. Try global queue
                    if let Some(task) = {
                        let mut queue = global_queue.lock().await;
                        queue.pop_front()
                    } {
                        let result = WorkStealingProcessor::process_task(task.item, processor.clone(), use_blocking).await;
                        if output_tx.send((task.index, result)).is_err() {
                            break;
                        }
                        found_work = true;
                        continue;
                    }

                    // 3. Try stealing from other workers
                    for (other_id, other_queue) in worker_queues.iter().enumerate() {
                        if other_id == worker_id {
                            continue;
                        }

                        if let Some(task) = {
                            let mut queue = other_queue.lock().await;
                            queue.pop_back() // Steal from back
                        } {
                            let result = WorkStealingProcessor::process_task(task.item, processor.clone(), use_blocking).await;
                            if output_tx.send((task.index, result)).is_err() {
                                break;
                            }
                            found_work = true;
                            break;
                        }
                    }

                    if found_work {
                        continue;
                    }

                    // No work found
                    // FIXED: Better exit condition
                    if input_done.load(Ordering::SeqCst) {
                        // Double-check all queues are empty
                        let global_empty = {
                            let queue = global_queue.lock().await;
                            queue.is_empty()
                        };

                        let workers_empty = {
                            let mut all_empty = true;
                            for queue in &worker_queues {
                                let q = queue.lock().await;
                                if !q.is_empty() {
                                    all_empty = false;
                                    break;
                                }
                            }
                            all_empty
                        };

                        // FIXED: Check if all workers are done
                        let remaining_workers = active_workers.load(Ordering::SeqCst);

                        // If this is the last worker and all queues are empty, we're done
                        if (remaining_workers <= 1 || (global_empty && workers_empty)) {
                            println!("Worker {} exiting. Remaining workers: {}, global_empty: {}, workers_empty: {}", 
                                     worker_id, remaining_workers, global_empty, workers_empty);
                            break;
                        }
                    }

                    // No work found, sleep briefly but not too long
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }

                // Signal this worker is done
                active_workers.fetch_sub(1, Ordering::SeqCst);
            })
        }).collect();

        // Drop the distributor's output sender
        drop(output_tx);

        // FIXED: Collect results in order with proper completion detection
        let mut results: HashMap<usize, O> = HashMap::new();
        let mut next_index = 0;
        let mut received_count = 0;

        println!("Starting main loop with {} workers", active_workers.load(Ordering::SeqCst));
        println!("Total tasks: {}", total_tasks.load(Ordering::SeqCst));

        // Add a timeout to prevent hanging
        let timeout_duration = std::time::Duration::from_secs(25);
        let start_time = std::time::Instant::now();

        while let Some((index, result)) = output_rx.recv().await {
            results.insert(index, result);
            received_count += 1;

            // Debug print every 10 items
            if received_count % 10 == 0 {
                println!("Received {} items, next_index: {}, active_workers: {}", 
                         received_count, next_index, active_workers.load(Ordering::SeqCst));
            }

            // Yield results in order
            while let Some(result) = results.remove(&next_index) {
                yield result;
                next_index += 1;
            }

            // FIXED: Check completion properly
            if input_done.load(Ordering::SeqCst) {
                let total = total_tasks.load(Ordering::SeqCst);
                println!("Input done. received: {}, next_index: {}, total: {}, active_workers: {}", 
                         received_count, next_index, total, active_workers.load(Ordering::SeqCst));

                if received_count >= total && next_index >= total {
                    println!("All tasks processed. Breaking out of loop.");
                    break;
                }

                // Check if all workers have exited but we haven't received all results
                if active_workers.load(Ordering::SeqCst) == 0 {
                    println!("All workers exited. received: {}, total: {}. Breaking out of loop.", 
                             received_count, total);
                    break;
                }
            }

            // Check for timeout
            if start_time.elapsed() > timeout_duration {
                println!("Timeout reached. Breaking out of loop.");
                break;
            }
        }

        println!("Main loop completed. received: {}, next_index: {}", received_count, next_index);

        // Clean up
        let _ = input_feeder.await;
        let _ = distributor.await;
        for worker in workers {
            let _ = worker.await;
        }
    };

    output_stream.boxed()
}

/// Work-stealing parallel evaluation with default configuration
pub fn par_eval_map_work_stealing_default<I, O, F, Fut>(
    stream: RS2Stream<I>,
    processor: F,
) -> RS2Stream<O>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    par_eval_map_work_stealing(stream, WorkStealingConfig::default(), processor)
}

/// Work-stealing parallel evaluation optimized for CPU-intensive tasks
pub fn par_eval_map_cpu_intensive<I, O, F, Fut>(
    stream: RS2Stream<I>,
    processor: F,
) -> RS2Stream<O>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    let config = WorkStealingConfig {
        num_workers: Some(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)),
        local_queue_size: 4, // Smaller queue for better load balancing
        steal_interval_ms: 1, // Aggressive stealing
        use_blocking: true, // Use blocking threads for CPU work
    };

    par_eval_map_work_stealing(stream, config, processor)
}

/// Extension trait for work-stealing operations on streams
pub trait WorkStealingExt<I> {
    /// Process elements in parallel using work stealing with default configuration
    fn par_eval_map_work_stealing_rs2<O, F, Fut>(self, processor: F) -> RS2Stream<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send + 'static;

    /// Process elements in parallel using work stealing with custom configuration
    fn par_eval_map_work_stealing_with_config_rs2<O, F, Fut>(
        self,
        config: WorkStealingConfig,
        processor: F,
    ) -> RS2Stream<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send + 'static;

    /// Process CPU-intensive tasks in parallel using work stealing
    fn par_eval_map_cpu_intensive_rs2<O, F, Fut>(self, processor: F) -> RS2Stream<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send + 'static;
}

impl<I> WorkStealingExt<I> for RS2Stream<I> {
    fn par_eval_map_work_stealing_rs2<O, F, Fut>(self, processor: F) -> RS2Stream<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        par_eval_map_work_stealing_default(self, processor)
    }

    fn par_eval_map_work_stealing_with_config_rs2<O, F, Fut>(
        self,
        config: WorkStealingConfig,
        processor: F,
    ) -> RS2Stream<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        par_eval_map_work_stealing(self, config, processor)
    }

    fn par_eval_map_cpu_intensive_rs2<O, F, Fut>(self, processor: F) -> RS2Stream<O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        par_eval_map_cpu_intensive(self, processor)
    }
}
