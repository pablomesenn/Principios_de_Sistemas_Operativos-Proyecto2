mod cache;
mod metrics;
mod operators;

use cache::{new_shared_cache, SharedCache};
use common::{Heartbeat, Logger, Partition, Task, TaskResult, WorkerRegistration};
use metrics::{new_shared_metrics, SharedMetrics};
use reqwest::Client;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

// Config
const HEARTBEAT_INTERVAL_SECS: u64 = 2;
const POLL_INTERVAL_MS: u64 = 100;
const POLL_INTERVAL_NO_TASK_MS: u64 = 200;
const POLL_INTERVAL_ERROR_SECS: u64 = 1;
const METRICS_REPORT_INTERVAL_SECS: u64 = 30;

// Per-task execution limits
const MAX_TASK_TIME_SECS: u64 = 5; // max allowed time per task

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log = Logger::new("WORKER");

    // Ensure base data directories exist (/tmp/minispark etc.)
    operators::ensure_data_dir();

    // Number of async executor loops per worker (logical "threads")
    let worker_threads: usize = std::env::var("WORKER_THREADS")
        .unwrap_or_else(|_| "9".into())
        .parse()
        .unwrap_or(9);

    // Shared HTTP client used to communicate with master
    let client = Client::new();

    // Master URL (configurable, defaults to localhost)
    let master_url = std::env::var("MASTER_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());

    // Worker port (base port, metrics server uses port+1000)
    let worker_port: u16 = std::env::var("WORKER_PORT")
        .unwrap_or_else(|_| "9000".into())
        .parse()
        .unwrap_or(9000);

    // Optional failure simulation probability (0–100)
    let fail_probability: u32 = std::env::var("FAIL_PROBABILITY")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap_or(0);

    // Startup log
    log.emit(
        log.info("Mini-Spark Worker iniciando")
            .field("master", &master_url)
            .field("port", worker_port)
            .field("threads", worker_threads.to_string()),
    );

    if fail_probability > 0 {
        log.emit(
            log.warn("Test mode activated")
                .field("fail_probability", format!("{}%", fail_probability)),
        );
    }

    // Initialize cache
    // Shared in-memory + spill-to-disk partition cache
    let cache = new_shared_cache();

    // Register worker instance in master; master will assign an ID
    let reg = WorkerRegistration {
        id: Uuid::nil(),
        host: "127.0.0.1".into(),
        port: worker_port,
    };

    let res = client
        .post(format!("{}/api/v1/workers/register", master_url))
        .json(&reg)
        .send()
        .await?;

    // Receive confirmed registration (with assigned worker_id)
    let reg_res: WorkerRegistration = res.json().await?;
    let worker_id = reg_res.id;

    log.emit(
        log.info("Worker registrado")
            .field("worker_id", worker_id.to_string()),
    );

    // Metrics collector for this worker instance
    let metrics = new_shared_metrics(worker_id.to_string());

    // Tracks number of tasks currently executing on this process
    let active_tasks = Arc::new(AtomicU32::new(0));

    // Heartbeats
    // Periodically send heartbeats to master with current active task count
    let hb_client = client.clone();
    let hb_master_url = master_url.clone();
    let hb_active = active_tasks.clone();
    tokio::spawn(async move {
        let log = Logger::new("HEARTBEAT");
        loop {
            let hb = Heartbeat {
                worker_id,
                active_tasks: hb_active.load(Ordering::Relaxed),
            };

            let resp = hb_client
                .post(format!("{}/api/v1/workers/heartbeat", hb_master_url))
                .json(&hb)
                .send()
                .await;

            if let Err(e) = resp {
                log.emit(
                    log.error("Error enviando heartbeat")
                        .field("error", e.to_string()),
                );
            }

            tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
        }
    });

    // Periodically log system and task metrics to stdout
    let m2 = metrics.clone();
    let c2 = cache.clone();
    tokio::spawn(async move {
        let log = Logger::new("METRICS");
        loop {
            tokio::time::sleep(Duration::from_secs(METRICS_REPORT_INTERVAL_SECS)).await;

            // Fetch cache + worker metrics snapshot
            let cache_stats = c2.lock().unwrap().stats();
            let worker_metrics = m2.lock().unwrap().get_metrics(
                cache_stats.hits,
                cache_stats.misses,
                cache_stats.current_memory_mb,
            );

            log.emit(
                log.info("Métricas del worker")
                    .field("uptime_secs", worker_metrics.uptime_secs)
                    .field("cpu", worker_metrics.system.cpu_usage_percent.to_string())
                    .field("mem", worker_metrics.system.memory_percent.to_string())
                    .field("tasks_executed", worker_metrics.tasks.total_executed)
                    .field("tasks_succeeded", worker_metrics.tasks.total_succeeded)
                    .field("cache_hits", worker_metrics.cache_hits)
                    .field("cache_misses", worker_metrics.cache_misses)
                    .field("Number of working threads", worker_threads),
            );
        }
    });

    // Spawn a lightweight HTTP server exposing this worker's metrics
    let m3 = metrics.clone();
    let c3 = cache.clone();
    tokio::spawn(async move {
        use axum::{routing::get, Json, Router};

        let app = Router::new().route(
            "/metrics",
            get(move || {
                let m = m3.clone();
                let c = c3.clone();
                async move {
                    let cache_stats = c.lock().unwrap().stats();
                    let data = m.lock().unwrap().get_metrics(
                        cache_stats.hits,
                        cache_stats.misses,
                        cache_stats.current_memory_mb,
                    );
                    Json(data)
                }
            }),
        );

        // Expose metrics on worker_port + 1000 to avoid collision with main worker port
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], worker_port + 1000));
        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            let _ = axum::serve(listener, app).await;
        }
    });

    // Spawn N independent executor loops that poll master for tasks
    for _ in 0..worker_threads {
        let worker_id = worker_id.clone();
        let master_url = master_url.clone();
        let client = client.clone();
        let cache = cache.clone();
        let metrics = metrics.clone();
        let active = active_tasks.clone();

        tokio::spawn(async move {
            executor_loop(
                worker_id,
                master_url,
                client,
                cache,
                metrics,
                active,
                fail_probability,
            )
            .await;
        });
    }

    // Keep worker process alive (actual work happens in spawned tasks)
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

// Main per-"thread" executor loop: poll master, run tasks, report completion
async fn executor_loop(
    worker_id: Uuid,
    master_url: String,
    client: Client,
    cache: SharedCache,
    metrics: SharedMetrics,
    active_tasks: Arc<AtomicU32>,
    fail_probability: u32,
) {
    let log = Logger::new("EXECUTOR");

    loop {
        // Ask the master for a task assigned to this worker
        let resp = client
            .get(format!(
                "{}/api/v1/workers/task?worker_id={}",
                master_url, worker_id
            ))
            .send()
            .await;

        match resp {
            // Received a task to execute
            Ok(res) if res.status().is_success() => {
                let task: Task = match res.json().await {
                    Ok(t) => t,
                    // If JSON decoding fails, back off a bit and retry
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                        continue;
                    }
                };

                // Track active tasks and notify metrics collector
                active_tasks.fetch_add(1, Ordering::Relaxed);
                metrics.lock().unwrap().task_started();

                // Measure task execution time
                let start = Instant::now();
                let mut result =
                    execute_task_with_failure_simulation(&task, fail_probability, &cache);
                let elapsed = start.elapsed();

                // Enforce max per-task time limit
                if elapsed > Duration::from_secs(MAX_TASK_TIME_SECS) {
                    result.success = false;
                    result.error = Some(format!(
                        "Tarea superó el límite de tiempo de {}s (duró {:.3}s)",
                        MAX_TASK_TIME_SECS,
                        elapsed.as_secs_f64(),
                    ));
                    result.records_processed = 0;
                }

                // Report completion back to master (fire-and-forget)
                let _ = client
                    .post(format!("{}/api/v1/workers/task/complete", master_url))
                    .json(&result)
                    .send()
                    .await;

                // Decrement active task count and update metrics
                active_tasks.fetch_sub(1, Ordering::Relaxed);
                metrics.lock().unwrap().task_completed(
                    result.success,
                    elapsed,
                    result.records_processed,
                );
            }

            // No tasks available for now
            Ok(res) if res.status() == reqwest::StatusCode::NO_CONTENT => {
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_NO_TASK_MS)).await;
            }

            // Any other HTTP error or network error: short backoff
            Ok(_) | Err(_) => {
                tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_ERROR_SECS)).await;
            }
        }
    }
}

// Wrap real task execution with optional failure simulation
fn execute_task_with_failure_simulation(
    task: &Task,
    fail_probability: u32,
    cache: &SharedCache,
) -> TaskResult {
    if fail_probability > 0 {
        // Very simple pseudo-random generator using current time nanoseconds
        let random: u32 = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos())
            % 100;

        // Simulated failure if random < fail_probability (percentage)
        if random < fail_probability {
            return TaskResult {
                task_id: task.id,
                job_id: task.job_id,
                attempt: task.attempt,
                success: false,
                error: Some("Fallo simulado para testing".to_string()),
                output_path: None,
                records_processed: 0,
                shuffle_outputs: vec![],
            };
        }
    }

    // Execute the real operator
    execute_task(task, cache)
}

// Execute one logical task: load input, run operator, write output and prepare TaskResult
fn execute_task(task: &Task, cache: &SharedCache) -> TaskResult {
    // Default output path for non-shuffle_write operators
    let output_path =
        operators::output_path(&task.job_id.to_string(), &task.node_id, task.partition_id);

    match operators::execute_operator(task, cache) {
        Ok(result) => {
            // Cache partition result under job:node:partition key
            let cache_key = format!("{}:{}:{}", task.job_id, task.node_id, task.partition_id);
            cache
                .lock()
                .unwrap()
                .put(cache_key, result.partition.clone());

            // For most operators, persist to disk; shuffle_write handles its own persistence
            if task.op != "shuffle_write" {
                if let Err(e) = operators::write_partition(&output_path, &result.partition) {
                    return TaskResult {
                        task_id: task.id,
                        job_id: task.job_id,
                        attempt: task.attempt,
                        success: false,
                        error: Some(e),
                        output_path: None,
                        records_processed: 0,
                        shuffle_outputs: vec![],
                    };
                }
            }

            // Successful task result
            TaskResult {
                task_id: task.id,
                job_id: task.job_id,
                attempt: task.attempt,
                success: true,
                error: None,
                output_path: Some(output_path),
                records_processed: result.records_processed,
                shuffle_outputs: result.shuffle_outputs,
            }
        }
        // Operator execution failed (logical or I/O error)
        Err(e) => TaskResult {
            task_id: task.id,
            job_id: task.job_id,
            attempt: task.attempt,
            success: false,
            error: Some(e),
            output_path: None,
            records_processed: 0,
            shuffle_outputs: vec![],
        },
    }
}
