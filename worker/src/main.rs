// worker/src/main.rs

mod cache;
mod metrics;
mod operators;

use cache::{new_shared_cache, SharedCache};
use common::{Heartbeat, Logger, Task, TaskResult, WorkerRegistration};
use metrics::{new_shared_metrics, SharedMetrics};
use reqwest::Client;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

// Configuración
const HEARTBEAT_INTERVAL_SECS: u64 = 2;
const POLL_INTERVAL_MS: u64 = 100;
const POLL_INTERVAL_NO_TASK_MS: u64 = 200;
const POLL_INTERVAL_ERROR_SECS: u64 = 1;
const METRICS_REPORT_INTERVAL_SECS: u64 = 30;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log = Logger::new("WORKER");

    operators::ensure_data_dir();

    // Número de hilos configurables
    let worker_threads: usize = std::env::var("WORKER_THREADS")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap_or(4);

    let client = Client::new();
    let master_url = std::env::var("MASTER_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let worker_port: u16 = std::env::var("WORKER_PORT")
        .unwrap_or_else(|_| "9000".into())
        .parse()
        .unwrap_or(9000);

    let fail_probability: u32 = std::env::var("FAIL_PROBABILITY")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap_or(0);

    log.emit(
        log.info("Mini-Spark Worker iniciando")
            .field("master", &master_url)
            .field("port", worker_port)
            .field("threads", worker_threads.to_string()),
    );

    if fail_probability > 0 {
        log.emit(
            log.warn("Modo test activado")
                .field("fail_probability", format!("{}%", fail_probability)),
        );
    }

    // Inicializar cache
    let cache = new_shared_cache();

    // Registro en el master
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

    let reg_res: WorkerRegistration = res.json().await?;
    let worker_id = reg_res.id;

    log.emit(
        log.info("Worker registrado")
            .field("worker_id", worker_id.to_string()),
    );

    // Inicializar métricas
    let metrics = new_shared_metrics(worker_id.to_string());
    let active_tasks = Arc::new(AtomicU32::new(0));

    // Heartbeats
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

    // Reporte periódico de métricas
    let m2 = metrics.clone();
    let c2 = cache.clone();
    tokio::spawn(async move {
        let log = Logger::new("METRICS");
        loop {
            tokio::time::sleep(Duration::from_secs(METRICS_REPORT_INTERVAL_SECS)).await;

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

    // Server para /metrics
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

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], worker_port + 1000));
        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            let _ = axum::serve(listener, app).await;
        }
    });

    // Lanzar pool de hilos asincrónicos ejecutores
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

    // Mantener vivo el worker
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

// Loop ejecutor por hilo
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
        // Poll por tarea
        let resp = client
            .get(format!(
                "{}/api/v1/workers/task?worker_id={}",
                master_url, worker_id
            ))
            .send()
            .await;

        match resp {
            Ok(res) if res.status().is_success() => {
                let task: Task = match res.json().await {
                    Ok(t) => t,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                        continue;
                    }
                };

                active_tasks.fetch_add(1, Ordering::Relaxed);
                metrics.lock().unwrap().task_started();

                let start = Instant::now();
                let result = execute_task_with_failure_simulation(&task, fail_probability, &cache);
                let elapsed = start.elapsed();

                let _ = client
                    .post(format!("{}/api/v1/workers/task/complete", master_url))
                    .json(&result)
                    .send()
                    .await;

                active_tasks.fetch_sub(1, Ordering::Relaxed);
                metrics.lock().unwrap().task_completed(
                    result.success,
                    elapsed,
                    result.records_processed,
                );
            }

            Ok(res) if res.status() == reqwest::StatusCode::NO_CONTENT => {
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_NO_TASK_MS)).await;
            }

            Ok(_) | Err(_) => {
                tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_ERROR_SECS)).await;
            }
        }
    }
}

fn execute_task_with_failure_simulation(
    task: &Task,
    fail_probability: u32,
    cache: &SharedCache,
) -> TaskResult {
    if fail_probability > 0 {
        let random: u32 = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos())
            % 100;

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

    execute_task(task, cache)
}

fn execute_task(task: &Task, cache: &SharedCache) -> TaskResult {
    let output_path =
        operators::output_path(&task.job_id.to_string(), &task.node_id, task.partition_id);

    match operators::execute_operator(task, cache) {
        Ok(result) => {
            let cache_key = format!("{}:{}:{}", task.job_id, task.node_id, task.partition_id);
            cache
                .lock()
                .unwrap()
                .put(cache_key, result.partition.clone());

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
