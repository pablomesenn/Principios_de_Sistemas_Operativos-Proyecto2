// Worker metrics: CPU, memory, tasks, latencies

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LATENCY_WINDOW_SIZE: usize = 100;

/// System metrics (host-level)
#[derive(Debug, Clone, serde::Serialize)]
pub struct SystemMetrics {
    /// Approximate CPU usage percentage (0–100)
    pub cpu_usage_percent: f64,
    /// Used memory in megabytes
    pub memory_used_mb: f64,
    /// Total memory in megabytes
    pub memory_total_mb: f64,
    /// Percentage of memory used (used / total * 100)
    pub memory_percent: f64,
}

/// Task-related metrics (per worker)
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskMetrics {
    /// Total tasks that have finished (success + failure)
    pub total_executed: u64,
    /// Total tasks that completed successfully
    pub total_succeeded: u64,
    /// Total tasks that failed
    pub total_failed: u64,
    /// Number of tasks currently running on this worker
    pub active_tasks: u32,
    /// Average latency in milliseconds (over a rolling window)
    pub avg_latency_ms: f64,
    /// 50th percentile latency (median) in milliseconds
    pub p50_latency_ms: f64,
    /// 99th percentile latency in milliseconds
    pub p99_latency_ms: f64,
    /// Total number of records processed by this worker
    pub records_processed: u64,
    /// Current throughput: records processed per second
    pub throughput_records_per_sec: f64,
}

/// Full worker metrics snapshot (system + tasks + cache)
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerMetrics {
    pub worker_id: String,
    pub uptime_secs: u64,
    pub system: SystemMetrics,
    pub tasks: TaskMetrics,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_memory_mb: f64,
}

/// Metrics collector for a single worker instance.
/// Tracks counters, latencies and throughput.
pub struct MetricsCollector {
    worker_id: String,
    start_time: Instant,
    total_executed: u64,
    total_succeeded: u64,
    total_failed: u64,
    active_tasks: u32,
    records_processed: u64,
    latencies: VecDeque<f64>,
    last_throughput_time: Instant,
    last_records_count: u64,
    current_throughput: f64,
}

impl MetricsCollector {
    /// Create a new metrics collector for a worker with the given id
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id,
            start_time: Instant::now(),
            total_executed: 0,
            total_succeeded: 0,
            total_failed: 0,
            active_tasks: 0,
            records_processed: 0,
            latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
            last_throughput_time: Instant::now(),
            last_records_count: 0,
            current_throughput: 0.0,
        }
    }

    /// Register the start of a task
    pub fn task_started(&mut self) {
        self.active_tasks += 1;
    }

    /// Register the completion of a task
    ///
    /// - `success`: whether the task finished successfully
    /// - `latency`: duration of the task execution
    /// - `records`: number of records processed by this task
    pub fn task_completed(&mut self, success: bool, latency: Duration, records: u64) {
        // Decrement active task count safely (avoid underflow)
        self.active_tasks = self.active_tasks.saturating_sub(1);
        self.total_executed += 1;

        // Update success / failure counters and records processed
        if success {
            self.total_succeeded += 1;
            self.records_processed += records;
        } else {
            self.total_failed += 1;
        }

        // Store latency (ms) in sliding window
        let latency_ms = latency.as_secs_f64() * 1000.0;
        if self.latencies.len() >= LATENCY_WINDOW_SIZE {
            self.latencies.pop_front();
        }
        self.latencies.push_back(latency_ms);

        // Update throughput every ~1 second
        let elapsed = self.last_throughput_time.elapsed();
        if elapsed >= Duration::from_secs(1) {
            let records_diff = self.records_processed - self.last_records_count;
            self.current_throughput = records_diff as f64 / elapsed.as_secs_f64();
            self.last_throughput_time = Instant::now();
            self.last_records_count = self.records_processed;
        }
    }

    /// Get a full metrics snapshot for this worker
    pub fn get_metrics(
        &self,
        cache_hits: u64,
        cache_misses: u64,
        cache_memory_mb: f64,
    ) -> WorkerMetrics {
        // Capture system metrics (CPU and memory) at this instant
        let system = get_system_metrics();

        // Compute latency statistics from the sliding window
        let (avg, p50, p99) = self.calculate_latency_stats();

        WorkerMetrics {
            worker_id: self.worker_id.clone(),
            uptime_secs: self.start_time.elapsed().as_secs(),
            system,
            tasks: TaskMetrics {
                total_executed: self.total_executed,
                total_succeeded: self.total_succeeded,
                total_failed: self.total_failed,
                active_tasks: self.active_tasks,
                avg_latency_ms: avg,
                p50_latency_ms: p50,
                p99_latency_ms: p99,
                records_processed: self.records_processed,
                throughput_records_per_sec: self.current_throughput,
            },
            cache_hits,
            cache_misses,
            cache_memory_mb,
        }
    }

    /// Calculate average, P50 and P99 latencies from the window
    fn calculate_latency_stats(&self) -> (f64, f64, f64) {
        if self.latencies.is_empty() {
            return (0.0, 0.0, 0.0);
        }

        // Sort latencies to compute percentiles
        let mut sorted: Vec<f64> = self.latencies.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Average latency
        let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;

        // 50th percentile (median)
        let p50 = sorted[sorted.len() / 2];

        // 99th percentile (clamped to last index)
        let p99_idx = ((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1);
        let p99 = sorted[p99_idx];

        (avg, p50, p99)
    }
}

/// Get system metrics (CPU, memory) from /proc on Linux
fn get_system_metrics() -> SystemMetrics {
    // Read /proc/meminfo to estimate memory usage
    let (memory_used_mb, memory_total_mb) = read_memory_info();

    // Read /proc/stat to approximate CPU usage
    let cpu_usage = read_cpu_usage();

    let memory_percent = if memory_total_mb > 0.0 {
        (memory_used_mb / memory_total_mb) * 100.0
    } else {
        0.0
    };

    SystemMetrics {
        cpu_usage_percent: cpu_usage,
        memory_used_mb,
        memory_total_mb,
        memory_percent,
    }
}

/// Read memory information from /proc/meminfo (Linux)
///
/// Returns (used_mb, total_mb)
fn read_memory_info() -> (f64, f64) {
    let content = match std::fs::read_to_string("/proc/meminfo") {
        Ok(c) => c,
        Err(_) => return (0.0, 0.0),
    };

    let mut total_kb: u64 = 0;
    let mut available_kb: u64 = 0;

    // Parse total and available memory from meminfo lines
    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            total_kb = parse_meminfo_value(line);
        } else if line.starts_with("MemAvailable:") {
            available_kb = parse_meminfo_value(line);
        }
    }

    let total_mb = total_kb as f64 / 1024.0;
    let used_mb = (total_kb - available_kb) as f64 / 1024.0;

    (used_mb, total_mb)
}

/// Parse a numeric value from a line in /proc/meminfo (in kB)
fn parse_meminfo_value(line: &str) -> u64 {
    line.split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Approximate CPU usage by reading /proc/stat once
fn read_cpu_usage() -> f64 {
    // Simplified CPU usage reading
    let content = match std::fs::read_to_string("/proc/stat") {
        Ok(c) => c,
        Err(_) => return 0.0,
    };

    let first_line = match content.lines().next() {
        Some(l) => l,
        None => return 0.0,
    };

    // Fields: user, nice, system, idle, ...
    let values: Vec<u64> = first_line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();

    if values.len() < 4 {
        return 0.0;
    }

    let user = values[0];
    let nice = values[1];
    let system = values[2];
    let idle = values[3];

    let total = user + nice + system + idle;
    let active = user + nice + system;

    if total == 0 {
        0.0
    } else {
        (active as f64 / total as f64) * 100.0
    }
}

/// Shared, thread-safe metrics handle used by the worker
pub type SharedMetrics = Arc<Mutex<MetricsCollector>>;

/// Helper to create a new shared metrics collector
pub fn new_shared_metrics(worker_id: String) -> SharedMetrics {
    Arc::new(Mutex::new(MetricsCollector::new(worker_id)))
}
