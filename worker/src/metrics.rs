// worker/src/metrics.rs
// Métricas del worker: CPU, memoria, tareas, latencias

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LATENCY_WINDOW_SIZE: usize = 100;

/// Métricas del sistema
#[derive(Debug, Clone, serde::Serialize)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f64,
    pub memory_used_mb: f64,
    pub memory_total_mb: f64,
    pub memory_percent: f64,
}

/// Métricas de tareas
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskMetrics {
    pub total_executed: u64,
    pub total_succeeded: u64,
    pub total_failed: u64,
    pub active_tasks: u32,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub records_processed: u64,
    pub throughput_records_per_sec: f64,
}

/// Métricas completas del worker
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

/// Colector de métricas
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

    /// Registrar inicio de tarea
    pub fn task_started(&mut self) {
        self.active_tasks += 1;
    }

    /// Registrar fin de tarea
    pub fn task_completed(&mut self, success: bool, latency: Duration, records: u64) {
        self.active_tasks = self.active_tasks.saturating_sub(1);
        self.total_executed += 1;
        
        if success {
            self.total_succeeded += 1;
            self.records_processed += records;
        } else {
            self.total_failed += 1;
        }

        // Guardar latencia
        let latency_ms = latency.as_secs_f64() * 1000.0;
        if self.latencies.len() >= LATENCY_WINDOW_SIZE {
            self.latencies.pop_front();
        }
        self.latencies.push_back(latency_ms);

        // Actualizar throughput cada segundo
        let elapsed = self.last_throughput_time.elapsed();
        if elapsed >= Duration::from_secs(1) {
            let records_diff = self.records_processed - self.last_records_count;
            self.current_throughput = records_diff as f64 / elapsed.as_secs_f64();
            self.last_throughput_time = Instant::now();
            self.last_records_count = self.records_processed;
        }
    }

    /// Obtener métricas actuales
    pub fn get_metrics(&self, cache_hits: u64, cache_misses: u64, cache_memory_mb: f64) -> WorkerMetrics {
        let system = get_system_metrics();
        
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

    fn calculate_latency_stats(&self) -> (f64, f64, f64) {
        if self.latencies.is_empty() {
            return (0.0, 0.0, 0.0);
        }

        let mut sorted: Vec<f64> = self.latencies.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;
        let p50 = sorted[sorted.len() / 2];
        let p99_idx = ((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1);
        let p99 = sorted[p99_idx];

        (avg, p50, p99)
    }
}

/// Obtener métricas del sistema (CPU, memoria)
fn get_system_metrics() -> SystemMetrics {
    // Leer /proc/meminfo para memoria
    let (memory_used_mb, memory_total_mb) = read_memory_info();
    
    // Leer /proc/stat para CPU (aproximado)
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

fn read_memory_info() -> (f64, f64) {
    let content = match std::fs::read_to_string("/proc/meminfo") {
        Ok(c) => c,
        Err(_) => return (0.0, 0.0),
    };

    let mut total_kb: u64 = 0;
    let mut available_kb: u64 = 0;

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

fn parse_meminfo_value(line: &str) -> u64 {
    line.split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn read_cpu_usage() -> f64 {
    // Lectura simplificada de CPU
    // En un sistema real, necesitaríamos dos lecturas con intervalo
    let content = match std::fs::read_to_string("/proc/stat") {
        Ok(c) => c,
        Err(_) => return 0.0,
    };

    let first_line = match content.lines().next() {
        Some(l) => l,
        None => return 0.0,
    };

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

pub type SharedMetrics = Arc<Mutex<MetricsCollector>>;

pub fn new_shared_metrics(worker_id: String) -> SharedMetrics {
    Arc::new(Mutex::new(MetricsCollector::new(worker_id)))
}