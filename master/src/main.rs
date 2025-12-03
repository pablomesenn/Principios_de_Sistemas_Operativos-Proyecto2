// master/src/main.rs
// Con Round-Robin y Shuffle automático

mod job_metrics;
mod persistence;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use common::{
    Dag, Heartbeat, JobInfo, JobMetrics, JobRequest, JobStatus, Logger, Task, TaskResult,
    TaskStatus, WorkerInfo, WorkerRegistration, WorkerStatus,
};
use job_metrics::{new_shared_job_metrics, SharedJobMetrics};
use persistence::PersistenceManager;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

// ============ Configuración ============
const MAX_TASK_ATTEMPTS: u32 = 3;
const HEARTBEAT_TIMEOUT_SECS: u64 = 10;
const MONITOR_INTERVAL_SECS: u64 = 5;
const RETRY_INTERVAL_SECS: u64 = 2;
const PERSISTENCE_INTERVAL_SECS: u64 = 10;

// Operadores que requieren shuffle (redistribución por clave)
const SHUFFLE_REQUIRED_OPS: &[&str] = &["reduce_by_key"];

// ============ Planificador Round-Robin ============
struct Scheduler {
    next_worker_index: AtomicUsize,
    worker_order: Mutex<Vec<Uuid>>,
    max_load_diff: u32,
}

impl Scheduler {
    fn new() -> Self {
        Self {
            next_worker_index: AtomicUsize::new(0),
            worker_order: Mutex::new(Vec::new()),
            max_load_diff: 2,
        }
    }

    fn register_worker(&self, worker_id: Uuid) {
        let mut order = self.worker_order.lock().unwrap();
        if !order.contains(&worker_id) {
            order.push(worker_id);
        }
    }

    fn can_worker_receive_task(&self, worker_id: Uuid, workers: &HashMap<Uuid, WorkerInfo>) -> bool {
        let worker = match workers.get(&worker_id) {
            Some(w) if w.status == WorkerStatus::Up => w,
            _ => return false,
        };

        let min_load = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Up)
            .map(|w| w.active_tasks)
            .min()
            .unwrap_or(0);

        worker.active_tasks <= min_load + self.max_load_diff
    }

    fn get_stats(&self, workers: &HashMap<Uuid, WorkerInfo>) -> SchedulerStats {
        let order = self.worker_order.lock().unwrap();
        
        let active_count = order
            .iter()
            .filter(|id| workers.get(*id).map(|w| w.status == WorkerStatus::Up).unwrap_or(false))
            .count();

        let total_load: u32 = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Up)
            .map(|w| w.active_tasks)
            .sum();

        SchedulerStats {
            total_workers: order.len(),
            active_workers: active_count,
            total_load,
            next_index: self.next_worker_index.load(Ordering::SeqCst),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct SchedulerStats {
    total_workers: usize,
    active_workers: usize,
    total_load: u32,
    next_index: usize,
}

// ============ Estado de la Aplicación ============
#[derive(Clone)]
struct AppState {
    workers: Arc<Mutex<HashMap<Uuid, WorkerInfo>>>,
    jobs: Arc<Mutex<HashMap<Uuid, JobInfo>>>,
    task_queue: Arc<Mutex<VecDeque<Task>>>,
    running_tasks: Arc<Mutex<HashMap<Uuid, Task>>>,
    completed_tasks: Arc<Mutex<HashMap<Uuid, TaskResult>>>,
    task_outputs: Arc<Mutex<HashMap<(Uuid, String, u32), String>>>,
    failed_tasks: Arc<Mutex<Vec<Task>>>,
    completed_nodes: Arc<Mutex<HashMap<Uuid, HashSet<String>>>>,
    completed_attempts: Arc<Mutex<HashMap<(Uuid, String, u32), u32>>>,
    failure_counts: Arc<Mutex<HashMap<Uuid, u32>>>,
    persistence: PersistenceManager,
    job_metrics: SharedJobMetrics,
    scheduler: Arc<Scheduler>,
}

impl AppState {
    fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            task_queue: Arc::new(Mutex::new(VecDeque::new())),
            running_tasks: Arc::new(Mutex::new(HashMap::new())),
            completed_tasks: Arc::new(Mutex::new(HashMap::new())),
            task_outputs: Arc::new(Mutex::new(HashMap::new())),
            failed_tasks: Arc::new(Mutex::new(Vec::new())),
            completed_nodes: Arc::new(Mutex::new(HashMap::new())),
            completed_attempts: Arc::new(Mutex::new(HashMap::new())),
            failure_counts: Arc::new(Mutex::new(HashMap::new())),
            persistence: PersistenceManager::new(),
            job_metrics: new_shared_job_metrics(),
            scheduler: Arc::new(Scheduler::new()),
        }
    }
}

#[tokio::main]
async fn main() {
    let log = Logger::new("MASTER");

    log.emit(
        log.info("Mini-Spark Master iniciando")
            .field("max_attempts", MAX_TASK_ATTEMPTS)
            .field("heartbeat_timeout", HEARTBEAT_TIMEOUT_SECS)
            .field("scheduler", "round-robin + load-aware")
            .field("shuffle", "auto-insert"),
    );

    let state = AppState::new();

    let monitor_state = state.clone();
    tokio::spawn(async move { monitor_workers(monitor_state).await });

    let retry_state = state.clone();
    tokio::spawn(async move { retry_failed_tasks(retry_state).await });

    let persist_state = state.clone();
    tokio::spawn(async move { periodic_persistence(persist_state).await });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/workers/register", post(register_worker))
        .route("/api/v1/workers/heartbeat", post(heartbeat))
        .route("/api/v1/workers/task", get(get_task_for_worker))
        .route("/api/v1/workers/task/complete", post(complete_task))
        .route("/api/v1/jobs", post(submit_job))
        .route("/api/v1/jobs/:id", get(get_job))
        .route("/api/v1/jobs/:id/results", get(get_job_results))
        .route("/api/v1/jobs/:id/metrics", get(get_job_metrics))
        .route("/api/v1/jobs/:id/stages", get(get_job_stages))
        .route("/api/v1/metrics/failures", get(get_failure_metrics))
        .route("/api/v1/metrics/jobs", get(get_all_job_metrics))
        .route("/api/v1/metrics/system", get(get_system_metrics))
        .route("/api/v1/metrics/scheduler", get(get_scheduler_metrics))
        .route("/api/v1/state", get(get_persisted_state))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    log.emit(log.info("Master escuchando").field("addr", addr.to_string()));

    let listener = tokio::net::TcpListener::bind(addr).await.expect("No se pudo bindear");
    axum::serve(listener, app).await.expect("Error en servidor");
}

async fn health() -> &'static str { "OK" }

async fn register_worker(
    State(state): State<AppState>,
    Json(mut reg): Json<WorkerRegistration>,
) -> Json<WorkerRegistration> {
    let log = Logger::new("WORKER");
    if reg.id.is_nil() { reg.id = Uuid::new_v4(); }

    let worker_info = WorkerInfo {
        registration: reg.clone(),
        status: WorkerStatus::Up,
        active_tasks: 0,
        last_heartbeat: current_timestamp(),
    };

    state.workers.lock().unwrap().insert(reg.id, worker_info);
    state.scheduler.register_worker(reg.id);

    log.emit(log.info("Worker registrado")
        .field("worker_id", reg.id.to_string())
        .field("host", &reg.host)
        .field("port", reg.port));

    Json(reg)
}

async fn heartbeat(State(state): State<AppState>, Json(hb): Json<Heartbeat>) -> StatusCode {
    let mut workers = state.workers.lock().unwrap();
    if let Some(worker) = workers.get_mut(&hb.worker_id) {
        let was_down = worker.status == WorkerStatus::Down;
        worker.last_heartbeat = current_timestamp();
        worker.active_tasks = hb.active_tasks;
        worker.status = WorkerStatus::Up;

        if was_down {
            Logger::new("WORKER").emit(
                Logger::new("WORKER").info("Worker recuperado")
                    .field("worker_id", hb.worker_id.to_string()));
        }
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn get_task_for_worker(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Task>, StatusCode> {
    let worker_id = params.get("worker_id")
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    {
        let workers = state.workers.lock().unwrap();
        match workers.get(&worker_id) {
            Some(w) if w.status == WorkerStatus::Up => {}
            _ => return Err(StatusCode::FORBIDDEN),
        }
    }

    {
        let workers = state.workers.lock().unwrap();
        if !state.scheduler.can_worker_receive_task(worker_id, &workers) {
            return Err(StatusCode::NO_CONTENT);
        }
    }

    let mut queue = state.task_queue.lock().unwrap();
    let completed_nodes = state.completed_nodes.lock().unwrap();
    let completed_attempts = state.completed_attempts.lock().unwrap();

    let ready_idx = queue.iter().position(|task| {
        let key = (task.job_id, task.node_id.clone(), task.partition_id);
        if completed_attempts.contains_key(&key) { return false; }
        is_task_ready(task, &completed_nodes)
    });

    drop(completed_nodes);
    drop(completed_attempts);

    if let Some(idx) = ready_idx {
        let mut task = queue.remove(idx).unwrap();
        task.status = TaskStatus::Assigned;
        task.assigned_worker = Some(worker_id);

        state.running_tasks.lock().unwrap().insert(task.id, task.clone());

        if let Some(w) = state.workers.lock().unwrap().get_mut(&worker_id) {
            w.active_tasks += 1;
        }

        Logger::new("SCHEDULER").emit(
            Logger::new("SCHEDULER").info("Tarea asignada")
                .field("task_id", task.id.to_string())
                .field("op", &task.op)
                .field("partition", task.partition_id)
                .field("worker", worker_id.to_string()));

        Ok(Json(task))
    } else {
        Err(StatusCode::NO_CONTENT)
    }
}

async fn complete_task(State(state): State<AppState>, Json(result): Json<TaskResult>) -> StatusCode {
    let log = Logger::new("TASK");
    let task_info = state.running_tasks.lock().unwrap().remove(&result.task_id);

    if let Some(task) = task_info {
        if let Some(worker_id) = task.assigned_worker {
            if let Some(w) = state.workers.lock().unwrap().get_mut(&worker_id) {
                w.active_tasks = w.active_tasks.saturating_sub(1);
            }
        }

        let task_key = (task.job_id, task.node_id.clone(), task.partition_id);
        
        {
            let completed_attempts = state.completed_attempts.lock().unwrap();
            if let Some(&completed_attempt) = completed_attempts.get(&task_key) {
                if result.attempt <= completed_attempt { return StatusCode::OK; }
            }
        }

        if result.success {
            state.completed_attempts.lock().unwrap().insert(task_key, result.attempt);
            state.completed_tasks.lock().unwrap().insert(result.task_id, result.clone());

            if let Some(ref path) = result.output_path {
                state.task_outputs.lock().unwrap().insert(
                    (task.job_id, task.node_id.clone(), task.partition_id), path.clone());
            }

            state.job_metrics.lock().unwrap().task_completed(task.job_id, &task.node_id, result.records_processed);
            check_node_completion(&state, &task);
            update_dependent_tasks(&state, &task, &result);

            log.emit(log.info("Tarea completada")
                .field("task_id", result.task_id.to_string())
                .field("op", &task.op)
                .field("records", result.records_processed));
        } else {
            *state.failure_counts.lock().unwrap().entry(task.job_id).or_insert(0) += 1;
            state.job_metrics.lock().unwrap().task_failed(task.job_id);

            if task.attempt < MAX_TASK_ATTEMPTS {
                let mut retry_task = task.clone();
                retry_task.id = Uuid::new_v4();
                retry_task.attempt += 1;
                retry_task.status = TaskStatus::Pending;
                retry_task.assigned_worker = None;
                state.failed_tasks.lock().unwrap().push(retry_task);

                log.emit(log.warn("Tarea fallida, reintentando")
                    .field("task_id", result.task_id.to_string())
                    .field("attempt", task.attempt)
                    .field("error", result.error.as_deref().unwrap_or("unknown")));
            } else {
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&task.job_id) {
                    job.status = JobStatus::Failed(format!("Tarea '{}' falló después de {} intentos", task.node_id, MAX_TASK_ATTEMPTS));
                    state.persistence.save_job(job);
                }
            }
        }

        update_job_progress(&state, task.job_id);
    }

    StatusCode::OK
}

async fn submit_job(State(state): State<AppState>, Json(req): Json<JobRequest>) -> Json<JobInfo> {
    let log = Logger::new("JOB");
    let job_id = Uuid::new_v4();

    let job = JobInfo {
        id: job_id,
        request: req.clone(),
        status: JobStatus::Running,
        progress: 0.0,
    };

    state.jobs.lock().unwrap().insert(job_id, job.clone());
    state.completed_nodes.lock().unwrap().insert(job_id, HashSet::new());
    state.failure_counts.lock().unwrap().insert(job_id, 0);

    // Generar tareas con shuffle automático
    let (tasks, stages) = generate_tasks_with_shuffle(&req.dag, job_id, req.parallelism);
    let task_count = tasks.len();

    state.job_metrics.lock().unwrap().register_job(job_id, req.name.clone(), stages, req.parallelism);
    state.persistence.save_job(&job);

    let mut queue = state.task_queue.lock().unwrap();
    for task in tasks {
        queue.push_back(task);
    }

    log.emit(log.info("Job iniciado")
        .field("job_id", job_id.to_string())
        .field("name", &req.name)
        .field("tasks", task_count)
        .field("parallelism", req.parallelism)
        .field("shuffle", "auto"));

    Json(job)
}

async fn get_job(State(state): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<JobInfo>, StatusCode> {
    state.jobs.lock().unwrap().get(&id).cloned().map(Json).ok_or(StatusCode::NOT_FOUND)
}

async fn get_job_results(State(state): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<Vec<TaskResult>>, StatusCode> {
    let completed = state.completed_tasks.lock().unwrap();
    let results: Vec<TaskResult> = completed.values().filter(|r| r.job_id == id && r.success).cloned().collect();
    if results.is_empty() { Err(StatusCode::NOT_FOUND) } else { Ok(Json(results)) }
}

async fn get_job_metrics(State(state): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<JobMetrics>, StatusCode> {
    state.job_metrics.lock().unwrap().get_metrics(id).map(Json).ok_or(StatusCode::NOT_FOUND)
}

async fn get_job_stages(State(state): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<Vec<job_metrics::StageStats>>, StatusCode> {
    state.job_metrics.lock().unwrap().get_stage_stats(id).map(Json).ok_or(StatusCode::NOT_FOUND)
}

async fn get_failure_metrics(State(state): State<AppState>) -> Json<HashMap<String, serde_json::Value>> {
    let failure_counts = state.failure_counts.lock().unwrap();
    let workers = state.workers.lock().unwrap();

    let mut metrics = HashMap::new();
    let job_failures: HashMap<String, u32> = failure_counts.iter().map(|(k, v)| (k.to_string(), *v)).collect();
    metrics.insert("job_failures".to_string(), serde_json::json!(job_failures));

    let worker_status: HashMap<String, String> = workers.iter().map(|(id, w)| (id.to_string(), format!("{:?}", w.status))).collect();
    metrics.insert("worker_status".to_string(), serde_json::json!(worker_status));
    metrics.insert("workers_down".to_string(), serde_json::json!(workers.values().filter(|w| w.status == WorkerStatus::Down).count()));

    Json(metrics)
}

async fn get_all_job_metrics(State(state): State<AppState>) -> Json<Vec<JobMetrics>> {
    Json(state.job_metrics.lock().unwrap().get_all_metrics())
}

async fn get_system_metrics(State(state): State<AppState>) -> Json<HashMap<String, serde_json::Value>> {
    let workers = state.workers.lock().unwrap();
    let jobs = state.jobs.lock().unwrap();
    let queue = state.task_queue.lock().unwrap();
    let running = state.running_tasks.lock().unwrap();

    let mut metrics = HashMap::new();
    metrics.insert("workers_total".to_string(), serde_json::json!(workers.len()));
    metrics.insert("workers_up".to_string(), serde_json::json!(workers.values().filter(|w| w.status == WorkerStatus::Up).count()));
    metrics.insert("jobs_total".to_string(), serde_json::json!(jobs.len()));
    metrics.insert("jobs_running".to_string(), serde_json::json!(jobs.values().filter(|j| j.status == JobStatus::Running).count()));
    metrics.insert("tasks_queued".to_string(), serde_json::json!(queue.len()));
    metrics.insert("tasks_running".to_string(), serde_json::json!(running.len()));

    let worker_loads: HashMap<String, u32> = workers.iter().map(|(id, w)| (id.to_string(), w.active_tasks)).collect();
    metrics.insert("worker_loads".to_string(), serde_json::json!(worker_loads));

    Json(metrics)
}

async fn get_scheduler_metrics(State(state): State<AppState>) -> Json<SchedulerStats> {
    let workers = state.workers.lock().unwrap();
    Json(state.scheduler.get_stats(&workers))
}

async fn get_persisted_state(State(state): State<AppState>) -> Json<persistence::PersistedState> {
    Json(state.persistence.get_state())
}

// ============ Generación de tareas con Shuffle automático ============

fn generate_tasks_with_shuffle(dag: &Dag, job_id: Uuid, parallelism: u32) -> (Vec<Task>, Vec<String>) {
    let log = Logger::new("DAG");
    let mut tasks = Vec::new();
    let mut all_stages = Vec::new();

    // Construir mapa de dependencias
    let mut deps: HashMap<&str, Vec<&str>> = HashMap::new();
    for (from, to) in &dag.edges {
        deps.entry(to.as_str()).or_default().push(from.as_str());
    }

    for node in &dag.nodes {
        let num_partitions = node.partitions.unwrap_or(parallelism);
        let requires_shuffle = SHUFFLE_REQUIRED_OPS.contains(&node.op.as_str());
        let has_dependencies = deps.get(node.id.as_str()).map(|d| !d.is_empty()).unwrap_or(false);

        if requires_shuffle && has_dependencies {
            // Insertar shuffle_write para cada padre
            let parents: Vec<&str> = deps.get(node.id.as_str()).cloned().unwrap_or_default();

            for parent in &parents {
                if node.join_with.as_deref() == Some(*parent) { continue; }

                let shuffle_write_id = format!("{}_shuffle_write", parent);
                all_stages.push(shuffle_write_id.clone());

                log.emit(log.info("Insertando shuffle").field("from", *parent).field("to", &node.id));

                for partition_id in 0..num_partitions {
                    tasks.push(Task {
                        id: Uuid::new_v4(),
                        job_id,
                        attempt: 0,
                        node_id: shuffle_write_id.clone(),
                        op: "shuffle_write".to_string(),
                        partition_id,
                        total_partitions: num_partitions,
                        input_path: None,
                        input_partitions: vec![format!("/tmp/minispark/{}_{}_p{}.json", job_id, parent, partition_id)],
                        fn_name: None,
                        key: node.key.clone(),
                        status: TaskStatus::Pending,
                        is_shuffle_read: false,
                        join_partitions: vec![],
                        assigned_worker: None,
                    });
                }
            }

            // Crear tarea del operador con shuffle_read
            all_stages.push(node.id.clone());

            for partition_id in 0..num_partitions {
                tasks.push(Task {
                    id: Uuid::new_v4(),
                    job_id,
                    attempt: 0,
                    node_id: node.id.clone(),
                    op: node.op.clone(),
                    partition_id,
                    total_partitions: num_partitions,
                    input_path: node.path.clone(),
                    input_partitions: vec![],
                    fn_name: node.fn_name.clone(),
                    key: node.key.clone(),
                    status: TaskStatus::Pending,
                    is_shuffle_read: true,
                    join_partitions: node.join_with.as_ref().map(|jn| {
                        (0..num_partitions).map(|p| format!("/tmp/minispark/{}_{}_p{}.json", job_id, jn, p)).collect()
                    }).unwrap_or_default(),
                    assigned_worker: None,
                });
            }
        } else {
            // Nodo normal sin shuffle
            all_stages.push(node.id.clone());

            for partition_id in 0..num_partitions {
                let input_partitions: Vec<String> = deps.get(node.id.as_str())
                    .map(|parents| parents.iter()
                        .filter(|p| **p != node.join_with.as_deref().unwrap_or(""))
                        .map(|parent| format!("/tmp/minispark/{}_{}_p{}.json", job_id, parent, partition_id))
                        .collect())
                    .unwrap_or_default();

                let join_partitions: Vec<String> = node.join_with.as_ref()
                    .map(|jn| (0..num_partitions).map(|p| format!("/tmp/minispark/{}_{}_p{}.json", job_id, jn, p)).collect())
                    .unwrap_or_default();

                tasks.push(Task {
                    id: Uuid::new_v4(),
                    job_id,
                    attempt: 0,
                    node_id: node.id.clone(),
                    op: node.op.clone(),
                    partition_id,
                    total_partitions: num_partitions,
                    input_path: node.path.clone(),
                    input_partitions,
                    fn_name: node.fn_name.clone(),
                    key: node.key.clone(),
                    status: TaskStatus::Pending,
                    is_shuffle_read: false,
                    join_partitions,
                    assigned_worker: None,
                });
            }
        }
    }

    log.emit(log.info("DAG procesado")
        .field("tasks", tasks.len())
        .field("stages", all_stages.len())
        .field("shuffle_stages", all_stages.iter().filter(|s| s.contains("shuffle")).count()));

    (tasks, all_stages)
}

// ============ Funciones auxiliares ============

fn current_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn is_task_ready(task: &Task, completed_nodes: &HashMap<Uuid, HashSet<String>>) -> bool {
    if task.input_partitions.is_empty() && task.join_partitions.is_empty() && !task.is_shuffle_read {
        return true;
    }

    if task.is_shuffle_read {
        let job_completed = match completed_nodes.get(&task.job_id) {
            Some(nodes) => nodes,
            None => return false,
        };
        return job_completed.iter().any(|node| node.contains("_shuffle_write"));
    }

    let job_completed = match completed_nodes.get(&task.job_id) {
        Some(nodes) => nodes,
        None => return task.input_partitions.is_empty() && task.join_partitions.is_empty(),
    };

    for input_path in &task.input_partitions {
        if let Some(node_id) = extract_node_id_from_path(input_path, &task.job_id.to_string()) {
            if !job_completed.contains(&node_id) { return false; }
        }
    }

    for join_path in &task.join_partitions {
        if let Some(node_id) = extract_node_id_from_path(join_path, &task.job_id.to_string()) {
            if !job_completed.contains(&node_id) { return false; }
        }
    }

    true
}

fn extract_node_id_from_path(path: &str, job_id: &str) -> Option<String> {
    let filename = path.rsplit('/').next()?;
    let prefix = format!("{}_", job_id);
    let rest = filename.strip_prefix(&prefix)?;

    let chars: Vec<char> = rest.chars().collect();
    let mut last_p_pos = None;
    for i in 0..chars.len().saturating_sub(2) {
        if chars[i] == '_' && chars[i + 1] == 'p' && chars.get(i + 2).map(|c| c.is_ascii_digit()).unwrap_or(false) {
            last_p_pos = Some(i);
        }
    }

    Some(rest[..last_p_pos?].to_string())
}

fn check_node_completion(state: &AppState, task: &Task) {
    let completed = state.completed_tasks.lock().unwrap();
    let total_partitions = task.total_partitions;

    let completed_partitions: u32 = completed.values()
        .filter(|r| r.job_id == task.job_id && r.success)
        .filter(|r| r.output_path.as_ref().and_then(|p| extract_node_id_from_path(p, &task.job_id.to_string())).as_deref() == Some(&task.node_id))
        .count() as u32;

    // Para shuffle_write, contar diferente
    let completed_partitions = if task.op == "shuffle_write" {
        completed.values()
            .filter(|r| r.job_id == task.job_id && r.success && !r.shuffle_outputs.is_empty())
            .count() as u32 / task.total_partitions.max(1)
    } else {
        completed_partitions
    };

    drop(completed);

    if completed_partitions >= total_partitions {
        state.completed_nodes.lock().unwrap().entry(task.job_id).or_default().insert(task.node_id.clone());
        Logger::new("NODE").emit(Logger::new("NODE").info("Nodo completado").field("node", &task.node_id));
    }
}

fn update_dependent_tasks(state: &AppState, completed_task: &Task, result: &TaskResult) {
    let mut queue = state.task_queue.lock().unwrap();
    let output_path = match &result.output_path { Some(p) => p, None => return };

    for task in queue.iter_mut() {
        if task.job_id != completed_task.job_id { continue; }

        let expected = format!("/tmp/minispark/{}_{}_p{}.json", completed_task.job_id, completed_task.node_id, completed_task.partition_id);

        for input in task.input_partitions.iter_mut() {
            if input == &expected { *input = output_path.clone(); }
        }
        for join_input in task.join_partitions.iter_mut() {
            if join_input == &expected { *join_input = output_path.clone(); }
        }
    }
}

fn update_job_progress(state: &AppState, job_id: Uuid) {
    let completed = state.completed_tasks.lock().unwrap();
    let running = state.running_tasks.lock().unwrap();
    let queue = state.task_queue.lock().unwrap();
    let failed = state.failed_tasks.lock().unwrap();

    let success_count = completed.values().filter(|r| r.job_id == job_id && r.success).count();
    let total = success_count + running.values().filter(|t| t.job_id == job_id).count()
        + queue.iter().filter(|t| t.job_id == job_id).count()
        + failed.iter().filter(|t| t.job_id == job_id).count();

    if total > 0 {
        let progress = (success_count as f32 / total as f32) * 100.0;
        drop(completed); drop(running); drop(queue); drop(failed);

        let mut jobs = state.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            job.progress = progress;
            if progress >= 100.0 && job.status == JobStatus::Running {
                job.status = JobStatus::Succeeded;
                state.persistence.save_job(job);
                state.job_metrics.lock().unwrap().job_finished(job_id);
                Logger::new("JOB").emit(Logger::new("JOB").info("Job completado").field("job_id", job_id.to_string()));
            }
        }
    }
}

async fn monitor_workers(state: AppState) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(MONITOR_INTERVAL_SECS)).await;
        let now = current_timestamp();
        let mut down_workers = Vec::new();

        {
            let mut workers = state.workers.lock().unwrap();
            for (id, worker) in workers.iter_mut() {
                if worker.status == WorkerStatus::Up && now - worker.last_heartbeat > HEARTBEAT_TIMEOUT_SECS {
                    worker.status = WorkerStatus::Down;
                    down_workers.push(*id);
                }
            }
        }

        for worker_id in down_workers {
            reschedule_worker_tasks(&state, worker_id);
        }
    }
}

fn reschedule_worker_tasks(state: &AppState, worker_id: Uuid) {
    let mut running = state.running_tasks.lock().unwrap();
    let mut queue = state.task_queue.lock().unwrap();
    let completed_attempts = state.completed_attempts.lock().unwrap();

    let tasks: Vec<Task> = running.values().filter(|t| t.assigned_worker == Some(worker_id)).cloned().collect();

    for task in tasks {
        let key = (task.job_id, task.node_id.clone(), task.partition_id);
        running.remove(&task.id);
        if completed_attempts.contains_key(&key) { continue; }

        if task.attempt < MAX_TASK_ATTEMPTS {
            let mut retry = task.clone();
            retry.id = Uuid::new_v4();
            retry.attempt += 1;
            retry.status = TaskStatus::Pending;
            retry.assigned_worker = None;
            queue.push_back(retry);
        }
    }
}

async fn retry_failed_tasks(state: AppState) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_INTERVAL_SECS)).await;

        let tasks: Vec<Task> = state.failed_tasks.lock().unwrap().drain(..).collect();
        if !tasks.is_empty() {
            let mut queue = state.task_queue.lock().unwrap();
            let completed = state.completed_attempts.lock().unwrap();
            for task in tasks {
                let key = (task.job_id, task.node_id.clone(), task.partition_id);
                if !completed.contains_key(&key) { queue.push_back(task); }
            }
        }
    }
}

async fn periodic_persistence(state: AppState) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(PERSISTENCE_INTERVAL_SECS)).await;
        let jobs = state.jobs.lock().unwrap().clone();
        for job in jobs.values() { state.persistence.save_job(job); }
        state.persistence.flush();
    }
}