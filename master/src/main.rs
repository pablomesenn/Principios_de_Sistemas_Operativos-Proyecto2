// master/src/main.rs

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use common::{
    Dag, DagNode, Heartbeat, JobInfo, JobRequest, JobStatus,
    Task, TaskResult, TaskStatus, WorkerInfo, WorkerRegistration, WorkerStatus,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

// ============ Configuración de tolerancia a fallos ============
const MAX_TASK_ATTEMPTS: u32 = 3;
const HEARTBEAT_TIMEOUT_SECS: u64 = 10;
const MONITOR_INTERVAL_SECS: u64 = 5;
const RETRY_INTERVAL_SECS: u64 = 2;

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
    // Para idempotencia: (job_id, node_id, partition_id) -> mejor attempt completado
    completed_attempts: Arc<Mutex<HashMap<(Uuid, String, u32), u32>>>,
    // Métricas de fallos
    failure_counts: Arc<Mutex<HashMap<Uuid, u32>>>,
}

impl Default for AppState {
    fn default() -> Self {
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
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== Mini-Spark Master ===");
    println!("Configuración de tolerancia a fallos:");
    println!("  - Max intentos por tarea: {}", MAX_TASK_ATTEMPTS);
    println!("  - Timeout heartbeat: {}s", HEARTBEAT_TIMEOUT_SECS);
    println!("  - Intervalo monitor: {}s", MONITOR_INTERVAL_SECS);
    println!();

    let state = AppState::default();

    // Monitor de workers
    let monitor_state = state.clone();
    tokio::spawn(async move {
        monitor_workers(monitor_state).await;
    });

    // Reintentos de tareas fallidas
    let retry_state = state.clone();
    tokio::spawn(async move {
        retry_failed_tasks(retry_state).await;
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/workers/register", post(register_worker))
        .route("/api/v1/workers/heartbeat", post(heartbeat))
        .route("/api/v1/workers/task", get(get_task_for_worker))
        .route("/api/v1/workers/task/complete", post(complete_task))
        .route("/api/v1/jobs", post(submit_job))
        .route("/api/v1/jobs/:id", get(get_job))
        .route("/api/v1/jobs/:id/results", get(get_job_results))
        .route("/api/v1/metrics/failures", get(get_failure_metrics))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("Master escuchando en {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("No se pudo bindear al puerto 8080");

    axum::serve(listener, app)
        .await
        .expect("Error en el servidor");
}

async fn health() -> &'static str {
    "OK"
}

async fn register_worker(
    State(state): State<AppState>,
    Json(mut reg): Json<WorkerRegistration>,
) -> Json<WorkerRegistration> {
    if reg.id.is_nil() {
        reg.id = Uuid::new_v4();
    }

    let worker_info = WorkerInfo {
        registration: reg.clone(),
        status: WorkerStatus::Up,
        active_tasks: 0,
        last_heartbeat: current_timestamp(),
    };

    state.workers.lock().unwrap().insert(reg.id, worker_info);
    println!("[WORKER] Registrado: {} ({}:{})", reg.id, reg.host, reg.port);

    Json(reg)
}

async fn heartbeat(
    State(state): State<AppState>,
    Json(hb): Json<Heartbeat>,
) -> StatusCode {
    let mut workers = state.workers.lock().unwrap();
    if let Some(worker) = workers.get_mut(&hb.worker_id) {
        let was_down = worker.status == WorkerStatus::Down;
        worker.last_heartbeat = current_timestamp();
        worker.active_tasks = hb.active_tasks;
        worker.status = WorkerStatus::Up;
        
        if was_down {
            println!("[WORKER] Recuperado: {} (tareas activas: {})", hb.worker_id, hb.active_tasks);
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
    let worker_id = params
        .get("worker_id")
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    // Verificar worker
    {
        let workers = state.workers.lock().unwrap();
        match workers.get(&worker_id) {
            Some(w) if w.status == WorkerStatus::Up => {}
            _ => return Err(StatusCode::FORBIDDEN),
        }
    }

    // Buscar tarea lista
    let mut queue = state.task_queue.lock().unwrap();
    let completed_nodes = state.completed_nodes.lock().unwrap();
    let completed_attempts = state.completed_attempts.lock().unwrap();
    
    // Filtrar tareas que ya fueron completadas (idempotencia)
    let ready_idx = queue.iter().position(|task| {
        // Verificar si ya hay un attempt exitoso para esta partición
        let key = (task.job_id, task.node_id.clone(), task.partition_id);
        if completed_attempts.contains_key(&key) {
            return false; // Ya completada, no asignar
        }
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
        
        println!("[TASK] Asignada: {} (op={}, p={}, attempt={}) -> worker {}", 
            task.id, task.op, task.partition_id, task.attempt, worker_id);
        Ok(Json(task))
    } else {
        Err(StatusCode::NO_CONTENT)
    }
}

async fn complete_task(
    State(state): State<AppState>,
    Json(result): Json<TaskResult>,
) -> StatusCode {
    let task_info = state.running_tasks.lock().unwrap().remove(&result.task_id);
    
    if let Some(task) = task_info {
        // Decrementar tareas activas del worker
        if let Some(worker_id) = task.assigned_worker {
            if let Some(w) = state.workers.lock().unwrap().get_mut(&worker_id) {
                w.active_tasks = w.active_tasks.saturating_sub(1);
            }
        }

        // Verificar idempotencia: si ya hay un attempt exitoso, ignorar
        let task_key = (task.job_id, task.node_id.clone(), task.partition_id);
        {
            let completed_attempts = state.completed_attempts.lock().unwrap();
            if let Some(&completed_attempt) = completed_attempts.get(&task_key) {
                if result.attempt <= completed_attempt {
                    println!("[TASK] Ignorada (duplicado): {} attempt={} (ya completado attempt={})", 
                        result.task_id, result.attempt, completed_attempt);
                    return StatusCode::OK;
                }
            }
        }

        if result.success {
            // Marcar como completada para idempotencia
            state.completed_attempts.lock().unwrap()
                .insert(task_key, result.attempt);
            
            // Guardar resultado
            state.completed_tasks.lock().unwrap()
                .insert(result.task_id, result.clone());

            // Guardar output path
            if let Some(ref path) = result.output_path {
                state.task_outputs.lock().unwrap().insert(
                    (task.job_id, task.node_id.clone(), task.partition_id),
                    path.clone(),
                );
            }
            
            // Verificar si el nodo completo terminó
            check_node_completion(&state, &task);
            
            // Actualizar inputs de tareas dependientes
            update_dependent_tasks(&state, &task, &result);
            
            println!("[TASK] Completada: {} (records={}, attempt={})", 
                result.task_id, result.records_processed, result.attempt);
        } else {
            // Incrementar contador de fallos
            *state.failure_counts.lock().unwrap()
                .entry(task.job_id)
                .or_insert(0) += 1;

            // Tarea falló - programar reintento si no excede límite
            if task.attempt < MAX_TASK_ATTEMPTS {
                let mut retry_task = task.clone();
                retry_task.id = Uuid::new_v4();
                retry_task.attempt += 1;
                retry_task.status = TaskStatus::Pending;
                retry_task.assigned_worker = None;
                
                state.failed_tasks.lock().unwrap().push(retry_task);
                println!("[TASK] Falló: {} - reintento {}/{} programado (error: {:?})", 
                    result.task_id, task.attempt + 1, MAX_TASK_ATTEMPTS, result.error);
            } else {
                // Marcar job como fallido
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&task.job_id) {
                    job.status = JobStatus::Failed(
                        format!("Tarea '{}' falló después de {} intentos: {:?}",
                            task.node_id, MAX_TASK_ATTEMPTS, result.error)
                    );
                }
                println!("[TASK] FATAL: {} falló después de {} intentos - JOB FALLIDO", 
                    result.task_id, MAX_TASK_ATTEMPTS);
            }
        }
        
        update_job_progress(&state, task.job_id);
    }
    
    StatusCode::OK
}

async fn submit_job(
    State(state): State<AppState>,
    Json(req): Json<JobRequest>,
) -> Json<JobInfo> {
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

    let tasks = generate_tasks_from_dag(&req.dag, job_id, req.parallelism);
    let task_count = tasks.len();
    
    let mut queue = state.task_queue.lock().unwrap();
    for task in tasks {
        println!("[TASK] Encolada: {} (op={}, p={})", task.id, task.op, task.partition_id);
        queue.push_back(task);
    }

    println!("[JOB] {} iniciado con {} tareas", job_id, task_count);
    Json(job)
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobInfo>, StatusCode> {
    state.jobs.lock().unwrap()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn get_job_results(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<TaskResult>>, StatusCode> {
    let completed = state.completed_tasks.lock().unwrap();
    let results: Vec<TaskResult> = completed
        .values()
        .filter(|r| r.job_id == id && r.success)
        .cloned()
        .collect();
    
    if results.is_empty() {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(Json(results))
    }
}

async fn get_failure_metrics(
    State(state): State<AppState>,
) -> Json<HashMap<String, serde_json::Value>> {
    let failure_counts = state.failure_counts.lock().unwrap();
    let workers = state.workers.lock().unwrap();
    
    let mut metrics = HashMap::new();
    
    // Fallos por job
    let job_failures: HashMap<String, u32> = failure_counts
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect();
    metrics.insert("job_failures".to_string(), serde_json::json!(job_failures));
    
    // Estado de workers
    let worker_status: HashMap<String, String> = workers
        .iter()
        .map(|(id, w)| (id.to_string(), format!("{:?}", w.status)))
        .collect();
    metrics.insert("worker_status".to_string(), serde_json::json!(worker_status));
    
    // Workers caídos
    let down_count = workers.values().filter(|w| w.status == WorkerStatus::Down).count();
    metrics.insert("workers_down".to_string(), serde_json::json!(down_count));
    
    Json(metrics)
}

// ============ Funciones auxiliares ============

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn is_task_ready(task: &Task, completed_nodes: &HashMap<Uuid, HashSet<String>>) -> bool {
    // Tareas sin dependencias siempre están listas
    if task.input_partitions.is_empty() && task.join_partitions.is_empty() {
        return true;
    }
    
    let job_completed = match completed_nodes.get(&task.job_id) {
        Some(nodes) => nodes,
        None => return task.input_partitions.is_empty() && task.join_partitions.is_empty(),
    };
    
    // Verificar dependencias de input_partitions
    for input_path in &task.input_partitions {
        if let Some(node_id) = extract_node_id_from_path(input_path, &task.job_id.to_string()) {
            if !job_completed.contains(&node_id) {
                return false;
            }
        }
    }
    
    // Verificar dependencias de join_partitions
    for join_path in &task.join_partitions {
        if let Some(node_id) = extract_node_id_from_path(join_path, &task.job_id.to_string()) {
            if !job_completed.contains(&node_id) {
                return false;
            }
        }
    }
    
    true
}

fn extract_node_id_from_path(path: &str, job_id: &str) -> Option<String> {
    let filename = path.rsplit('/').next()?;
    let prefix = format!("{}_", job_id);
    let rest = filename.strip_prefix(&prefix)?;
    
    let mut last_p_pos = None;
    let chars: Vec<char> = rest.chars().collect();
    
    for i in 0..chars.len().saturating_sub(2) {
        if chars[i] == '_' && chars[i + 1] == 'p' && chars.get(i + 2).map(|c| c.is_ascii_digit()).unwrap_or(false) {
            last_p_pos = Some(i);
        }
    }
    
    let node_id = match last_p_pos {
        Some(pos) => &rest[..pos],
        None => return None,
    };
    
    Some(node_id.to_string())
}

fn check_node_completion(state: &AppState, task: &Task) {
    let completed = state.completed_tasks.lock().unwrap();
    let total_partitions = task.total_partitions;
    
    let mut completed_partitions = 0u32;
    for r in completed.values() {
        if r.job_id == task.job_id && r.success {
            if let Some(ref path) = r.output_path {
                if let Some(node_id) = extract_node_id_from_path(path, &task.job_id.to_string()) {
                    if node_id == task.node_id {
                        completed_partitions += 1;
                    }
                }
            }
        }
    }
    
    drop(completed);
    
    if completed_partitions >= total_partitions {
        state.completed_nodes.lock().unwrap()
            .entry(task.job_id)
            .or_default()
            .insert(task.node_id.clone());
        
        println!("[NODE] '{}' completado ({}/{} particiones)", 
            task.node_id, completed_partitions, total_partitions);
    }
}

fn update_dependent_tasks(state: &AppState, completed_task: &Task, result: &TaskResult) {
    let mut queue = state.task_queue.lock().unwrap();
    
    let output_path = match &result.output_path {
        Some(p) => p,
        None => return,
    };
    
    for task in queue.iter_mut() {
        if task.job_id != completed_task.job_id {
            continue;
        }
        
        // Actualizar input_partitions
        for input in task.input_partitions.iter_mut() {
            let expected = format!("/tmp/minispark/{}_{}_p{}.json",
                completed_task.job_id,
                completed_task.node_id,
                completed_task.partition_id);
            
            if input == &expected {
                *input = output_path.clone();
            }
        }
        
        // Actualizar join_partitions
        for join_input in task.join_partitions.iter_mut() {
            let expected = format!("/tmp/minispark/{}_{}_p{}.json",
                completed_task.job_id,
                completed_task.node_id,
                completed_task.partition_id);
            
            if join_input == &expected {
                *join_input = output_path.clone();
            }
        }
    }
}

fn generate_tasks_from_dag(dag: &Dag, job_id: Uuid, parallelism: u32) -> Vec<Task> {
    let mut tasks = Vec::new();
    
    let mut deps: HashMap<&str, Vec<&str>> = HashMap::new();
    for (from, to) in &dag.edges {
        deps.entry(to.as_str()).or_default().push(from.as_str());
    }
    
    for node in &dag.nodes {
        let num_partitions = node.partitions.unwrap_or(parallelism);
        
        for partition_id in 0..num_partitions {
            let input_partitions: Vec<String> = deps
                .get(node.id.as_str())
                .map(|parents| {
                    parents.iter()
                        .filter(|p| **p != node.join_with.as_deref().unwrap_or(""))
                        .map(|parent| {
                            format!("/tmp/minispark/{}_{}_p{}.json", 
                                job_id, parent, partition_id)
                        })
                        .collect()
                })
                .unwrap_or_default();
            
            let join_partitions: Vec<String> = node.join_with.as_ref()
                .map(|join_node| {
                    (0..num_partitions)
                        .map(|p| format!("/tmp/minispark/{}_{}_p{}.json", job_id, join_node, p))
                        .collect()
                })
                .unwrap_or_default();
            
            let task = Task {
                id: Uuid::new_v4(),
                job_id,
                attempt: 0, // Empieza en 0, no en 1
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
            };
            tasks.push(task);
        }
    }
    
    tasks
}

fn update_job_progress(state: &AppState, job_id: Uuid) {
    let completed = state.completed_tasks.lock().unwrap();
    let running = state.running_tasks.lock().unwrap();
    let queue = state.task_queue.lock().unwrap();
    let failed = state.failed_tasks.lock().unwrap();
    
    let success_count = completed.values()
        .filter(|r| r.job_id == job_id && r.success)
        .count();
    let running_count = running.values().filter(|t| t.job_id == job_id).count();
    let pending_count = queue.iter().filter(|t| t.job_id == job_id).count();
    let retry_count = failed.iter().filter(|t| t.job_id == job_id).count();
    
    let total = success_count + running_count + pending_count + retry_count;
    
    if total > 0 {
        let progress = (success_count as f32 / total as f32) * 100.0;
        
        drop(completed);
        drop(running);
        drop(queue);
        drop(failed);
        
        let mut jobs = state.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            job.progress = progress;
            if progress >= 100.0 && job.status == JobStatus::Running {
                job.status = JobStatus::Succeeded;
                println!("[JOB] {} completado exitosamente!", job_id);
            }
        }
    }
}

async fn monitor_workers(state: AppState) {
    println!("[MONITOR] Iniciado - verificando workers cada {}s", MONITOR_INTERVAL_SECS);
    
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
                    println!("[MONITOR] Worker {} marcado DOWN (sin heartbeat por {}s)", 
                        id, now - worker.last_heartbeat);
                }
            }
        }
        
        // Replanificar tareas de workers caídos
        for worker_id in down_workers {
            reschedule_worker_tasks(&state, worker_id);
        }
    }
}

fn reschedule_worker_tasks(state: &AppState, worker_id: Uuid) {
    let mut running = state.running_tasks.lock().unwrap();
    let mut queue = state.task_queue.lock().unwrap();
    let completed_attempts = state.completed_attempts.lock().unwrap();
    
    let tasks_to_reschedule: Vec<Task> = running
        .values()
        .filter(|t| t.assigned_worker == Some(worker_id))
        .cloned()
        .collect();
    
    let mut rescheduled_count = 0;
    
    for task in tasks_to_reschedule {
        // Verificar si ya fue completada (idempotencia)
        let key = (task.job_id, task.node_id.clone(), task.partition_id);
        if completed_attempts.contains_key(&key) {
            println!("[RESCHEDULE] Ignorando tarea {} (ya completada)", task.id);
            running.remove(&task.id);
            continue;
        }
        
        running.remove(&task.id);
        
        if task.attempt < MAX_TASK_ATTEMPTS {
            let mut retry_task = task.clone();
            retry_task.id = Uuid::new_v4();
            retry_task.attempt += 1;
            retry_task.status = TaskStatus::Pending;
            retry_task.assigned_worker = None;
            
            println!("[RESCHEDULE] Tarea {} replanificada (intento {}/{})", 
                retry_task.node_id, retry_task.attempt + 1, MAX_TASK_ATTEMPTS);
            queue.push_back(retry_task);
            rescheduled_count += 1;
        } else {
            println!("[RESCHEDULE] Tarea {} excedió reintentos - marcando job fallido", task.node_id);
            drop(running);
            drop(queue);
            drop(completed_attempts);
            
            let mut jobs = state.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(&task.job_id) {
                job.status = JobStatus::Failed(
                    format!("Worker {} caído y tarea '{}' excedió reintentos", worker_id, task.node_id)
                );
            }
            return;
        }
    }
    
    if rescheduled_count > 0 {
        println!("[RESCHEDULE] {} tareas replanificadas del worker {}", rescheduled_count, worker_id);
    }
}

async fn retry_failed_tasks(state: AppState) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(RETRY_INTERVAL_SECS)).await;
        
        let tasks: Vec<Task> = {
            let mut failed = state.failed_tasks.lock().unwrap();
            failed.drain(..).collect()
        };
        
        if !tasks.is_empty() {
            let mut queue = state.task_queue.lock().unwrap();
            let completed_attempts = state.completed_attempts.lock().unwrap();
            
            for task in tasks {
                // Verificar idempotencia antes de reintentar
                let key = (task.job_id, task.node_id.clone(), task.partition_id);
                if completed_attempts.contains_key(&key) {
                    println!("[RETRY] Ignorando tarea {} (ya completada)", task.id);
                    continue;
                }
                
                println!("[RETRY] Reintentando tarea {} (intento {}/{})", 
                    task.node_id, task.attempt + 1, MAX_TASK_ATTEMPTS);
                queue.push_back(task);
            }
        }
    }
}