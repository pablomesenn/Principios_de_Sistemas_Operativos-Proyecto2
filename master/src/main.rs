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

const MAX_TASK_ATTEMPTS: u32 = 3;

#[derive(Clone)]
struct AppState {
    workers: Arc<Mutex<HashMap<Uuid, WorkerInfo>>>,
    jobs: Arc<Mutex<HashMap<Uuid, JobInfo>>>,
    task_queue: Arc<Mutex<VecDeque<Task>>>,
    running_tasks: Arc<Mutex<HashMap<Uuid, Task>>>,
    completed_tasks: Arc<Mutex<HashMap<Uuid, TaskResult>>>,
    task_outputs: Arc<Mutex<HashMap<(Uuid, String, u32), String>>>,
    // Tareas fallidas para reintentar
    failed_tasks: Arc<Mutex<Vec<Task>>>,
    // Nodos completados por job (para dependencias)
    completed_nodes: Arc<Mutex<HashMap<Uuid, HashSet<String>>>>,
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
        }
    }
}

#[tokio::main]
async fn main() {
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
    println!("Worker registrado: {}", reg.id);

    Json(reg)
}

async fn heartbeat(
    State(state): State<AppState>,
    Json(hb): Json<Heartbeat>,
) -> StatusCode {
    let mut workers = state.workers.lock().unwrap();
    if let Some(worker) = workers.get_mut(&hb.worker_id) {
        worker.last_heartbeat = current_timestamp();
        worker.active_tasks = hb.active_tasks;
        worker.status = WorkerStatus::Up;
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
    
    let ready_idx = queue.iter().position(|task| {
        is_task_ready(task, &completed_nodes)
    });
    
    drop(completed_nodes);

    if let Some(idx) = ready_idx {
        let mut task = queue.remove(idx).unwrap();
        task.status = TaskStatus::Assigned;
        task.assigned_worker = Some(worker_id);
        
        state.running_tasks.lock().unwrap().insert(task.id, task.clone());
        
        if let Some(w) = state.workers.lock().unwrap().get_mut(&worker_id) {
            w.active_tasks += 1;
        }
        
        println!("Asignada: {} (op={}, p={}, attempt={}) -> worker {}", 
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

        // IMPORTANTE: Guardar resultado ANTES de verificar completitud
        state.completed_tasks.lock().unwrap().insert(result.task_id, result.clone());

        if result.success {
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
            
            println!("Completada: {} (records={})", result.task_id, result.records_processed);
        } else {
            // Tarea falló - programar reintento
            if task.attempt < MAX_TASK_ATTEMPTS {
                let mut retry_task = task.clone();
                retry_task.id = Uuid::new_v4();
                retry_task.attempt += 1;
                retry_task.status = TaskStatus::Pending;
                retry_task.assigned_worker = None;
                
                state.failed_tasks.lock().unwrap().push(retry_task);
                println!("Falló: {} - reintento #{} programado", 
                    result.task_id, task.attempt + 1);
            } else {
                let mut jobs = state.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&task.job_id) {
                    job.status = JobStatus::Failed(
                        format!("Tarea {} falló después de {} intentos: {:?}",
                            task.node_id, MAX_TASK_ATTEMPTS, result.error)
                    );
                }
                println!("FATAL: {} falló después de {} intentos", 
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

    let tasks = generate_tasks_from_dag(&req.dag, job_id, req.parallelism);
    let task_count = tasks.len();
    
    let mut queue = state.task_queue.lock().unwrap();
    for task in tasks {
        println!("Encolada: {} (op={}, p={})", task.id, task.op, task.partition_id);
        queue.push_back(task);
    }

    println!("Job {} iniciado con {} tareas", job_id, task_count);
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
    // Path format: /tmp/minispark/{job_id}_{node_id}_p{partition}.json
    let filename = path.rsplit('/').next()?;
    let prefix = format!("{}_", job_id);
    let rest = filename.strip_prefix(&prefix)?;
    
    // Buscar "_p" seguido de un dígito (el partition number)
    // rest = "{node_id}_p{partition}.json"
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
                // Extraer node_id del path y comparar
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
        
        println!("Nodo '{}' completado ({}/{} particiones)", 
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
    
    // Construir mapa de dependencias
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
            
            // Para join: paths del segundo dataset
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
                attempt: 1,
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
                println!("Job {} completado!", job_id);
            }
        }
    }
}

async fn monitor_workers(state: AppState) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        let now = current_timestamp();
        let mut down_workers = Vec::new();
        
        {
            let mut workers = state.workers.lock().unwrap();
            for (id, worker) in workers.iter_mut() {
                if worker.status == WorkerStatus::Up && now - worker.last_heartbeat > 10 {
                    worker.status = WorkerStatus::Down;
                    down_workers.push(*id);
                    println!("Worker {} DOWN (sin heartbeat)", id);
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
    
    let tasks_to_reschedule: Vec<Task> = running
        .values()
        .filter(|t| t.assigned_worker == Some(worker_id))
        .cloned()
        .collect();
    
    for task in tasks_to_reschedule {
        running.remove(&task.id);
        
        let mut retry_task = task.clone();
        retry_task.id = Uuid::new_v4();
        retry_task.attempt += 1;
        retry_task.status = TaskStatus::Pending;
        retry_task.assigned_worker = None;
        
        println!("Replanificando tarea {} (intento {})", retry_task.node_id, retry_task.attempt);
        queue.push_back(retry_task);
    }
}

async fn retry_failed_tasks(state: AppState) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        let tasks: Vec<Task> = {
            let mut failed = state.failed_tasks.lock().unwrap();
            failed.drain(..).collect()
        };
        
        if !tasks.is_empty() {
            let mut queue = state.task_queue.lock().unwrap();
            for task in tasks {
                println!("Reintentando tarea {} (intento {})", task.node_id, task.attempt);
                queue.push_back(task);
            }
        }
    }
}