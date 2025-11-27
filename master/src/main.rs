// master/src/main.rs

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use common::{
    Dag, Heartbeat, JobInfo, JobRequest, JobStatus,
    Task, TaskResult, TaskStatus, WorkerInfo, WorkerRegistration, WorkerStatus,
};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

// ============ Estado ============

#[derive(Clone)]
struct AppState {
    workers: Arc<Mutex<HashMap<Uuid, WorkerInfo>>>,
    jobs: Arc<Mutex<HashMap<Uuid, JobInfo>>>,
    task_queue: Arc<Mutex<VecDeque<Task>>>,
    running_tasks: Arc<Mutex<HashMap<Uuid, Task>>>,
    completed_tasks: Arc<Mutex<HashMap<Uuid, TaskResult>>>,
    // Mapeo: (job_id, node_id, partition) -> output_path
    task_outputs: Arc<Mutex<HashMap<(Uuid, String, u32), String>>>,
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
        }
    }
}

#[tokio::main]
async fn main() {
    let state = AppState::default();

    let monitor_state = state.clone();
    tokio::spawn(async move {
        monitor_workers(monitor_state).await;
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

    // Buscar tarea lista (con dependencias satisfechas)
    let mut queue = state.task_queue.lock().unwrap();
    let task_outputs = state.task_outputs.lock().unwrap();
    
    let ready_idx = queue.iter().position(|task| {
        task.input_partitions.iter().all(|dep| {
            // Si es un path de dependencia interna, verificar que exista
            if dep.starts_with("/tmp/minispark") {
                // Parsear job_id, node_id, partition del path
                task_outputs.values().any(|v| v == dep)
            } else {
                true // path externo (archivo de entrada)
            }
        })
    });
    
    drop(task_outputs);

    if let Some(idx) = ready_idx {
        let mut task = queue.remove(idx).unwrap();
        task.status = TaskStatus::Assigned;
        
        state.running_tasks.lock().unwrap().insert(task.id, task.clone());
        
        if let Some(w) = state.workers.lock().unwrap().get_mut(&worker_id) {
            w.active_tasks += 1;
        }
        
        println!("Tarea {} asignada (op={}, partition={})", 
            task.id, task.op, task.partition_id);
        Ok(Json(task))
    } else {
        Err(StatusCode::NO_CONTENT)
    }
}

async fn complete_task(
    State(state): State<AppState>,
    Json(result): Json<TaskResult>,
) -> StatusCode {
    // Obtener info de la tarea
    let task_info = state.running_tasks.lock().unwrap().remove(&result.task_id);
    
    if let Some(task) = task_info {
        // Guardar output path para dependencias
        if let Some(ref path) = result.output_path {
            state.task_outputs.lock().unwrap().insert(
                (task.job_id, task.node_id.clone(), task.partition_id),
                path.clone(),
            );
        }
        
        // Actualizar particiones de entrada para tareas dependientes
        if result.success {
            if let Some(ref output_path) = result.output_path {
                update_dependent_tasks(&state, &task, output_path);
            }
        }
    }
    
    // Guardar resultado
    state.completed_tasks.lock().unwrap().insert(result.task_id, result.clone());
    
    // Actualizar progreso
    update_job_progress(&state, result.job_id);
    
    println!("Tarea {} completada (success={}, records={})", 
        result.task_id, result.success, result.records_processed);
    
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

    // Generar tareas
    let tasks = generate_tasks_from_dag(&req.dag, job_id, req.parallelism);
    
    let mut queue = state.task_queue.lock().unwrap();
    for task in tasks {
        println!("Tarea encolada: {} (op={}, partition={})", 
            task.id, task.op, task.partition_id);
        queue.push_back(task);
    }

    println!("Job {} iniciado con {} tareas", job_id, queue.len());
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
        .filter(|r| r.job_id == id)
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

fn generate_tasks_from_dag(dag: &Dag, job_id: Uuid, parallelism: u32) -> Vec<Task> {
    let mut tasks = Vec::new();
    
    // Construir mapa de dependencias
    let mut deps: HashMap<&str, Vec<&str>> = HashMap::new();
    for (from, to) in &dag.edges {
        deps.entry(to.as_str()).or_default().push(from.as_str());
    }
    
    // Ordenar nodos topológicamente (simple: por orden en el DAG)
    for node in &dag.nodes {
        let num_partitions = node.partitions.unwrap_or(parallelism);
        
        for partition_id in 0..num_partitions {
            // Construir paths de entrada
            let input_partitions: Vec<String> = deps
                .get(node.id.as_str())
                .map(|parents| {
                    parents.iter()
                        .map(|parent| {
                            format!("/tmp/minispark/{}_{}_p{}.json", 
                                job_id, parent, partition_id)
                        })
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
            };
            tasks.push(task);
        }
    }
    
    tasks
}

fn update_dependent_tasks(state: &AppState, completed_task: &Task, output_path: &str) {
    let mut queue = state.task_queue.lock().unwrap();
    
    for task in queue.iter_mut() {
        if task.job_id == completed_task.job_id {
            // Actualizar referencias a este output
            for input in task.input_partitions.iter_mut() {
                let expected = format!("/tmp/minispark/{}_{}_p{}.json",
                    completed_task.job_id,
                    completed_task.node_id,
                    completed_task.partition_id);
                
                if input == &expected {
                    *input = output_path.to_string();
                }
            }
        }
    }
}

fn update_job_progress(state: &AppState, job_id: Uuid) {
    let completed = state.completed_tasks.lock().unwrap();
    let running = state.running_tasks.lock().unwrap();
    let queue = state.task_queue.lock().unwrap();
    
    let completed_count = completed.values().filter(|r| r.job_id == job_id).count();
    let running_count = running.values().filter(|t| t.job_id == job_id).count();
    let pending_count = queue.iter().filter(|t| t.job_id == job_id).count();
    
    let total = completed_count + running_count + pending_count;
    
    if total > 0 {
        let progress = (completed_count as f32 / total as f32) * 100.0;
        
        drop(completed);
        drop(running);
        drop(queue);
        
        let mut jobs = state.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            job.progress = progress;
            if progress >= 100.0 {
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
        let mut workers = state.workers.lock().unwrap();
        
        for (id, worker) in workers.iter_mut() {
            if worker.status == WorkerStatus::Up && now - worker.last_heartbeat > 10 {
                worker.status = WorkerStatus::Down;
                println!("Worker {} DOWN", id);
            }
        }
    }
}