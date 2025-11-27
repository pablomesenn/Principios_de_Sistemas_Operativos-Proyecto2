// common/src/lib.rs

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============ Registro de datos ============

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Record {
    pub key: Option<String>,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Partition {
    pub records: Vec<Record>,
}

// ============ DAG y Job ============

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DagNode {
    pub id: String,
    pub op: String,
    pub path: Option<String>,
    pub partitions: Option<u32>,
    pub fn_name: Option<String>,
    pub key: Option<String>,
    // Para join: segundo input
    pub join_with: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Dag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<(String, String)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobRequest {
    pub name: String,
    pub dag: Dag,
    pub parallelism: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum JobStatus {
    Accepted,
    Running,
    Failed(String),
    Succeeded,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobInfo {
    pub id: Uuid,
    pub request: JobRequest,
    pub status: JobStatus,
    pub progress: f32,
}

// ============ Task ============

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt: u32,
    pub node_id: String,
    pub op: String,
    pub partition_id: u32,
    pub total_partitions: u32,
    pub input_path: Option<String>,
    pub input_partitions: Vec<String>,
    pub fn_name: Option<String>,
    pub key: Option<String>,
    pub status: TaskStatus,
    // Para shuffle: indica si debe leer de shuffle files
    pub is_shuffle_read: bool,
    // Para join: paths del segundo dataset
    pub join_partitions: Vec<String>,
    // Worker asignado (para replanificación)
    pub assigned_worker: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskResult {
    pub task_id: Uuid,
    pub job_id: Uuid,
    pub attempt: u32,
    pub success: bool,
    pub error: Option<String>,
    pub output_path: Option<String>,
    pub records_processed: u64,
    // Paths de shuffle output (uno por partición destino)
    pub shuffle_outputs: Vec<String>,
}

// ============ Worker ============

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerRegistration {
    pub id: Uuid,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum WorkerStatus {
    Up,
    Down,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerInfo {
    pub registration: WorkerRegistration,
    pub status: WorkerStatus,
    pub active_tasks: u32,
    pub last_heartbeat: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub worker_id: Uuid,
    pub active_tasks: u32,
}

// ============ Mensajes ============

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskAssignment {
    pub task: Task,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskStatusUpdate {
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub result: Option<TaskResult>,
}