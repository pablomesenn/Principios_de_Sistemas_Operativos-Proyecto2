// common/src/lib.rs

pub mod logging;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Re-exportar logging
pub use logging::{LogEntry, LogLevel, Logger};

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
    pub is_shuffle_read: bool,
    pub join_partitions: Vec<String>,
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

// ============ Métricas ============

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct JobMetrics {
    pub job_id: String,
    pub name: String,
    pub status: String,
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub duration_ms: Option<u64>,
    pub total_tasks: u32,
    pub completed_tasks: u32,
    pub failed_tasks: u32,
    pub total_records: u64,
    pub stages_completed: u32,
    pub total_stages: u32,
    pub throughput_records_per_sec: f64,
}