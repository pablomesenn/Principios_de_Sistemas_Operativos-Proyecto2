use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DagNode {
    pub id: String,
    pub op: String, // "read_csv", "map", "filter", "flat_map", "reduce_by_key", "join"
    pub path: Option<String>,
    pub partitions: Option<u32>,
    pub fn_name: Option<String>, // para referenciar funciones predefinidas
    pub key: Option<String>,     // para reduce_by_key / join
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Dag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<(String, String)>, // (from, to)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobRequest {
    pub name: String,
    pub dag: Dag,
    pub parallelism: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    pub progress: f32, // 0.0 - 100.0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerRegistration {
    pub id: Uuid,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub worker_id: Uuid,
    pub num_active_tasks: u32,
}
