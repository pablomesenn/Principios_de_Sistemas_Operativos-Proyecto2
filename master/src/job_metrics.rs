// Metric per job: time, stages, throughput

use common::JobMetrics;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Statistics of a stage (DAG node)
#[derive(Debug, Clone, serde::Serialize)]
pub struct StageStats {
    pub node_id: String,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub tasks_total: u32,
    pub tasks_completed: u32,
    pub records_processed: u64,
}

/// Tracker of metrics for a job
#[derive(Debug, Clone)]
pub struct JobTracker {
    pub job_id: Uuid,
    pub name: String,
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub total_tasks: u32,
    pub completed_tasks: u32,
    pub failed_tasks: u32,
    pub total_records: u64,
    pub stages: HashMap<String, StageStats>,
}

impl JobTracker {
    pub fn new(job_id: Uuid, name: String, stages: Vec<String>, tasks_per_stage: u32) -> Self {
        let mut stage_map = HashMap::new();
        for node_id in stages {
            stage_map.insert(
                node_id.clone(),
                StageStats {
                    node_id,
                    start_time: None,
                    end_time: None,
                    tasks_total: tasks_per_stage,
                    tasks_completed: 0,
                    records_processed: 0,
                },
            );
        }

        Self {
            job_id,
            name,
            start_time: current_timestamp(),
            end_time: None,
            total_tasks: stage_map.len() as u32 * tasks_per_stage,
            completed_tasks: 0,
            failed_tasks: 0,
            total_records: 0,
            stages: stage_map,
        }
    }

    pub fn task_completed(&mut self, node_id: &str, records: u64) {
        // Update metrics
        self.completed_tasks += 1;
        self.total_records += records;

        // Update stage metrics
        if let Some(stage) = self.stages.get_mut(node_id) {
            if stage.start_time.is_none() {
                stage.start_time = Some(current_timestamp());
            }
            stage.tasks_completed += 1;
            stage.records_processed += records;

            if stage.tasks_completed >= stage.tasks_total {
                stage.end_time = Some(current_timestamp());
            }
        }
    }

    pub fn task_failed(&mut self) {
        self.failed_tasks += 1;
    }

    pub fn job_finished(&mut self) {
        self.end_time = Some(current_timestamp());
    }

    pub fn to_metrics(&self) -> JobMetrics {
        let duration_ms = self.end_time.map(|end| (end - self.start_time) * 1000);
        let stages_completed = self
            .stages
            .values()
            .filter(|s| s.end_time.is_some())
            .count() as u32;

        let throughput = if let Some(dur) = duration_ms {
            if dur > 0 {
                (self.total_records as f64 / dur as f64) * 1000.0
            } else {
                0.0
            }
        } else {
            let elapsed = current_timestamp() - self.start_time;
            if elapsed > 0 {
                self.total_records as f64 / elapsed as f64
            } else {
                0.0
            }
        };

        JobMetrics {
            job_id: self.job_id.to_string(),
            name: self.name.clone(),
            status: if self.end_time.is_some() {
                "Completed".to_string()
            } else {
                "Running".to_string()
            },
            start_time: self.start_time,
            end_time: self.end_time,
            duration_ms,
            total_tasks: self.total_tasks,
            completed_tasks: self.completed_tasks,
            failed_tasks: self.failed_tasks,
            total_records: self.total_records,
            stages_completed,
            total_stages: self.stages.len() as u32,
            throughput_records_per_sec: throughput,
        }
    }
}

/// All jobs metrics manager
pub struct JobMetricsManager {
    trackers: HashMap<Uuid, JobTracker>,
}

impl JobMetricsManager {
    pub fn new() -> Self {
        Self {
            trackers: HashMap::new(),
        }
    }

    pub fn register_job(
        &mut self,
        job_id: Uuid,
        name: String,
        stages: Vec<String>,
        parallelism: u32,
    ) {
        let tracker = JobTracker::new(job_id, name, stages, parallelism);
        self.trackers.insert(job_id, tracker);
    }

    pub fn task_completed(&mut self, job_id: Uuid, node_id: &str, records: u64) {
        if let Some(tracker) = self.trackers.get_mut(&job_id) {
            tracker.task_completed(node_id, records);
        }
    }

    pub fn task_failed(&mut self, job_id: Uuid) {
        if let Some(tracker) = self.trackers.get_mut(&job_id) {
            tracker.task_failed();
        }
    }

    pub fn job_finished(&mut self, job_id: Uuid) {
        if let Some(tracker) = self.trackers.get_mut(&job_id) {
            tracker.job_finished();
        }
    }

    pub fn get_metrics(&self, job_id: Uuid) -> Option<JobMetrics> {
        self.trackers.get(&job_id).map(|t| t.to_metrics())
    }

    pub fn get_all_metrics(&self) -> Vec<JobMetrics> {
        self.trackers.values().map(|t| t.to_metrics()).collect()
    }

    pub fn get_stage_stats(&self, job_id: Uuid) -> Option<Vec<StageStats>> {
        self.trackers
            .get(&job_id)
            .map(|t| t.stages.values().cloned().collect())
    }
}

impl Default for JobMetricsManager {
    fn default() -> Self {
        Self::new()
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub type SharedJobMetrics = Arc<Mutex<JobMetricsManager>>;

pub fn new_shared_job_metrics() -> SharedJobMetrics {
    Arc::new(Mutex::new(JobMetricsManager::new()))
}
