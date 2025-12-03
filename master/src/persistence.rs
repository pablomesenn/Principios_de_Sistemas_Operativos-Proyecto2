// Persistence of job state to a JSON file on disk

use common::{JobInfo, JobStatus};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Directory where persistence files are stored
const STATE_DIR: &str = "/tmp/minispark/state";
/// Main JSON file that stores job metadata
const JOBS_FILE: &str = "/tmp/minispark/state/jobs.json";
/// Minimum interval (in seconds) between consecutive saves
const SAVE_INTERVAL_SECS: u64 = 5;

/// Persistable global state snapshot
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedState {
    /// Jobs stored by ID as string (UUID string)
    pub jobs: HashMap<String, PersistedJob>,
    /// Last time the state was saved (UNIX timestamp in seconds)
    pub last_saved: u64,
}

/// Persistable representation of a single job
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedJob {
    /// Job ID (UUID as string)
    pub id: String,
    /// Human-readable job name
    pub name: String,
    /// Status as string (Accepted/Running/Succeeded/Failed: <msg>)
    pub status: String,
    /// Progress in percentage [0.0, 100.0]
    pub progress: f32,
    /// Parallelism level used for the job
    pub parallelism: u32,
    /// Creation timestamp (UNIX seconds)
    pub created_at: u64,
    /// Last update timestamp (UNIX seconds)
    pub updated_at: u64,
}

impl PersistedState {
    /// Create a new empty persisted state
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            last_saved: current_timestamp(),
        }
    }

    /// Load persisted state from disk if it exists, otherwise create a new one
    pub fn load() -> Self {
        // If no jobs file exists, start from an empty state
        if !Path::new(JOBS_FILE).exists() {
            println!("[PERSISTENCE] No hay estado previo, iniciando vacío");
            return Self::new();
        }

        // Read JSON file and deserialize it into PersistedState
        match fs::read_to_string(JOBS_FILE) {
            Ok(content) => {
                match serde_json::from_str(&content) {
                    Ok(state) => {
                        println!("[PERSISTENCE] Estado cargado desde {}", JOBS_FILE);
                        state
                    }
                    Err(e) => {
                        // If parsing fails, report error and start with empty state
                        println!("[PERSISTENCE] Error parseando estado: {}", e);
                        Self::new()
                    }
                }
            }
            Err(e) => {
                // If reading file fails, report error and start with empty state
                println!("[PERSISTENCE] Error leyendo estado: {}", e);
                Self::new()
            }
        }
    }

    /// Save in-memory state to disk as pretty JSON
    pub fn save(&mut self) -> Result<(), String> {
        // Ensure the directory for state files exists
        fs::create_dir_all(STATE_DIR).map_err(|e| format!("Error creando directorio: {}", e))?;

        // Update last_saved timestamp
        self.last_saved = current_timestamp();

        // Serialize state to human-readable JSON
        let content =
            serde_json::to_string_pretty(self).map_err(|e| format!("Error serializando: {}", e))?;

        // Write JSON content to jobs file
        fs::write(JOBS_FILE, content).map_err(|e| format!("Error escribiendo: {}", e))?;

        Ok(())
    }

    /// Update or insert a job entry in the persisted state
    pub fn update_job(&mut self, job: &JobInfo) {
        // Convert JobStatus enum into a human-readable string
        let status_str = match &job.status {
            JobStatus::Accepted => "Accepted".to_string(),
            JobStatus::Running => "Running".to_string(),
            JobStatus::Succeeded => "Succeeded".to_string(),
            JobStatus::Failed(msg) => format!("Failed: {}", msg),
        };

        // Preserve original created_at if job already exists, otherwise use current timestamp
        let persisted = PersistedJob {
            id: job.id.to_string(),
            name: job.request.name.clone(),
            status: status_str,
            progress: job.progress,
            parallelism: job.request.parallelism,
            created_at: self
                .jobs
                .get(&job.id.to_string())
                .map(|j| j.created_at)
                .unwrap_or_else(current_timestamp),
            updated_at: current_timestamp(),
        };

        // Insert or replace the job entry
        self.jobs.insert(job.id.to_string(), persisted);
    }

    /// Return jobs that were Running or Accepted when last persisted
    /// (useful to implement recovery / replay logic on master startup)
    pub fn get_incomplete_jobs(&self) -> Vec<&PersistedJob> {
        self.jobs
            .values()
            .filter(|j| j.status == "Running" || j.status == "Accepted")
            .collect()
    }
}

impl Default for PersistedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Return current UNIX timestamp in seconds
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Thread-safe persistence manager wrapping PersistedState in Arc<Mutex<_>>
pub struct PersistenceManager {
    state: Arc<Mutex<PersistedState>>,
}

impl PersistenceManager {
    /// Create a new manager, loading state from disk and reporting incomplete jobs
    pub fn new() -> Self {
        let state = PersistedState::load();

        // Report jobs that were left incomplete in the previous master run
        let incomplete = state.get_incomplete_jobs();
        if !incomplete.is_empty() {
            println!(
                "[PERSISTENCE] {} jobs incompletos encontrados del estado anterior:",
                incomplete.len()
            );
            for job in &incomplete {
                println!("  - {} ({}) - {}", job.name, job.id, job.status);
            }
        }

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Update a job and save to disk if enough time has passed since last save
    pub fn save_job(&self, job: &JobInfo) {
        let mut state = self.state.lock().unwrap();
        // Update in-memory representation
        state.update_job(job);

        // Only persist to disk if SAVE_INTERVAL_SECS has passed since last_saved
        let now = current_timestamp();
        if now - state.last_saved >= SAVE_INTERVAL_SECS {
            if let Err(e) = state.save() {
                println!("[PERSISTENCE] Error guardando: {}", e);
            }
        }
    }

    /// Force immediate save to disk regardless of SAVE_INTERVAL_SECS
    pub fn flush(&self) {
        let mut state = self.state.lock().unwrap();
        if let Err(e) = state.save() {
            println!("[PERSISTENCE] Error en flush: {}", e);
        } else {
            println!("[PERSISTENCE] Estado guardado a disco");
        }
    }

    /// Get a snapshot copy of the current persisted state
    pub fn get_state(&self) -> PersistedState {
        self.state.lock().unwrap().clone()
    }
}

impl Default for PersistenceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PersistenceManager {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}
