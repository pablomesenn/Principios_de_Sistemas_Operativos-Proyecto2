// master/src/persistence.rs
// Persistencia del estado de jobs a archivo JSON

use common::{JobInfo, JobStatus};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const STATE_DIR: &str = "/tmp/minispark/state";
const JOBS_FILE: &str = "/tmp/minispark/state/jobs.json";
const SAVE_INTERVAL_SECS: u64 = 5;

/// Estado persistible
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedState {
    pub jobs: HashMap<String, PersistedJob>,
    pub last_saved: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedJob {
    pub id: String,
    pub name: String,
    pub status: String,
    pub progress: f32,
    pub parallelism: u32,
    pub created_at: u64,
    pub updated_at: u64,
}

impl PersistedState {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            last_saved: current_timestamp(),
        }
    }
    
    /// Cargar estado desde disco
    pub fn load() -> Self {
        if !Path::new(JOBS_FILE).exists() {
            println!("[PERSISTENCE] No hay estado previo, iniciando vacío");
            return Self::new();
        }
        
        match fs::read_to_string(JOBS_FILE) {
            Ok(content) => {
                match serde_json::from_str(&content) {
                    Ok(state) => {
                        println!("[PERSISTENCE] Estado cargado desde {}", JOBS_FILE);
                        state
                    }
                    Err(e) => {
                        println!("[PERSISTENCE] Error parseando estado: {}", e);
                        Self::new()
                    }
                }
            }
            Err(e) => {
                println!("[PERSISTENCE] Error leyendo estado: {}", e);
                Self::new()
            }
        }
    }
    
    /// Guardar estado a disco
    pub fn save(&mut self) -> Result<(), String> {
        fs::create_dir_all(STATE_DIR)
            .map_err(|e| format!("Error creando directorio: {}", e))?;
        
        self.last_saved = current_timestamp();
        
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Error serializando: {}", e))?;
        
        fs::write(JOBS_FILE, content)
            .map_err(|e| format!("Error escribiendo: {}", e))?;
        
        Ok(())
    }
    
    /// Actualizar job en estado persistido
    pub fn update_job(&mut self, job: &JobInfo) {
        let status_str = match &job.status {
            JobStatus::Accepted => "Accepted".to_string(),
            JobStatus::Running => "Running".to_string(),
            JobStatus::Succeeded => "Succeeded".to_string(),
            JobStatus::Failed(msg) => format!("Failed: {}", msg),
        };
        
        let persisted = PersistedJob {
            id: job.id.to_string(),
            name: job.request.name.clone(),
            status: status_str,
            progress: job.progress,
            parallelism: job.request.parallelism,
            created_at: self.jobs
                .get(&job.id.to_string())
                .map(|j| j.created_at)
                .unwrap_or_else(current_timestamp),
            updated_at: current_timestamp(),
        };
        
        self.jobs.insert(job.id.to_string(), persisted);
    }
    
    /// Obtener jobs que estaban en Running (para posible recuperación)
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

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Manejador de persistencia thread-safe
pub struct PersistenceManager {
    state: Arc<Mutex<PersistedState>>,
}

impl PersistenceManager {
    pub fn new() -> Self {
        let state = PersistedState::load();
        
        // Reportar jobs incompletos
        let incomplete = state.get_incomplete_jobs();
        if !incomplete.is_empty() {
            println!("[PERSISTENCE] {} jobs incompletos encontrados del estado anterior:", incomplete.len());
            for job in &incomplete {
                println!("  - {} ({}) - {}", job.name, job.id, job.status);
            }
        }
        
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }
    
    /// Actualizar y guardar job
    pub fn save_job(&self, job: &JobInfo) {
        let mut state = self.state.lock().unwrap();
        state.update_job(job);
        
        // Guardar solo si pasó suficiente tiempo
        let now = current_timestamp();
        if now - state.last_saved >= SAVE_INTERVAL_SECS {
            if let Err(e) = state.save() {
                println!("[PERSISTENCE] Error guardando: {}", e);
            }
        }
    }
    
    /// Forzar guardado inmediato
    pub fn flush(&self) {
        let mut state = self.state.lock().unwrap();
        if let Err(e) = state.save() {
            println!("[PERSISTENCE] Error en flush: {}", e);
        } else {
            println!("[PERSISTENCE] Estado guardado a disco");
        }
    }
    
    /// Obtener estado actual
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