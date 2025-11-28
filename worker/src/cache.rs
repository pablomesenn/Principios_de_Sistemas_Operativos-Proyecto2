// worker/src/cache.rs
// Cache en memoria con spill a disco cuando supera umbral

use common::Partition;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_MAX_MEMORY_MB: usize = 128;
const SPILL_DIR: &str = "/tmp/minispark/spill";

/// Entrada de cache con metadata para LRU
struct CacheEntry {
    partition: Partition,
    size_bytes: usize,
    last_access: Instant,
    spilled: bool,
    spill_path: Option<String>,
}

/// Cache de particiones con límite de memoria y spill a disco
pub struct PartitionCache {
    entries: HashMap<String, CacheEntry>,
    current_memory_bytes: usize,
    max_memory_bytes: usize,
    stats: CacheStats,
}

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub spills: u64,
    pub evictions: u64,
    pub current_memory_mb: f64,
    pub max_memory_mb: f64,
    pub cached_partitions: usize,
    pub spilled_partitions: usize,
}

impl PartitionCache {
    pub fn new() -> Self {
        let max_mb = std::env::var("CACHE_MAX_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_MEMORY_MB);
        
        fs::create_dir_all(SPILL_DIR).ok();
        
        println!("[CACHE] Inicializado con límite de {}MB", max_mb);
        
        Self {
            entries: HashMap::new(),
            current_memory_bytes: 0,
            max_memory_bytes: max_mb * 1024 * 1024,
            stats: CacheStats {
                max_memory_mb: max_mb as f64,
                ..Default::default()
            },
        }
    }
    
    /// Obtener partición del cache (memoria o disco)
    pub fn get(&mut self, key: &str) -> Option<Partition> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_access = Instant::now();
            
            if entry.spilled {
                // Leer de disco
                if let Some(ref path) = entry.spill_path {
                    match fs::read_to_string(path) {
                        Ok(content) => {
                            match serde_json::from_str(&content) {
                                Ok(partition) => {
                                    self.stats.hits += 1;
                                    return Some(partition);
                                }
                                Err(e) => {
                                    println!("[CACHE] Error parseando spill {}: {}", path, e);
                                    self.stats.misses += 1;
                                    return None;
                                }
                            }
                        }
                        Err(e) => {
                            println!("[CACHE] Error leyendo spill {}: {}", path, e);
                            self.stats.misses += 1;
                            return None;
                        }
                    }
                }
                self.stats.misses += 1;
                None
            } else {
                self.stats.hits += 1;
                Some(entry.partition.clone())
            }
        } else {
            self.stats.misses += 1;
            None
        }
    }
    
    /// Guardar partición en cache
    pub fn put(&mut self, key: String, partition: Partition) {
        let size = Self::estimate_size(&partition);
        
        // Si ya existe, actualizar
        if let Some(entry) = self.entries.get_mut(&key) {
            if !entry.spilled {
                self.current_memory_bytes -= entry.size_bytes;
            }
            entry.partition = partition;
            entry.size_bytes = size;
            entry.last_access = Instant::now();
            entry.spilled = false;
            entry.spill_path = None;
            self.current_memory_bytes += size;
        } else {
            // Nueva entrada
            self.entries.insert(key, CacheEntry {
                partition,
                size_bytes: size,
                last_access: Instant::now(),
                spilled: false,
                spill_path: None,
            });
            self.current_memory_bytes += size;
        }
        
        // Verificar si necesitamos hacer spill
        self.maybe_spill();
        self.update_stats();
    }
    
    /// Verificar si una key existe en cache
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }
    
    /// Eliminar entrada del cache
    pub fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            if !entry.spilled {
                self.current_memory_bytes -= entry.size_bytes;
            }
            // Eliminar archivo spill si existe
            if let Some(path) = entry.spill_path {
                fs::remove_file(&path).ok();
            }
            self.stats.evictions += 1;
        }
        self.update_stats();
    }
    
    /// Limpiar cache de un job específico
    pub fn clear_job(&mut self, job_id: &str) {
        let keys_to_remove: Vec<String> = self.entries
            .keys()
            .filter(|k| k.contains(job_id))
            .cloned()
            .collect();
        
        for key in keys_to_remove {
            self.remove(&key);
        }
    }
    
    /// Obtener estadísticas del cache
    pub fn stats(&self) -> CacheStats {
        self.stats.clone()
    }
    
    /// Hacer spill de particiones menos usadas a disco
    fn maybe_spill(&mut self) {
        while self.current_memory_bytes > self.max_memory_bytes {
            // Encontrar entrada menos recientemente usada que no esté en spill
            let lru_key = self.entries
                .iter()
                .filter(|(_, e)| !e.spilled)
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone());
            
            if let Some(key) = lru_key {
                self.spill_to_disk(&key);
            } else {
                break; // No hay más entradas para hacer spill
            }
        }
    }
    
    /// Escribir partición a disco y liberar memoria
    fn spill_to_disk(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.spilled {
                return;
            }
            
            let spill_path = format!("{}/{}.spill.json", SPILL_DIR, 
                key.replace('/', "_").replace(':', "_"));
            
            match serde_json::to_string(&entry.partition) {
                Ok(content) => {
                    match fs::write(&spill_path, &content) {
                        Ok(_) => {
                            self.current_memory_bytes -= entry.size_bytes;
                            entry.partition = Partition::default(); // Liberar memoria
                            entry.spilled = true;
                            entry.spill_path = Some(spill_path.clone());
                            self.stats.spills += 1;
                            println!("[CACHE] Spill a disco: {} ({} bytes)", key, entry.size_bytes);
                        }
                        Err(e) => {
                            println!("[CACHE] Error escribiendo spill: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("[CACHE] Error serializando para spill: {}", e);
                }
            }
        }
    }
    
    /// Estimar tamaño en bytes de una partición
    fn estimate_size(partition: &Partition) -> usize {
        let mut size = std::mem::size_of::<Partition>();
        for record in &partition.records {
            size += std::mem::size_of::<common::Record>();
            size += record.value.len();
            if let Some(ref key) = record.key {
                size += key.len();
            }
        }
        size
    }
    
    fn update_stats(&mut self) {
        self.stats.current_memory_mb = self.current_memory_bytes as f64 / (1024.0 * 1024.0);
        self.stats.cached_partitions = self.entries.iter().filter(|(_, e)| !e.spilled).count();
        self.stats.spilled_partitions = self.entries.iter().filter(|(_, e)| e.spilled).count();
    }
}

impl Default for PartitionCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache global thread-safe
pub type SharedCache = Arc<Mutex<PartitionCache>>;

pub fn new_shared_cache() -> SharedCache {
    Arc::new(Mutex::new(PartitionCache::new()))
}