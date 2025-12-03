// In-memory cache with spill to disk when memory threshold is exceeded

use common::Partition;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_MAX_MEMORY_MB: usize = 128;
/// Directory where spilled partitions are stored on disk
const SPILL_DIR: &str = "/tmp/minispark/spill";

/// Entrada de cache con metadata para LRU
/// Single cache entry with LRU metadata and optional spill info
struct CacheEntry {
    /// Cached partition (only valid if `spilled == false`)
    partition: Partition,
    /// Estimated size of the partition in bytes
    size_bytes: usize,
    /// Last time this entry was accessed (for LRU eviction)
    last_access: Instant,
    /// Whether this partition has been spilled to disk
    spilled: bool,
    /// Path to spill file on disk, if spilled
    spill_path: Option<String>,
}

/// Cache de particiones con límite de memoria y spill a disco
/// Partition cache with memory limit and automatic spill-to-disk for LRU entries
pub struct PartitionCache {
    /// Map from cache key to cache entry
    entries: HashMap<String, CacheEntry>,
    /// Current memory usage in bytes (for entries still in memory)
    current_memory_bytes: usize,
    /// Maximum allowed memory usage in bytes before spilling
    max_memory_bytes: usize,
    /// Aggregated cache statistics
    stats: CacheStats,
}

/// Public cache statistics structure, used by metrics endpoints
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of successful cache lookups
    pub hits: u64,
    /// Number of failed cache lookups
    pub misses: u64,
    /// Number of times an entry has been spilled to disk
    pub spills: u64,
    /// Number of evicted entries (removed from cache)
    pub evictions: u64,
    /// Current memory usage in MB
    pub current_memory_mb: f64,
    /// Maximum configured memory in MB
    pub max_memory_mb: f64,
    /// Number of partitions currently kept in memory
    pub cached_partitions: usize,
    /// Number of partitions that have been spilled to disk
    pub spilled_partitions: usize,
}

impl PartitionCache {
    /// Create a new cache, reading the max size from CACHE_MAX_MB (or using default)
    pub fn new() -> Self {
        let max_mb = std::env::var("CACHE_MAX_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_MEMORY_MB);

        // Ensure spill directory exists (ignore errors)
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
    /// Get partition from cache (either from memory or from spilled file)
    pub fn get(&mut self, key: &str) -> Option<Partition> {
        if let Some(entry) = self.entries.get_mut(key) {
            // Update LRU access time
            entry.last_access = Instant::now();

            if entry.spilled {
                // Entry is spilled: read from disk
                if let Some(ref path) = entry.spill_path {
                    match fs::read_to_string(path) {
                        Ok(content) => match serde_json::from_str(&content) {
                            Ok(partition) => {
                                self.stats.hits += 1;
                                return Some(partition);
                            }
                            Err(e) => {
                                println!("[CACHE] Error parseando spill {}: {}", path, e);
                                self.stats.misses += 1;
                                return None;
                            }
                        },
                        Err(e) => {
                            println!("[CACHE] Error leyendo spill {}: {}", path, e);
                            self.stats.misses += 1;
                            return None;
                        }
                    }
                }
                // If there was no valid spill path, count as miss
                self.stats.misses += 1;
                None
            } else {
                // Entry is still in memory
                self.stats.hits += 1;
                Some(entry.partition.clone())
            }
        } else {
            // Entry not found in cache
            self.stats.misses += 1;
            None
        }
    }

    /// Guardar partición en cache
    /// Insert or update a partition in the cache
    pub fn put(&mut self, key: String, partition: Partition) {
        let size = Self::estimate_size(&partition);

        // Si ya existe, actualizar
        // If the key already exists, update the existing entry
        if let Some(entry) = self.entries.get_mut(&key) {
            // If entry was in memory, adjust current memory usage
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
            // New entry
            self.entries.insert(
                key,
                CacheEntry {
                    partition,
                    size_bytes: size,
                    last_access: Instant::now(),
                    spilled: false,
                    spill_path: None,
                },
            );
            self.current_memory_bytes += size;
        }

        // Verificar si necesitamos hacer spill
        // Check if we need to spill to disk due to memory pressure
        self.maybe_spill();
        self.update_stats();
    }

    /// Verificar si una key existe en cache
    /// Check if a key exists in the cache (either in memory or spilled)
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Eliminar entrada del cache
    /// Remove a cache entry and delete spill file if present
    pub fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            // Adjust memory usage only if it was still in memory
            if !entry.spilled {
                self.current_memory_bytes -= entry.size_bytes;
            }
            // Eliminar archivo spill si existe
            // Remove spill file on disk if any
            if let Some(path) = entry.spill_path {
                fs::remove_file(&path).ok();
            }
            self.stats.evictions += 1;
        }
        self.update_stats();
    }

    /// Limpiar cache de un job específico
    /// Remove all cache entries whose key contains the given job_id
    pub fn clear_job(&mut self, job_id: &str) {
        let keys_to_remove: Vec<String> = self
            .entries
            .keys()
            .filter(|k| k.contains(job_id))
            .cloned()
            .collect();

        for key in keys_to_remove {
            self.remove(&key);
        }
    }

    /// Obtener estadísticas del cache
    /// Return a snapshot of current cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.clone()
    }

    /// Hacer spill de particiones menos usadas a disco
    /// Spill least recently used (LRU) partitions to disk until under memory limit
    fn maybe_spill(&mut self) {
        while self.current_memory_bytes > self.max_memory_bytes {
            // Encontrar entrada menos recientemente usada que no esté en spill
            // Find LRU entry that is still in memory (not already spilled)
            let lru_key = self
                .entries
                .iter()
                .filter(|(_, e)| !e.spilled)
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone());

            if let Some(key) = lru_key {
                self.spill_to_disk(&key);
            } else {
                // No more entries available to spill (all are already spilled)
                break; // No hay más entradas para hacer spill
            }
        }
    }

    /// Escribir partición a disco y liberar memoria
    /// Serialize a partition to disk and mark the entry as spilled (freeing memory)
    fn spill_to_disk(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            // Avoid duplicating spill for the same entry
            if entry.spilled {
                return;
            }

            // Generate a safe filename based on the cache key
            let spill_path = format!(
                "{}/{}.spill.json",
                SPILL_DIR,
                key.replace('/', "_").replace(':', "_")
            );

            // Serialize partition as JSON
            match serde_json::to_string(&entry.partition) {
                Ok(content) => {
                    // Write serialized data to disk
                    match fs::write(&spill_path, &content) {
                        Ok(_) => {
                            // Adjust memory usage and mark entry as spilled
                            self.current_memory_bytes -= entry.size_bytes;
                            entry.partition = Partition::default(); // Liberar memoria
                            entry.spilled = true;
                            entry.spill_path = Some(spill_path.clone());
                            self.stats.spills += 1;
                            println!(
                                "[CACHE] Spill a disco: {} ({} bytes)",
                                key, entry.size_bytes
                            );
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
    /// Rough estimation of a partition size in bytes (records + strings)
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

    /// Update derived statistics such as memory usage and counts of entries
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
/// Global, thread-safe handle to the partition cache
pub type SharedCache = Arc<Mutex<PartitionCache>>;

/// Helper to create a new shared cache instance
pub fn new_shared_cache() -> SharedCache {
    Arc::new(Mutex::new(PartitionCache::new()))
}
