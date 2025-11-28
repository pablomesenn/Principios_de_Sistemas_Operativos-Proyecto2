// worker/src/operators.rs

use crate::cache::SharedCache;
use common::{Partition, Record, Task};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

const DATA_DIR: &str = "/tmp/minispark";

pub fn ensure_data_dir() {
    fs::create_dir_all(DATA_DIR).ok();
    fs::create_dir_all(format!("{}/spill", DATA_DIR)).ok();
}

pub fn output_path(job_id: &str, node_id: &str, partition_id: u32) -> String {
    format!("{}/{}_{}_p{}.json", DATA_DIR, job_id, node_id, partition_id)
}

pub fn shuffle_output_path(job_id: &str, node_id: &str, from_part: u32, to_part: u32) -> String {
    format!("{}/{}_{}_shuffle_p{}_to_p{}.json", DATA_DIR, job_id, node_id, from_part, to_part)
}

pub fn read_partition(path: &str) -> Result<Partition, String> {
    if !Path::new(path).exists() {
        return Ok(Partition::default());
    }
    
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Error leyendo {}: {}", path, e))?;
    
    serde_json::from_str(&content)
        .map_err(|e| format!("Error parseando {}: {}", path, e))
}

pub fn write_partition(path: &str, partition: &Partition) -> Result<(), String> {
    ensure_data_dir();
    
    let content = serde_json::to_string(partition)
        .map_err(|e| format!("Error serializando: {}", e))?;
    
    fs::write(path, content)
        .map_err(|e| format!("Error escribiendo {}: {}", path, e))
}

/// Resultado de ejecución incluyendo shuffle outputs
pub struct ExecutionResult {
    pub partition: Partition,
    pub records_processed: u64,
    pub shuffle_outputs: Vec<String>,
}

pub fn execute_operator(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    match task.op.as_str() {
        "read_csv" => op_read_csv(task),
        "read_jsonl" => op_read_jsonl(task),
        "map" => op_map(task, cache),
        "filter" => op_filter(task, cache),
        "flat_map" => op_flat_map(task, cache),
        "reduce_by_key" => op_reduce_by_key(task, cache),
        "shuffle_write" => op_shuffle_write(task, cache),
        "shuffle_read" => op_shuffle_read(task),
        "join" => op_join(task, cache),
        other => Err(format!("Operador desconocido: {}", other)),
    }
}

fn op_read_csv(task: &Task) -> Result<ExecutionResult, String> {
    let path = task.input_path.as_ref()
        .ok_or("read_csv requiere input_path")?;
    
    let file = File::open(path)
        .map_err(|e| format!("No se pudo abrir {}: {}", path, e))?;
    
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut line_num = 0;
    
    for line in reader.lines() {
        let line = line.map_err(|e| format!("Error leyendo línea: {}", e))?;
        
        if line_num == 0 {
            line_num += 1;
            continue;
        }
        
        if (line_num - 1) % task.total_partitions == task.partition_id {
            let value = line.split(',').next().unwrap_or(&line).to_string();
            records.push(Record { key: None, value });
        }
        
        line_num += 1;
    }
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_read_jsonl(task: &Task) -> Result<ExecutionResult, String> {
    let path = task.input_path.as_ref()
        .ok_or("read_jsonl requiere input_path")?;
    
    let file = File::open(path)
        .map_err(|e| format!("No se pudo abrir {}: {}", path, e))?;
    
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut line_num = 0;
    
    for line in reader.lines() {
        let line = line.map_err(|e| format!("Error leyendo línea: {}", e))?;
        
        if line_num % task.total_partitions == task.partition_id {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
                let value = json.to_string();
                let key = json.get("key")
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string());
                records.push(Record { key, value });
            }
        }
        
        line_num += 1;
    }
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_map(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    let input = load_input_partitions(task, cache)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("identity");
    
    let records: Vec<Record> = input.records.into_iter()
        .map(|r| apply_map_fn(fn_name, r))
        .collect();
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_filter(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    let input = load_input_partitions(task, cache)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("not_empty");
    
    let records: Vec<Record> = input.records.into_iter()
        .filter(|r| apply_filter_fn(fn_name, r))
        .collect();
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_flat_map(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    let input = load_input_partitions(task, cache)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("split_words");
    
    let records: Vec<Record> = input.records.into_iter()
        .flat_map(|r| apply_flat_map_fn(fn_name, r))
        .collect();
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_reduce_by_key(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    let input = if task.is_shuffle_read {
        load_shuffle_inputs(task)?
    } else {
        load_input_partitions(task, cache)?
    };
    
    let fn_name = task.fn_name.as_deref().unwrap_or("sum");
    
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for record in input.records {
        let key = record.key.unwrap_or_default();
        groups.entry(key).or_default().push(record.value);
    }
    
    let records: Vec<Record> = groups.into_iter()
        .map(|(key, values)| {
            let reduced = apply_reduce_fn(fn_name, &values);
            Record { key: Some(key), value: reduced }
        })
        .collect();
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_shuffle_write(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    let input = load_input_partitions(task, cache)?;
    let total_partitions = task.total_partitions;
    
    let mut buckets: HashMap<u32, Vec<Record>> = HashMap::new();
    for i in 0..total_partitions {
        buckets.insert(i, Vec::new());
    }
    
    for record in input.records {
        let key = record.key.as_deref().unwrap_or("");
        let target_partition = hash_key(key, total_partitions);
        buckets.get_mut(&target_partition).unwrap().push(record);
    }
    
    let mut shuffle_outputs = Vec::new();
    let mut total_records = 0u64;
    
    for (target_part, records) in buckets {
        let path = shuffle_output_path(
            &task.job_id.to_string(),
            &task.node_id,
            task.partition_id,
            target_part,
        );
        
        total_records += records.len() as u64;
        let partition = Partition { records };
        write_partition(&path, &partition)?;
        shuffle_outputs.push(path);
    }
    
    Ok(ExecutionResult {
        partition: Partition::default(),
        records_processed: total_records,
        shuffle_outputs,
    })
}

fn op_shuffle_read(task: &Task) -> Result<ExecutionResult, String> {
    let partition = load_shuffle_inputs(task)?;
    let count = partition.records.len() as u64;
    
    Ok(ExecutionResult {
        partition,
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

fn op_join(task: &Task, cache: &SharedCache) -> Result<ExecutionResult, String> {
    let left = load_input_partitions(task, cache)?;
    let right = load_join_partitions(task, cache)?;
    
    let mut right_index: HashMap<String, Vec<String>> = HashMap::new();
    for record in right.records {
        let key = record.key.unwrap_or_default();
        right_index.entry(key).or_default().push(record.value);
    }
    
    let mut records = Vec::new();
    for left_record in left.records {
        let key = left_record.key.clone().unwrap_or_default();
        
        if let Some(right_values) = right_index.get(&key) {
            for right_value in right_values {
                records.push(Record {
                    key: Some(key.clone()),
                    value: format!("({},{})", left_record.value, right_value),
                });
            }
        }
    }
    
    let count = records.len() as u64;
    Ok(ExecutionResult {
        partition: Partition { records },
        records_processed: count,
        shuffle_outputs: vec![],
    })
}

// ============ Funciones auxiliares ============

/// Cargar particiones de input, intentando primero del cache
fn load_input_partitions(task: &Task, cache: &SharedCache) -> Result<Partition, String> {
    let mut all_records = Vec::new();
    
    for path in &task.input_partitions {
        // Intentar extraer cache key del path
        if let Some(cache_key) = path_to_cache_key(path, &task.job_id.to_string()) {
            let mut cache_guard = cache.lock().unwrap();
            if let Some(cached) = cache_guard.get(&cache_key) {
                all_records.extend(cached.records);
                continue;
            }
        }
        
        // Fallback: leer de disco
        let partition = read_partition(path)?;
        all_records.extend(partition.records);
    }
    
    Ok(Partition { records: all_records })
}

fn load_shuffle_inputs(task: &Task) -> Result<Partition, String> {
    let mut all_records = Vec::new();
    
    let pattern = "_shuffle_p";
    let target = format!("_to_p{}.json", task.partition_id);
    
    if let Ok(entries) = fs::read_dir(DATA_DIR) {
        for entry in entries.flatten() {
            let path = entry.path();
            let filename = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            
            if filename.contains(&task.job_id.to_string()) 
                && filename.contains(pattern)
                && filename.ends_with(&target) 
            {
                let partition = read_partition(path.to_str().unwrap())?;
                all_records.extend(partition.records);
            }
        }
    }
    
    Ok(Partition { records: all_records })
}

fn load_join_partitions(task: &Task, cache: &SharedCache) -> Result<Partition, String> {
    let mut all_records = Vec::new();
    
    for path in &task.join_partitions {
        // Intentar cache primero
        if let Some(cache_key) = path_to_cache_key(path, &task.job_id.to_string()) {
            let mut cache_guard = cache.lock().unwrap();
            if let Some(cached) = cache_guard.get(&cache_key) {
                all_records.extend(cached.records);
                continue;
            }
        }
        
        let partition = read_partition(path)?;
        all_records.extend(partition.records);
    }
    
    Ok(Partition { records: all_records })
}

/// Convertir path de archivo a cache key
fn path_to_cache_key(path: &str, job_id: &str) -> Option<String> {
    // Path format: /tmp/minispark/{job_id}_{node_id}_p{partition}.json
    let filename = path.rsplit('/').next()?;
    let prefix = format!("{}_", job_id);
    let rest = filename.strip_prefix(&prefix)?;
    
    // Buscar "_p" seguido de dígito
    let mut last_p_pos = None;
    let chars: Vec<char> = rest.chars().collect();
    
    for i in 0..chars.len().saturating_sub(2) {
        if chars[i] == '_' && chars[i + 1] == 'p' && chars.get(i + 2).map(|c| c.is_ascii_digit()).unwrap_or(false) {
            last_p_pos = Some(i);
        }
    }
    
    let p_pos = last_p_pos?;
    let node_id = &rest[..p_pos];
    
    // Extraer partition_id
    let partition_str = &rest[p_pos + 2..];
    let partition_id: u32 = partition_str.strip_suffix(".json")?.parse().ok()?;
    
    Some(format!("{}:{}:{}", job_id, node_id, partition_id))
}

fn hash_key(key: &str, num_partitions: u32) -> u32 {
    let mut hash: u32 = 0;
    for byte in key.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
    }
    hash % num_partitions
}

fn apply_map_fn(fn_name: &str, record: Record) -> Record {
    match fn_name {
        "to_lower" => Record {
            key: record.key,
            value: record.value.to_lowercase(),
        },
        "to_upper" => Record {
            key: record.key,
            value: record.value.to_uppercase(),
        },
        "pair_with_one" => Record {
            key: Some(record.value),
            value: "1".to_string(),
        },
        "extract_key" => {
            let parts: Vec<&str> = record.value.splitn(2, ',').collect();
            Record {
                key: Some(parts[0].to_string()),
                value: parts.get(1).unwrap_or(&"").to_string(),
            }
        },
        "identity" | _ => record,
    }
}

fn apply_filter_fn(fn_name: &str, record: &Record) -> bool {
    match fn_name {
        "not_empty" => !record.value.trim().is_empty(),
        "is_long" => record.value.len() > 5,
        _ => true,
    }
}

fn apply_flat_map_fn(fn_name: &str, record: Record) -> Vec<Record> {
    match fn_name {
        "split_words" => {
            record.value
                .split_whitespace()
                .map(|word| Record {
                    key: None,
                    value: word.to_string(),
                })
                .collect()
        }
        "split_chars" => {
            record.value
                .chars()
                .map(|c| Record {
                    key: None,
                    value: c.to_string(),
                })
                .collect()
        }
        _ => vec![record],
    }
}

fn apply_reduce_fn(fn_name: &str, values: &[String]) -> String {
    match fn_name {
        "sum" => {
            let sum: i64 = values.iter()
                .filter_map(|v| v.parse::<i64>().ok())
                .sum();
            sum.to_string()
        }
        "count" => values.len().to_string(),
        "concat" => values.join(","),
        "min" => {
            values.iter()
                .filter_map(|v| v.parse::<i64>().ok())
                .min()
                .map(|v| v.to_string())
                .unwrap_or_default()
        }
        "max" => {
            values.iter()
                .filter_map(|v| v.parse::<i64>().ok())
                .max()
                .map(|v| v.to_string())
                .unwrap_or_default()
        }
        _ => values.first().cloned().unwrap_or_default(),
    }
}