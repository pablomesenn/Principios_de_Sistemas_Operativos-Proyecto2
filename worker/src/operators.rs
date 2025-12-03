// worker/src/operators.rs
// Operadores de procesamiento de datos con pruebas unitarias

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

fn load_input_partitions(task: &Task, cache: &SharedCache) -> Result<Partition, String> {
    let mut all_records = Vec::new();
    
    for path in &task.input_partitions {
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

fn path_to_cache_key(path: &str, job_id: &str) -> Option<String> {
    let filename = path.rsplit('/').next()?;
    let prefix = format!("{}_", job_id);
    let rest = filename.strip_prefix(&prefix)?;
    
    let mut last_p_pos = None;
    let chars: Vec<char> = rest.chars().collect();
    
    for i in 0..chars.len().saturating_sub(2) {
        if chars[i] == '_' && chars[i + 1] == 'p' && chars.get(i + 2).map(|c| c.is_ascii_digit()).unwrap_or(false) {
            last_p_pos = Some(i);
        }
    }
    
    let p_pos = last_p_pos?;
    let node_id = &rest[..p_pos];
    let partition_str = &rest[p_pos + 2..];
    let partition_id: u32 = partition_str.strip_suffix(".json")?.parse().ok()?;
    
    Some(format!("{}:{}:{}", job_id, node_id, partition_id))
}

pub fn hash_key(key: &str, num_partitions: u32) -> u32 {
    let mut hash: u32 = 0;
    for byte in key.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
    }
    hash % num_partitions
}

pub fn apply_map_fn(fn_name: &str, record: Record) -> Record {
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

pub fn apply_filter_fn(fn_name: &str, record: &Record) -> bool {
    match fn_name {
        "not_empty" => !record.value.trim().is_empty(),
        "is_long" => record.value.len() > 5,
        "has_key" => record.key.is_some(),
        _ => true,
    }
}

pub fn apply_flat_map_fn(fn_name: &str, record: Record) -> Vec<Record> {
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
        "split_lines" => {
            record.value
                .lines()
                .map(|line| Record {
                    key: None,
                    value: line.to_string(),
                })
                .collect()
        }
        _ => vec![record],
    }
}

pub fn apply_reduce_fn(fn_name: &str, values: &[String]) -> String {
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
        "avg" => {
            let nums: Vec<i64> = values.iter()
                .filter_map(|v| v.parse::<i64>().ok())
                .collect();
            if nums.is_empty() {
                "0".to_string()
            } else {
                (nums.iter().sum::<i64>() / nums.len() as i64).to_string()
            }
        }
        _ => values.first().cloned().unwrap_or_default(),
    }
}

// ============ PRUEBAS UNITARIAS ============

#[cfg(test)]
mod tests {
    use super::*;
    use common::Record;

    // ============ Tests de Map ============

    #[test]
    fn test_map_to_lower() {
        let record = Record { key: None, value: "HELLO WORLD".to_string() };
        let result = apply_map_fn("to_lower", record);
        assert_eq!(result.value, "hello world");
    }

    #[test]
    fn test_map_to_upper() {
        let record = Record { key: None, value: "hello world".to_string() };
        let result = apply_map_fn("to_upper", record);
        assert_eq!(result.value, "HELLO WORLD");
    }

    #[test]
    fn test_map_pair_with_one() {
        let record = Record { key: None, value: "word".to_string() };
        let result = apply_map_fn("pair_with_one", record);
        assert_eq!(result.key, Some("word".to_string()));
        assert_eq!(result.value, "1");
    }

    #[test]
    fn test_map_extract_key() {
        let record = Record { key: None, value: "product_1,laptop".to_string() };
        let result = apply_map_fn("extract_key", record);
        assert_eq!(result.key, Some("product_1".to_string()));
        assert_eq!(result.value, "laptop");
    }

    #[test]
    fn test_map_identity() {
        let record = Record { key: Some("k".to_string()), value: "v".to_string() };
        let result = apply_map_fn("identity", record.clone());
        assert_eq!(result.key, record.key);
        assert_eq!(result.value, record.value);
    }

    // ============ Tests de Filter ============

    #[test]
    fn test_filter_not_empty_true() {
        let record = Record { key: None, value: "hello".to_string() };
        assert!(apply_filter_fn("not_empty", &record));
    }

    #[test]
    fn test_filter_not_empty_false() {
        let record = Record { key: None, value: "   ".to_string() };
        assert!(!apply_filter_fn("not_empty", &record));
    }

    #[test]
    fn test_filter_is_long_true() {
        let record = Record { key: None, value: "hello world".to_string() };
        assert!(apply_filter_fn("is_long", &record));
    }

    #[test]
    fn test_filter_is_long_false() {
        let record = Record { key: None, value: "hi".to_string() };
        assert!(!apply_filter_fn("is_long", &record));
    }

    #[test]
    fn test_filter_has_key_true() {
        let record = Record { key: Some("k".to_string()), value: "v".to_string() };
        assert!(apply_filter_fn("has_key", &record));
    }

    #[test]
    fn test_filter_has_key_false() {
        let record = Record { key: None, value: "v".to_string() };
        assert!(!apply_filter_fn("has_key", &record));
    }

    // ============ Tests de FlatMap ============

    #[test]
    fn test_flatmap_split_words() {
        let record = Record { key: None, value: "hello world foo".to_string() };
        let results = apply_flat_map_fn("split_words", record);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, "hello");
        assert_eq!(results[1].value, "world");
        assert_eq!(results[2].value, "foo");
    }

    #[test]
    fn test_flatmap_split_chars() {
        let record = Record { key: None, value: "abc".to_string() };
        let results = apply_flat_map_fn("split_chars", record);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, "a");
        assert_eq!(results[1].value, "b");
        assert_eq!(results[2].value, "c");
    }

    #[test]
    fn test_flatmap_split_lines() {
        let record = Record { key: None, value: "line1\nline2\nline3".to_string() };
        let results = apply_flat_map_fn("split_lines", record);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, "line1");
        assert_eq!(results[1].value, "line2");
        assert_eq!(results[2].value, "line3");
    }

    #[test]
    fn test_flatmap_empty_input() {
        let record = Record { key: None, value: "".to_string() };
        let results = apply_flat_map_fn("split_words", record);
        assert_eq!(results.len(), 0);
    }

    // ============ Tests de Reduce ============

    #[test]
    fn test_reduce_sum() {
        let values = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        assert_eq!(apply_reduce_fn("sum", &values), "6");
    }

    #[test]
    fn test_reduce_sum_with_invalid() {
        let values = vec!["1".to_string(), "invalid".to_string(), "3".to_string()];
        assert_eq!(apply_reduce_fn("sum", &values), "4");
    }

    #[test]
    fn test_reduce_count() {
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(apply_reduce_fn("count", &values), "3");
    }

    #[test]
    fn test_reduce_concat() {
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(apply_reduce_fn("concat", &values), "a,b,c");
    }

    #[test]
    fn test_reduce_min() {
        let values = vec!["5".to_string(), "1".to_string(), "9".to_string()];
        assert_eq!(apply_reduce_fn("min", &values), "1");
    }

    #[test]
    fn test_reduce_max() {
        let values = vec!["5".to_string(), "1".to_string(), "9".to_string()];
        assert_eq!(apply_reduce_fn("max", &values), "9");
    }

    #[test]
    fn test_reduce_avg() {
        let values = vec!["10".to_string(), "20".to_string(), "30".to_string()];
        assert_eq!(apply_reduce_fn("avg", &values), "20");
    }

    #[test]
    fn test_reduce_empty() {
        let values: Vec<String> = vec![];
        assert_eq!(apply_reduce_fn("sum", &values), "0");
        assert_eq!(apply_reduce_fn("count", &values), "0");
    }

    // ============ Tests de Hash ============

    #[test]
    fn test_hash_key_deterministic() {
        let key = "test_key";
        let hash1 = hash_key(key, 4);
        let hash2 = hash_key(key, 4);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_key_distribution() {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let mut partitions = vec![0u32; 4];
        
        for key in keys {
            let partition = hash_key(key, 4);
            assert!(partition < 4);
            partitions[partition as usize] += 1;
        }
        
        // Verificar que hay al menos algo de distribución
        assert!(partitions.iter().filter(|&&c| c > 0).count() >= 2);
    }

    #[test]
    fn test_hash_key_empty() {
        let partition = hash_key("", 4);
        assert!(partition < 4);
    }

    // ============ Tests de Partition I/O ============

    #[test]
    fn test_write_read_partition() {
        ensure_data_dir();
        
        let partition = Partition {
            records: vec![
                Record { key: Some("k1".to_string()), value: "v1".to_string() },
                Record { key: Some("k2".to_string()), value: "v2".to_string() },
            ],
        };
        
        let path = "/tmp/minispark/test_partition.json";
        write_partition(path, &partition).unwrap();
        
        let read_back = read_partition(path).unwrap();
        assert_eq!(read_back.records.len(), 2);
        assert_eq!(read_back.records[0].key, Some("k1".to_string()));
        assert_eq!(read_back.records[0].value, "v1");
        
        // Limpiar
        fs::remove_file(path).ok();
    }

    #[test]
    fn test_read_nonexistent_partition() {
        let result = read_partition("/tmp/minispark/nonexistent.json");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().records.len(), 0);
    }

    // ============ Tests de Path Utilities ============

    #[test]
    fn test_output_path() {
        let path = output_path("job123", "map", 2);
        assert_eq!(path, "/tmp/minispark/job123_map_p2.json");
    }

    #[test]
    fn test_shuffle_output_path() {
        let path = shuffle_output_path("job123", "reduce", 1, 3);
        assert_eq!(path, "/tmp/minispark/job123_reduce_shuffle_p1_to_p3.json");
    }

    #[test]
    fn test_path_to_cache_key() {
        let path = "/tmp/minispark/job123_map_p2.json";
        let key = path_to_cache_key(path, "job123");
        assert_eq!(key, Some("job123:map:2".to_string()));
    }

    #[test]
    fn test_path_to_cache_key_complex_node() {
        let path = "/tmp/minispark/job123_reduce_by_key_p0.json";
        let key = path_to_cache_key(path, "job123");
        assert_eq!(key, Some("job123:reduce_by_key:0".to_string()));
    }

    // ============ Tests de integración simple ============

    #[test]
    fn test_wordcount_pipeline() {
        // Simular pipeline: split_words -> pair_with_one -> reduce sum
        let input = Record { key: None, value: "hello world hello".to_string() };
        
        // Step 1: flat_map split_words
        let words = apply_flat_map_fn("split_words", input);
        assert_eq!(words.len(), 3);
        
        // Step 2: map pair_with_one
        let pairs: Vec<Record> = words.into_iter()
            .map(|r| apply_map_fn("pair_with_one", r))
            .collect();
        
        assert_eq!(pairs.len(), 3);
        assert!(pairs.iter().all(|r| r.value == "1"));
        
        // Step 3: group by key y reduce sum
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();
        for record in pairs {
            let key = record.key.unwrap_or_default();
            groups.entry(key).or_default().push(record.value);
        }
        
        let results: HashMap<String, String> = groups.into_iter()
            .map(|(k, v)| (k, apply_reduce_fn("sum", &v)))
            .collect();
        
        assert_eq!(results.get("hello"), Some(&"2".to_string()));
        assert_eq!(results.get("world"), Some(&"1".to_string()));
    }
}