// worker/src/operators.rs

use common::{Partition, Record, Task};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

const DATA_DIR: &str = "/tmp/minispark";

/// Asegura que el directorio de datos existe
pub fn ensure_data_dir() {
    fs::create_dir_all(DATA_DIR).ok();
}

/// Genera path para output de una tarea
pub fn output_path(job_id: &str, node_id: &str, partition_id: u32) -> String {
    format!("{}/{}_{}_p{}.json", DATA_DIR, job_id, node_id, partition_id)
}

/// Lee una partición desde archivo
pub fn read_partition(path: &str) -> Result<Partition, String> {
    if !Path::new(path).exists() {
        return Ok(Partition::default());
    }
    
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Error leyendo {}: {}", path, e))?;
    
    serde_json::from_str(&content)
        .map_err(|e| format!("Error parseando {}: {}", path, e))
}

/// Escribe una partición a archivo
pub fn write_partition(path: &str, partition: &Partition) -> Result<(), String> {
    ensure_data_dir();
    
    let content = serde_json::to_string(partition)
        .map_err(|e| format!("Error serializando: {}", e))?;
    
    fs::write(path, content)
        .map_err(|e| format!("Error escribiendo {}: {}", path, e))
}

/// Ejecuta un operador según el tipo
pub fn execute_operator(task: &Task) -> Result<(Partition, u64), String> {
    match task.op.as_str() {
        "read_csv" => op_read_csv(task),
        "map" => op_map(task),
        "filter" => op_filter(task),
        "flat_map" => op_flat_map(task),
        "reduce_by_key" => op_reduce_by_key(task),
        other => Err(format!("Operador desconocido: {}", other)),
    }
}

/// Lee CSV y retorna partición correspondiente
fn op_read_csv(task: &Task) -> Result<(Partition, u64), String> {
    let path = task.input_path.as_ref()
        .ok_or("read_csv requiere input_path")?;
    
    let file = File::open(path)
        .map_err(|e| format!("No se pudo abrir {}: {}", path, e))?;
    
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut line_num = 0;
    
    for line in reader.lines() {
        let line = line.map_err(|e| format!("Error leyendo línea: {}", e))?;
        
        // Saltar header
        if line_num == 0 {
            line_num += 1;
            continue;
        }
        
        // Particionar por número de línea
        if (line_num - 1) % task.total_partitions == task.partition_id {
            // Tomar primera columna como value
            let value = line.split(',').next().unwrap_or(&line).to_string();
            records.push(Record { key: None, value });
        }
        
        line_num += 1;
    }
    
    let count = records.len() as u64;
    Ok((Partition { records }, count))
}

/// Map: aplica función a cada registro
fn op_map(task: &Task) -> Result<(Partition, u64), String> {
    let input = load_input_partitions(task)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("identity");
    
    let records: Vec<Record> = input.records.into_iter()
        .map(|r| apply_map_fn(fn_name, r))
        .collect();
    
    let count = records.len() as u64;
    Ok((Partition { records }, count))
}

/// Filter: filtra registros según función
fn op_filter(task: &Task) -> Result<(Partition, u64), String> {
    let input = load_input_partitions(task)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("not_empty");
    
    let records: Vec<Record> = input.records.into_iter()
        .filter(|r| apply_filter_fn(fn_name, r))
        .collect();
    
    let count = records.len() as u64;
    Ok((Partition { records }, count))
}

/// FlatMap: genera múltiples registros por cada input
fn op_flat_map(task: &Task) -> Result<(Partition, u64), String> {
    let input = load_input_partitions(task)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("split_words");
    
    let records: Vec<Record> = input.records.into_iter()
        .flat_map(|r| apply_flat_map_fn(fn_name, r))
        .collect();
    
    let count = records.len() as u64;
    Ok((Partition { records }, count))
}

/// ReduceByKey: agrupa por clave y reduce
fn op_reduce_by_key(task: &Task) -> Result<(Partition, u64), String> {
    let input = load_input_partitions(task)?;
    let fn_name = task.fn_name.as_deref().unwrap_or("sum");
    
    // Agrupar por clave
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for record in input.records {
        let key = record.key.unwrap_or_else(|| "".to_string());
        groups.entry(key).or_default().push(record.value);
    }
    
    // Reducir cada grupo
    let records: Vec<Record> = groups.into_iter()
        .map(|(key, values)| {
            let reduced = apply_reduce_fn(fn_name, &values);
            Record { key: Some(key), value: reduced }
        })
        .collect();
    
    let count = records.len() as u64;
    Ok((Partition { records }, count))
}

// ============ Funciones auxiliares ============

fn load_input_partitions(task: &Task) -> Result<Partition, String> {
    let mut all_records = Vec::new();
    
    for path in &task.input_partitions {
        let partition = read_partition(path)?;
        all_records.extend(partition.records);
    }
    
    Ok(Partition { records: all_records })
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