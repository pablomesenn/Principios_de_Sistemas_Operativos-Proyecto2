// worker/src/main.rs

mod operators;

use common::{Heartbeat, Task, TaskResult, WorkerRegistration};
use reqwest::Client;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// Configuración de tolerancia a fallos
const HEARTBEAT_INTERVAL_SECS: u64 = 2;
const POLL_INTERVAL_MS: u64 = 100;
const POLL_INTERVAL_NO_TASK_MS: u64 = 200;
const POLL_INTERVAL_ERROR_SECS: u64 = 1;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    operators::ensure_data_dir();
    
    let client = Client::new();
    let master_url = std::env::var("MASTER_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let worker_port: u16 = std::env::var("WORKER_PORT")
        .unwrap_or_else(|_| "9000".into())
        .parse()
        .unwrap_or(9000);
    
    // Probabilidad de fallo simulado (0-100)
    let fail_probability: u32 = std::env::var("FAIL_PROBABILITY")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap_or(0);

    println!("=== Mini-Spark Worker ===");
    println!("Master: {}", master_url);
    println!("Puerto: {}", worker_port);
    if fail_probability > 0 {
        println!("MODO TEST: Probabilidad de fallo simulado: {}%", fail_probability);
    }
    println!();

    let reg = WorkerRegistration {
        id: Uuid::nil(),
        host: "127.0.0.1".into(),
        port: worker_port,
    };

    let res = client
        .post(format!("{}/api/v1/workers/register", master_url))
        .json(&reg)
        .send()
        .await?;

    let reg_res: WorkerRegistration = res.json().await?;
    let worker_id = reg_res.id;
    println!("[WORKER] Registrado con ID: {}", worker_id);

    let active_tasks = Arc::new(AtomicU32::new(0));

    // Heartbeats
    let hb_client = client.clone();
    let hb_master_url = master_url.clone();
    let hb_active_tasks = active_tasks.clone();
    tokio::spawn(async move {
        loop {
            let hb = Heartbeat {
                worker_id,
                active_tasks: hb_active_tasks.load(Ordering::Relaxed),
            };

            let result = hb_client
                .post(format!("{}/api/v1/workers/heartbeat", hb_master_url))
                .json(&hb)
                .send()
                .await;
            
            if let Err(e) = result {
                println!("[HEARTBEAT] Error enviando heartbeat: {}", e);
            }

            tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
        }
    });

    // Loop principal
    loop {
        let task_result = client
            .get(format!(
                "{}/api/v1/workers/task?worker_id={}",
                master_url, worker_id
            ))
            .send()
            .await;

        match task_result {
            Ok(res) if res.status().is_success() => {
                let task: Task = match res.json().await {
                    Ok(t) => t,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                        continue;
                    }
                };

                println!("[TASK] Ejecutando: {} (op={}, partition={}, attempt={})", 
                    task.id, task.op, task.partition_id, task.attempt);
                
                active_tasks.fetch_add(1, Ordering::Relaxed);

                let result = execute_task_with_failure_simulation(&task, fail_probability);

                let send_result = client
                    .post(format!("{}/api/v1/workers/task/complete", master_url))
                    .json(&result)
                    .send()
                    .await;

                if let Err(e) = send_result {
                    println!("[TASK] Error reportando resultado: {}", e);
                }

                active_tasks.fetch_sub(1, Ordering::Relaxed);
                
                if result.success {
                    println!("[TASK] OK: {} registros procesados", result.records_processed);
                    if !result.shuffle_outputs.is_empty() {
                        println!("[TASK] Shuffle outputs: {}", result.shuffle_outputs.len());
                    }
                } else {
                    println!("[TASK] ERROR: {:?}", result.error);
                }
            }
            Ok(res) if res.status() == reqwest::StatusCode::NO_CONTENT => {
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_NO_TASK_MS)).await;
            }
            Ok(res) if res.status() == reqwest::StatusCode::FORBIDDEN => {
                println!("[WORKER] Rechazado por master - posiblemente marcado DOWN");
                tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_ERROR_SECS)).await;
            }
            _ => {
                tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_ERROR_SECS)).await;
            }
        }
    }
}

fn execute_task_with_failure_simulation(task: &Task, fail_probability: u32) -> TaskResult {
    // Simular fallo aleatorio si está habilitado
    if fail_probability > 0 {
        let random: u32 = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()) % 100;
        
        if random < fail_probability {
            println!("[TASK] FALLO SIMULADO para tarea {}", task.id);
            return TaskResult {
                task_id: task.id,
                job_id: task.job_id,
                attempt: task.attempt,
                success: false,
                error: Some("Fallo simulado para testing".to_string()),
                output_path: None,
                records_processed: 0,
                shuffle_outputs: vec![],
            };
        }
    }

    execute_task(task)
}

fn execute_task(task: &Task) -> TaskResult {
    let output_path = operators::output_path(
        &task.job_id.to_string(),
        &task.node_id,
        task.partition_id,
    );

    match operators::execute_operator(task) {
        Ok(result) => {
            // Guardar resultado (excepto shuffle_write que ya guardó)
            if task.op != "shuffle_write" {
                if let Err(e) = operators::write_partition(&output_path, &result.partition) {
                    return TaskResult {
                        task_id: task.id,
                        job_id: task.job_id,
                        attempt: task.attempt,
                        success: false,
                        error: Some(e),
                        output_path: None,
                        records_processed: 0,
                        shuffle_outputs: vec![],
                    };
                }
            }

            TaskResult {
                task_id: task.id,
                job_id: task.job_id,
                attempt: task.attempt,
                success: true,
                error: None,
                output_path: Some(output_path),
                records_processed: result.records_processed,
                shuffle_outputs: result.shuffle_outputs,
            }
        }
        Err(e) => TaskResult {
            task_id: task.id,
            job_id: task.job_id,
            attempt: task.attempt,
            success: false,
            error: Some(e),
            output_path: None,
            records_processed: 0,
            shuffle_outputs: vec![],
        },
    }
}