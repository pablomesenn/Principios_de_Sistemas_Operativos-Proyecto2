// worker/src/main.rs

mod operators;

use common::{Heartbeat, Task, TaskResult, WorkerRegistration};
use reqwest::Client;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    operators::ensure_data_dir();
    
    let client = Client::new();
    let master_url = std::env::var("MASTER_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let worker_port: u16 = std::env::var("WORKER_PORT")
        .unwrap_or_else(|_| "9000".into())
        .parse()
        .unwrap_or(9000);

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
    println!("Worker registrado con ID: {}", worker_id);

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

            let _ = hb_client
                .post(format!("{}/api/v1/workers/heartbeat", hb_master_url))
                .json(&hb)
                .send()
                .await;

            tokio::time::sleep(Duration::from_secs(2)).await;
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
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                println!("Ejecutando: {} (op={}, partition={}, attempt={})", 
                    task.id, task.op, task.partition_id, task.attempt);
                
                active_tasks.fetch_add(1, Ordering::Relaxed);

                let result = execute_task(&task);

                let _ = client
                    .post(format!("{}/api/v1/workers/task/complete", master_url))
                    .json(&result)
                    .send()
                    .await;

                active_tasks.fetch_sub(1, Ordering::Relaxed);
                
                if result.success {
                    println!("  -> OK: {} registros", result.records_processed);
                    if !result.shuffle_outputs.is_empty() {
                        println!("  -> Shuffle outputs: {}", result.shuffle_outputs.len());
                    }
                } else {
                    println!("  -> ERROR: {:?}", result.error);
                }
            }
            Ok(res) if res.status() == reqwest::StatusCode::NO_CONTENT => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
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