use common::{Heartbeat, WorkerRegistration};
use reqwest::Client;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let master_url = std::env::var("MASTER_URL").unwrap_or("http://127.0.0.1:8080".into());

    // 1. Registro
    let reg = WorkerRegistration {
        id: Uuid::nil(), // lo genera el master si viene nil
        host: "127.0.0.1".into(),
        port: 9000, // esto para recibir tareas
    };

    let res = client
        .post(format!("{}/api/v1/workers/register", master_url))
        .json(&reg)
        .send()
        .await?;

    let reg_res: WorkerRegistration = res.json().await?;
    println!("Registrado con ID: {}", reg_res.id);

    // 2. Heartbeats
    let worker_id = reg_res.id;
    loop {
        let hb = Heartbeat {
            worker_id,
            num_active_tasks: 0, // TODO: luego poner el real
        };

        let _ = client
            .post(format!("{}/api/v1/workers/heartbeat", master_url))
            .json(&hb)
            .send()
            .await;

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
