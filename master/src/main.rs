use axum::{
    Json, Router,
    routing::{get, post},
};
use common::{Heartbeat, WorkerRegistration};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

// Contain all master and worker state
#[derive(Clone, Default)]
struct AppState {
    workers: Arc<Mutex<Vec<WorkerRegistration>>>,
}

#[tokio::main]
async fn main() {
    // Initialize the application state
    let state = AppState::default();

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/workers/register", post(register_worker))
        .route("/api/v1/workers/heartbeat", post(heartbeat))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("Master escuchando en {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("No se pudo bindear al puerto 8080");

    axum::serve(listener, app)
        .await
        .expect("Error en el servidor");
}

async fn health() -> &'static str {
    "OK"
}

async fn register_worker(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(mut reg): Json<WorkerRegistration>,
) -> Json<WorkerRegistration> {
    // Si viene sin ID, generalo
    if reg.id.is_nil() {
        reg.id = Uuid::new_v4();
    }

    {
        let mut workers = state.workers.lock().unwrap();
        workers.push(reg.clone());
        println!("Worker registrado: {:?}", reg);
    }

    Json(reg)
}

async fn heartbeat(
    axum::extract::State(_state): axum::extract::State<AppState>,
    Json(hb): Json<Heartbeat>,
) -> &'static str {
    // Aquí podrías actualizar métricas del worker
    println!(
        "Heartbeat de worker {:?}: {} tareas activas",
        hb.worker_id, hb.num_active_tasks
    );

    "OK"
}
