// client/src/main.rs

use clap::{Parser, Subcommand};
use common::{Dag, DagNode, JobInfo, JobRequest, TaskResult};
use reqwest::Client;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "miniclient")]
#[command(about = "Cliente CLI para mini-Spark")]
struct Cli {
    /// URL del master (default: http://127.0.0.1:8080)
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    master: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Enviar un job de ejemplo (wordcount)
    Submit {
        /// Nombre del job
        #[arg(short, long, default_value = "wordcount")]
        name: String,
        /// Paralelismo
        #[arg(short, long, default_value = "4")]
        parallelism: u32,
        /// Archivo de entrada
        #[arg(short, long, default_value = "data/input.csv")]
        input: String,
    },
    /// Consultar estado de un job
    Status {
        /// ID del job
        id: Uuid,
    },
    /// Obtener resultados de un job
    Results {
        /// ID del job
        id: Uuid,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::new();

    match cli.command {
        Commands::Submit { name, parallelism, input } => {
            let dag = create_wordcount_dag(&input, parallelism);
            let job = JobRequest {
                name,
                dag,
                parallelism,
            };

            let res = client
                .post(format!("{}/api/v1/jobs", cli.master))
                .json(&job)
                .send()
                .await?;

            if res.status().is_success() {
                let job_info: JobInfo = res.json().await?;
                println!("Job enviado exitosamente!");
                println!("  ID: {}", job_info.id);
                println!("  Estado: {:?}", job_info.status);
                println!("\nPara consultar el estado:");
                println!("  cargo run --bin client -- status {}", job_info.id);
            } else {
                eprintln!("Error: {}", res.status());
                eprintln!("{}", res.text().await?);
            }
        }
        Commands::Status { id } => {
            let res = client
                .get(format!("{}/api/v1/jobs/{}", cli.master, id))
                .send()
                .await?;

            if res.status().is_success() {
                let job_info: JobInfo = res.json().await?;
                println!("Job: {}", job_info.id);
                println!("  Nombre: {}", job_info.request.name);
                println!("  Estado: {:?}", job_info.status);
                println!("  Progreso: {:.1}%", job_info.progress);
            } else if res.status() == reqwest::StatusCode::NOT_FOUND {
                eprintln!("Job no encontrado: {}", id);
            } else {
                eprintln!("Error: {}", res.status());
            }
        }
        Commands::Results { id } => {
            let res = client
                .get(format!("{}/api/v1/jobs/{}/results", cli.master, id))
                .send()
                .await?;

            if res.status().is_success() {
                let results: Vec<TaskResult> = res.json().await?;
                println!("Resultados del job {}:", id);
                for result in results {
                    println!(
                        "  Tarea {}: success={}, records={}",
                        result.task_id, result.success, result.records_processed
                    );
                    if let Some(path) = result.output_path {
                        println!("    Output: {}", path);
                    }
                    if let Some(err) = result.error {
                        println!("    Error: {}", err);
                    }
                }
            } else if res.status() == reqwest::StatusCode::NOT_FOUND {
                println!("No hay resultados todavía para el job: {}", id);
            } else {
                eprintln!("Error: {}", res.status());
            }
        }
    }

    Ok(())
}

fn create_wordcount_dag(input_path: &str, partitions: u32) -> Dag {
    Dag {
        nodes: vec![
            DagNode {
                id: "read".into(),
                op: "read_csv".into(),
                path: Some(input_path.into()),
                partitions: Some(partitions),
                fn_name: None,
                key: None,
            },
            DagNode {
                id: "flatmap".into(),
                op: "flat_map".into(),
                path: None,
                partitions: None,
                fn_name: Some("split_words".into()),
                key: None,
            },
            DagNode {
                id: "map".into(),
                op: "map".into(),
                path: None,
                partitions: None,
                fn_name: Some("pair_with_one".into()),
                key: None,
            },
            DagNode {
                id: "reduce".into(),
                op: "reduce_by_key".into(),
                path: None,
                partitions: None,
                fn_name: Some("sum".into()),
                key: Some("word".into()),
            },
        ],
        edges: vec![
            ("read".into(), "flatmap".into()),
            ("flatmap".into(), "map".into()),
            ("map".into(), "reduce".into()),
        ],
    }
}
