// client/src/main.rs

use clap::{Parser, Subcommand};
use common::{Dag, DagNode, JobInfo, JobRequest, TaskResult};
use reqwest::Client;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "miniclient")]
#[command(about = "Cliente CLI para mini-Spark")]
struct Cli {
    /// URL del master (también se puede configurar con MASTER_URL)
    #[arg(short, long, default_value_t = get_default_master_url())]
    master: String,

    #[command(subcommand)]
    command: Commands,
}

/// Obtener URL del master desde variable de entorno o usar default
fn get_default_master_url() -> String {
    std::env::var("MASTER_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
}

#[derive(Subcommand)]
enum MetricsMode {
    System,
    Jobs,
    Failures,
    Job { id: String },
    Stages { id: String },
}

#[derive(Subcommand)]
enum Commands {
    /// Enviar job wordcount
    Submit {
        #[arg(short, long, default_value = "wordcount")]
        name: String,
        #[arg(short, long, default_value = "4")]
        parallelism: u32,
        #[arg(short, long, default_value = "data/input.csv")]
        input: String,
    },
    /// Enviar job con join
    SubmitJoin {
        #[arg(short, long, default_value = "join-job")]
        name: String,
        #[arg(short, long, default_value = "4")]
        parallelism: u32,
        #[arg(long, default_value = "data/sales.csv")]
        sales: String,
        #[arg(long, default_value = "data/products.csv")]
        products: String,
    },
    /// Consultar estado
    Status { id: Uuid },
    /// Obtener resultados
    Results { id: Uuid },
    /// Ver métricas
    Metrics {
        #[command(subcommand)]
        mode: MetricsMode,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::new();

    println!("Conectando a master: {}", cli.master);

    match cli.command {
        Commands::Submit {
            name,
            parallelism,
            input,
        } => {
            let dag = create_wordcount_dag(&input, parallelism);
            submit_job(&client, &cli.master, name, dag, parallelism).await?;
        }
        Commands::SubmitJoin {
            name,
            parallelism,
            sales,
            products,
        } => {
            let dag = create_join_dag(&sales, &products, parallelism);
            submit_job(&client, &cli.master, name, dag, parallelism).await?;
        }
        Commands::Status { id } => {
            get_status(&client, &cli.master, id).await?;
        }
        Commands::Results { id } => {
            get_results(&client, &cli.master, id).await?;
        }
        Commands::Metrics { mode } => match mode {
            MetricsMode::System => {
                let url = format!("{}/api/v1/metrics/system", &cli.master);
                let res = client.get(&url).send().await?;
                println!("{}", res.text().await?);
            }
            MetricsMode::Jobs => {
                let url = format!("{}/api/v1/metrics/jobs", &cli.master);
                let res = client.get(&url).send().await?;
                println!("{}", res.text().await?);
            }
            MetricsMode::Failures => {
                let url = format!("{}/api/v1/metrics/failures", &cli.master);
                let res = client.get(&url).send().await?;
                println!("{}", res.text().await?);
            }
            MetricsMode::Job { id } => {
                let url = format!("{}/api/v1/jobs/{}/metrics", &cli.master, id);
                let res = client.get(&url).send().await?;
                println!("{}", res.text().await?);
            }
            MetricsMode::Stages { id } => {
                let url = format!("{}/api/v1/jobs/{}/stages", &cli.master, id);
                let res = client.get(&url).send().await?;
                println!("{}", res.text().await?);
            }
        },
    }

    Ok(())
}

async fn submit_job(
    client: &Client,
    master: &str,
    name: String,
    dag: Dag,
    parallelism: u32,
) -> anyhow::Result<()> {
    let job = JobRequest {
        name,
        dag,
        parallelism,
    };

    let res = client
        .post(format!("{}/api/v1/jobs", master))
        .json(&job)
        .send()
        .await?;

    if res.status().is_success() {
        let job_info: JobInfo = res.json().await?;
        println!("Job enviado!");
        println!("  ID: {}", job_info.id);
        println!("  Estado: {:?}", job_info.status);
        println!("\nConsultar estado:");
        println!("  cargo run --bin client -- status {}", job_info.id);
    } else {
        eprintln!("Error: {}", res.status());
        eprintln!("Body: {}", res.text().await?);
    }

    Ok(())
}

async fn get_status(client: &Client, master: &str, id: Uuid) -> anyhow::Result<()> {
    let res = client
        .get(format!("{}/api/v1/jobs/{}", master, id))
        .send()
        .await?;

    if res.status().is_success() {
        let job_info: JobInfo = res.json().await?;
        println!("Job: {}", job_info.id);
        println!("  Nombre: {}", job_info.request.name);
        println!("  Estado: {:?}", job_info.status);
        println!("  Progreso: {:.1}%", job_info.progress);
    } else {
        eprintln!("Job no encontrado");
    }

    Ok(())
}

async fn get_results(client: &Client, master: &str, id: Uuid) -> anyhow::Result<()> {
    let res = client
        .get(format!("{}/api/v1/jobs/{}/results", master, id))
        .send()
        .await?;

    if res.status().is_success() {
        let results: Vec<TaskResult> = res.json().await?;
        println!("Resultados ({} tareas completadas):", results.len());
        for result in results {
            if let Some(path) = result.output_path {
                println!("  {} -> {} registros", path, result.records_processed);
            }
        }
    } else {
        println!("No hay resultados todavía");
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
                join_with: None,
            },
            DagNode {
                id: "flatmap".into(),
                op: "flat_map".into(),
                path: None,
                partitions: None,
                fn_name: Some("split_words".into()),
                key: None,
                join_with: None,
            },
            DagNode {
                id: "map".into(),
                op: "map".into(),
                path: None,
                partitions: None,
                fn_name: Some("pair_with_one".into()),
                key: None,
                join_with: None,
            },
            DagNode {
                id: "reduce".into(),
                op: "reduce_by_key".into(),
                path: None,
                partitions: None,
                fn_name: Some("sum".into()),
                key: Some("word".into()),
                join_with: None,
            },
        ],
        edges: vec![
            ("read".into(), "flatmap".into()),
            ("flatmap".into(), "map".into()),
            ("map".into(), "reduce".into()),
        ],
    }
}

fn create_join_dag(sales_path: &str, products_path: &str, partitions: u32) -> Dag {
    Dag {
        nodes: vec![
            DagNode {
                id: "read_sales".into(),
                op: "read_csv".into(),
                path: Some(sales_path.into()),
                partitions: Some(partitions),
                fn_name: None,
                key: None,
                join_with: None,
            },
            DagNode {
                id: "read_products".into(),
                op: "read_csv".into(),
                path: Some(products_path.into()),
                partitions: Some(partitions),
                fn_name: None,
                key: None,
                join_with: None,
            },
            DagNode {
                id: "map_sales".into(),
                op: "map".into(),
                path: None,
                partitions: None,
                fn_name: Some("extract_key".into()),
                key: None,
                join_with: None,
            },
            DagNode {
                id: "map_products".into(),
                op: "map".into(),
                path: None,
                partitions: None,
                fn_name: Some("extract_key".into()),
                key: None,
                join_with: None,
            },
            DagNode {
                id: "join".into(),
                op: "join".into(),
                path: None,
                partitions: None,
                fn_name: None,
                key: Some("product_id".into()),
                join_with: Some("map_products".into()),
            },
        ],
        edges: vec![
            ("read_sales".into(), "map_sales".into()),
            ("read_products".into(), "map_products".into()),
            ("map_sales".into(), "join".into()),
            ("map_products".into(), "join".into()),
        ],
    }
}