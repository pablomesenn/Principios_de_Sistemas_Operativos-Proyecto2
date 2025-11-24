use common::{Dag, DagNode, JobRequest};
use reqwest::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dag = Dag {
        nodes: vec![
            DagNode {
                id: "read".into(),
                op: "read_csv".into(),
                path: Some("data/books.csv".into()),
                partitions: Some(4),
                fn_name: None,
                key: None,
            },
            DagNode {
                id: "flatmap".into(),
                op: "flat_map".into(),
                path: None,
                partitions: None,
                fn_name: Some("split_words".into()), // función que luego podrás definir
                key: None,
            },
            DagNode {
                id: "map".into(),
                op: "map".into(),
                path: None,
                partitions: None,
                fn_name: Some("pair_with_one".into()), // ("word", 1)
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
    };

    let client = Client::new();
    let master_url = "http://127.0.0.1:8080";

    let job = JobRequest {
        name: "wordcount-batch".into(),
        dag,
        parallelism: 4,
    };

    let res = client
        .post(format!("{}/api/v1/jobs", master_url))
        .json(&job)
        .send()
        .await?;

    let body = res.text().await?;
    println!("Respuesta del master: {}", body);

    Ok(())
}
