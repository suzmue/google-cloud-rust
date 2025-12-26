use clap::Parser;
use futures::future::join_all;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::model::PubsubMessage;
use std::sync::Arc;
use tokio::{
    runtime::Builder,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant},
};

#[derive(Parser, Debug)]
#[clap(name = "pubsub-benchmark", version)]
struct Args {
    #[clap(long, help = "Project ID")]
    project_id: String,
    #[clap(long, help = "Topic ID")]
    topic_id: String,
    #[clap(long, help = "Message size in bytes")]
    payload_size: usize,
    #[clap(long, help = "Number of publisher threads")]
    publisher_thread_count: usize,
    #[clap(long, help = "Maximum runtime for the benchmark")]
    maximum_runtime: humantime::Duration,
    #[clap(long, help = "Iteration duration for reporting throughput")]
    iteration_duration: humantime::Duration,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let runtime = Builder::new_multi_thread()
        .worker_threads(args.publisher_thread_count + 2) // +2 for main and metrics
        .enable_all()
        .build()?;

    runtime.block_on(async {
        run_benchmark(args).await
    })?;

    Ok(())
}

async fn run_benchmark(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running publisher benchmark with args: {:?}", args);

    let client = Client::builder().build().await?;
    let topic_name = format!("projects/{}/topics/{}", args.project_id, args.topic_id);
    let publisher = Arc::new(client.publisher(topic_name).build());

    let (tx_published, mut rx_published): (Sender<usize>, Receiver<usize>) = mpsc::channel(100_000);
    let (tx_acknowledged, mut rx_acknowledged): (Sender<usize>, Receiver<usize>) = mpsc::channel(100_000);

    let mut workers = JoinSet::new();
    for i in 0..args.publisher_thread_count {
        let p = Arc::clone(&publisher);
        let tx_pub = tx_published.clone();
        let tx_ack = tx_acknowledged.clone();
        let payload = vec![0u8; args.payload_size];
        workers.spawn(async move {
            log::info!("Publisher worker {} started", i);
            let mut handles = Vec::new();
            loop {
                let msg = PubsubMessage::new().set_data(payload.clone());
                
                // The publish call returns a future (handle).
                handles.push(p.publish(msg));
                if tx_pub.send(1).await.is_err() {
                    // Main thread has dropped the receiver, time to shut down.
                    break;
                }
                
                // Once we have a decent batch of futures, await them all.
                if handles.len() >= 1000 {
                    let results = join_all(handles.drain(..)).await;
                    let ack_count = results.into_iter().filter(|r| r.is_ok()).count();
                    if tx_ack.send(ack_count).await.is_err() {
                        break;
                    }
                }
            }
            // Await any remaining handles before exiting.
            if !handles.is_empty() {
                let results = join_all(handles.drain(..)).await;
                let ack_count = results.into_iter().filter(|r| r.is_ok()).count();
                tx_ack.send(ack_count).await.unwrap_or_default();
            }
            log::info!("Publisher worker {} finished", i);
        });
    }

    let start_time = Instant::now();
    let mut last_report_time = start_time;
    let mut total_published = 0;
    let mut total_acknowledged = 0;
    let mut interval_published = 0;
    let mut interval_acknowledged = 0;

    println!("Time(s),Publish(msg/s),Ack(msg/s)");

    let main_loop = async {
        loop {
            tokio::select! {
                Some(p_count) = rx_published.recv() => {
                    interval_published += p_count;
                    total_published += p_count;
                },
                Some(a_count) = rx_acknowledged.recv() => {
                    interval_acknowledged += a_count;
                    total_acknowledged += a_count;
                },
                else => break,
            }

            let now = Instant::now();
            if now - last_report_time >= std::time::Duration::from(args.iteration_duration) {
                let elapsed = now - last_report_time;
                let publish_throughput = interval_published as f64 / elapsed.as_secs_f64();
                let ack_throughput = interval_acknowledged as f64 / elapsed.as_secs_f64();
                println!(
                    "{},{},{}",
                    now.duration_since(start_time).as_secs_f64(),
                    publish_throughput,
                    ack_throughput
                );
                interval_published = 0;
                interval_acknowledged = 0;
                last_report_time = now;
            }
        }
    };
    
    tokio::select! {
        _ = main_loop => {},
        _ = tokio::time::sleep(args.maximum_runtime.into()) => {
            println!("Maximum runtime reached. Shutting down.");
        }
    }

    // To shut down, we drop the senders and the publisher.
    // Dropping the publisher will close the worker channel, causing the publisher's background
    // task to exit. Dropping our metric senders will cause the worker loops to exit.
    drop(tx_published);
    drop(tx_acknowledged);
    drop(publisher);
    
    // Wait for all worker tasks to complete.
    while workers.join_next().await.is_some() {}

    println!("Benchmark finished.");
    println!("Total published messages: {}", total_published);
    println!("Total acknowledged messages: {}", total_acknowledged);

    Ok(())
}
