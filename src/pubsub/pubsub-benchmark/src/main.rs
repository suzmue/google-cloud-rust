use clap::Parser;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::model::PubsubMessage;
use tokio::{
    runtime::Builder,
    sync::Semaphore,
    task::JoinSet,
    time::{Instant},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(name = "pubsub-benchmark", version)]
struct Args {
    #[clap(long = "project_id", help = "Project ID")]
    project_id: String,
    #[clap(long = "topic_id", help = "Topic ID")]
    topic_id: String,
    #[clap(long = "message-size", help = "Message size in bytes")]
    message_size: usize,
    #[clap(long = "maximum-runtime", help = "Maximum runtime for the benchmark")]
    maximum_runtime: humantime::Duration,
    #[clap(long = "iteration-duration", help = "Iteration duration for reporting throughput", default_value = "5s")]
    iteration_duration: humantime::Duration,
    #[clap(long = "max-messages", help = "Maximum number of messages to have outstanding", default_value = "100000")]
    max_outstanding_messages: usize,
    #[clap(long = "publisher-target-messages-per-second", help = "Target messages per second (0 for unlimited)", default_value = "0")]
    target_messages_per_second: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let runtime = Builder::new_multi_thread()
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
    
    let publisher = client.publisher(topic_name)
        .set_message_count_threshold(1000)
        .set_byte_threshold(10 * 1024 * 1024)
        .set_delay_threshold(Duration::from_secs(60))
        .build();

    let pub_count = Arc::new(AtomicU64::new(0));
    let ack_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(Semaphore::new(args.max_outstanding_messages));
    let cancelled = Arc::new(AtomicBool::new(false));

    let mut workers = JoinSet::new();
    
    let p = publisher.clone();
    let pub_c = Arc::clone(&pub_count);
    let ack_c = Arc::clone(&ack_count);
    let err_c = Arc::clone(&err_count);
    let sem = Arc::clone(&semaphore);
    let is_cancelled = Arc::clone(&cancelled);
    let payload = bytes::Bytes::from(vec![0u8; args.message_size]);
    let target_qps = args.target_messages_per_second;

    workers.spawn(async move {
        log::info!("Publisher loop started");
        let pacing_count = 8192;
        let mut iteration_count = 0u64;
        let pacing_period = if target_qps > 0 {
            Some(Duration::from_micros(1_000_000 * pacing_count / target_qps))
        } else {
            None
        };
        let mut pacing_time = Instant::now();

        loop {
            if is_cancelled.load(Ordering::Relaxed) {
                break;
            }

            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };

            let msg = PubsubMessage::new().set_data(payload.clone());
            let handle = p.publish(msg);
            pub_c.fetch_add(1, Ordering::Relaxed);

            let ack_c = Arc::clone(&ack_c);
            let err_c = Arc::clone(&err_c);
            tokio::spawn(async move {
                let _permit = permit;
                match handle.await {
                    Ok(_) => {
                        ack_c.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        err_c.fetch_add(1, Ordering::Relaxed);
                        log::error!("Publish error: {:?}", e);
                    }
                }
            });

            iteration_count += 1;
            if let Some(period) = pacing_period {
                if iteration_count % pacing_count == 0 {
                    let now = Instant::now();
                    if now < pacing_time + period {
                        tokio::time::sleep(pacing_time + period - now).await;
                    }
                    pacing_time = Instant::now();
                }
            }
        }
        log::info!("Publisher loop finished");
    });

    let start_time = Instant::now();
    let mut last_report_time = start_time;
    let mut last_pub = 0;
    let mut last_ack = 0;
    let mut last_err = 0;

    println!("Time(s),Publish(msg/s),Ack(msg/s),Error(msg/s)");

    let iteration_duration = Duration::from(args.iteration_duration);
    let maximum_runtime = Duration::from(args.maximum_runtime);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(iteration_duration) => {
                let now = Instant::now();
                let elapsed = now.duration_since(start_time);
                let interval_elapsed = now.duration_since(last_report_time).as_secs_f64();
                
                let current_pub = pub_count.load(Ordering::Relaxed);
                let current_ack = ack_count.load(Ordering::Relaxed);
                let current_err = err_count.load(Ordering::Relaxed);

                let publish_throughput = (current_pub - last_pub) as f64 / interval_elapsed;
                let ack_throughput = (current_ack - last_ack) as f64 / interval_elapsed;
                let err_throughput = (current_err - last_err) as f64 / interval_elapsed;

                println!(
                    "{:.1}, {:.2}, {:.2}, {:.2}",
                    elapsed.as_secs_f64(),
                    publish_throughput,
                    ack_throughput,
                    err_throughput
                );

                last_pub = current_pub;
                last_ack = current_ack;
                last_err = current_err;
                last_report_time = now;

                if elapsed >= maximum_runtime {
                    println!("Maximum runtime reached. Shutting down.");
                    break;
                }
            },
            _ = tokio::signal::ctrl_c() => {
                println!("Ctrl-C received. Shutting down.");
                break;
            }
        }
    }

    cancelled.store(true, Ordering::Relaxed);
    println!("Waiting for workers to finish...");
    while workers.join_next().await.is_some() {}
    
    println!("Waiting for all messages to be acknowledged...");
    // We need to drop the publisher so any buffered messages are flushed/sent.
    drop(publisher);
    
    // Acquire the whole semaphore to ensure all background result tasks are done.
    let _ = semaphore.acquire_many(args.max_outstanding_messages as u32).await;
    
    println!("Benchmark finished.");
    println!("Total published messages: {}", pub_count.load(Ordering::Relaxed));
    println!("Total acknowledged messages: {}", ack_count.load(Ordering::Relaxed));
    println!("Total error messages: {}", err_count.load(Ordering::Relaxed));

    Ok(())
}
