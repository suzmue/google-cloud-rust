use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use google_cloud_pubsub::client::{Publisher, TopicAdminClient};
use google_cloud_pubsub::model::{PubsubMessage, Topic};

mod args;

static SEND_COUNT: AtomicI64 = AtomicI64::new(0);
static SEND_BYTES: AtomicI64 = AtomicI64::new(0);
static ACK_COUNT: AtomicI64 = AtomicI64::new(0);
static ACK_BYTES: AtomicI64 = AtomicI64::new(0);
static ERROR_COUNT: AtomicI64 = AtomicI64::new(0);

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let config = Arc::new(crate::args::parse_args());
    println!(
        "# Running Cloud Pub/Sub benchmark with config: {:?}",
        config
    );

    let mut topic_id = config.topic_id.clone();
    let topic_owned;
    if topic_id.is_empty() {
        let mut client = TopicAdminClient::new(config.endpoint.clone()).await?;
        let topic_name = format!(
            "projects/{}/topics/rust-benchmark-{}",
            config.project_id,
            timestamp()
        );
        let topic = client
            .create_topic(Topic {
                name: topic_name,
                ..Default::default()
            })
            .await?;
        topic_id = topic.name.clone();
        topic_owned = true;
    } else {
        topic_owned = false;
    }

    println!("timestamp,elapsed(us),op,iteration,count,msgs/s,bytes,MB/s");

    publisher_task(config.clone(), topic_id.clone()).await;

    if topic_owned {
        let mut client = TopicAdminClient::new(config.endpoint.clone()).await?;
        client.delete_topic(topic_id).await?;
    }

    Ok(())
}

fn done(config: &args::Config, samples: i64, start: Instant) -> bool {
    let now = Instant::now();
    if now >= start + Duration::from_secs(config.maximum_runtime) {
        return true;
    }
    if samples >= config.maximum_samples {
        return true;
    }
    if now < start + Duration::from_secs(config.minimum_runtime) {
        return false;
    }
    samples >= config.minimum_samples
}

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn print_result(operation: &str, iteration: i64, count: i64, bytes: i64, elapsed: Duration) {
    let elapsed_us = elapsed.as_micros();
    let mbs = (bytes as f64) / (elapsed_us as f64);
    let msgs = (count as f64) * 1_000_000.0 / (elapsed_us as f64);
    println!(
        "{},{},{},{},{},{:.2},{},{:.2}",
        timestamp(),
        elapsed_us,
        operation,
        iteration,
        count,
        msgs,
        bytes,
        mbs
    );
}

async fn publisher_task(config: Arc<args::Config>, topic_id: String) {
    let publisher = Publisher::builder(topic_id).build().await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..config.publisher_thread_count {
        let publisher = publisher.clone();
        let config = config.clone();
        let task = tokio::spawn(async move {
            let data = vec![0; config.payload_size as usize];
            loop {
                let p = publisher.publish(PubsubMessage {
                    data: data.clone().into(),
                    ..Default::default()
                });
                SEND_COUNT.fetch_add(1, Ordering::Relaxed);
                SEND_BYTES.fetch_add(config.payload_size, Ordering::Relaxed);
                tokio::spawn(async move {
                    match p.await {
                        Ok(_) => {
                            ACK_COUNT.fetch_add(1, Ordering::Relaxed);
                            ACK_BYTES.fetch_add(config.payload_size, Ordering::Relaxed);
                        }
                        Err(_) => {
                            ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        tasks.push(task);
    }

    let start = Instant::now();
    for i in 0.. {
        if done(&config, i, start) {
            break;
        }
        let timer = Instant::now();
        let start_send_count = SEND_COUNT.load(Ordering::Relaxed);
        let start_send_bytes = SEND_BYTES.load(Ordering::Relaxed);
        let start_ack_count = ACK_COUNT.load(Ordering::Relaxed);
        let start_ack_bytes = ACK_BYTES.load(Ordering::Relaxed);

        tokio::time::sleep(Duration::from_secs(config.iteration_duration)).await;

        let send_count_last = SEND_COUNT.load(Ordering::Relaxed) - start_send_count;
        let send_bytes_last = SEND_BYTES.load(Ordering::Relaxed) - start_send_bytes;
        let ack_count_last = ACK_COUNT.load(Ordering::Relaxed) - start_ack_count;
        let ack_bytes_last = ACK_BYTES.load(Ordering::Relaxed) - start_ack_bytes;
        let usage = timer.elapsed();

        print_result("Pub", i, send_count_last, send_bytes_last, usage);
        print_result("Ack", i, ack_count_last, ack_bytes_last, usage);
    }

    for task in tasks {
        task.abort();
    }

    println!(
        "# Publisher: error_count={}, ack_count={}, send_count={}",
        ERROR_COUNT.load(Ordering::Relaxed),
        ACK_COUNT.load(Ordering::Relaxed),
        SEND_COUNT.load(Ordering::Relaxed)
    );
}
