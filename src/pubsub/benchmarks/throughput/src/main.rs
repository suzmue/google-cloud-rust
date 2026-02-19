// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use google_cloud_pubsub::client::{Publisher, TopicAdmin};
use google_cloud_pubsub::model::Message;
use rand::RngExt as _;
use rand::distr::Alphanumeric;

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
        let client = TopicAdmin::builder().build().await?;
        topic_id = random_topic_id();
        let topic_name = format!("projects/{}/topics/{}", config.project, topic_id);
        let topic = client.create_topic().set_name(topic_name).send().await?;
        topic_id = topic.name.clone();
        topic_owned = true;
    } else {
        topic_owned = false;
    }

    println!("timestamp,elapsed(s),op,iteration,count,msgs/s,bytes,MB/s");
    let topic_name = format!("projects/{}/topics/{}", config.project, topic_id);
    run_publisher(config.clone(), topic_name.clone()).await;

    if topic_owned {
        let client = TopicAdmin::builder().build().await?;
        client.delete_topic().set_topic(topic_name).send().await?;
    }
    Ok(())
}

fn done(config: &args::Config, start: Instant) -> bool {
    let now = Instant::now();
    if now >= start + config.maximum_runtime {
        return true;
    }
    now >= start + config.minimum_runtime
}

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn print_result(operation: &str, iteration: i64, count: i64, bytes: i64, elapsed: Duration) {
    let elapsed_s = elapsed.as_secs_f64();
    let mbs = (bytes as f64) / (elapsed_s) / (1_000_000.0 as f64);
    let msgs = (count as f64) / (elapsed_s);
    println!(
        "{},{},{},{},{},{:.2},{},{:.2}",
        timestamp(),
        elapsed_s,
        operation,
        iteration,
        count,
        msgs,
        bytes,
        mbs
    );
}

async fn create_publisher(config: Arc<args::Config>, topic_name: String) -> Publisher {
    Publisher::builder(topic_name)
        .set_byte_threshold(config.publisher_max_batch_bytes)
        .set_message_count_threshold(config.publisher_max_batch_size)
        .set_delay_threshold(config.maximum_runtime) // don't send batches that are small.
        .with_grpc_subchannel_count(config.publisher_io_channels)
        .build()
        .await
        .unwrap()
}

async fn run_publisher(config: Arc<args::Config>, topic_name: String) {
    let publisher = create_publisher(config.clone(), topic_name).await;
    let payload_size = config.payload_size;
    let data = bytes::Bytes::from(vec![0u8; payload_size as usize]);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_outstanding_messages));

    // Start a background task to publish messages.
    tokio::task::spawn(async move {
        loop {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let p = publisher.publish(Message::new().set_data(data.clone()));
            SEND_COUNT.fetch_add(1, Ordering::Relaxed);
            SEND_BYTES.fetch_add(payload_size, Ordering::Relaxed);

            tokio::spawn(async move {
                let _permit = permit;
                match p.await {
                    Ok(_) => {
                        ACK_COUNT.fetch_add(1, Ordering::Relaxed);
                        ACK_BYTES.fetch_add(payload_size, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let start = Instant::now();
    for i in 0.. {
        if done(&config,start) {
            break;
        }
        let timer = Instant::now();
        let start_send_count = SEND_COUNT.load(Ordering::Relaxed);
        let start_send_bytes = SEND_BYTES.load(Ordering::Relaxed);
        let start_ack_count = ACK_COUNT.load(Ordering::Relaxed);
        let start_ack_bytes = ACK_BYTES.load(Ordering::Relaxed);

        tokio::time::sleep(config.iteration_duration).await;

        let send_count_last = SEND_COUNT.load(Ordering::Relaxed) - start_send_count;
        let send_bytes_last = SEND_BYTES.load(Ordering::Relaxed) - start_send_bytes;
        let ack_count_last = ACK_COUNT.load(Ordering::Relaxed) - start_ack_count;
        let ack_bytes_last = ACK_BYTES.load(Ordering::Relaxed) - start_ack_bytes;
        let usage = timer.elapsed();

        print_result("Pub", i, send_count_last, send_bytes_last, usage);
        print_result("Ack", i, ack_count_last, ack_bytes_last, usage);
    }

    println!(
        "# Publisher: error_count={}, ack_count={}, send_count={}",
        ERROR_COUNT.load(Ordering::Relaxed),
        ACK_COUNT.load(Ordering::Relaxed),
        SEND_COUNT.load(Ordering::Relaxed)
    );
}

pub const TOPIC_ID_LENGTH: usize = 255;

fn random_topic_id() -> String {
    let prefix = "topic-";
    let topic_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(TOPIC_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("{prefix}{topic_id}")
}
