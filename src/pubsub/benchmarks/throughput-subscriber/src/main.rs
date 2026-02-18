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

use google_cloud_pubsub::client::Subscriber;

mod args;

static RECV_COUNT: AtomicI64 = AtomicI64::new(0);
static RECV_BYTES: AtomicI64 = AtomicI64::new(0);
static ERROR_COUNT: AtomicI64 = AtomicI64::new(0);

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let config = Arc::new(crate::args::parse_args());
    println!(
        "# Running Cloud Pub/Sub subscriber benchmark with config: {:?}",
        config
    );

    println!("timestamp,elapsed(s),op,iteration,count,msgs/s,bytes,MB/s,errors,errors/s");
    let subscription_name = format!(
        "projects/{}/subscriptions/{}",
        config.project, config.subscription_id
    );
    run_subscriber(config.clone(), &subscription_name).await;

    Ok(())
}

fn done(config: &args::Config, start: Instant) -> bool {
    let now = Instant::now();
    now >= start + config.maximum_runtime
}

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn print_result(
    operation: &str,
    iteration: i64,
    count: i64,
    bytes: i64,
    errors: i64,
    elapsed: Duration,
) {
    let elapsed_s = elapsed.as_secs_f64();
    let mbs = (bytes as f64) / (elapsed_s) / (1_000_000.0 as f64);
    let msgs = (count as f64) / (elapsed_s);
    let errs = (errors as f64) / (elapsed_s);
    println!(
        "{},{},{},{},{},{:.2},{},{:.2},{},{:.2}",
        timestamp(),
        elapsed_s,
        operation,
        iteration,
        count,
        msgs,
        bytes,
        mbs,
        errors,
        errs
    );
}

async fn subscriber_task(config: Arc<args::Config>, subscription_name: String) {
    let subscriber = Subscriber::builder()
        .with_grpc_subchannel_count(config.subscriber_io_channels)
        .build()
        .await
        .unwrap();

    let mut stream = subscriber
        .streaming_pull(subscription_name)
        .set_max_outstanding_messages(config.max_outstanding_messages)
        .start();
    while let Some(result) = stream.next().await {
        match result {
            Ok((m, h)) => {
                RECV_COUNT.fetch_add(1, Ordering::Relaxed);
                RECV_BYTES.fetch_add(m.data.len() as i64, Ordering::Relaxed);
                h.ack();
            }
            Err(_) => {
                ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn run_subscriber(config: Arc<args::Config>, subscription_name: &str) {
    let mut tasks = Vec::new();
    for _ in 0..config.subscriber_thread_count {
        tasks.push(tokio::spawn(subscriber_task(
            config.clone(),
            subscription_name.to_string(),
        )));
    }

    let start = Instant::now();
    for i in 0.. {
        if done(&config, start) {
            break;
        }
        let timer = Instant::now();
        let start_recv_count = RECV_COUNT.load(Ordering::Relaxed);
        let start_recv_bytes = RECV_BYTES.load(Ordering::Relaxed);
        let start_error_count = ERROR_COUNT.load(Ordering::Relaxed);

        tokio::time::sleep(config.iteration_duration).await;

        let recv_count_last = RECV_COUNT.load(Ordering::Relaxed) - start_recv_count;
        let recv_bytes_last = RECV_BYTES.load(Ordering::Relaxed) - start_recv_bytes;
        let error_count_last = ERROR_COUNT.load(Ordering::Relaxed) - start_error_count;
        let usage = timer.elapsed();

        print_result(
            "Recv",
            i,
            recv_count_last,
            recv_bytes_last,
            error_count_last,
            usage,
        );
    }

    for task in tasks {
        task.abort();
    }

    println!(
        "# Subscriber: recv_count={}, error_count={}",
        RECV_COUNT.load(Ordering::Relaxed),
        ERROR_COUNT.load(Ordering::Relaxed)
    );
}
