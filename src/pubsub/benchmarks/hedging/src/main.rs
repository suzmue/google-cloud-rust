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

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use google_cloud_pubsub::publisher::HedgingOptions;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

mod args;
mod mock_server;
mod stats;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = args::parse_args();

    println!("================================================================================");
    println!("             Starting Google Cloud Pub/Sub Hedging Benchmark                    ");
    println!("================================================================================");
    println!("Parameters:");
    println!("  Project:              {}", args.project);
    println!("  Topic:                {}", args.topic);
    println!("  Duration:             {:.2?}", args.duration);
    println!("  Message Rate:         {} msg/s", args.message_rate);
    println!("  Payload Size:         {} bytes", args.payload_size);
    println!(
        "  Batch Settings:       size={}, bytes={}, delay={:.2?}",
        args.batch_size, args.batch_bytes, args.batch_delay
    );
    println!("  Hedging Enabled:      {}", args.enable_hedging);
    if args.enable_hedging {
        println!(
            "  Hedging Settings:     delay={:.2?}, max_tokens={}, refill_ratio={}",
            args.hedge_delay, args.hedge_max_tokens, args.hedge_refill_ratio
        );
    }

    let (endpoint, mock_handle) = if let Some(ref ep) = args.endpoint {
        println!("  Endpoint (External):  {}", ep);
        (ep.clone(), None)
    } else {
        println!(
            "  Mock Latencies:       fast={:.2?} ({:.0}%), degraded={:.2?} ({:.0}%), stall={:.2?} ({:.0}%)",
            args.fast_latency,
            args.fast_ratio * 100.0,
            args.degraded_latency,
            args.degraded_ratio * 100.0,
            args.stall_latency,
            args.stall_ratio * 100.0
        );
        let mock_cfg = mock_server::MockServerConfig {
            fast_latency: args.fast_latency,
            fast_ratio: args.fast_ratio,
            degraded_latency: args.degraded_latency,
            degraded_ratio: args.degraded_ratio,
            stall_latency: args.stall_latency,
            stall_ratio: args.stall_ratio,
        };
        let (ep, handle) = mock_server::start_mock_server(mock_cfg, args.server_port).await?;
        println!("  Mock Server Endpoint: {}", ep);

        if args.server_only {
            println!(
                "================================================================================"
            );
            println!("Mock server running at: {}", ep);
            println!("Press Ctrl+C to stop...");
            println!(
                "================================================================================"
            );
            tokio::signal::ctrl_c().await?;
            println!("\nServer shutting down.");
            println!(
                "Total Publish RPCs: {}",
                handle.stats.total_publish_requests.load(Ordering::Relaxed)
            );
            println!(
                "Hedged Publish RPCs: {}",
                handle.stats.hedged_publish_requests.load(Ordering::Relaxed)
            );
            return Ok(());
        }

        (ep, Some(handle))
    };
    println!("================================================================================\n");

    let topic_name = format!("projects/{}/topics/{}", args.project, args.topic);
    let mut builder = Publisher::builder(topic_name)
        .with_endpoint(&endpoint)
        .with_credentials(Anonymous::default().build())
        .set_message_count_threshold(args.batch_size)
        .set_byte_threshold(args.batch_bytes)
        .set_delay_threshold(args.batch_delay);

    if args.enable_hedging {
        builder = builder.set_hedging_options(
            HedgingOptions::new()
                .set_delay(args.hedge_delay)
                .set_max_tokens(args.hedge_max_tokens)
                .set_refill_ratio(args.hedge_refill_ratio),
        );
    }

    let publisher = builder.build().await?;
    let payload = bytes::Bytes::from(vec![b'x'; args.payload_size]);

    let interval_duration = Duration::from_secs_f64(1.0 / args.message_rate as f64);
    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let mut join_set = JoinSet::new();
    let bench_start = Instant::now();
    let mut total_sent = 0u64;

    while bench_start.elapsed() < args.duration {
        interval.tick().await;
        if bench_start.elapsed() >= args.duration {
            break;
        }

        total_sent += 1;
        let pub_clone = publisher.clone();
        let msg_payload = payload.clone();

        join_set.spawn(async move {
            let send_start = Instant::now();
            let msg = Message::new().set_data(msg_payload);
            let res = pub_clone.publish(msg).await;
            let elapsed = send_start.elapsed();
            (res.is_ok(), elapsed)
        });
    }

    println!(
        "Benchmark duration reached. Sent {} messages. Waiting for in-flight requests...",
        total_sent
    );

    let mut latencies = Vec::with_capacity(total_sent as usize);
    let mut total_succeeded = 0u64;
    let mut total_errors = 0u64;

    let drain_timeout = args.stall_latency + Duration::from_secs(5);
    let drain_deadline = Instant::now() + drain_timeout;

    while let Some(res) = join_set.join_next().await {
        match res {
            Ok((true, latency)) => {
                total_succeeded += 1;
                latencies.push(latency);
            }
            Ok((false, _)) => {
                total_errors += 1;
            }
            Err(_) => {
                total_errors += 1;
            }
        }
        if Instant::now() > drain_deadline {
            eprintln!("Warning: Timed out waiting for remaining publish tasks to finish.");
            break;
        }
    }

    let total_elapsed = bench_start.elapsed();

    let (total_rpcs, hedged_rpcs) = if let Some(ref handle) = mock_handle {
        (
            Some(handle.stats.total_publish_requests.load(Ordering::Relaxed)),
            Some(handle.stats.hedged_publish_requests.load(Ordering::Relaxed)),
        )
    } else {
        (None, None)
    };

    let mut benchmark_stats = stats::BenchmarkStats {
        elapsed: total_elapsed,
        total_sent,
        total_succeeded,
        total_errors,
        payload_size: args.payload_size,
        latencies,
        total_rpc_requests: total_rpcs,
        hedged_rpc_requests: hedged_rpcs,
    };

    benchmark_stats.print_summary();

    if let Some(handle) = mock_handle {
        handle.server_task.abort();
    }

    Ok(())
}
