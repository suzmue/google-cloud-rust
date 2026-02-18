# Pub/Sub Subscriber Throughput Benchmark

A throughput benchmark for the Cloud Pub/Sub Rust client library subscriber.

This tool measures the performance of receiving messages from a Google Cloud Pub/Sub subscription. It reports receive rates in messages per second and megabytes per second.

## Usage

```bash
cargo run --release -- \
    --project <your-gcp-project-id> \
    --subscription-id <your-subscription-id> \
    --iteration-duration <duration> \
    --minimum-runtime <duration> \
    --maximum-runtime <duration> \
    --subscriber-io-channels <count> \
    --max-outstanding-messages <count>
```

### Arguments

*   `--project`: The Google Cloud project ID.
*   `--subscription_id`: The ID of the Pub/Sub subscription to receive from.
*   `--iteration_duration`: The duration of each test iteration (e.g., `5s`, `1m`; default: `5s`).
*   `--minimum_runtime`: The minimum duration to run the benchmark (e.g., `5s`, `1m`; default: `5s`).
*   `--maximum_runtime`: The maximum duration to run the benchmark (e.g., `5m`, `1h`; default: `5m`).
*   `--subscriber_io_channels`: The number of gRPC channels to use for each subscriber (default: `1`).
*   `--subscriber_thread_count`: The number of subscriber tasks to run concurrently (default: `1`).
*   `--max_outstanding_messages`: The maximum number of unacknowledged messages (default: `100000`).

## Output Format

The benchmark outputs data in CSV format with the following columns:

*   `timestamp`: The Unix timestamp in milliseconds.
*   `elapsed(s)`: The elapsed time for the operation in seconds.
*   `op`: The operation being measured (`Recv` for received messages).
*   `iteration`: The current iteration number.
*   `count`: The number of messages processed in the operation.
*   `msgs/s`: The number of messages per second.
*   `bytes`: The total number of bytes processed.
*   `MB/s`: The throughput in megabytes per second.

## Example

```bash
cargo run --release -- \
    --project my-gcp-project \
    --subscription_id my-subscription \
    --iteration_duration 10s \
    --maximum_runtime 1m
```
