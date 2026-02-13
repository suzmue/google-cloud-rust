# Pub/Sub Throughput Benchmark

A throughput benchmark for the Cloud Pub/Sub Rust client library.

This tool measures the performance of publishing messages to a Google Cloud Pub/Sub topic. It reports publish and acknowledge rates in messages per second and megabytes per second.

## Usage

```bash
cargo run --release -- \
    --project <your-gcp-project-id> \
    --topic_id <your-topic-id> \
    --payload_size <size-in-bytes> \
    --iteration_duration <duration> \
    --publisher_max_batch_size <messages> \
    --publisher_max_batch_bytes <bytes> \
    --minimum_samples <samples> \
    --maximum_samples <samples> \
    --minimum_runtime <duration> \
    --maximum_runtime <duration> \
    --publisher_io_channels <count> \
    --max_outstanding_messages <count>
```

### Arguments

*   `--project`: The Google Cloud project ID.
*   `--topic_id`: The ID of the Pub/Sub topic to publish to. If not specified, a temporary topic will be created.
*   `--payload_size`: The size of each message payload in bytes (default: `1024`).
*   `--iteration_duration`: The duration of each test iteration (e.g., `5s`, `1m`; default: `5s`).
*   `--publisher_max_batch_size`: The maximum number of messages in a batch (default: `1000`).
*   `--publisher_max_batch_bytes`: The maximum size of a batch in bytes (default: `10485760` which is 10 MB).
*   `--minimum_samples`: The minimum number of samples to collect (default: `10`).
*   `--maximum_samples`: The maximum number of samples to collect (default: `i64::MAX`).
*   `--minimum_runtime`: The minimum duration to run the benchmark (e.g., `5s`, `1m`; default: `5s`).
*   `--maximum_runtime`: The maximum duration to run the benchmark (e.g., `5m`, `1h`; default: `5m`).
*   `--publisher_io_channels`: The number of gRPC channels to use for publishing (default: `1`).
*   `--max_outstanding_messages`: The maximum number of unacknowledged messages (default: `100000`).

## Output Format

The benchmark outputs data in CSV format with the following columns:

*   `timestamp`: The Unix timestamp in milliseconds.
*   `elapsed(s)`: The elapsed time for the operation in seconds.
*   `op`: The operation being measured (`Pub` for publishing, `Ack` for acknowledged messages).
*   `iteration`: The current iteration number.
*   `count`: The number of messages processed in the operation.
*   `msgs/s`: The number of messages per second.
*   `bytes`: The total number of bytes processed.
*   `MB/s`: The throughput in megabytes per second.

## Example

```bash
cargo run --release -- \
    --project my-gcp-project \
    --topic_id my-topic \
    --payload_size 2048 \
    --iteration_duration 10s \
    --maximum_runtime 1m
```
