# Pub/Sub Benchmark

This crate provides benchmarks for the `google-cloud-pubsub` Rust client library.

## Publisher Benchmark

This benchmark measures the throughput of publishing messages to Google Cloud Pub/Sub.

### Running the Benchmark

1.  **Build the benchmark:**
    ```bash
    cargo build --release --bin pubsub-benchmark
    ```

2.  **Run the publisher benchmark:**
    Replace `<YOUR_PROJECT_ID>` and `<YOUR_TOPIC_ID>` with your Google Cloud Project ID and a Pub/Sub topic ID, respectively.
    ```bash
    cargo run --release --bin pubsub-benchmark -- \
        --project_id <YOUR_PROJECT_ID> \
        --topic_id <YOUR_TOPIC_ID> \
        --message-size 1024 \
        --maximum-runtime 10m \
        --iteration_duration 5s \
        --max-messages 100000 \
        --publisher-target-messages-per-second 0
    ```

### Command-line Arguments

*   `--project_id`: Your Google Cloud Project ID.
*   `--topic_id`: The ID of the Pub/Sub topic to publish to.
*   `--message-size`: The size of each message payload in bytes (e.g., `1024` for 1KB).
*   `--maximum-runtime`: The maximum duration for the benchmark to run (e.g., `10m` for 10 minutes, `30s` for 30 seconds).
*   `--iteration_duration`: The interval at which to report throughput metrics (e.g., `5s`).
*   `--max-messages`: The maximum number of messages to publish before stopping the benchmark.
*   `--publisher-target-messages-per-second`: The target rate of messages to publish per second. Set to 0 for unlimited.

## Output

The benchmark will print periodic reports in CSV format, including:

*   `Time(s)`: Elapsed time since the start of the benchmark.
*   `Publish(msg/s)`: Messages published per second.
*   `Ack(msg/s)`: Messages acknowledged by the Pub/Sub service per second.

