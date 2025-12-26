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
        --payload_size 1024 \
        --publisher_thread_count 16 \
        --maximum_runtime 10m \
        --iteration_duration 5s
    ```

### Command-line Arguments

*   `--project_id`: Your Google Cloud Project ID.
*   `--topic_id`: The ID of the Pub/Sub topic to publish to.
*   `--payload_size`: The size of each message payload in bytes (e.g., `1024` for 1KB).
*   `--publisher_thread_count`: The number of threads to use for publishing messages.
*   `--maximum_runtime`: The maximum duration for the benchmark to run (e.g., `10m` for 10 minutes, `30s` for 30 seconds).
*   `--iteration_duration`: The interval at which to report throughput metrics (e.g., `5s`).

## Output

The benchmark will print periodic reports in CSV format, including:

*   `Time(s)`: Elapsed time since the start of the benchmark.
*   `Publish(msg/s)`: Messages published per second.
*   `Ack(msg/s)`: Messages acknowledged by the Pub/Sub service per second.

