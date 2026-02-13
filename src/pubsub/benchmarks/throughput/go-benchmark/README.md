# Go Pub/Sub v2 Benchmark

This directory contains a Go benchmark for the Google Cloud Pub/Sub v2 client. It is designed to be compatible with the Rust Pub/Sub throughput benchmark.

## Running the Benchmark

First, build the benchmark:

```bash
go build -o benchmark main.go
```

Then run it:

```bash
./benchmark \
    -project <YOUR_PROJECT_ID> \
    -topic_id <YOUR_TOPIC_ID> \
    -payload_size 1024 \
    -iteration_duration 5s \
    -maximum_runtime 60s
```

### Arguments

*   `-project`: Google Cloud Project ID.
*   `-topic_id`: Pub/Sub Topic ID.
*   `-payload_size`: Size of the message payload in bytes (default: 1024).
*   `-iteration_duration`: Duration of each reporting interval (default: 5s).
*   `-publisher_max_batch_size`: Maximum number of messages in a batch (default: 1000).
*   `-publisher_max_batch_bytes`: Maximum size of a batch in bytes (default: 10MB).
*   `-minimum_samples`: Minimum number of samples to collect (default: 10).
*   `-maximum_samples`: Maximum number of samples to collect (default: MaxInt64).
*   `-minimum_runtime`: Minimum duration to run the benchmark (default: 5s).
*   `-maximum_runtime`: Maximum duration to run the benchmark (default: 5m).
*   `-publisher_io_channels`: Number of gRPC channels to use (default: 1).

## Output Format

The benchmark outputs data in CSV format with the following columns:

*   `timestamp`: The Unix timestamp in milliseconds.
*   `elapsed(s)`: The elapsed time for the operation in seconds.
*   `op`: The operation being measured (`Pub` or `Ack`).
*   `iteration`: The current iteration number.
*   `count`: The number of messages processed.
*   `msgs/s`: The number of messages per second.
*   `bytes`: The total number of bytes processed.
*   `MB/s`: The throughput in megabytes per second.
