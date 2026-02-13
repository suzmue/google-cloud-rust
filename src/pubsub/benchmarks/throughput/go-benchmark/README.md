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
    -topic-id <YOUR_TOPIC_ID> \
    -payload-size 1024 \
    -iteration-duration 5s \
    -maximum-runtime 60s \
    -max-outstanding-messages 100000
```

### Arguments

*   `-project`: Google Cloud Project ID.
*   `-topic-id`: Pub/Sub Topic ID.
*   `-payload-size`: Size of the message payload in bytes (default: 1024).
*   `-iteration-duration`: Duration of each reporting interval (default: 5s).
*   `-publisher-max-batch-size`: Maximum number of messages in a batch (default: 1000).
*   `-publisher-max-batch-bytes`: Maximum size of a batch in bytes (default: 10MB).
*   `-minimum-runtime`: Minimum duration to run the benchmark (default: 5s).
*   `-maximum-runtime`: Maximum duration to run the benchmark (default: 5m).
*   `-publisher-io-channels`: Number of gRPC channels to use (default: 1).
*   `-max-outstanding-messages`: Maximum number of unacknowledged messages (default: 100000).

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
