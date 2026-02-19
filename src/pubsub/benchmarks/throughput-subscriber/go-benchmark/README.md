# Go Pub/Sub Subscriber Benchmark

This benchmark measures the throughput of the Go Pub/Sub subscriber client.

## Usage

```bash
# Run the benchmark
go run . -project <project_id> -subscription-id <subscription_id>
```

### Arguments

*   `-project`: (Required) The Google Cloud project ID.
*   `-subscription-id`: (Required) The Pub/Sub subscription ID.
*   `-iteration-duration`: The duration of each iteration (default: 5s).
*   `-maximum-runtime`: The maximum runtime of the benchmark (default: 1m).
*   `-max-outstanding-messages`: The maximum number of outstanding messages (default: 100000).
*   `-subscriber-io-channels`: The number of subscriber I/O channels (default: 1).
*   `-subscriber-thread-count`: The number of subscriber threads (default: 1).
