# Cloud Pub/Sub Publish Hedging Benchmark

This benchmark evaluates the latency distribution of Cloud Pub/Sub message publishing with and without request hedging.

Request hedging automatically sends duplicate/hedged publish requests when an in-flight batch publish exceeds a configured delay threshold (and tokens are available in the hedging token bucket). This effectively mitigates tail latency (p99/p99.9) caused by network degradations or slow backend servers.

## Features

- **Built-in Latency Simulator / Mock Server**: Runs an in-process gRPC mock server with configurable latency distributions (fast, degraded, and stalled requests) by default, or connects to a real Pub/Sub endpoint with `--endpoint`.
- **Comprehensive Latency Metrics**: Reports Min, p50, p90, p95, p99, p99.9, p99.99, Max, Avg latency and overall throughput.
- **Hedging Overhead Tracking**: Tracks total publish RPCs and hedged RPCs (`x-goog-pubsub-client-telemetry` header) to show the exact traffic overhead of hedging.

## Usage

### Display Help

```bash
cargo run --release -p pubsub-hedging -- --help
```

### Baseline Run (Without Hedging)

```bash
cargo run --release -p pubsub-hedging -- \
  --duration 30s \
  --message-rate 200 \
  --fast-latency 5ms --fast-ratio 0.95 \
  --degraded-latency 300ms --degraded-ratio 0.04 \
  --stall-latency 4000ms --stall-ratio 0.01
```

### Hedged Run (With Hedging)

```bash
cargo run --release -p pubsub-hedging -- \
  --enable-hedging \
  --hedge-delay 100ms \
  --hedge-max-tokens 50 \
  --hedge-refill-ratio 0.1 \
  --duration 30s \
  --message-rate 200 \
  --fast-latency 5ms --fast-ratio 0.95 \
  --degraded-latency 300ms --degraded-ratio 0.04 \
  --stall-latency 4000ms --stall-ratio 0.01
```

### Options Reference

| Option | Default | Description |
| --- | --- | --- |
| `--project` | `test-project` | GCP project ID |
| `--topic` | `test-topic` | Topic ID or name |
| `--duration` | `30s` | Total duration of the benchmark run (e.g. `30s`, `1m`) |
| `--message-rate` | `200` | Target message production rate in messages/second |
| `--payload-size` | `1024` | Size in bytes of each message payload |
| `--batch-size` | `100` | Maximum messages per batch |
| `--batch-bytes` | `1048576` (1MB) | Maximum bytes per batch |
| `--batch-delay` | `10ms` | Maximum delay before sending a batch |
| `--enable-hedging` | `false` | Enable request hedging on the publisher |
| `--hedge-delay` | `100ms` | Time before sending a hedged request |
| `--hedge-max-tokens` | `50` | Maximum tokens in the token bucket for hedging |
| `--hedge-refill-ratio`| `0.1` | Fraction of successful RPCs that refill tokens |
| `--endpoint` | `None` (built-in mock) | Optional custom gRPC endpoint (e.g. `http://localhost:8085`) |
| `--fast-latency` | `5ms` | Latency for normal/fast requests |
| `--fast-ratio` | `0.95` | Fraction of fast requests (0.0 to 1.0) |
| `--degraded-latency`| `300ms` | Latency for degraded requests |
| `--degraded-ratio` | `0.04` | Fraction of degraded requests |
| `--stall-latency` | `4000ms` | Latency for stalled requests |
| `--stall-ratio` | `0.01` | Fraction of stalled requests |
