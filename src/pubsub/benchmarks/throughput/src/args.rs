use clap::Parser;

/// A throughput vs. CPU benchmark for the Cloud Pub/Sub C++ client library.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value = "", env = "GOOGLE_CLOUD_PROJECT")]
    pub project_id: String,

    #[arg(long, default_value = "")]
    pub topic_id: String,

    #[arg(long, default_value_t = 1024)]
    pub payload_size: i64,

    #[arg(long, default_value_t = 5)]
    pub iteration_duration: u64,

    #[arg(long, default_value_t = 1)]
    pub publisher_thread_count: usize,

    #[arg(long, default_value_t = 1000)]
    pub publisher_max_batch_size: usize,

    #[arg(long, default_value_t = 10 * 1024 * 1024)] // 10 MB
    pub publisher_max_batch_bytes: usize,

    #[arg(long, default_value_t = 10)]
    pub minimum_samples: i64,

    #[arg(long, default_value_t = i64::MAX)]
    pub maximum_samples: i64,

    #[arg(long, default_value_t = 5)]
    pub minimum_runtime: u64,

    #[arg(long, default_value_t = 300)]
    pub maximum_runtime: u64,

    #[arg(long, default_value = "")]
    pub endpoint: String,

    #[arg(long, default_value_t = 0)]
    pub publisher_io_threads: usize,

    #[arg(long, default_value_t = 0)]
    pub publisher_io_channels: usize,

    #[arg(long, default_value_t = 112 * 1024 * 1024)] // 112 MiB
    pub publisher_pending_lwm: usize,

    #[arg(long, default_value_t = 128 * 1024 * 1024)] // 128 MiB
    pub publisher_pending_hwm: usize,

    #[arg(long, default_value_t = 1200 * 2000)]
    pub publisher_target_messages_per_second: i64,
}

pub fn parse_args() -> Config {
    Config::parse()
}
