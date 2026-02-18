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

use clap::Parser;
use humantime::parse_duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value = "", env = "GOOGLE_CLOUD_PROJECT")]
    pub project: String,

    #[arg(long, default_value = "")]
    pub subscription_id: String,

    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    pub iteration_duration: std::time::Duration,

    #[arg(long, value_parser = parse_duration, default_value = "1m")]
    pub maximum_runtime: std::time::Duration,

    #[arg(long, default_value_t = 100000)]
    pub max_outstanding_messages: i64,

    #[arg(long, default_value_t = 1)]
    pub subscriber_io_channels: usize,

    #[arg(long, default_value_t = 1)]
    pub subscriber_thread_count: usize,
}

pub fn parse_args() -> Config {
    Config::parse()
}
