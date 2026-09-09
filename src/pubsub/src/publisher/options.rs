// Copyright 2025 Google LLC
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

/// Configure publisher batching behavior.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct BatchingOptions {
    pub message_count_threshold: u32,
    pub byte_threshold: u32,
    pub delay_threshold: std::time::Duration,
}

impl BatchingOptions {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [BatchingOptions][Self::message_count_threshold] field.
    pub fn set_message_count_threshold<V: Into<u32>>(mut self, v: V) -> Self {
        self.message_count_threshold = v.into();
        self
    }

    /// Set the [BatchingOptions][Self::byte_threshold] field.
    pub(crate) fn set_byte_threshold<V: Into<u32>>(mut self, v: V) -> Self {
        self.byte_threshold = v.into();
        self
    }

    /// Set the [BatchingOptions][Self::delay_threshold] field.
    pub fn set_delay_threshold<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.delay_threshold = v.into();
        self
    }
}

impl std::default::Default for BatchingOptions {
    fn default() -> Self {
        Self {
            message_count_threshold: 100_u32,
            byte_threshold: 1_000_000_u32, // 1 MB
            delay_threshold: std::time::Duration::from_millis(10),
        }
    }
}

/// Configure publisher request hedging behavior.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct HedgingOptions {
    pub delay: std::time::Duration,
    pub max_tokens: u32,
    pub refill_ratio: f32,
}

impl HedgingOptions {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [HedgingOptions][Self::delay] field.
    pub fn set_delay<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.delay = v.into();
        self
    }

    /// Set the [HedgingOptions][Self::max_tokens] field.
    pub fn set_max_tokens<V: Into<u32>>(mut self, v: V) -> Self {
        self.max_tokens = v.into();
        self
    }

    /// Set the [HedgingOptions][Self::refill_ratio] field.
    pub fn set_refill_ratio<V: Into<f32>>(mut self, v: V) -> Self {
        self.refill_ratio = v.into();
        self
    }
}

impl std::default::Default for HedgingOptions {
    fn default() -> Self {
        Self {
            delay: std::time::Duration::from_secs(1),
            max_tokens: 50_u32,
            refill_ratio: 0.1_f32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchingOptions, HedgingOptions};
    use std::time::Duration;

    #[tokio::test]
    async fn batching_options() -> anyhow::Result<()> {
        let options = BatchingOptions::new()
            .set_byte_threshold(1_234_u32)
            .set_message_count_threshold(123_u32)
            .set_delay_threshold(std::time::Duration::from_millis(12));
        assert_eq!(options.byte_threshold, 1_234_u32);
        assert_eq!(options.message_count_threshold, 123_u32);
        assert_eq!(
            options.delay_threshold,
            std::time::Duration::from_millis(12)
        );
        Ok(())
    }

    #[test]
    fn hedging_options_defaults_and_builder() {
        let default_opts = HedgingOptions::default();
        assert_eq!(default_opts.delay, Duration::from_secs(1));
        assert_eq!(default_opts.max_tokens, 50);
        assert_eq!(default_opts.refill_ratio, 0.1);

        let custom_opts = HedgingOptions::new()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(100_u32)
            .set_refill_ratio(0.05_f32);
        assert_eq!(custom_opts.delay, Duration::from_millis(500));
        assert_eq!(custom_opts.max_tokens, 100);
        assert_eq!(custom_opts.refill_ratio, 0.05);
    }
}
