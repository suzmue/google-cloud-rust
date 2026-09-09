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

use std::sync::atomic::{AtomicU32, Ordering};

const SCALE: u32 = 1000;

/// A lock-free atomic token bucket with fixed fractional scaling factor of 1000.
///
/// Starts empty (0 tokens). Successful requests refill fractional tokens, and
/// hedged attempts decrement 1 whole token (1000 scaled units) when available.
#[derive(Debug)]
pub(crate) struct TokenBucket {
    tokens: AtomicU32,
    scaled_max_tokens: u32,
    scaled_refill_ratio: u32,
}

impl TokenBucket {
    /// Creates a new TokenBucket.
    pub(crate) fn new(max_tokens: u32, refill_ratio: f32) -> Self {
        let scaled_max_tokens = max_tokens.saturating_mul(SCALE);
        let scaled_refill_ratio = (refill_ratio * SCALE as f32).round() as u32;
        Self {
            tokens: AtomicU32::new(0),
            scaled_max_tokens,
            scaled_refill_ratio,
        }
    }

    /// Atomically attempts to acquire 1 token (1000 scaled units).
    ///
    /// Returns `true` and decrements 1000 if balance >= 1000, otherwise returns `false`.
    pub(crate) fn try_acquire(&self) -> bool {
        self.tokens
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current >= SCALE {
                    Some(current - SCALE)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Atomically increments the token balance by the scaled refill ratio, up to the scaled max tokens.
    pub(crate) fn refill(&self) {
        let _ = self
            .tokens
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(std::cmp::min(
                    current.saturating_add(self.scaled_refill_ratio),
                    self.scaled_max_tokens,
                ))
            });
    }

    #[cfg(test)]
    pub(crate) fn available_scaled_tokens(&self) -> u32 {
        self.tokens.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_empty_and_cannot_acquire() {
        let bucket = TokenBucket::new(50, 0.1);
        assert_eq!(bucket.available_scaled_tokens(), 0);
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn refill_and_acquire() {
        // refill_ratio 0.1 -> 100 scaled units per refill
        let bucket = TokenBucket::new(10, 0.1);

        for _ in 0..9 {
            bucket.refill();
            assert!(!bucket.try_acquire());
        }
        assert_eq!(bucket.available_scaled_tokens(), 900);

        // 10th refill brings tokens to 1000 (1 token)
        bucket.refill();
        assert_eq!(bucket.available_scaled_tokens(), 1000);

        // Now try_acquire succeeds
        assert!(bucket.try_acquire());
        assert_eq!(bucket.available_scaled_tokens(), 0);

        // Cannot acquire again without refill
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn capped_at_max_tokens() {
        // max_tokens = 2 (scaled = 2000), refill_ratio = 0.5 (scaled = 500)
        let bucket = TokenBucket::new(2, 0.5);

        for _ in 0..10 {
            bucket.refill();
        }
        assert_eq!(bucket.available_scaled_tokens(), 2000);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.available_scaled_tokens(), 1000);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.available_scaled_tokens(), 0);

        assert!(!bucket.try_acquire());
    }

    #[test]
    fn concurrent_refill_and_acquire() {
        use std::sync::Arc;

        let bucket = Arc::new(TokenBucket::new(50, 0.1));
        let mut handles = Vec::new();

        // 10 threads doing 10 refills each = 100 refills = 10000 scaled tokens = 10 tokens
        for _ in 0..10 {
            let b = bucket.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..10 {
                    b.refill();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(bucket.available_scaled_tokens(), 10_000);

        // Exactly 10 acquires should succeed
        for _ in 0..10 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
    }
}
