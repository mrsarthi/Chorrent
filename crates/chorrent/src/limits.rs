use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Token bucket for bandwidth: allows bursts of up to one second's worth,
/// then paces callers to `bytes_per_sec` on average. Callers may go into
/// debt (a 64 KiB piece on a 10 KiB/s limit) and simply wait it off.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    bytes_per_sec: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        Self { bytes_per_sec: rate, state: Mutex::new((rate, Instant::now())) }
    }

    pub async fn acquire(&self, bytes: u64) {
        let wait = {
            let mut state = self.state.lock().await;
            let (tokens, last) = &mut *state;
            let now = Instant::now();
            *tokens = (*tokens + now.duration_since(*last).as_secs_f64() * self.bytes_per_sec).min(self.bytes_per_sec);
            *last = now;
            *tokens -= bytes as f64;
            if *tokens < 0.0 { Duration::from_secs_f64(-*tokens / self.bytes_per_sec) } else { Duration::ZERO }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn paces_to_the_configured_rate() {
        let limiter = RateLimiter::new(1000);
        let start = Instant::now();
        for _ in 0..4 {
            limiter.acquire(1000).await; // 1 s burst, then ~1 s per call
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(2900) && elapsed <= Duration::from_millis(3100), "{elapsed:?}");
    }
}
