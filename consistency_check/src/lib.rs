use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub async fn update_rate(
    cancellation_token: CancellationToken,
    assets_processed: Arc<AtomicU64>,
    rate: Arc<Mutex<f64>>,
) {
    let mut last_time = std::time::Instant::now();
    let mut last_count = assets_processed.load(Ordering::Relaxed);

    loop {
        let sleep = tokio::time::sleep(std::time::Duration::from_secs(1));

        tokio::select! {
            _ = sleep => {}
            _ = cancellation_token.cancelled() => { break; }
        }

        let current_time = std::time::Instant::now();
        let current_count = assets_processed.load(Ordering::Relaxed);

        let elapsed = current_time.duration_since(last_time).as_secs_f64();
        let count = current_count - last_count;

        let current_rate = if elapsed > 0.0 { (count as f64) / elapsed } else { 0.0 };

        // Update rate
        {
            let mut rate_guard = rate.lock().await;
            *rate_guard = current_rate;
        }

        // Update for next iteration
        last_time = current_time;
        last_count = current_count;
    }
}
