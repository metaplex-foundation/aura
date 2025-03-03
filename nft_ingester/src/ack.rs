use std::collections::HashMap;

use plerkle_messenger::{redis_messenger::RedisMessenger, Messenger, MessengerConfig};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::{interval, Duration},
};
use tokio_util::sync::CancellationToken;
use tracing::log::error;

pub async fn create_ack_channel(
    config: MessengerConfig,
    cancellation_token: CancellationToken,
) -> UnboundedSender<(&'static str, String)> {
    let (tx, mut rx) = unbounded_channel::<(&'static str, String)>();
    usecase::executor::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        let mut acks: HashMap<&str, Vec<String>> = HashMap::new();
        let source = RedisMessenger::new(config).await;
        if let Ok(mut msg) = source {
            while !cancellation_token.is_cancelled() {
                tokio::select! {
                    _ = interval.tick() => {
                        if acks.is_empty() {
                            continue;
                        }
                        for (stream, msgs)  in acks.iter_mut() {
                            if let Err(e) = msg.ack_msg(stream, msgs).await {
                                error!("Error acking message: {}", e);
                            }
                            msgs.clear();
                        }

                    }
                    Some(msg) = rx.recv() => {
                        let (stream, msg) = msg;
                        let ackstream = acks.entry(stream).or_default();
                        ackstream.push(msg);
                    }
                }
            }
        }
    });

    tx
}
