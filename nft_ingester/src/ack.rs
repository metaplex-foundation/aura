use std::{collections::HashMap, sync::Arc};

use plerkle_messenger::{redis_messenger::RedisMessenger, Messenger, MessengerConfig};
use tokio::{
    sync::{
        broadcast::Receiver,
        mpsc::{unbounded_channel, UnboundedSender},
        Mutex,
    },
    task::{JoinError, JoinSet},
    time::{interval, Duration},
};
use tracing::log::error;

pub async fn create_ack_channel(
    shutdown_rx: Receiver<()>,
    config: MessengerConfig,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> UnboundedSender<(&'static str, String)> {
    let (tx, mut rx) = unbounded_channel::<(&'static str, String)>();
    tasks.lock().await.spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        let mut acks: HashMap<&str, Vec<String>> = HashMap::new();
        let source = RedisMessenger::new(config).await;
        if let Ok(mut msg) = source {
            while shutdown_rx.is_empty() {
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
        Ok(())
    });

    tx
}
