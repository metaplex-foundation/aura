use crate::message_handler::MessageHandler;
use log::{debug, error, info};
use std::convert::TryInto;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

const HEADER_BYTE_SIZE: usize = 4;

pub struct TcpReceiver {
    callback: Arc<MessageHandler>,
    reconnect_interval: Duration,
}

impl TcpReceiver {
    pub fn new(callback: Arc<MessageHandler>, reconnect_interval: Duration) -> Self {
        TcpReceiver {
            callback,
            reconnect_interval,
        }
    }

    pub async fn connect(&self, addr: SocketAddr, keep_running: Arc<AtomicBool>) -> io::Result<()> {
        loop {
            if !keep_running.load(Ordering::SeqCst) {
                return Ok(());
            }
            info!("Receiver Connect {:?}", addr);

            if let Err(e) = self.connect_and_read(addr, keep_running.clone()).await {
                error!("receiver: read error: {:?}", e);
            }

            sleep(self.reconnect_interval).await;
        }
    }

    async fn connect_and_read(
        &self,
        addr: SocketAddr,
        keep_running: Arc<AtomicBool>,
    ) -> io::Result<()> {
        let stream = TcpStream::connect(&addr).await?;
        let mut stream = tokio::io::BufReader::new(stream);

        loop {
            if !keep_running.load(Ordering::SeqCst) {
                return Ok(());
            }

            match self.read_response(&mut stream).await {
                Ok((bytes_read, duration, num_elements)) => {
                    debug!(
                        "TCP Socket: Received {} elements, {} in {:?}",
                        num_elements, bytes_read, duration
                    );
                }
                Err(e) => {
                    error!("read_response: {}", e)
                }
            };
        }
    }

    async fn read_response(
        &self,
        stream: &mut tokio::io::BufReader<TcpStream>,
    ) -> io::Result<(usize, Duration, u32)> {
        let mut header = [0; HEADER_BYTE_SIZE];
        stream.read_exact(&mut header).await?;

        let mut body = vec![0; u32::from_le_bytes(header) as usize];
        let now = Instant::now();
        stream.read_exact(&mut body).await?;

        let duration = now.elapsed();
        let bytes_read = header.len() + body.len();
        let mut num_elements = 0;

        let mut i = 0;
        while i < body.len() {
            let mut end = i + HEADER_BYTE_SIZE;
            let size_bytes = body[i..end].try_into().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to convert size: {}", e),
                )
            })?;
            let size = u32::from_le_bytes(size_bytes) as usize;
            i = end;

            end = i + size;
            let handler_clone = self.callback.clone();
            let body_clone = body[i..end].to_vec();
            if let Err(e) = tokio::spawn(async move { handler_clone.call(body_clone).await }).await
            {
                error!("callback panic: {:?}", e);
            };
            i = end;

            num_elements += 1;
        }

        Ok((bytes_read, duration, num_elements as u32))
    }
}
