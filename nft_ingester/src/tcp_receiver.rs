use interface::message_handler::MessageHandler;
use log::{debug, error, info};
use std::convert::TryInto;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;

const HEADER_BYTE_SIZE: usize = 4;

pub struct TcpReceiver<M: MessageHandler> {
    callback: Arc<M>,
    reconnect_interval: Duration,
}

impl<M: MessageHandler> TcpReceiver<M> {
    pub fn new(callback: Arc<M>, reconnect_interval: Duration) -> Self {
        TcpReceiver {
            callback,
            reconnect_interval,
        }
    }

    pub async fn connect(&self, addr: SocketAddr, rx: Receiver<()>) -> io::Result<()> {
        while rx.is_empty() {
            info!("Receiver Connect {:?}", addr);

            if let Err(e) = self.connect_and_read(addr, rx.resubscribe()).await {
                error!("receiver: read error: {:?}", e);
            }

            sleep(self.reconnect_interval).await;
        }

        Ok(())
    }

    async fn connect_and_read(&self, addr: SocketAddr, rx: Receiver<()>) -> io::Result<()> {
        let stream = TcpStream::connect(&addr).await?;
        let mut stream = tokio::io::BufReader::new(stream);

        while rx.is_empty() {
            let (bytes_read, duration, num_elements) = self.read_response(&mut stream).await?;
            debug!(
                "TCP Socket: Received {} elements, {} in {:?}",
                num_elements, bytes_read, duration
            );
        }

        Ok(())
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
            self.callback.call(body[i..end].to_vec()).await;
            i = end;

            num_elements += 1;
        }

        Ok((bytes_read, duration, num_elements as u32))
    }
}
