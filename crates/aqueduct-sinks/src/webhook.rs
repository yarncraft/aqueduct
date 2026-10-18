use std::time::Duration;

use aqueduct_core::{ChangeEvent, Error, Result, Sink};
use async_trait::async_trait;

const MAX_ATTEMPTS: u32 = 5;

/// POSTs each event as JSON. Retries network errors, 5xx and 429 with
/// exponential backoff; other 4xx fail the pipeline (it will not be checkpointed).
pub struct WebhookSink {
    client: reqwest::Client,
    url: String,
}

impl WebhookSink {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::Sink(e.to_string()))?;
        Ok(Self { client, url: url.into() })
    }
}

fn retryable(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

#[async_trait]
impl Sink for WebhookSink {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()> {
        let mut delay = Duration::from_millis(200);
        for attempt in 1..=MAX_ATTEMPTS {
            let err = match self.client.post(&self.url).json(event).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) if !retryable(resp.status()) => {
                    return Err(Error::Sink(format!("webhook returned {}", resp.status())));
                }
                Ok(resp) => format!("status {}", resp.status()),
                Err(e) => e.to_string(),
            };
            if attempt == MAX_ATTEMPTS {
                return Err(Error::Sink(format!("webhook failed after {attempt} attempts: {err}")));
            }
            tracing::warn!(attempt, %err, "webhook delivery failed, retrying");
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Tiny one-shot HTTP server answering each connection with `statuses[i]`.
    async fn server(statuses: Vec<&'static str>) -> (String, tokio::task::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let h = tokio::spawn(async move {
            let mut n = 0;
            for s in statuses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!("HTTP/1.1 {s}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                sock.write_all(resp.as_bytes()).await.unwrap();
                n += 1;
            }
            n
        });
        (url, h)
    }

    #[tokio::test]
    async fn retries_5xx_then_succeeds() {
        let (url, h) = server(vec!["503 Service Unavailable", "200 OK"]).await;
        let mut sink = WebhookSink::new(url).unwrap();
        sink.send(&crate::sample()).await.unwrap();
        assert_eq!(h.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn does_not_retry_4xx() {
        let (url, h) = server(vec!["400 Bad Request"]).await;
        let mut sink = WebhookSink::new(url).unwrap();
        assert!(sink.send(&crate::sample()).await.is_err());
        assert_eq!(h.await.unwrap(), 1);
    }
}
