use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
pub struct Client {
    url: reqwest::Url,
    http_client: reqwest::Client,
}

pub struct ClientOptions {
    request_timeout: std::time::Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            request_timeout: std::time::Duration::from_secs(30),
        }
    }
}

impl Client {
    pub fn new(url: impl reqwest::IntoUrl, options: ClientOptions) -> Self {
        Self {
            url: url.into_url().unwrap(),
            http_client: reqwest::Client::builder()
                .timeout(options.request_timeout)
                .build()
                .unwrap(),
        }
    }

    pub async fn enqueue_job<T: Serialize>(
        &self,
        queue: &str,
        job_params: T,
    ) -> Result<EnqueueResponse, reqwest::Error> {
        let mut url = self.url.clone();

        url.set_path("jobs/enqueue");

        self.http_client
            .post(url)
            .query(&[("queue", queue)])
            .json(&job_params)
            .send()
            .await?
            .json()
            .await
    }

    pub async fn try_receive_job(&self) {}
    pub async fn receive_job(&self) {}
    pub async fn complete_job(&self) {}

    pub async fn list_queues(&self) {}

    pub async fn create_queue(&self, queue: CreateQueueRequest) -> Result<(), reqwest::Error> {
        let mut url = self.url.clone();

        url.set_path("queues");

        self.http_client.post(url).json(&queue).send().await?;

        Ok(())
    }
}

#[derive(Serialize)]
pub struct CreateQueueRequest {
    name: String,
    max_attempts: i64,
    visibility_timeout_seconds: i64,
}

#[derive(Deserialize)]
pub struct EnqueueResponse {
    pub job_id: Uuid,
}

#[cfg(test)]
mod tests {
    use server::Options;

    use super::*;
    use std::{collections::HashMap, sync::atomic::AtomicU16};

    #[tokio::test]
    async fn creates_queue() {
        let (port, _server_handle) = serve().await;
        let client = Client::new(format!("http://localhost:{port}"), ClientOptions::default());

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn enqueues_job() {
        let (port, _server_handle) = serve().await;
        let client = Client::new(format!("http://localhost:{port}"), ClientOptions::default());

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        client
            .enqueue_job("some_queue", HashMap::from([("foo", "bar")]))
            .await
            .unwrap();
    }

    async fn serve() -> (u16, ShutdownOnDrop) {
        static PORT: AtomicU16 = AtomicU16::new(10000);

        let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let options = Options {
            port,
            request_timeout: Some(5),
            database: "testdb".to_string(),
            in_memory: true,
        };

        let router = server::app(options).await.unwrap();

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
                .await
                .unwrap();

            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    rx.await.unwrap();
                })
                .await
        });

        (port, ShutdownOnDrop { tx: Some(tx) })
    }

    struct ShutdownOnDrop {
        tx: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            let tx = self.tx.take().unwrap();
            tx.send(()).unwrap();
        }
    }
}
