use serde::{Deserialize, Serialize, de::DeserializeOwned};
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
    pub fn new(url: impl reqwest::IntoUrl, options: ClientOptions) -> Result<Self, reqwest::Error> {
        Ok(Self {
            url: url.into_url().unwrap(),
            http_client: reqwest::Client::builder()
                .timeout(options.request_timeout)
                .build()?,
        })
    }

    pub async fn enqueue_message<T: Serialize>(
        &self,
        queue: &str,
        message_params: &T,
    ) -> Result<EnqueueResponse, reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend(["queues", queue, "enqueue"]);
        }

        self.http_client
            .post(url)
            .json(message_params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    pub async fn receive_message<T: DeserializeOwned>(
        &self,
        queue: &str,
    ) -> Result<Option<Message<T>>, reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend(["queues", queue, "receive"]);
        }

        let message: Option<Message<T>> = self
            .http_client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(message)
    }

    pub async fn complete_message(&self, message_id: Uuid) -> Result<(), reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend([
                "messages",
                &message_id.as_hyphenated().to_string(),
                "complete",
            ]);
        }

        self.http_client.put(url).send().await?.error_for_status()?;

        Ok(())
    }

    pub async fn fail_message(&self, message_id: Uuid) -> Result<(), reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend(["messages", &message_id.as_hyphenated().to_string(), "fail"]);
        }

        self.http_client.put(url).send().await?.error_for_status()?;

        Ok(())
    }

    pub async fn list_queues(&self) -> Result<Vec<ListQueuesResponse>, reqwest::Error> {
        let mut url = self.url.clone();

        url.set_path("queues");

        self.http_client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
    }

    pub async fn create_queue(&self, queue: CreateQueueRequest) -> Result<(), reqwest::Error> {
        let mut url = self.url.clone();

        url.set_path("queues");

        let mut qp = url.query_pairs_mut();

        qp.append_pair("name", &queue.name);

        qp.append_pair("max_attempts", &queue.max_attempts.to_string());

        qp.append_pair(
            "visibility_timeout_seconds",
            &queue.visibility_timeout_seconds.to_string(),
        );

        let url: reqwest::Url = qp.finish().to_owned();

        self.http_client
            .post(url)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn get_queue(&self, queue: &str) -> Result<Option<Queue>, reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend(["queues", queue]);
        }

        let queue: Option<Queue> = self
            .http_client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(queue)
    }

    pub async fn update_queue(
        &self,
        queue: &str,
        params: UpdateQueueRequest,
    ) -> Result<(), reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend(["queues", queue]);
        }

        let mut qp = url.query_pairs_mut();

        if let Some(max_attempts) = params.max_attempts {
            qp.append_pair("max_attempts", &max_attempts.to_string());
        }

        if let Some(vts) = params.visibility_timeout_seconds {
            qp.append_pair("visibility_timeout_seconds", &vts.to_string());
        }

        let url: reqwest::Url = qp.finish().to_owned();

        self.http_client.put(url).send().await?.error_for_status()?;

        Ok(())
    }

    pub async fn delete_queue(&self, queue: &str) -> Result<(), reqwest::Error> {
        let mut url = self.url.clone();

        {
            let mut path_segments = url.path_segments_mut().unwrap();
            path_segments.extend(["queues", queue]);
        }

        self.http_client
            .delete(url)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Queue {
    pub name: String,
    pub max_attempts: i64,
    pub visibility_timeout_seconds: i64,
}

#[derive(Serialize)]
pub struct UpdateQueueRequest {
    max_attempts: Option<i64>,
    visibility_timeout_seconds: Option<i64>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Message<T> {
    pub id: Uuid,
    pub args: T,
    pub queue: String,
    pub attempts: i64,
}

#[derive(Deserialize)]
pub struct ListQueuesResponse {
    pub name: String,
    pub max_attempts: i64,
}

#[derive(Serialize)]
pub struct CreateQueueRequest {
    name: String,
    max_attempts: i64,
    visibility_timeout_seconds: i64,
}

#[derive(Deserialize)]
pub struct EnqueueResponse {
    pub message_id: Uuid,
}

#[cfg(test)]
mod tests {
    use server::Options;

    use super::*;
    use std::{collections::HashMap, sync::atomic::AtomicU16};

    #[tokio::test]
    async fn creates_queue() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

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
    async fn lists_queues_with_queues() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queue_name = "some_queue".to_string();
        let max_attempts = 5;

        client
            .create_queue(CreateQueueRequest {
                name: queue_name.clone(),
                max_attempts,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        let queues = client.list_queues().await.unwrap();

        assert_eq!(queues[0].name, queue_name);
        assert_eq!(queues[0].max_attempts, max_attempts)
    }

    #[tokio::test]
    async fn lists_queues_no_queues() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queues = client.list_queues().await.unwrap();

        assert!(queues.is_empty())
    }

    #[tokio::test]
    async fn get_queue_none() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let q: Option<Queue> = client.get_queue("some_queue").await.unwrap();

        assert!(q.is_none())
    }

    #[tokio::test]
    async fn get_queue_some() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();

        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.visibility_timeout_seconds, 30);
    }

    #[tokio::test]
    async fn updates_queue_max_attempts() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();
        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.visibility_timeout_seconds, 30);

        client
            .update_queue(
                "some_queue",
                UpdateQueueRequest {
                    max_attempts: Some(6),
                    visibility_timeout_seconds: None,
                },
            )
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();
        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 6);
        assert_eq!(q.visibility_timeout_seconds, 30);
    }

    #[tokio::test]
    async fn updates_queue_max_visibility_timeout() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();
        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.visibility_timeout_seconds, 30);

        client
            .update_queue(
                "some_queue",
                UpdateQueueRequest {
                    max_attempts: None,
                    visibility_timeout_seconds: Some(10),
                },
            )
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();
        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.visibility_timeout_seconds, 10);
    }

    #[tokio::test]
    async fn updates_queue_none() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();
        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.visibility_timeout_seconds, 30);

        client
            .update_queue(
                "some_queue",
                UpdateQueueRequest {
                    max_attempts: None,
                    visibility_timeout_seconds: None,
                },
            )
            .await
            .unwrap();

        let q = client.get_queue("some_queue").await.unwrap().unwrap();
        assert_eq!(q.name, "some_queue");
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.visibility_timeout_seconds, 30);
    }

    #[tokio::test]
    async fn delete_queue_none() {
        let (port, _server_handle) = serve().await;

        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client.delete_queue("some_queue").await.unwrap();

        assert!(client.list_queues().await.unwrap().is_empty())
    }

    #[tokio::test]
    async fn delete_queue_some() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        assert!(!client.list_queues().await.unwrap().is_empty());

        client.delete_queue("some_queue").await.unwrap();

        assert!(client.list_queues().await.unwrap().is_empty())
    }

    #[tokio::test]
    async fn enqueues_message() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        client
            .create_queue(CreateQueueRequest {
                name: "some_queue".to_string(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        client
            .enqueue_message("some_queue", &HashMap::from([("foo", "bar")]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn receive_no_message() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queue = "some_queue".to_string();

        client
            .create_queue(CreateQueueRequest {
                name: queue.clone(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        #[derive(Serialize, Deserialize)]
        struct Somemessage {
            foo: String,
        }

        let message_response: Option<Message<Somemessage>> =
            client.receive_message(&queue).await.unwrap();

        assert!(message_response.is_none())
    }

    #[tokio::test]
    async fn receive_with_message() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queue = "some_queue".to_string();

        client
            .create_queue(CreateQueueRequest {
                name: queue.clone(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        #[derive(Serialize, Deserialize)]
        struct Somemessage {
            foo: String,
        }

        let message = Somemessage {
            foo: "bar".to_string(),
        };

        client.enqueue_message(&queue, &message).await.unwrap();

        let message_response: Message<Somemessage> =
            client.receive_message(&queue).await.unwrap().unwrap();

        assert_eq!(message_response.args.foo, message.foo);
        assert_eq!(message_response.queue, queue);
    }

    #[tokio::test]
    async fn completes_uncompleted_message() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queue = "some_queue".to_string();

        client
            .create_queue(CreateQueueRequest {
                name: queue.clone(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        #[derive(Serialize, Deserialize, Debug)]
        struct Somemessage {
            foo: String,
        }

        let message = Somemessage {
            foo: "bar".to_string(),
        };

        client.enqueue_message(&queue, &message).await.unwrap();

        let message_response: Message<Somemessage> =
            client.receive_message(&queue).await.unwrap().unwrap();

        client.complete_message(message_response.id).await.unwrap();

        let message_response: Option<Message<Somemessage>> =
            client.receive_message(&queue).await.unwrap();

        assert!(message_response.is_none());
    }

    #[tokio::test]
    async fn fails_message() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queue = "some_queue".to_string();

        client
            .create_queue(CreateQueueRequest {
                name: queue.clone(),
                max_attempts: 5,
                visibility_timeout_seconds: 30,
            })
            .await
            .unwrap();

        #[derive(Serialize, Deserialize, Debug)]
        struct Somemessage {
            foo: String,
        }

        let message = Somemessage {
            foo: "bar".to_string(),
        };

        client.enqueue_message(&queue, &message).await.unwrap();

        let message_response: Message<Somemessage> =
            client.receive_message(&queue).await.unwrap().unwrap();

        client.fail_message(message_response.id).await.unwrap();

        let message_response: Option<Message<Somemessage>> =
            client.receive_message(&queue).await.unwrap();

        assert!(message_response.is_none());
    }

    #[tokio::test]
    async fn visibility_timeout_unlocks_locked_message_and_respects_max_attempts() {
        let (port, _server_handle) = serve().await;
        let client =
            Client::new(format!("http://localhost:{port}"), ClientOptions::default()).unwrap();

        let queue = "some_queue".to_string();

        client
            .create_queue(CreateQueueRequest {
                name: queue.clone(),
                max_attempts: 2,
                visibility_timeout_seconds: 1,
            })
            .await
            .unwrap();

        #[derive(Serialize, Deserialize, Debug)]
        struct Somemessage {
            foo: String,
        }

        let message = Somemessage {
            foo: "bar".to_string(),
        };

        client.enqueue_message(&queue, &message).await.unwrap();

        let message_response1: Message<Somemessage> =
            client.receive_message(&queue).await.unwrap().unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let message_response2: Message<Somemessage> =
            client.receive_message(&queue).await.unwrap().unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let message_response3: Option<Message<Somemessage>> =
            client.receive_message(&queue).await.unwrap();

        assert_eq!(message_response1.id, message_response2.id);
        assert_eq!(message_response1.attempts, 1);
        assert_eq!(message_response2.attempts, 2);
        assert!(message_response3.is_none());
    }

    async fn serve() -> (u16, ServerHandle) {
        static PORT: AtomicU16 = AtomicU16::new(10000);

        let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let options = Options {
            port,
            request_timeout: Some(5),
            database: ":memory:".to_string(),
        };

        let router = server::app(options).await.unwrap();

        let listener = tokio::net::TcpListener::bind(("localhost", port))
            .await
            .unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    rx.await.unwrap();
                })
                .await
                .unwrap()
        });

        (port, ServerHandle { tx: Some(tx) })
    }

    struct ServerHandle {
        tx: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl Drop for ServerHandle {
        fn drop(&mut self) {
            let tx = self.tx.take().unwrap();
            tx.send(()).unwrap();
        }
    }
}
