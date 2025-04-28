use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateQueueRequest {
    pub name: String,
    pub max_attempts: i64,
    pub visibility_timeout_seconds: i64,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct ShowQueueResponse {
    pub name: String,
    pub max_attempts: i64,
    pub visibility_timeout_seconds: i64,
    pub inserted_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateQueueRequest {
    pub max_attempts: Option<i64>,
    pub visibility_timeout_seconds: Option<i64>,
}

impl UpdateQueueRequest {
    pub fn is_some(&self) -> bool {
        self.max_attempts.is_some() || self.visibility_timeout_seconds.is_some()
    }
}

#[derive(Serialize, Deserialize)]
pub struct EnqueueResponse {
    pub message_id: Uuid,
}
