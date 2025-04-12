use crate::{AppError, AppState};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;
use uuid::Uuid;

#[derive(sqlx::FromRow, Serialize, Debug)]
pub struct Job {
    pub id: sqlx::types::Uuid,
    pub args: serde_json::Value,
    pub queue: String,
    pub attempts: i64,
}

#[derive(Serialize)]
pub struct EnqueueResponse {
    job_id: Uuid,
}

#[derive(Deserialize, Debug)]
pub struct QueueQuery {
    queue: String,
}

#[instrument(skip(state))]
pub async fn enqueue(
    State(state): State<Arc<Mutex<AppState>>>,
    queue_query: Query<QueueQuery>,
    body: String,
) -> axum::response::Result<Json<EnqueueResponse>, AppError> {
    let state = state.lock().await;

    let job_id = state.repo.enqueue_job(&queue_query.queue, &body).await?;

    Ok(axum::Json(EnqueueResponse { job_id }))
}

#[derive(Serialize)]
pub struct ReceiveResponse {
    job: Option<Job>,
}

pub async fn try_receive(
    State(state): State<Arc<Mutex<AppState>>>,
    queue_query: Query<QueueQuery>,
) -> axum::response::Result<Json<ReceiveResponse>, AppError> {
    let state = state.lock().await;

    let job = state.repo.try_receive_job(&queue_query.queue).await?;

    Ok(axum::Json(ReceiveResponse { job }))
}

pub async fn complete(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(job_id): Path<Uuid>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    state.repo.complete_job(job_id).await?;

    Ok(())
}
