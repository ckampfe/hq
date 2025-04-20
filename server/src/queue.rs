use crate::repo::Repo;
use crate::{AppError, AppState};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;
use uuid::Uuid;

#[instrument(skip(state))]
pub async fn list(
    State(state): State<Arc<Mutex<AppState>>>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    let queues = state.repo.get_queues().await?;

    Ok(axum::Json(queues))
}

#[derive(Deserialize, Debug)]
pub struct CreateQueueRequest {
    name: String,
    max_attempts: i64,
    visibility_timeout_seconds: i64,
}

#[instrument(skip(state))]
pub async fn create(
    State(state): State<Arc<Mutex<AppState>>>,
    create_queue: Query<CreateQueueRequest>,
) -> axum::response::Result<impl IntoResponse> {
    if create_queue.max_attempts < 1 {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "max_attempts must be >= 1",
        )
            .into());
    }

    if create_queue.visibility_timeout_seconds < 1 {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "visibility_timeout_seconds must be >= 1",
        )
            .into());
    }

    let state = state.lock().await;

    state
        .repo
        .create_queue(
            &create_queue.name,
            create_queue.max_attempts,
            create_queue.visibility_timeout_seconds,
        )
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(ref database_error) => {
                if database_error.is_unique_violation() {
                    (StatusCode::CONFLICT, "Error: queue name must be unique").into_response()
                } else {
                    AppError(e.into()).into_response()
                }
            }
            _ => AppError(e.into()).into_response(),
        })?;

    Ok(())
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ShowQueueResponse {
    name: String,
    max_attempts: i64,
    visibility_timeout_seconds: i64,
}

#[instrument(skip(state))]
pub async fn show(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(queue): Path<String>,
) -> axum::response::Result<Json<Option<ShowQueueResponse>>, AppError> {
    let state = state.lock().await;

    let queue = state.repo.get_queue(queue).await?;

    Ok(axum::Json(queue))
}

#[derive(Deserialize, Debug)]
pub struct UpdateQueueRequest {
    pub max_attempts: Option<i64>,
    pub visibility_timeout_seconds: Option<i64>,
}

impl UpdateQueueRequest {
    pub fn is_some(&self) -> bool {
        self.max_attempts.is_some() || self.visibility_timeout_seconds.is_some()
    }
}

#[instrument(skip(state))]
pub async fn update(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(queue_name): Path<String>,
    update_queue: Query<UpdateQueueRequest>,
) -> axum::response::Result<impl IntoResponse> {
    if let Some(max_attempts) = update_queue.max_attempts {
        if max_attempts < 1 {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                "max_attempts must be >= 1",
            )
                .into());
        }
    }

    if let Some(visibility_timeout_seconds) = update_queue.visibility_timeout_seconds {
        if visibility_timeout_seconds < 1 {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                "visibility_timeout_seconds must be >= 1",
            )
                .into());
        }
    }

    let state = state.lock().await;

    state
        .repo
        .update_queue(&queue_name, update_queue.deref())
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(ref database_error) => {
                if database_error.is_unique_violation() {
                    (StatusCode::CONFLICT, "Error: queue name must be unique").into_response()
                } else {
                    AppError(e.into()).into_response()
                }
            }
            _ => AppError(e.into()).into_response(),
        })?;

    Ok(())
}

#[instrument(skip(state))]
pub async fn delete(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(queue_name): Path<String>,
) -> axum::response::Result<(), AppError> {
    let state = state.lock().await;

    state.repo.delete_queue(&queue_name).await?;

    Ok(())
}

#[derive(Serialize)]
pub struct EnqueueResponse {
    job_id: Uuid,
}

#[instrument(skip(state))]
pub async fn enqueue(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(queue): Path<String>,
    body: String,
) -> axum::response::Result<Json<EnqueueResponse>, AppError> {
    let state = state.lock().await;

    let job_id = state.repo.enqueue_job(&queue, &body).await?;

    Ok(axum::Json(EnqueueResponse { job_id }))
}

#[instrument(skip(state))]
pub async fn receive(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(queue): Path<String>,
) -> axum::response::Result<Json<Option<crate::job::Job>>, AppError> {
    let state = state.lock().await;

    let job = state.repo.receive_job(&queue).await?;

    Ok(axum::Json(job))
}

#[instrument]
pub fn start_lock_task(
    repo: Repo,
    tick: std::time::Duration,
) -> tokio::task::JoinHandle<Result<(), sqlx::Error>> {
    tokio::spawn(async move {
        loop {
            repo.unlock_jobs_locked_longer_than_timeout().await?;
            tokio::time::sleep(tick).await;
        }
    })
}
