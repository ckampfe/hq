use crate::{AppError, AppState, repo::Repo};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn list(
    State(state): State<Arc<Mutex<AppState>>>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    let queues = state.repo.get_queues().await?;

    Ok(axum::Json(queues))
}

#[derive(Deserialize)]
pub struct CreateQueueRequest {
    name: String,
    max_attempts: i64,
    visibility_timeout_seconds: i64,
}

pub async fn create(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(create_queue): Json<CreateQueueRequest>,
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

pub fn start_lock_task(
    repo: Repo,
    tick: std::time::Duration,
) -> tokio::task::JoinHandle<Result<(), sqlx::Error>> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tick).await;
            repo.unlock_jobs_locked_longer_than_timeout().await?;
        }
    })
}
