use crate::{AppError, AppState};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use serde::Serialize;
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

#[instrument(skip(state))]
pub async fn complete(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(job_id): Path<Uuid>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    state.repo.complete_job(job_id).await?;

    Ok(())
}

#[instrument(skip(state))]
pub async fn fail(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(job_id): Path<Uuid>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    state.repo.fail_job(job_id).await?;

    Ok(())
}
