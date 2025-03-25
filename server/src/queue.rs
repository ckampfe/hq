use crate::{AppError, AppState};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(sqlx::FromRow, Serialize)]
struct ListQueuesResponse {
    name: String,
    max_attempts: i64,
}

pub async fn list(
    State(state): State<Arc<Mutex<AppState>>>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    let mut conn = state.pool.acquire().await?;

    let queues: Vec<ListQueuesResponse> = sqlx::query_as(
        "
    select
        name,
        max_attempts
    from hq_queues
    order by name
    ",
    )
    .fetch_all(&mut *conn)
    .await?;

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

    let state = state.lock().await;

    let mut conn = state.pool.acquire().await.map_err(|e| AppError(e.into()))?;

    let queue_id = Uuid::new_v4();

    sqlx::query(
        "
    insert into hq_queues (id, name, max_attempts) values (?, ?, ?);
    ",
    )
    .bind(queue_id)
    .bind(create_queue.name)
    .bind(create_queue.max_attempts)
    .execute(&mut *conn)
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
