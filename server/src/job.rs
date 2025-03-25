use crate::{AppError, AppState};
use axum::{
    Json,
    extract::{Path, Query, State},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sqlx::Acquire;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;
use uuid::Uuid;

#[derive(sqlx::FromRow, Serialize)]
struct Job {
    id: sqlx::types::Uuid,
    args: serde_json::Value,
    queue: String,
    attempts: i64,
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

    let _valid_json_args: serde::de::IgnoredAny = serde_json::from_str(&body)?;

    let mut conn = state.pool.acquire().await?;

    let mut txn = conn.begin().await?;

    let (queue_id,): (Uuid,) = sqlx::query_as(
        "
    select
        id
    from hq_queues
    where name = ?
        ",
    )
    .bind(&queue_query.queue)
    .fetch_one(&mut *txn)
    .await?;

    let job_id = Uuid::new_v4();

    sqlx::query(
        "
    insert into hq_jobs(id, args, queue_id)
    values (?, ?, ?)
    ",
    )
    .bind(&job_id.as_bytes()[..])
    .bind(&body)
    .bind(queue_id)
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;

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

    let mut conn = state.pool.acquire().await?;

    let job: Option<Job> = sqlx::query_as(
        "
        update hq_jobs
        set
            attempts = attempts + 1,
            locked_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
        where id = (
            select
                hq_jobs.id
            from hq_jobs
            inner join hq_queues
                on hq_queues.id = hq_jobs.queue_id
                and hq_queues.name = ?
            and completed_at is null
            and locked_at is null
            and attempts <= ?
            order by hq_jobs.updated_at asc
            limit 1
        )
        returning
            id,
            args,
            '' as queue,
            attempts;
            ",
    )
    .bind(&queue_query.queue)
    .bind(5)
    .fetch_optional(&mut *conn)
    .await?;

    if let Some(mut job) = job {
        // TODO probably a better way,
        // but can't figure it out in the query yet
        job.queue = queue_query.queue.clone();
        Ok(axum::Json(ReceiveResponse { job: Some(job) }))
    } else {
        Ok(axum::Json(ReceiveResponse { job: None }))
    }
}

pub async fn complete(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(id): Path<Uuid>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;
    let mut conn = state.pool.acquire().await?;

    // TODO think about this,
    // should we have a notion of "receipt handle"?
    // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
    sqlx::query(
        "
    update hq_jobs
    set
        completed_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
        locked_at = null
    where id = ?
    and locked_at is not null
    and completed_at is null
    returning
        id
    ",
    )
    .bind(id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}
