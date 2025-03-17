#![forbid(unsafe_code)]
// todo
//
// queues
// - [x] create
// - [x] list
// - [ ] update
// - [ ] delete (and all jobs)
// jobs
// - [x] enqueue job
// - [x] receive job
// - [ ] mark job complete
// - [ ] mark job as failed after N attempts
// - [ ] figure out visibility timeout
// - [ ] some web ui thing

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Sqlite};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;
use uuid::Uuid;

#[cfg(feature = "web")]
mod web;

#[derive(Deserialize, Debug)]
struct QueueQuery {
    queue: String,
}

#[derive(Serialize)]
struct EnqueueResponse {
    job_id: Uuid,
}

#[axum::debug_handler]
#[instrument(skip(state))]
async fn enqueue_job(
    State(state): State<Arc<Mutex<AppState>>>,
    queue_query: Query<QueueQuery>,
    body: String,
) -> axum::response::Result<Json<EnqueueResponse>, AppError> {
    let state = state.lock().await;

    let valid_json_args: serde_json::Value = serde_json::from_str(&body)?;

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
    .bind(&valid_json_args)
    .bind(queue_id)
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;

    Ok(axum::Json(EnqueueResponse { job_id }))
}

#[derive(sqlx::FromRow, Serialize)]
struct Job {
    id: sqlx::types::Uuid,
    args: serde_json::Value,
    queue: String,
    attempts: i64,
    inserted_at: String,
    updated_at: String,
}

#[derive(Serialize)]
struct ReceiveResponse {
    job: Option<Job>,
}

async fn receive_job(
    State(state): State<Arc<Mutex<AppState>>>,
    queue_query: Query<QueueQuery>,
) -> axum::response::Result<Json<ReceiveResponse>, AppError> {
    let state = state.lock().await;

    let mut conn = state.pool.acquire().await?;

    let mut txn = conn.begin().await?;

    let job: Option<Job> = sqlx::query_as(
        "
        update hq_jobs
        set
            attempts = attempts + 1,
            locked_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
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
            order by hq_jobs.inserted_at asc
            limit 1
        )
        returning
            id,
            args,
            '' as queue,
            attempts,
            inserted_at,
            updated_at;
            ",
    )
    .bind(&queue_query.queue)
    .bind(5)
    .fetch_optional(&mut *txn)
    .await?;

    txn.commit().await?;

    if let Some(mut job) = job {
        // TODO probably a better way,
        // but can't figure it out in the query yet
        job.queue = queue_query.queue.clone();
        Ok(axum::Json(ReceiveResponse { job: Some(job) }))
    } else {
        Ok(axum::Json(ReceiveResponse { job: None }))
    }
}

async fn delete_job(
    State(state): State<Arc<Mutex<AppState>>>,
    Path(id): Path<Uuid>,
) -> axum::response::Result<impl IntoResponse> {
    Ok("hi")
}

#[derive(Deserialize)]
struct CreateQueueRequest {
    name: String,
    max_attempts: i64,
    visibility_timeout_seconds: i64,
}

async fn create_queue(
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

async fn update_queue() -> axum::response::Result<impl IntoResponse, AppError> {
    todo!();
    Ok(())
}

#[derive(sqlx::FromRow, Serialize)]
struct ListQueuesResponse {
    name: String,
    max_attempts: i64,
    inserted_at: String,
    updated_at: String,
}

async fn list_queues(
    State(state): State<Arc<Mutex<AppState>>>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    let mut conn = state.pool.acquire().await?;

    let queues: Vec<ListQueuesResponse> = sqlx::query_as(
        "
    select
        name,
        max_attempts,
        inserted_at,
        updated_at
    from hq_queues
    order by name
    ",
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(axum::Json(queues))
}

#[derive(Parser, Clone, Debug)]
struct Options {
    /// the port to bind the server to
    #[arg(short, long, env, default_value = "9999")]
    port: u16,
    /// the maximum request timeout, in seconds
    #[arg(short, long, env)]
    request_timeout: Option<u64>,
    /// the database path
    #[arg(short, long, env)]
    database: String,
    /// should the db be in memory
    #[arg(short, long, env, default_value_t = false)]
    in_memory: bool,
}

#[derive(Debug)]
struct AppState {
    pool: sqlx::Pool<Sqlite>,
    options: Options,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let options = Options::parse();

    let db_name = if options.in_memory {
        "sqlite::memory:".to_string()
    } else {
        options.database.clone()
    };

    let opts = sqlx::sqlite::SqliteConnectOptions::from_str(&db_name)?
        .busy_timeout(std::time::Duration::from_secs(5))
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .create_if_missing(true)
        .foreign_keys(true)
        .in_memory(options.in_memory);

    let pool = sqlx::SqlitePool::connect_with(opts).await?;

    let mut conn = pool.acquire().await?;

    sqlx::raw_sql(
        "
        create table if not exists hq_queues (
            id blob primary key,
            name text not null,
            max_attempts integer not null default -1,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            updated_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
        );

        create unique index if not exists name_idx on hq_queues(name);

        create trigger if not exists hq_queues_updated_at after update on hq_queues
        begin
            update hq_queues set updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
            where id = old.id;
        end;

        create table if not exists hq_jobs (
            id blob primary key,
            args text not null,
            queue_id integer not null,
            attempts integer not null default 0,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            updated_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            locked_at datetime,
            completed_at datetime,
            failed_at datetime,

            foreign key(queue_id) references hq_queues(id)
        );

        create trigger if not exists hq_jobs_updated_at after update on hq_jobs
        begin
            update hq_jobs set updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
            where id = old.id;
        end;

        create index if not exists queue_id_idx on hq_jobs(queue_id);
        create index if not exists inserted_at_idx on hq_jobs(inserted_at);
        create index if not exists locked_at_idx on hq_jobs(locked_at);
        create index if not exists completed_at_idx on hq_jobs(completed_at);
    ",
    )
    .execute(&mut *conn)
    .await?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", options.port)).await?;

    let state = AppState {
        pool,
        options: options.clone(),
    };

    let state = Arc::new(Mutex::new(state));

    let queue_routes = Router::new()
        .route("/jobs/enqueue", post(enqueue_job))
        .route("/jobs/receive", get(receive_job))
        .route("/jobs/{id}", delete(delete_job))
        .route("/queues", get(list_queues))
        .route("/queues", post(create_queue))
        .route("/queues/{id}", put(update_queue));

    let router = Router::new();

    #[cfg(feature = "web")]
    let router = {
        let web_routes = web::routes(Arc::clone(&state));
        router.merge(web_routes)
    };

    let router = router
        .merge(queue_routes)
        .with_state(Arc::clone(&state))
        .layer(tower_http::normalize_path::NormalizePathLayer::trim_trailing_slash())
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let router = if let Some(request_timeout) = options.request_timeout {
        router.layer(tower_http::timeout::TimeoutLayer::new(
            std::time::Duration::from_secs(request_timeout),
        ))
    } else {
        router
    };

    Ok(axum::serve(listener, router).await?)
}

// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
