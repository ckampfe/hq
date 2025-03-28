#![forbid(unsafe_code)]
// todo
//
// - [ ] curl/shell client
// - [ ] rust client
//
// queues
// - [x] create
// - [x] list
// - [ ] update
// - [ ] delete (and all jobs)
// jobs
// - [x] enqueue job
// - [x] receive job
// - [x] mark job complete
// - [ ] mark job as failed after N attempts
// - [ ] figure out visibility timeout
// - [ ] some web ui thing
// - [ ] investigate queueing order: does updated_at make sense?
//       it would send jobs to the back of the queue in the event that they fail,
//       does this matter? should we retry jobs consecutively?

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::Sqlite;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// async fn update_queue() -> axum::response::Result<impl IntoResponse, AppError> {
//     todo!();
//     Ok(())
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let options = server::Options::parse();

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

    let state = server::AppState {
        pool,
        options: options.clone(),
    };

    let state = Arc::new(Mutex::new(state));

    let queue_routes = Router::new()
        .route("/jobs/enqueue", post(server::job::enqueue))
        .route("/jobs/try_receive", get(server::job::try_receive))
        .route("/jobs/{id}/complete", put(server::job::complete))
        .route("/queues", get(server::queue::list))
        .route("/queues", post(server::queue::create))
        // .route("/queues/{id}", put(update_queue))
        ;

    let router = Router::new();

    #[cfg(feature = "web")]
    let router = {
        let web_routes = server::web::routes(Arc::clone(&state));
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
