use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use clap::Parser;
use sqlx::Sqlite;
use std::{str::FromStr, sync::Arc};
use tokio::sync::Mutex;

pub mod job;
pub mod queue;
#[cfg(feature = "web")]
pub mod web;

#[derive(Parser, Clone, Debug)]
pub struct Options {
    /// the port to bind the server to
    #[arg(short, long, env, default_value = "9999")]
    pub port: u16,
    /// the maximum request timeout, in seconds
    #[arg(short, long, env)]
    pub request_timeout: Option<u64>,
    /// the database path
    #[arg(short, long, env)]
    pub database: String,
    /// should the db be in memory
    #[arg(short, long, env, default_value_t = false)]
    pub in_memory: bool,
}

#[derive(Debug)]
pub struct AppState {
    pub pool: sqlx::Pool<Sqlite>,
    pub options: Options,
}

pub async fn app(options: Options) -> anyhow::Result<Router> {
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
    let state = AppState {
        pool,
        options: options.clone(),
    };

    let state = Arc::new(Mutex::new(state));

    let queue_routes = Router::new()
        .route("/jobs/enqueue", post(job::enqueue))
        .route("/jobs/try_receive", get(job::try_receive))
        .route("/jobs/{id}/complete", put(job::complete))
        .route("/queues", get(queue::list))
        .route("/queues", post(queue::create))
        // .route("/queues/{id}", put(update_queue))
        ;

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

    Ok(router)
}

// Make our own error that wraps `anyhow::Error`.
pub struct AppError(anyhow::Error);

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
