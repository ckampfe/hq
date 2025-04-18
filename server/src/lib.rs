use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use clap::Parser;
use queue::start_lock_task;
use repo::Repo;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod job;
pub mod queue;
pub mod repo;
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
    pub repo: Repo,
    pub options: Options,
}

pub async fn app(options: Options) -> anyhow::Result<Router> {
    let db_name = if options.in_memory {
        "sqlite::memory:".to_string()
    } else {
        options.database.clone()
    };

    let repo = Repo::new(repo::Options {
        db_name,
        in_memory: options.in_memory,
    })
    .await?;

    repo.migrate().await?;

    // TODO start a supervisor task to watch this task,
    // and restart it if it fails
    start_lock_task(repo.clone(), std::time::Duration::from_secs(1));

    let state = AppState {
        repo,
        options: options.clone(),
    };

    let state = Arc::new(Mutex::new(state));

    let queue_routes = Router::new()
        .route("/jobs/enqueue", post(job::enqueue))
        .route("/jobs/receive", get(job::receive))
        .route("/jobs/{id}/complete", put(job::complete))
        .route("/jobs/{id}/fail", put(job::fail))
        .route("/queues/{name}", get(queue::show))
        .route("/queues/{name}", put(queue::update))
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
