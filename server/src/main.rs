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
// - [x] visibility timeout and failures
// - [ ] figure out visibility timeout
// - [ ] some web ui thing
// - [ ] investigate queueing order: does updated_at make sense?
//       it would send jobs to the back of the queue in the event that they fail,
//       does this matter? should we retry jobs consecutively?
//
// for visibility timeout, mark on job "visible_at", and on subsequent receives,
// check if visible_at <= now, and completed_at is null.
// have a checkpointer run every second that runs and sees for jobs if
// visible_at <= now and completed_at is null, and increment their number of attempts,
// indicating a timeout failure

use clap::Parser;

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

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", options.port)).await?;

    let app = server::app(options).await?;

    Ok(axum::serve(listener, app).await?)
}
