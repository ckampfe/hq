#![forbid(unsafe_code)]
// todo

// misc
// - [ ] curl/shell client
// - [ ] rust client
// - [ ] set up tracing
// - [ ] where to keep db files
// - [ ] job pruning strategy
// - [ ] some web ui thing
// - [ ] initial readme
// queues
// - [x] create
// - [x] list
// - [x] get/show
// - [x] update
// - [x] delete (and all jobs)
// - [x] use query params for most stuff instead of json bodies
// jobs
// - [x] enqueue job
// - [x] receive job
// - [x] mark job complete
// - [x] mark job as failed after N attempts
// - [x] visibility timeout and failures
// - [x] do visibility timeout as float comparison (1.3s > 1.0s, etc)
// - [x] figure out visibility timeout
// - [x] investigate queueing order: does updated_at make sense?
//       it would send jobs to the back of the queue in the event that they fail,
//       does this matter? should we retry jobs consecutively?
// - [ ] "make visible on timeout" configurable?
//
// for visibility timeout, mark on job "visible_at", and on subsequent receives,
// check if visible_at <= now, and completed_at is null.
// have a checkpointer run every second that runs and sees for jobs if
// visible_at <= now and completed_at is null, and increment their number of attempts,
// indicating a timeout failure

use clap::Parser;

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
