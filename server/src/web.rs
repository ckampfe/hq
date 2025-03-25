use crate::{AppError, AppState};
use axum::{Router, extract::State, response::IntoResponse, routing::get};
use maud::html;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

macro_rules! layout {
    ($content:expr) => {
        maud::html! {
            (maud::DOCTYPE)
            head {
                meta charset="UTF-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title {
                    "hq web ui"
                }
                // minified
                script
                    src="https://unpkg.com/htmx.org@2.0.4"
                    integrity="sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+"
                    crossorigin="anonymous" {}
                script src="https://unpkg.com/htmx-ext-sse@2.2.2/sse.js" {}
                // unminified
                // script
                //     src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.js"
                //     integrity="sha384-oeUn82QNXPuVkGCkcrInrS1twIxKhkZiFfr2TdiuObZ3n3yIeMiqcRzkIcguaof1"
                //     crossorigin="anonymous" {}
                // link
                //     rel="stylesheet"
                //     href="https://cdn.jsdelivr.net/npm/bulma@1.0.2/css/bulma.min.css";
                style {
                    "
                    pre {
                        white-space: pre-wrap;
                    }
                    "
                }
            }
            body {
                ($content)
            }
        }
    }
}

#[derive(sqlx::FromRow, Debug)]
struct Job {
    id: Vec<u8>,
    args: String,
    queue: String,
    attempts: i64,
    inserted_at: String,
    updated_at: String,
    locked_at: String,
    completed_at: String,
    failed_at: String,
}

async fn web_index(
    State(state): State<Arc<Mutex<AppState>>>,
) -> axum::response::Result<impl IntoResponse, AppError> {
    let state = state.lock().await;

    let mut conn = state.pool.acquire().await.unwrap();

    let jobs_sample: Vec<Job> = sqlx::query_as(
        "
    select
        id,
        args,
        queue,
        attempts,
        inserted_at,
        updated_at,
        locked_at,
        completed_at,
        failed_at
    from hq_jobs
    order by inserted_at desc
    limit 10;
    ",
    )
    .fetch_all(&mut *conn)
    .await?;

    Ok(layout! {
        html! {
            h1 {
                "jerbs"
            }
            table {
                thead {
                    tr {
                        th { "queue" }
                        th { "id" }
                        th { "args" }
                        th { "attempts" }
                        th { "inserted_at" }
                        th { "updated_at" }
                        th { "locked_at" }
                        th { "completed_at" }
                        th { "failed_at" }
                    }
                }
                tbody {
                    @for job in jobs_sample {
                        tr {
                            td { (job.queue) }
                            td { (Uuid::from_bytes(job.id.try_into().unwrap())) }
                            td { (job.args) }
                            td { (job.attempts) }
                            td { (job.inserted_at) }
                            td { (job.updated_at) }
                            td { (job.locked_at) }
                            td { (job.completed_at) }
                            td { (job.failed_at) }
                        }
                    }
                }
            }

        }
    })
}

pub fn routes(state: Arc<Mutex<AppState>>) -> Router<Arc<Mutex<AppState>>> {
    Router::new()
        .route("/web", get(web_index))
        .with_state(state)
}
