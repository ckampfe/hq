use std::str::FromStr;

use serde::Serialize;
use sqlx::{Connection, Sqlite};
use tracing::instrument;
use uuid::Uuid;

use crate::{message::Message, queue, web};

#[derive(Debug)]
pub struct Options {
    pub db_name: String,
    pub in_memory: bool,
}

#[derive(Clone, Debug)]
pub struct Repo {
    pool: sqlx::Pool<Sqlite>,
}

impl Repo {
    #[instrument]
    pub async fn new(options: Options) -> anyhow::Result<Repo> {
        let opts = sqlx::sqlite::SqliteConnectOptions::from_str(&options.db_name)?
            .busy_timeout(std::time::Duration::from_secs(5))
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .create_if_missing(true)
            .foreign_keys(true)
            .in_memory(options.in_memory);

        let pool = sqlx::SqlitePool::connect_with(opts).await?;

        Ok(Repo { pool })
    }

    #[instrument]
    pub async fn enqueue_message(&self, queue: &str, body: &str) -> anyhow::Result<Uuid> {
        const GET_QUEUE_ID_QUERY: &str = "
    select
        id
    from hq_queues
    where name = ?
        ";

        const INSERT_MESSAGE_QUERY: &str = "
    insert into hq_messages(id, args, queue_id)
    values (?, ?, ?)
    ";

        let _valid_json_args: serde::de::IgnoredAny = serde_json::from_str(body)?;

        let mut conn = self.pool.acquire().await?;

        let mut txn = conn.begin_with("BEGIN IMMEDIATE").await?;

        let (queue_id,): (Uuid,) = sqlx::query_as(GET_QUEUE_ID_QUERY)
            .bind(queue)
            .fetch_one(&mut *txn)
            .await?;

        let message_id = Uuid::new_v4();

        sqlx::query(INSERT_MESSAGE_QUERY)
            .bind(&message_id.as_bytes()[..])
            .bind(body)
            .bind(queue_id)
            .execute(&mut *txn)
            .await?;

        txn.commit().await?;

        Ok(message_id)
    }

    #[instrument]
    pub async fn receive_message(&self, queue: &str) -> anyhow::Result<Option<Message>> {
        const QUERY: &str = "
        update hq_messages
        set
            attempts = attempts + 1,
            locked_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
        where id = (
            select
                hq_messages.id
            from hq_messages
            inner join hq_queues
                on hq_queues.id = hq_messages.queue_id
                and hq_queues.name = ?
            and completed_at is null
            and locked_at is null
            and failed_at is null
            and attempts < hq_queues.max_attempts
            order by hq_messages.updated_at asc
            limit 1
        )
        returning
            id,
            args,
            '' as queue,
            attempts;
            ";

        let mut conn = self.pool.acquire().await?;

        let message: Option<Message> = sqlx::query_as(QUERY)
            .bind(queue)
            .fetch_optional(&mut *conn)
            .await?;

        Ok(message.map(|mut message| {
            message.queue = queue.to_owned();
            message
        }))
    }

    #[instrument]
    pub async fn complete_message(&self, message_id: Uuid) -> anyhow::Result<()> {
        const QUERY: &str = "
        update hq_messages
        set
            completed_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            locked_at = null
        where id = ?
        and locked_at is not null
        and completed_at is null
        and failed_at is null
        ";

        let mut conn = self.pool.acquire().await?;

        // TODO think about this,
        // should we have a notion of "receipt handle"?
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
        sqlx::query(QUERY)
            .bind(message_id)
            .execute(&mut *conn)
            .await?;

        Ok(())
    }

    #[instrument]
    pub async fn fail_message(&self, message_id: Uuid) -> anyhow::Result<()> {
        const QUERY: &str = "
        update hq_messages
        set
            failed_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            locked_at = null
        where id = ?
        and locked_at is not null
        and completed_at is null
        and failed_at is null
        ";

        let mut conn = self.pool.acquire().await?;

        // TODO think about this,
        // should we have a notion of "receipt handle"?
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
        sqlx::query(QUERY)
            .bind(message_id)
            .execute(&mut *conn)
            .await?;

        Ok(())
    }

    #[instrument]
    pub async fn messages_sample(&self, limit: i64) -> sqlx::Result<Vec<web::Message>> {
        const QUERY: &str = "
        select
            hq_messages.id,
            hq_messages.args,
            hq_queues.name as queue_name,
            hq_messages.attempts,
            hq_messages.inserted_at,
            hq_messages.updated_at,
            hq_messages.locked_at,
            hq_messages.completed_at,
            hq_messages.failed_at
        from hq_messages
        inner join hq_queues
            on hq_queues.id = hq_messages.queue_id
        order by 
            hq_messages.updated_at desc,
            hq_messages.inserted_at desc,
            hq_messages.locked_at desc,
            hq_messages.completed_at desc,
            hq_messages.failed_at desc
        limit ?;
        ";
        let mut conn = self.pool.acquire().await.unwrap();

        sqlx::query_as(QUERY)
            .bind(limit)
            .fetch_all(&mut *conn)
            .await
    }

    #[instrument]
    pub async fn create_queue(
        &self,
        name: &str,
        max_attempts: i64,
        visibility_timeout_seconds: i64,
    ) -> sqlx::Result<()> {
        const QUERY: &str = "
        insert into hq_queues (id, name, max_attempts, visibility_timeout_seconds) values (?, ?, ?, ?);
        ";

        let mut conn = self.pool.acquire().await?;

        let queue_id = Uuid::new_v4();

        sqlx::query(QUERY)
            .bind(queue_id)
            .bind(name)
            .bind(max_attempts)
            .bind(visibility_timeout_seconds)
            .execute(&mut *conn)
            .await?;

        Ok(())
    }

    #[instrument]
    pub async fn update_queue(
        &self,
        name: &str,
        update_queue_params: &crate::queue::UpdateQueueRequest,
    ) -> sqlx::Result<()> {
        if update_queue_params.is_some() {
            let mut query = "update hq_queues set\n".to_string();

            if update_queue_params.max_attempts.is_some() {
                query.push_str("max_attempts = ?\n")
            }

            if update_queue_params.visibility_timeout_seconds.is_some() {
                query.push_str("visibility_timeout_seconds = ?\n")
            }

            query.push_str("where name = ?");

            let mut conn = self.pool.acquire().await?;

            let mut q = sqlx::query(&query);

            if let Some(max_attempts) = update_queue_params.max_attempts {
                q = q.bind(max_attempts);
            }

            if let Some(visibility_timeout_seconds) = update_queue_params.visibility_timeout_seconds
            {
                q = q.bind(visibility_timeout_seconds);
            }

            q = q.bind(name);

            q.execute(&mut *conn).await?;
        }

        Ok(())
    }

    #[instrument]
    pub async fn delete_queue(&self, name: &str) -> sqlx::Result<()> {
        const QUERY: &str = "
        delete from hq_queues
        where name = ?
        ";

        let mut conn = self.pool.acquire().await?;

        sqlx::query(QUERY).bind(name).execute(&mut *conn).await?;

        Ok(())
    }

    #[instrument]
    pub(crate) async fn get_queue(
        &self,
        queue: String,
    ) -> Result<Option<queue::ShowQueueResponse>, sqlx::Error> {
        const QUERY: &str = "
        select
            name,
            max_attempts,
            visibility_timeout_seconds
        from hq_queues
        where name = ?
        limit 1
        ";

        let mut conn = self.pool.acquire().await?;

        sqlx::query_as(QUERY)
            .bind(queue)
            .fetch_optional(&mut *conn)
            .await
    }

    #[instrument]
    pub async fn get_queues(&self) -> sqlx::Result<Vec<Queue>> {
        const QUERY: &str = "
        select
            name,
            max_attempts
        from hq_queues
        order by name
        ";

        let mut conn = self.pool.acquire().await?;

        sqlx::query_as(QUERY).fetch_all(&mut *conn).await
    }

    #[instrument]
    pub(crate) async fn unlock_messages_locked_longer_than_timeout(&self) -> sqlx::Result<()> {
        // unlock queries that have been locked
        // for longer than timeout and have attempts <= allowed
        const UNLOCK_LOCKED_TIMEOUT_QUERY: &str = "
        update hq_messages
        set
            locked_at = null
        where id in (
            select
                hq_messages.id
            from hq_messages
            inner join hq_queues
                on hq_queues.id = hq_messages.queue_id
            where locked_at is not null
            and completed_at is null
            and failed_at is null
            and ((julianday(current_timestamp) - julianday(locked_at)) * 86400.0) > cast(hq_queues.visibility_timeout_seconds as real)
            and attempts <= hq_queues.max_attempts
        )
        ";

        // unlocked and fail queries that have been locked
        // for longer than timeout and have attempts > allowed
        const FAIL_LOCKED_TIMEOUT_QUERY: &str = "
        update hq_messages
        set
            failed_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            locked_at = null
        where id in (
            select
                hq_messages.id
            from hq_messages
            inner join hq_queues
                on hq_queues.id = hq_messages.queue_id
            where locked_at is not null
            and completed_at is null
            and failed_at is null
            and ((julianday(current_timestamp) - julianday(locked_at)) * 86400.0) > cast(hq_queues.visibility_timeout_seconds as real)
            and attempts > hq_queues.max_attempts
        )
        ";

        let mut conn = self.pool.acquire().await?;

        let mut txn = conn.begin_with("BEGIN IMMEDIATE").await?;

        sqlx::query(UNLOCK_LOCKED_TIMEOUT_QUERY)
            .execute(&mut *txn)
            .await?;

        sqlx::query(FAIL_LOCKED_TIMEOUT_QUERY)
            .execute(&mut *txn)
            .await?;

        txn.commit().await?;

        Ok(())
    }

    #[instrument]
    pub async fn migrate(&self) -> anyhow::Result<()> {
        const QUERY: &str = "
        create table if not exists hq_queues (
            id blob primary key,
            name text not null,
            max_attempts integer not null default -1,
            visibility_timeout_seconds integer not null default -1,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            updated_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
        );

        create unique index if not exists name_idx on hq_queues(name);

        create trigger if not exists hq_queues_updated_at after update on hq_queues
        begin
            update hq_queues set updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
            where id = old.id;
        end;

        create table if not exists hq_messages (
            id blob primary key,
            args text not null,
            queue_id integer not null,
            attempts integer not null default 0,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            updated_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            locked_at datetime,
            completed_at datetime,
            failed_at datetime,

            foreign key(queue_id) references hq_queues(id) on delete cascade
        );

        create trigger if not exists hq_messages_updated_at after update on hq_messages
        begin
            update hq_messages set updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
            where id = old.id;
        end;

        create index if not exists queue_id_idx on hq_messages(queue_id);
        create index if not exists inserted_at_idx on hq_messages(inserted_at);
        create index if not exists locked_at_idx on hq_messages(locked_at);
        create index if not exists completed_at_idx on hq_messages(completed_at);
    ";

        let mut conn = self.pool.acquire().await?;

        sqlx::raw_sql(QUERY).execute(&mut *conn).await?;

        Ok(())
    }
}

#[derive(sqlx::FromRow, Serialize, Debug)]
pub struct Queue {
    pub name: String,
    pub max_attempts: i64,
}
