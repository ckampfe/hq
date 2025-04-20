# hq

[![Rust](https://github.com/ckampfe/hq/actions/workflows/rust.yml/badge.svg)](https://github.com/ckampfe/hq/actions/workflows/rust.yml)

---

## what

This is a message queue/job queue designed with an interface like [SQS](https://aws.amazon.com/sqs/), but targeted at use cases where SQS might be overkill or where you can't access it, like on low-powered or embedded devices, or where you have low message volume.

It can run with an on-disk or in-memory SQLite database if you don't need persistence.

## API

It's an HTTP API so you can write your own client in your favorite language.
Paramters for creation are not optional.
Paramters for update are optional.

This is an partial API description.
For the whole API see the Rust client in `client`.

```
Return values are "happy" cases. Everything can error.

POST "/queues/{name}/enqueue" with JSON body
    returns JSON `{"job_id" -> uuid}`

GET "/queues/{name}/receive"
    returns optional JSON `{ id: string uuid, args: json, queue: string, attempts: integer }`

GET "/queues/{name}"
    returns optional JSON `{name: string, max_attempts: integer, visibility_timeout_seconds: integer}`

PUT "/queues/{name}?max_attempts=integer&visibility_timeout_seconds=integer"
    returns ()

DELETE "/queues/{name}"
    returns ()

GET "/queues"
    returns JSON [{"name": string, "max_attempts": integer}]

POST "/queues?name=string&max_attempts=integer&visibility_timeout_seconds=integer"
    returns ()

PUT "/jobs/{id}/complete"
    returns ()

PUT "/jobs/{id}/fail"
    returns ()
```