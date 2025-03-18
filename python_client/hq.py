import httpx
import dataclasses
from dataclasses import dataclass


@dataclass
class Queue:
    name: str
    max_attempts: int = 5
    visibility_timeout_seconds: int = 30


@dataclass
class Enqueued:
    job_id: str


@dataclass
class Job:
    id: str
    queue: str
    args: dict[str, object]
    attempts: int


class Client:
    def __init__(self, url: str):
        self.url = url
        self.client = httpx.Client()

    def create_queue(self, queue_options: Queue) -> None:
        self.client.post(
            self.url + "/queues", json=dataclasses.asdict(queue_options)
        ).raise_for_status()

    def list_queues(self) -> list[Queue]:
        return [
            Queue(**q)
            for q in self.client.get(self.url + "/queues").raise_for_status().json()
        ]

    def enqueue_job(self, queue: str, job_args: dict) -> Enqueued:
        body = (
            self.client.post(
                self.url + "/jobs/enqueue", params={"queue": queue}, json=job_args
            )
            .raise_for_status()
            .json()
        )

        return Enqueued(**body)

    def receive_job(self, queue: str) -> Job | None:
        decoded = (
            self.client.get(self.url + "/jobs/receive", params={"queue": queue})
            .raise_for_status()
            .json()
        )

        if decoded["job"]:
            return Job(**decoded["job"])

    def complete_job(self, job_id: str) -> None:
        self.client.put(self.url + f"/jobs/{job_id}/complete").raise_for_status()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()
