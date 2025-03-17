import httpx
import dataclasses
from dataclasses import dataclass


@dataclass
class Queue:
    name: str
    max_attempts: int = 5
    visibility_timeout_seconds: int = 30


@dataclass
class Job:
    id: str


class Client:
    def __init__(self, url: str):
        self.url = url
        self.client = httpx.Client()

    def create_queue(self, queue_options: Queue):
        return self.client.post(
            self.url + "/queues", json=dataclasses.asdict(queue_options)
        )

    def list_queues(self):
        return self.client.get(self.url + "/queues")

    def enqueue_job(self, queue: str, job_args: dict):
        return self.client.post(
            self.url + "/jobs/enqueue", params={"queue": queue}, json=job_args
        )

    def receive_job(self, queue: str):
        return self.client.get(self.url + "/jobs/receive", params={"queue": queue})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()
