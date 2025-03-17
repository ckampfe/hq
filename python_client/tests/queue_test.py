import hq
import pytest
import subprocess
import time
import shlex


def test_create_queue(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        r = client.create_queue(queue)

    assert r.text == ""
    assert r.status_code == 200


def test_list_queues_empty(hq_server):
    with hq.Client("http://localhost:9999") as client:
        r = client.list_queues()

    body = r.json()

    assert r.status_code == 200
    assert len(body) == 0
    assert isinstance(body, list)


def test_list_queues_nonempty(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        r = client.create_queue(queue)
        r = client.list_queues()

    body = r.json()

    print(body)

    assert r.status_code == 200
    assert len(body) == 1
    assert isinstance(body, list)
    assert set(body[0].keys()) == set(
        ["name", "max_attempts", "inserted_at", "updated_at"]
    )
    assert body[0]["name"] == "abc"
    assert body[0]["max_attempts"] == 5


@pytest.fixture(scope="function")
def hq_server():
    command = "cargo run -- --database=foo --in-memory"
    command = shlex.split(command)

    proc = subprocess.Popen(
        command,
        cwd="../server",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    time.sleep(2)

    assert not proc.poll(), proc.stdout.read().decode("utf-8")

    yield proc

    proc.terminate()
