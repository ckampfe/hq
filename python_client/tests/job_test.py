import hq


def test_enqueue_job(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        r = client.enqueue_job("abc", {"x": "y"})

    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == set(["job_id"])
    assert isinstance(body["job_id"], str)


def test_receive_job_present(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        client.enqueue_job("abc", {"x": "yyyyy"})
        r = client.receive_job("abc")

    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == set(["job"])
    assert isinstance(body["job"], dict)
    job = body["job"]
    assert set(job.keys()) == set(
        ["id", "queue", "args", "attempts", "inserted_at", "updated_at"]
    )

    assert job["queue"] == "abc"

    assert isinstance(job["args"], dict)
    assert set(job["args"].keys()) == set(["x"])
    assert job["args"]["x"] == "yyyyy"

    assert isinstance(job["attempts"], int)

    assert isinstance(job["id"], str)
    assert isinstance(job["queue"], str)
    assert isinstance(job["inserted_at"], str)
    assert isinstance(job["updated_at"], str)


def test_receive_job_empty(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        r = client.receive_job("abc")

    body = r.json()
    assert r.status_code == 200
    assert not body["job"]
