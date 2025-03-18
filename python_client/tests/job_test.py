import hq


def test_enqueue_job(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        enqueued = client.enqueue_job("abc", {"x": "y"})

    assert isinstance(enqueued.job_id, str)


def test_receive_job_present(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        client.enqueue_job("abc", {"x": "yyyyy"})
        job = client.receive_job("abc")

    if job:
        assert job.queue == "abc"

        assert isinstance(job.args, dict)
        assert set(job.args.keys()) == set(["x"])
        assert job.args["x"] == "yyyyy"

        assert isinstance(job.attempts, int)

        assert isinstance(job.id, str)
        assert isinstance(job.queue, str)


def test_receive_job_empty(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        job = client.receive_job("abc")

    assert not job


def test_complete_job(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        client.enqueue_job("abc", {"x": "yyyyy"})
        job = client.receive_job("abc")
        if job:
            client.complete_job(job.id)
            next_job = client.receive_job("abc")
            # should not be a next job, it is empty
            assert not next_job
        else:
            assert False


def test_empty_queue(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        job = client.receive_job("abc")
        assert not job
