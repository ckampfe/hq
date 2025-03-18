import hq


def test_create_queue(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)

    assert True


def test_list_queues_empty(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queues = client.list_queues()

    assert len(queues) == 0
    assert isinstance(queues, list)


def test_list_queues_nonempty(hq_server):
    with hq.Client("http://localhost:9999") as client:
        queue = hq.Queue("abc")
        client.create_queue(queue)
        queues = client.list_queues()

    assert len(queues) == 1
    assert isinstance(queues, list)
    assert queues[0].name == "abc"
    assert queues[0].max_attempts == 5
