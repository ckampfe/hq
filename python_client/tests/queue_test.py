import hq


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
