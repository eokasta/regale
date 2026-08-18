import fakeredis
import pytest

from regale.core.errors import RegaleError
from regale.distributed.deadletter import DeadLetterQueue
from regale.distributed.redis_broker import RedisBroker


@pytest.fixture
def client():
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def dlq(client):
    return DeadLetterQueue(client, stream="dead")


def test_add_and_list_round_trips(dlq):
    dlq.add({"pipeline_id": "vendas", "params": {"ano": 2025}}, error="boom", attempts=3)

    entries = dlq.list()

    assert len(entries) == 1
    assert entries[0]["task"] == {"pipeline_id": "vendas", "params": {"ano": 2025}}
    assert entries[0]["error"] == "boom"
    assert entries[0]["attempts"] == 3


def test_retry_republishes_to_broker_and_removes_from_dlq(client, dlq):
    broker = RedisBroker(client, stream="tasks", group="workers")
    dlq.add({"pipeline_id": "vendas", "params": {}}, error="boom", attempts=3)
    dead_id = dlq.list()[0]["id"]

    dlq.retry(dead_id, broker)

    assert dlq.list() == []
    ((_message_id, task),) = broker.consume("worker1", block_ms=100)
    assert task == {"pipeline_id": "vendas", "params": {}}


def test_retry_raises_for_unknown_message_id(client, dlq):
    broker = RedisBroker(client, stream="tasks", group="workers")
    with pytest.raises(RegaleError, match="no dead-lettered task"):
        dlq.retry("0-0", broker)


def test_purge_removes_entries_older_than_cutoff(dlq):
    dlq.add({"pipeline_id": "vendas", "params": {}}, error="boom", attempts=1)

    removed = dlq.purge(older_than_seconds=-1)  # cutoff in the future — everything is "older"

    assert removed == 1
    assert dlq.list() == []


def test_purge_keeps_entries_newer_than_cutoff(dlq):
    dlq.add({"pipeline_id": "vendas", "params": {}}, error="boom", attempts=1)

    removed = dlq.purge(older_than_seconds=3600)  # nothing is an hour old yet

    assert removed == 0
    assert len(dlq.list()) == 1
