import fakeredis
import pytest

from regale.distributed.redis_broker import RedisBroker


@pytest.fixture
def client():
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def broker(client):
    return RedisBroker(client, stream="tasks", group="workers")


def test_publish_and_consume_round_trips_payload(broker):
    broker.publish({"pipeline_id": "vendas", "params": {"ano": 2025}})

    entries = broker.consume("worker1", block_ms=100)

    assert len(entries) == 1
    _message_id, task = entries[0]
    assert task == {"pipeline_id": "vendas", "params": {"ano": 2025}}


def test_consume_does_not_redeliver_unacked_message_to_same_call(broker):
    broker.publish({"pipeline_id": "vendas", "params": {}})
    broker.consume("worker1", block_ms=100)

    entries = broker.consume("worker1", block_ms=100)  # no new messages

    assert entries == []


def test_ack_removes_message_from_pending(broker):
    broker.publish({"pipeline_id": "vendas", "params": {}})
    ((message_id, _task),) = broker.consume("worker1", block_ms=100)

    broker.ack(message_id)

    assert broker.delivery_count(message_id) == 0


def test_delivery_count_starts_at_one_after_first_read(broker):
    broker.publish({"pipeline_id": "vendas", "params": {}})
    ((message_id, _task),) = broker.consume("worker1", block_ms=100)

    assert broker.delivery_count(message_id) == 1


def test_claim_stale_redelivers_unacked_message_to_another_consumer(broker):
    broker.publish({"pipeline_id": "vendas", "params": {}})
    ((message_id, _task),) = broker.consume(
        "worker1", block_ms=100
    )  # never acked — simulated crash

    claimed = broker.claim_stale("worker2", min_idle_ms=0)

    assert len(claimed) == 1
    assert claimed[0][0] == message_id
    assert broker.delivery_count(message_id) == 2


def test_claim_stale_ignores_messages_still_within_idle_window(broker):
    broker.publish({"pipeline_id": "vendas", "params": {}})
    broker.consume("worker1", block_ms=100)

    claimed = broker.claim_stale("worker2", min_idle_ms=60_000)

    assert claimed == []


def test_second_broker_instance_reuses_existing_group(client):
    RedisBroker(client, stream="tasks", group="workers")
    RedisBroker(client, stream="tasks", group="workers")  # must not raise BUSYGROUP
