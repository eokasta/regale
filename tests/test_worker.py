import fakeredis
import pandas as pd
import pytest
from sqlalchemy import text

import regale
from regale.distributed.deadletter import DeadLetterQueue
from regale.distributed.redis_broker import RedisBroker
from regale.distributed.worker import Worker


@pytest.fixture
def client():
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def broker(client):
    return RedisBroker(client, stream="tasks", group="workers")


@pytest.fixture
def dlq(client):
    return DeadLetterQueue(client, stream="dead")


@pytest.fixture
def wiring(tmp_path):
    source_url = f"sqlite:///{tmp_path / 'source.db'}"
    dest_url = f"sqlite:///{tmp_path / 'dest.db'}"
    regale.configure.add_db("vendas_db", regale.SQLSource(url=source_url))
    regale.configure.add_db("dw", regale.SQLTarget(url=dest_url))
    return regale.configure.source("vendas_db").engine(), regale.configure.target("dw").engine()


def _seed(engine, table: str, rows: dict) -> None:
    pd.DataFrame(rows).to_sql(table, engine, if_exists="replace", index=False)


def test_worker_processes_task_and_acks_on_success(wiring, broker, dlq):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"pedido_id": [1], "valor": [10.0]})

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    broker.publish({"pipeline_id": "vendas", "params": {}})
    worker = Worker(broker=broker, dlq=dlq, consumer_name="w1")
    worker.start()
    worker.drain_once(block_ms=100)

    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 1
    assert dlq.list() == []


def test_worker_dead_letters_permanent_failure_immediately(wiring, broker, dlq):
    source_engine, dest_engine = wiring
    # duplicate pedido_id -> a genuine IntegrityError against the PK, not simulated
    _seed(source_engine, "pedidos", {"pedido_id": [1, 1], "valor": [10.0, 20.0]})

    with dest_engine.connect() as connection:
        connection.execute(
            text("CREATE TABLE fato_pedidos (pedido_id INTEGER PRIMARY KEY, valor REAL)")
        )
        connection.commit()

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    broker.publish({"pipeline_id": "vendas", "params": {}})
    worker = Worker(broker=broker, dlq=dlq, consumer_name="w1")
    worker.start()
    worker.drain_once(block_ms=100)

    entries = dlq.list()
    assert len(entries) == 1
    assert (
        entries[0]["attempts"] == 1
    )  # never redelivered — permanent failures dead-letter on first try


def test_worker_dead_letters_after_max_deliveries_for_ambiguous_failure(
    monkeypatch, wiring, broker, dlq
):
    source_engine, _dest_engine = wiring
    _seed(source_engine, "pedidos", {"pedido_id": [1], "valor": [10.0]})

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.transform("vendas", priority=10)
    def sempre_falha(df):
        raise RuntimeError("flaky forever")

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    monkeypatch.setattr("regale.core.retry.time.sleep", lambda s: None)
    policy = regale.RetryPolicy(max_attempts_transient=1, max_attempts_ambiguous=1)

    broker.publish({"pipeline_id": "vendas", "params": {}})
    worker = Worker(
        broker=broker,
        dlq=dlq,
        consumer_name="w1",
        policy=policy,
        max_deliveries=2,
        claim_min_idle_ms=0,
    )
    worker.start()

    worker.drain_once(block_ms=100)  # delivery 1: fails, left pending (1 < max_deliveries)
    assert dlq.list() == []

    worker.drain_once(
        block_ms=100
    )  # claim_stale reclaims it immediately: delivery 2, dead-lettered
    entries = dlq.list()
    assert len(entries) == 1
    assert entries[0]["attempts"] == 2


def test_worker_reclaims_task_left_pending_by_a_different_crashed_consumer(wiring, broker, dlq):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"pedido_id": [1], "valor": [10.0]})

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    broker.publish({"pipeline_id": "vendas", "params": {}})
    broker.consume(
        "crashed_worker", block_ms=100
    )  # read the task, then "died" before processing it

    survivor = Worker(broker=broker, dlq=dlq, consumer_name="w2", claim_min_idle_ms=0)
    survivor.start()
    survivor.drain_once(block_ms=100)

    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 1
