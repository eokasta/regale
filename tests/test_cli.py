import fakeredis
import pandas as pd
import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import regale
from regale.cli import app
from regale.distributed.redis_broker import RedisBroker

runner = CliRunner()


@pytest.fixture
def wiring(tmp_path):
    source_url = f"sqlite:///{tmp_path / 'source.db'}"
    dest_url = f"sqlite:///{tmp_path / 'dest.db'}"
    regale.configure.add_db("vendas_db", regale.SQLSource(url=source_url))
    regale.configure.add_db("dw", regale.SQLTarget(url=dest_url))
    return regale.configure.source("vendas_db").engine(), regale.configure.target("dw").engine()


@pytest.fixture
def fake_redis_client(monkeypatch):
    shared_client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr(
        "regale.distributed.redis_broker.redis.Redis.from_url",
        classmethod(lambda cls, url, decode_responses=True: shared_client),
    )
    return shared_client


def test_help_lists_subcommands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "submit" in result.output
    assert "worker" in result.output
    assert "dlq" in result.output


def test_run_command_executes_pipeline_locally(wiring):
    source_engine, dest_engine = wiring
    pd.DataFrame({"pedido_id": [1], "valor": [10.0]}).to_sql("pedidos", source_engine, index=False)

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    result = runner.invoke(app, ["run", "vendas"])

    assert result.exit_code == 0, result.output
    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 1


def test_run_command_reports_unknown_pipeline_cleanly():
    result = runner.invoke(app, ["run", "does-not-exist"])

    assert result.exit_code == 1
    assert "no pipeline registered" in result.output


def test_submit_command_publishes_tasks(fake_redis_client):
    @regale.partitions("vendas")
    def por_ano(ctx):
        yield {"ano": 2025}
        yield {"ano": 2026}

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    result = runner.invoke(app, ["submit", "vendas", "--broker", "redis://fake:6379/0"])

    assert result.exit_code == 0, result.output
    assert "published 2 partition" in result.output

    checker = RedisBroker.from_url(
        "redis://fake:6379/0", stream="regale:tasks:vendas", group="checker-group"
    )
    entries = checker.consume("checker", count=10, block_ms=100)
    assert len(entries) == 2


def test_worker_command_processes_one_task_then_stops(wiring, fake_redis_client):
    source_engine, dest_engine = wiring
    pd.DataFrame({"pedido_id": [1], "valor": [10.0]}).to_sql("pedidos", source_engine, index=False)

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    producer = RedisBroker.from_url("redis://fake:6379/0", stream="vendas-tasks", group="regale")
    producer.publish({"pipeline_id": "vendas", "params": {}})

    result = runner.invoke(
        app,
        [
            "worker",
            "--stream",
            "vendas-tasks",
            "--broker",
            "redis://fake:6379/0",
            "--block-ms",
            "50",
            "--max-iterations",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 1


def test_worker_command_without_redis_extra_fails_cleanly(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "regale.distributed.redis_broker", None)

    result = runner.invoke(
        app,
        ["worker", "--stream", "s", "--broker", "redis://fake:6379/0", "--max-iterations", "0"],
    )

    assert result.exit_code == 1
    assert "pip install 'regale[redis]'" in result.output


def test_dlq_list_shows_dead_lettered_tasks(fake_redis_client):
    from regale.distributed.deadletter import DeadLetterQueue

    dlq = DeadLetterQueue.from_url("redis://fake:6379/0", stream="vendas-tasks:dead")
    dlq.add({"pipeline_id": "vendas", "params": {"ano": 2025}}, error="boom", attempts=3)

    result = runner.invoke(
        app, ["dlq", "list", "--stream", "vendas-tasks", "--broker", "redis://fake:6379/0"]
    )

    assert result.exit_code == 0, result.output
    assert "attempts=3" in result.output
    assert "boom" in result.output


def test_dlq_retry_requeues_task(fake_redis_client):
    from regale.distributed.deadletter import DeadLetterQueue

    dlq = DeadLetterQueue.from_url("redis://fake:6379/0", stream="vendas-tasks:dead")
    dlq.add({"pipeline_id": "vendas", "params": {}}, error="boom", attempts=3)
    dead_id = dlq.list()[0]["id"]

    result = runner.invoke(
        app,
        [
            "dlq",
            "retry",
            dead_id,
            "--stream",
            "vendas-tasks",
            "--broker",
            "redis://fake:6379/0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert dlq.list() == []


def test_dlq_purge_removes_old_entries(fake_redis_client):
    from regale.distributed.deadletter import DeadLetterQueue

    dlq = DeadLetterQueue.from_url("redis://fake:6379/0", stream="vendas-tasks:dead")
    dlq.add({"pipeline_id": "vendas", "params": {}}, error="boom", attempts=3)

    result = runner.invoke(
        app,
        [
            "dlq",
            "purge",
            "--older-than",
            "-1",
            "--stream",
            "vendas-tasks",
            "--broker",
            "redis://fake:6379/0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "purged 1 entry" in result.output
    assert dlq.list() == []
