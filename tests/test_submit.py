import sys

import fakeredis
import pytest

import regale
from regale.distributed.redis_broker import RedisBroker


@pytest.fixture(autouse=True)
def _fake_redis(monkeypatch):
    shared_client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr(
        "regale.distributed.redis_broker.redis.Redis.from_url",
        classmethod(lambda cls, url, decode_responses=True: shared_client),
    )
    return shared_client


def test_submit_publishes_one_task_per_partition():
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

    count = regale.submit("vendas", broker="redis://fake:6379/0")

    assert count == 2

    consuming_broker = RedisBroker.from_url(
        "redis://fake:6379/0", stream="regale:tasks:vendas", group="workers"
    )
    entries = consuming_broker.consume("checker", count=10, block_ms=100)
    tasks = [task for _id, task in entries]
    assert {"pipeline_id": "vendas", "params": {"ano": 2025}} in tasks
    assert {"pipeline_id": "vendas", "params": {"ano": 2026}} in tasks


def test_submit_raises_actionable_error_without_redis_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "regale.distributed.redis_broker", None)

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    with pytest.raises(regale.RegaleError, match=r"pip install 'regale\[redis\]'"):
        regale.submit("vendas", broker="redis://fake:6379/0")
