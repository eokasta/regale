import pandas as pd
import pytest
from sqlalchemy import text

import regale
from regale.core.runner import run_partition_with_retry


@pytest.fixture
def wiring(tmp_path, monkeypatch):
    monkeypatch.setattr("regale.core.retry.time.sleep", lambda seconds: None)
    source_url = f"sqlite:///{tmp_path / 'source.db'}"
    dest_url = f"sqlite:///{tmp_path / 'dest.db'}"
    regale.configure.add_db("vendas_db", regale.SQLSource(url=source_url))
    regale.configure.add_db("dw", regale.SQLTarget(url=dest_url))
    return regale.configure.source("vendas_db").engine(), regale.configure.target("dw").engine()


def _seed(engine, table: str, rows: dict) -> None:
    pd.DataFrame(rows).to_sql(table, engine, if_exists="replace", index=False)


def test_run_partition_with_retry_retries_ambiguous_failures_then_succeeds(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"pedido_id": [1], "valor": [10.0]})

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    calls = {"n": 0}

    @regale.transform("vendas", priority=10)
    def falha_uma_vez(df):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("flaky")
        return df

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    run_partition_with_retry("vendas", {})

    assert calls["n"] == 2
    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 1


def test_run_partition_with_retry_never_retries_permanent_failures(wiring):
    source_engine, dest_engine = wiring
    # two rows sharing the same pedido_id, which the destination's PRIMARY
    # KEY rejects — a genuine IntegrityError, not a simulated one.
    _seed(source_engine, "pedidos", {"pedido_id": [1, 1], "valor": [10.0, 20.0]})

    with dest_engine.connect() as connection:
        connection.execute(
            text("CREATE TABLE fato_pedidos (pedido_id INTEGER PRIMARY KEY, valor REAL)")
        )
        connection.commit()

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    calls = {"n": 0}

    @regale.transform("vendas", priority=10)
    def contar(df):
        calls["n"] += 1
        return df

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    with pytest.raises(Exception, match="UNIQUE constraint|IntegrityError"):
        run_partition_with_retry("vendas", {})

    assert calls["n"] == 1  # a permanent failure is never retried
