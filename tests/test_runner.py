import pandas as pd
import pytest
from sqlalchemy import text

import regale
from regale.core.runner import run_partition


@pytest.fixture
def wiring(tmp_path):
    source_url = f"sqlite:///{tmp_path / 'source.db'}"
    dest_url = f"sqlite:///{tmp_path / 'dest.db'}"
    regale.configure.add_db("vendas_db", regale.SQLSource(url=source_url))
    regale.configure.add_db("dw", regale.SQLTarget(url=dest_url))
    return regale.configure.source("vendas_db").engine(), regale.configure.target("dw").engine()


def _seed(engine, table: str, rows: dict) -> None:
    pd.DataFrame(rows).to_sql(table, engine, if_exists="replace", index=False)


def test_run_partition_end_to_end_append(wiring):
    source_engine, dest_engine = wiring
    _seed(
        source_engine,
        "pedidos",
        {"pedido_id": [1, 2, 3], "ano": [2025, 2025, 2025], "valor": [10.0, 20.0, 30.0]},
    )

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, ano, valor FROM pedidos WHERE ano = :ano"

    @regale.transform("vendas", priority=10)
    def com_margem(df):
        return df.assign(margem=df["valor"] * 0.1)

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    run_partition("vendas", {"ano": 2025})

    with dest_engine.connect() as connection:
        rows = connection.execute(
            text("SELECT pedido_id, margem FROM fato_pedidos ORDER BY pedido_id")
        ).fetchall()
    assert rows == [(1, 1.0), (2, 2.0), (3, 3.0)]


def test_run_partition_load_func_can_reshape_before_writing(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"pedido_id": [1], "valor": [10.0]})

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df.rename(columns={"valor": "total"})

    run_partition("vendas", {})

    with dest_engine.connect() as connection:
        rows = connection.execute(text("SELECT pedido_id, total FROM fato_pedidos")).fetchall()
    assert rows == [(1, 10.0)]


def test_run_partition_streams_in_chunks_when_all_transforms_are_chunked(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "numeros", {"n": list(range(5))})

    calls = []

    @regale.query("numeros", source="vendas_db", chunksize=2)
    def extrair(p):
        return "SELECT n FROM numeros ORDER BY n"

    @regale.transform("numeros", priority=10)
    def contar(df):
        calls.append(len(df))
        return df

    @regale.load("numeros", target="dw", table="numeros_dest", mode="append")
    def gravar(df, p):
        return df

    run_partition("numeros", {})

    assert calls == [2, 2, 1]


def test_run_partition_materializes_when_any_transform_is_unchunked(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "numeros", {"n": list(range(5))})

    calls = []

    @regale.query("numeros", source="vendas_db", chunksize=2)
    def extrair(p):
        return "SELECT n FROM numeros ORDER BY n"

    @regale.transform("numeros", priority=10, chunked=False)
    def tudo_de_uma_vez(df):
        calls.append(len(df))
        return df

    @regale.load("numeros", target="dw", table="numeros_dest", mode="append")
    def gravar(df, p):
        return df

    run_partition("numeros", {})

    assert calls == [5]


def test_run_partition_rolls_back_all_chunks_when_a_transform_raises_partway(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "numeros", {"n": list(range(6))})

    @regale.query("numeros", source="vendas_db", chunksize=2)
    def extrair(p):
        return "SELECT n FROM numeros ORDER BY n"

    calls = {"n": 0}

    @regale.transform("numeros", priority=10)
    def falha_no_segundo_chunk(df):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return df

    @regale.load("numeros", target="dw", table="numeros_dest", mode="append")
    def gravar(df, p):
        return df

    with pytest.raises(RuntimeError, match="boom"):
        run_partition("numeros", {})

    with dest_engine.connect() as connection:
        exists = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='numeros_dest'")
        ).fetchone()
    assert exists is None  # first chunk's insert rolled back along with the second chunk's failure


def test_run_partition_upsert_is_idempotent_on_retry(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"pedido_id": [1, 2], "valor": [10.0, 20.0]})

    with dest_engine.connect() as connection:
        connection.execute(
            text("CREATE TABLE fato_pedidos (pedido_id INTEGER PRIMARY KEY, valor REAL)")
        )
        connection.commit()

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="upsert", keys=["pedido_id"])
    def gravar(df, p):
        return df

    run_partition("vendas", {})
    run_partition("vendas", {})  # simulated retry — must not duplicate

    with dest_engine.connect() as connection:
        rows = connection.execute(
            text("SELECT pedido_id, valor FROM fato_pedidos ORDER BY pedido_id")
        ).fetchall()
    assert rows == [(1, 10.0), (2, 20.0)]


def test_run_partition_replace_partition_deletes_then_reinserts(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"ano": [2025], "pedido_id": [1], "valor": [10.0]})

    existing = pd.DataFrame({"ano": [2025, 2026], "pedido_id": [99, 3], "valor": [0.0, 3.0]})
    existing.to_sql("fato_pedidos", dest_engine, if_exists="replace", index=False)

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT ano, pedido_id, valor FROM pedidos WHERE ano = :ano"

    @regale.load(
        "vendas",
        target="dw",
        table="fato_pedidos",
        mode="replace_partition",
        partition_keys=["ano"],
    )
    def gravar(df, p):
        return df

    run_partition("vendas", {"ano": 2025})

    with dest_engine.connect() as connection:
        rows = connection.execute(
            text("SELECT ano, pedido_id FROM fato_pedidos ORDER BY ano, pedido_id")
        ).fetchall()
    assert rows == [(2025, 1), (2026, 3)]


def test_run_partition_replace_partition_with_empty_source_still_clears_destination(wiring):
    source_engine, dest_engine = wiring
    _seed(
        source_engine, "pedidos", {"ano": [2026], "pedido_id": [1], "valor": [10.0]}
    )  # no 2025 rows

    existing = pd.DataFrame({"ano": [2025, 2026], "pedido_id": [99, 3], "valor": [0.0, 3.0]})
    existing.to_sql("fato_pedidos", dest_engine, if_exists="replace", index=False)

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT ano, pedido_id, valor FROM pedidos WHERE ano = :ano"

    @regale.load(
        "vendas",
        target="dw",
        table="fato_pedidos",
        mode="replace_partition",
        partition_keys=["ano"],
    )
    def gravar(df, p):
        return df

    run_partition("vendas", {"ano": 2025})  # matches zero source rows

    with dest_engine.connect() as connection:
        rows = connection.execute(
            text("SELECT ano, pedido_id FROM fato_pedidos ORDER BY ano, pedido_id")
        ).fetchall()
    assert rows == [(2026, 3)]  # 2025's stale row (99) was cleared even though source had none


def test_run_partition_append_with_empty_source_creates_empty_table(wiring):
    source_engine, dest_engine = wiring
    _seed(source_engine, "pedidos", {"ano": [2026], "pedido_id": [1], "valor": [10.0]})

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT ano, pedido_id, valor FROM pedidos WHERE ano = :ano"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    run_partition("vendas", {"ano": 1999})  # matches nothing

    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 0
