import pandas as pd
from sqlalchemy import text

import regale


def test_run_facade_executes_every_partition_sequentially(tmp_path):
    source_url = f"sqlite:///{tmp_path / 'source.db'}"
    dest_url = f"sqlite:///{tmp_path / 'dest.db'}"
    regale.configure.add_db("vendas_db", regale.SQLSource(url=source_url))
    regale.configure.add_db("dw", regale.SQLTarget(url=dest_url))

    source_engine = regale.configure.source("vendas_db").engine()
    pd.DataFrame(
        {"pedido_id": [1, 2, 3], "ano": [2025, 2025, 2026], "valor": [1.0, 2.0, 3.0]}
    ).to_sql("pedidos", source_engine, index=False)

    @regale.partitions("vendas")
    def por_ano(ctx):
        yield {"ano": 2025}
        yield {"ano": 2026}

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT pedido_id, valor FROM pedidos WHERE ano = :ano"

    @regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
    def gravar(df, p):
        return df

    regale.run("vendas")

    dest_engine = regale.configure.target("dw").engine()
    with dest_engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 3


def test_run_facade_uses_generated_run_id_when_none_given(tmp_path):
    source_url = f"sqlite:///{tmp_path / 'source.db'}"
    dest_url = f"sqlite:///{tmp_path / 'dest.db'}"
    regale.configure.add_db("vendas_db", regale.SQLSource(url=source_url))
    regale.configure.add_db("dw", regale.SQLTarget(url=dest_url))

    seen_run_ids = []

    @regale.partitions("vendas")
    def uma_particao(ctx):
        seen_run_ids.append(ctx.run_id)
        yield {}

    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1 AS n WHERE 1=0"

    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    regale.run("vendas")

    assert len(seen_run_ids) == 1
    assert seen_run_ids[0]  # non-empty, auto-generated
