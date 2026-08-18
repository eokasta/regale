import pandas as pd

import regale


@regale.partitions("vendas")
def por_ano(ctx):
    for ano in (2025, 2026):
        yield {"ano": ano}


@regale.query("vendas", source="vendas_db", chunksize=1000)
def extrair(p):
    return (
        "SELECT pedido_id, cliente_id, dt, receita, custo FROM pedidos "
        "WHERE EXTRACT(YEAR FROM dt) = :ano"
    )


@regale.transform("vendas", priority=10)
def limpar(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["cliente_id"])


@regale.transform("vendas", priority=20)
def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(margem=df["receita"] - df["custo"])


@regale.load(
    "vendas",
    target="dw",
    table="fato_pedidos",
    mode="upsert",
    keys=["pedido_id"],
)
def gravar(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    return df
