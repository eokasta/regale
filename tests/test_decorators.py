import pytest

import regale
from regale.core.errors import RegistrationError
from regale.core.steps import LoadMode


def test_query_registers_step():
    @regale.query("vendas", source="vendas_db", chunksize=1000)
    def extrair(p):
        return "SELECT 1"

    entry = regale.registry.get("vendas")
    assert entry.query is not None
    assert entry.query.source == "vendas_db"
    assert entry.query.chunksize == 1000
    assert entry.query.func is extrair


def test_duplicate_query_raises():
    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    with pytest.raises(RegistrationError, match="already has a @query"):

        @regale.query("vendas", source="vendas_db")
        def extrair2(p):
            return "SELECT 2"


def test_transform_registers_in_priority_order():
    @regale.transform("vendas", priority=20)
    def enriquecer(df):
        return df

    @regale.transform("vendas", priority=10)
    def limpar(df):
        return df

    entry = regale.registry.get("vendas")
    ordered = sorted(entry.transforms, key=lambda t: t.priority)
    assert [t.func for t in ordered] == [limpar, enriquecer]


def test_duplicate_transform_priority_raises():
    @regale.transform("vendas", priority=10)
    def a(df):
        return df

    with pytest.raises(RegistrationError, match="priority=10"):

        @regale.transform("vendas", priority=10)
        def b(df):
            return df


def test_transform_invalid_frame_raises():
    with pytest.raises(RegistrationError, match="frame must be"):

        @regale.transform("vendas", frame="polars")
        def limpar(df):
            return df


def test_load_upsert_requires_keys():
    with pytest.raises(RegistrationError, match="requires keys"):

        @regale.load("vendas", target="dw", table="fato_pedidos", mode="upsert")
        def gravar(df, p):
            return df


def test_load_upsert_with_keys_registers():
    @regale.load("vendas", target="dw", table="fato_pedidos", mode="upsert", keys=["pedido_id"])
    def gravar(df, p):
        return df

    entry = regale.registry.get("vendas")
    assert entry.loads[0].keys == ("pedido_id",)
    assert entry.loads[0].mode is LoadMode.UPSERT


def test_load_append_with_commit_every_raises():
    with pytest.raises(RegistrationError, match="commit_every"):

        @regale.load("vendas", target="dw", table="fato_pedidos", mode="append", commit_every=1000)
        def gravar(df, p):
            return df


def test_load_invalid_mode_raises():
    with pytest.raises(RegistrationError, match="mode must be one of"):

        @regale.load("vendas", target="dw", table="fato_pedidos", mode="bogus")
        def gravar(df, p):
            return df


def test_load_replace_partition_requires_partition_keys():
    with pytest.raises(RegistrationError, match="requires partition_keys"):

        @regale.load("vendas", target="dw", table="fato_pedidos", mode="replace_partition")
        def gravar(df, p):
            return df


def test_load_replace_partition_with_partition_keys_registers():
    @regale.load(
        "vendas",
        target="dw",
        table="fato_pedidos",
        mode="replace_partition",
        partition_keys=["ano"],
    )
    def gravar(df, p):
        return df

    entry = regale.registry.get("vendas")
    assert entry.loads[0].partition_keys == ("ano",)
    assert entry.loads[0].mode is LoadMode.REPLACE_PARTITION


def test_partitions_registers_step():
    @regale.partitions("vendas")
    def por_ano(ctx):
        yield {"ano": 2025}

    entry = regale.registry.get("vendas")
    assert entry.partitions is not None
    assert entry.partitions.func is por_ano


def test_duplicate_partitions_raises():
    @regale.partitions("vendas")
    def por_ano(ctx):
        yield {"ano": 2025}

    with pytest.raises(RegistrationError, match="already has a @partitions"):

        @regale.partitions("vendas")
        def por_loja(ctx):
            yield {"loja_id": 1}


def test_pipeline_without_query_fails_validation():
    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    with pytest.raises(RegistrationError, match="no @query"):
        regale.registry.validate("vendas")


def test_pipeline_without_load_fails_validation():
    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    with pytest.raises(RegistrationError, match="no @load"):
        regale.registry.validate("vendas")


def test_complete_pipeline_validates():
    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    regale.registry.validate("vendas")


def test_validate_all_checks_every_registered_pipeline():
    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    @regale.query("estoque", source="estoque_db")
    def extrair_estoque(p):
        return "SELECT 1"

    with pytest.raises(RegistrationError, match="'estoque' has no @load"):
        regale.registry.validate_all()


def test_unknown_pipeline_raises():
    with pytest.raises(RegistrationError, match="no pipeline registered"):
        regale.registry.get("inexistente")
