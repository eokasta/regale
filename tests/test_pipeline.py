import pytest

import regale
from regale.core.context import Context
from regale.core.pipeline import Pipeline


def _complete_pipeline(pipeline_id: str = "vendas"):
    @regale.query(pipeline_id, source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    @regale.load(pipeline_id, target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    return regale.registry.get(pipeline_id)


def test_from_registration_orders_transforms_by_priority():
    _complete_pipeline()

    @regale.transform("vendas", priority=20)
    def enriquecer(df):
        return df

    @regale.transform("vendas", priority=10)
    def limpar(df):
        return df

    pipeline = Pipeline.from_registration(regale.registry.get("vendas"))
    assert [t.func for t in pipeline.transforms] == [limpar, enriquecer]


def test_from_registration_raises_without_query():
    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    entry = regale.registry.get("vendas")
    with pytest.raises(ValueError, match="no query step"):
        Pipeline.from_registration(entry)


def test_from_registration_raises_without_load():
    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    entry = regale.registry.get("vendas")
    with pytest.raises(ValueError, match="no load step"):
        Pipeline.from_registration(entry)


def test_requires_full_materialization_false_by_default():
    _complete_pipeline()
    pipeline = Pipeline.from_registration(regale.registry.get("vendas"))
    assert pipeline.requires_full_materialization is False


def test_requires_full_materialization_true_when_any_transform_unchunked():
    _complete_pipeline()

    @regale.transform("vendas", chunked=False)
    def tudo_de_uma_vez(df):
        return df

    pipeline = Pipeline.from_registration(regale.registry.get("vendas"))
    assert pipeline.requires_full_materialization is True


def test_partition_params_defaults_to_single_empty_dict_without_partitions_step():
    _complete_pipeline()
    pipeline = Pipeline.from_registration(regale.registry.get("vendas"))
    assert pipeline.partition_params(Context(run_id="run1")) == [{}]


def test_partition_params_uses_partitions_generator():
    _complete_pipeline()

    @regale.partitions("vendas")
    def por_ano(ctx):
        for ano in (2025, 2026):
            yield {"ano": ano}

    pipeline = Pipeline.from_registration(regale.registry.get("vendas"))
    assert pipeline.partition_params(Context(run_id="run1")) == [{"ano": 2025}, {"ano": 2026}]
