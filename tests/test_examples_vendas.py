import sys

import pandas as pd
import pytest

import regale


@pytest.fixture(autouse=True)
def _example_env(monkeypatch):
    # Never a live Postgres server — engines are lazy, so this is enough to
    # prove discover()/config wiring works without one.
    monkeypatch.setenv("VENDAS_DB_URL", "postgresql+psycopg://regale:regale@localhost:5433/vendas")
    monkeypatch.setenv("DW_URL", "postgresql+psycopg://regale:regale@localhost:5434/dw")
    for name in list(sys.modules):
        if name.startswith("examples"):
            del sys.modules[name]
    yield
    for name in list(sys.modules):
        if name.startswith("examples"):
            del sys.modules[name]


def test_discover_registers_vendas_pipeline_without_importing_submit_script():
    regale.discover("examples.vendas")

    entry = regale.registry.get("vendas")
    assert entry.query is not None
    assert entry.loads
    # discover() walks the whole examples.vendas package tree — this proves
    # it never sweeps in the sibling submit script and runs submit() as an
    # unintended side effect of mere discovery.
    assert "examples.submit_vendas" not in sys.modules


def test_discover_wires_source_and_target_connections():
    regale.discover("examples.vendas")

    source = regale.configure.source("vendas_db")
    target = regale.configure.target("dw")
    assert source.engine() is not None
    assert target.engine() is not None


def test_vendas_pipeline_transforms_apply_in_priority_order():
    regale.discover("examples.vendas")
    entry = regale.registry.get("vendas")
    ordered = sorted(entry.transforms, key=lambda t: t.priority)

    df = pd.DataFrame(
        {
            "pedido_id": [1, 2],
            "cliente_id": [100, None],
            "receita": [500.0, 200.0],
            "custo": [300.0, 150.0],
        }
    )
    for step in ordered:
        df = step.func(df)

    assert df["pedido_id"].tolist() == [1]  # row without cliente_id was dropped by "limpar"
    assert df["margem"].tolist() == [200.0]


def test_vendas_load_step_requires_pedido_id_key():
    regale.discover("examples.vendas")
    entry = regale.registry.get("vendas")
    assert entry.loads[0].keys == ("pedido_id",)
