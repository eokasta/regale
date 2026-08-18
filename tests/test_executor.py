import uuid

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

import regale
from regale.core.errors import RegaleError
from regale.core.executor import LocalExecutor

MODULE_TEMPLATE = """
import regale

regale.configure.add_db("vendas_db", regale.SQLSource(url="__SOURCE_URL__"))
regale.configure.add_db("dw", regale.SQLTarget(url="__DEST_URL__", connect_args={"timeout": 30}))

@regale.partitions("vendas_mp")
def por_ano(ctx):
    yield {"ano": 2025}
    yield {"ano": 2026}

@regale.query("vendas_mp", source="vendas_db")
def extrair(p):
    return "SELECT pedido_id, valor FROM pedidos WHERE ano = :ano"

@regale.load("vendas_mp", target="dw", table="fato_pedidos", mode="append")
def gravar(df, p):
    return df
"""


def test_local_executor_sequential_processes_every_partition_in_order(monkeypatch):
    calls = []

    def fake_run_partition_with_retry(pipeline_id, params, *, policy):
        calls.append((pipeline_id, params))

    monkeypatch.setattr(
        "regale.core.executor.run_partition_with_retry", fake_run_partition_with_retry
    )

    executor = LocalExecutor(workers=1)
    executor.run("vendas", [{"ano": 2025}, {"ano": 2026}])

    assert calls == [("vendas", {"ano": 2025}), ("vendas", {"ano": 2026})]


def test_local_executor_raises_without_discover_packages_when_workers_gt_1():
    executor = LocalExecutor(workers=2)
    with pytest.raises(RegaleError, match="discover_packages"):
        executor.run("vendas", [{}])


def test_local_executor_multiprocess_runs_every_partition(tmp_path, monkeypatch):
    # Windows (and macOS by default) spawn fresh child processes rather
    # than forking, so this is the only way to prove workers>1 actually
    # rebuilds the registry in each worker via discover(), instead of
    # silently relying on inherited parent-process memory that wouldn't
    # be there in a real deployment.
    monkeypatch.syspath_prepend(str(tmp_path))

    source_url = f"sqlite:///{(tmp_path / 'source.db').as_posix()}"
    dest_url = f"sqlite:///{(tmp_path / 'dest.db').as_posix()}"

    source_engine = create_engine(source_url)
    pd.DataFrame(
        {
            "pedido_id": [1, 2, 3, 4],
            "ano": [2025, 2025, 2026, 2026],
            "valor": [1.0, 2.0, 3.0, 4.0],
        }
    ).to_sql("pedidos", source_engine, index=False)

    package_name = f"pkg_{uuid.uuid4().hex}"
    package_dir = tmp_path / package_name
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    module_source = MODULE_TEMPLATE.replace("__SOURCE_URL__", source_url).replace(
        "__DEST_URL__", dest_url
    )
    (package_dir / "pipeline.py").write_text(module_source)

    regale.discover(package_name)  # populate this (parent) process too

    regale.run("vendas_mp", workers=2, discover_packages=(package_name,))

    dest_engine = create_engine(dest_url)
    with dest_engine.connect() as connection:
        rows = connection.execute(
            text("SELECT pedido_id, valor FROM fato_pedidos ORDER BY pedido_id")
        ).fetchall()
    assert rows == [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]
