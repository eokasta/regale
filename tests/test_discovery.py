import pytest

import regale
from regale.core.errors import RegistrationError

COMPLETE_PIPELINE = """
import regale

@regale.query("vendas", source="vendas_db")
def extrair(p):
    return "SELECT 1"

@regale.load("vendas", target="dw", table="fato_pedidos", mode="append")
def gravar(df, p):
    return df
"""

INCOMPLETE_PIPELINE = """
import regale

@regale.query("vendas", source="vendas_db")
def extrair(p):
    return "SELECT 1"
"""


def test_discover_imports_modules_and_validates(make_pipeline_package):
    package_name = make_pipeline_package(COMPLETE_PIPELINE)

    regale.discover(package_name)

    entry = regale.registry.get("vendas")
    assert entry.query is not None
    assert entry.loads


def test_discover_raises_on_incomplete_pipeline(make_pipeline_package):
    package_name = make_pipeline_package(INCOMPLETE_PIPELINE)

    with pytest.raises(RegistrationError, match="no @load"):
        regale.discover(package_name)


def test_discover_with_no_packages_and_empty_registry_does_not_raise():
    regale.discover()


def test_discover_imports_nested_submodules(make_pipeline_package, tmp_path):
    package_name = make_pipeline_package("")
    package_dir = tmp_path / package_name
    (package_dir / "sub").mkdir()
    (package_dir / "sub" / "__init__.py").write_text("")
    (package_dir / "sub" / "vendas.py").write_text(COMPLETE_PIPELINE)

    regale.discover(package_name)

    assert regale.registry.get("vendas").query is not None


def test_discover_loads_regale_pipelines_entry_points(make_pipeline_package, monkeypatch):
    package_name = make_pipeline_package(COMPLETE_PIPELINE)

    class FakeEntryPoint:
        def load(self):
            import importlib

            return importlib.import_module(f"{package_name}.pipeline")

    monkeypatch.setattr("regale.api.discovery.entry_points", lambda group: [FakeEntryPoint()])

    regale.discover()  # no packages passed — only the fake entry point

    assert regale.registry.get("vendas").query is not None
