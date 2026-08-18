import pytest

import regale
from regale.core.errors import RegistrationError
from regale.sources.base import SQLSource
from regale.targets.base import SQLTarget


def test_env_returns_value_when_set(monkeypatch):
    monkeypatch.setenv("VENDAS_DB_URL", "sqlite:///:memory:")
    assert regale.env("VENDAS_DB_URL") == "sqlite:///:memory:"


def test_env_raises_when_missing(monkeypatch):
    monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
    with pytest.raises(RegistrationError, match="DOES_NOT_EXIST"):
        regale.env("DOES_NOT_EXIST")


def test_add_db_registers_source():
    regale.configure.add_db("vendas_db", SQLSource(url="sqlite:///:memory:"))
    assert isinstance(regale.configure.source("vendas_db"), SQLSource)


def test_add_db_registers_target():
    regale.configure.add_db("dw", SQLTarget(url="sqlite:///:memory:"))
    assert isinstance(regale.configure.target("dw"), SQLTarget)


def test_add_db_rejects_unknown_type():
    with pytest.raises(RegistrationError, match="SQLSource or SQLTarget"):
        regale.configure.add_db("vendas_db", object())


def test_add_db_duplicate_source_raises():
    regale.configure.add_db("vendas_db", SQLSource(url="sqlite:///:memory:"))
    with pytest.raises(RegistrationError, match="already configured"):
        regale.configure.add_db("vendas_db", SQLSource(url="sqlite:///:memory:"))


def test_source_not_found_raises():
    with pytest.raises(RegistrationError, match="no source configured"):
        regale.configure.source("missing")


def test_target_not_found_raises():
    with pytest.raises(RegistrationError, match="no target configured"):
        regale.configure.target("missing")


def test_sql_source_repr_masks_password():
    source = SQLSource(url="postgresql://user:secret@host:5432/db")
    assert "secret" not in repr(source)
    assert "user" in repr(source)


def test_sql_target_repr_masks_password():
    target = SQLTarget(url="postgresql://user:secret@host:5432/db")
    assert "secret" not in repr(target)


def test_sql_source_engine_is_lazy_and_reused():
    source = SQLSource(url="sqlite:///:memory:")
    assert source.engine() is source.engine()


def test_validate_pipeline_connections_catches_unconfigured_source():
    @regale.query("vendas", source="vendas_db")
    def extrair(p):
        return "SELECT 1"

    @regale.load("vendas", target="dw", table="t", mode="append")
    def gravar(df, p):
        return df

    with pytest.raises(RegistrationError, match="vendas_db"):
        regale.configure.validate_pipeline_connections()

    regale.configure.add_db("vendas_db", SQLSource(url="sqlite:///:memory:"))
    with pytest.raises(RegistrationError, match="'dw'"):
        regale.configure.validate_pipeline_connections()

    regale.configure.add_db("dw", SQLTarget(url="sqlite:///:memory:"))
    regale.configure.validate_pipeline_connections()
