import regale
from regale.core.context import Context
from regale.sources.base import SQLSource
from regale.targets.base import SQLTarget


def test_context_source_delegates_to_configure():
    regale.configure.add_db("vendas_db", SQLSource(url="sqlite:///:memory:"))
    ctx = Context(run_id="run1")
    assert ctx.source("vendas_db") is regale.configure.source("vendas_db")


def test_context_target_delegates_to_configure():
    regale.configure.add_db("dw", SQLTarget(url="sqlite:///:memory:"))
    ctx = Context(run_id="run1")
    assert ctx.target("dw") is regale.configure.target("dw")
