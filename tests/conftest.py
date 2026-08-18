import sys
import uuid

import pytest

from regale.api.config import configure
from regale.core.registry import registry


@pytest.fixture(autouse=True)
def _clear_registry():
    registry.clear()
    yield
    registry.clear()


@pytest.fixture(autouse=True)
def _clear_configure():
    configure.clear()
    yield
    configure.clear()


@pytest.fixture
def make_pipeline_package(tmp_path, monkeypatch):
    """Write a throwaway, uniquely-named package to disk and make it
    importable.

    discover() tests need a real, never-before-imported module tree: once
    Python has imported a module, re-importing it is a no-op that skips its
    top-level @regale decorators, which would silently hide bugs in
    discover() behind a stale sys.modules cache.
    """
    monkeypatch.syspath_prepend(str(tmp_path))

    def _make(module_source: str) -> str:
        package_name = f"pkg_{uuid.uuid4().hex}"
        package_dir = tmp_path / package_name
        package_dir.mkdir()
        (package_dir / "__init__.py").write_text("")
        (package_dir / "pipeline.py").write_text(module_source)
        return package_name

    yield _make

    for name in list(sys.modules):
        if name.startswith("pkg_"):
            del sys.modules[name]
