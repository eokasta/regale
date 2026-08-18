import os

from regale.core.errors import RegistrationError
from regale.core.registry import registry
from regale.sources.base import SQLSource
from regale.targets.base import SQLTarget


def env(name: str) -> str:
    """Read a required environment variable.

    The variable's name is entirely the caller's choice — Regale imposes no
    prefix — so failing fast and by name is the only safety net when it's
    missing.
    """
    try:
        return os.environ[name]
    except KeyError:
        raise RegistrationError(f"environment variable {name!r} is not set") from None


class ConnectionRegistry:
    """Process-wide registry of named database connections.

    Populated incrementally via add_db() so different modules can each
    register the connections they own, instead of one monolithic dict.
    """

    def __init__(self) -> None:
        self._sources: dict[str, SQLSource] = {}
        self._targets: dict[str, SQLTarget] = {}

    def add_db(self, name: str, connection: SQLSource | SQLTarget) -> None:
        if isinstance(connection, SQLSource):
            if name in self._sources:
                raise RegistrationError(f"source {name!r} is already configured")
            self._sources[name] = connection
        elif isinstance(connection, SQLTarget):
            if name in self._targets:
                raise RegistrationError(f"target {name!r} is already configured")
            self._targets[name] = connection
        else:
            raise RegistrationError(
                f"add_db expects a SQLSource or SQLTarget, got {type(connection).__name__}"
            )

    def source(self, name: str) -> SQLSource:
        try:
            return self._sources[name]
        except KeyError:
            raise RegistrationError(f"no source configured under name {name!r}") from None

    def target(self, name: str) -> SQLTarget:
        try:
            return self._targets[name]
        except KeyError:
            raise RegistrationError(f"no target configured under name {name!r}") from None

    def validate_pipeline_connections(self) -> None:
        """Confirm every source/target referenced by a registered step exists.

        Meant to run once at worker startup, right after discover(), so a
        typo'd connection name fails immediately instead of 40 minutes into
        a query.
        """
        for pipeline_id in registry.pipeline_ids():
            entry = registry.get(pipeline_id)
            if entry.query is not None and entry.query.source not in self._sources:
                raise RegistrationError(
                    f"pipeline {pipeline_id!r} references source {entry.query.source!r}, "
                    "which was never configured via regale.configure.add_db(...)"
                )
            for load_step in entry.loads:
                if load_step.target not in self._targets:
                    raise RegistrationError(
                        f"pipeline {pipeline_id!r} references target {load_step.target!r}, "
                        "which was never configured via regale.configure.add_db(...)"
                    )

    def clear(self) -> None:
        self._sources.clear()
        self._targets.clear()


configure = ConnectionRegistry()
