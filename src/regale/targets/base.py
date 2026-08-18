from dataclasses import dataclass, field
from typing import Any, Protocol

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from regale.core.engines import build_engine


class Target(Protocol):
    """A configured data destination a @regale.load step can write to."""

    def engine(self) -> Engine: ...


@dataclass(repr=False)
class SQLTarget:
    """Describes a SQL connection by name.

    Same lazy-engine, never-serialize rule as SQLSource applies here.
    """

    url: str
    schema: str | None = None
    pool_size: int = 5
    connect_args: dict[str, Any] = field(default_factory=dict)
    _engine: Engine | None = field(default=None, init=False, repr=False, compare=False)

    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = build_engine(
                self.url, pool_size=self.pool_size, connect_args=self.connect_args
            )
        return self._engine

    def __repr__(self) -> str:
        masked = make_url(self.url).render_as_string(hide_password=True)
        return f"SQLTarget(url={masked!r}, schema={self.schema!r})"
