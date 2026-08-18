from dataclasses import dataclass, field
from typing import Any, Protocol

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url


class Source(Protocol):
    """A configured data source a @regale.query step can read from."""

    def engine(self) -> Engine: ...


@dataclass(repr=False)
class SQLSource:
    """Describes a SQL connection by name.

    The engine is created lazily on first use and reused for the lifetime
    of the process — never serialize this across a process/machine
    boundary. A distributed worker re-imports the module that calls
    add_db() and builds its own engine from the same url.
    """

    url: str
    pool_size: int = 5
    min_pool: int = 1  # reserved: not every pool implementation supports a minimum size
    connect_args: dict[str, Any] = field(default_factory=dict)
    _engine: Engine | None = field(default=None, init=False, repr=False, compare=False)

    def engine(self) -> Engine:
        if self._engine is None:
            kwargs: dict[str, Any] = {"connect_args": self.connect_args}
            # SQLite's default pools (NullPool / SingletonThreadPool) don't accept pool_size.
            if not self.url.startswith("sqlite"):
                kwargs["pool_size"] = self.pool_size
            self._engine = create_engine(self.url, **kwargs)
        return self._engine

    def __repr__(self) -> str:
        masked = make_url(self.url).render_as_string(hide_password=True)
        return f"SQLSource(url={masked!r})"
