from collections.abc import Iterator
from typing import Any, Protocol

import pandas as pd
from sqlalchemy.engine import Connection, Engine

from regale.core.errors import ErrorClass
from regale.core.steps import LoadMode


class Driver(Protocol):
    """Backend-specific implementation of the bulk read/write path.

    Everything else — pool, dialect, DDL, transaction boundaries — is
    handled by SQLAlchemy, or by core/runner.py at the call site. A Driver
    only ever touches: turning query results into DataFrame chunks, and
    writing one DataFrame chunk into a destination table using a
    connection and transaction the caller already opened. The caller
    controls commit/rollback so several chunks can share one atomic
    transaction per partition.
    """

    def read_batches(
        self,
        engine: Engine,
        sql: str,
        params: dict[str, Any],
        chunksize: int | None,
    ) -> Iterator[pd.DataFrame]:
        """Yield query results as DataFrame chunks, lazily."""
        ...

    def write_chunk(
        self,
        connection: Connection,
        table: str,
        df: pd.DataFrame,
        *,
        mode: LoadMode,
        keys: tuple[str, ...],
        partition_keys: tuple[str, ...],
        partition: dict[str, Any],
        schema: str | None,
    ) -> None:
        """Write one chunk using the given connection, without committing."""
        ...

    def classify(self, exc: Exception) -> ErrorClass:
        """Decide whether retrying after this exception could help."""
        ...
