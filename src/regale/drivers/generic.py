from collections.abc import Iterator
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, ProgrammingError

from regale.core.errors import ErrorClass
from regale.core.steps import LoadMode


class GenericDriver:
    """SQLAlchemy-only fallback: works against any dialect SQLAlchemy
    supports, at the cost of a dialect's native bulk path (COPY, LOAD DATA,
    bcp/BULK INSERT). Defines the contract a native driver must match.

    upsert requires the destination table (and a unique/primary key
    constraint on `keys`) to already exist — ON CONFLICT needs a matching
    constraint, and Regale does not manage destination DDL beyond the
    auto-create-on-first-insert that append/replace_partition get for free
    from pandas.to_sql.
    """

    def read_batches(
        self,
        engine: Engine,
        sql: str,
        params: dict[str, Any],
        chunksize: int | None,
    ) -> Iterator[pd.DataFrame]:
        with engine.connect() as connection:
            if chunksize:
                yield from pd.read_sql_query(
                    text(sql), connection, params=params, chunksize=chunksize
                )
            else:
                yield pd.read_sql_query(text(sql), connection, params=params)

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
        full_table = f"{schema}.{table}" if schema else table

        if mode is LoadMode.APPEND:
            df.to_sql(table, connection, schema=schema, if_exists="append", index=False)
            return

        if mode is LoadMode.REPLACE_PARTITION:
            predicate = " AND ".join(f"{col} = :{col}" for col in partition_keys)
            values = {col: partition[col] for col in partition_keys}
            connection.execute(text(f"DELETE FROM {full_table} WHERE {predicate}"), values)
            df.to_sql(table, connection, schema=schema, if_exists="append", index=False)
            return

        self._upsert(connection, table, df, keys=keys, schema=schema)

    def _upsert(
        self,
        connection: Connection,
        table: str,
        df: pd.DataFrame,
        *,
        keys: tuple[str, ...],
        schema: str | None,
    ) -> None:
        full_table = f"{schema}.{table}" if schema else table
        staging = f"_regale_staging_{table}"
        full_staging = f"{schema}.{staging}" if schema else staging

        df.to_sql(staging, connection, schema=schema, if_exists="replace", index=False)

        columns = list(df.columns)
        insert_cols = ", ".join(columns)
        conflict_cols = ", ".join(keys)
        update_cols = [c for c in columns if c not in keys]
        if update_cols:
            do_update = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
            conflict_clause = f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {do_update}"
        else:
            conflict_clause = f"ON CONFLICT ({conflict_cols}) DO NOTHING"

        # SQLite's grammar requires a WHERE clause before an upsert-clause
        # attached to an INSERT ... SELECT (unlike INSERT ... VALUES); the
        # always-true predicate is a no-op on every other dialect too.
        connection.execute(
            text(
                f"INSERT INTO {full_table} ({insert_cols}) "
                f"SELECT {insert_cols} FROM {full_staging} WHERE 1=1 "
                f"{conflict_clause}"
            )
        )
        connection.execute(text(f"DROP TABLE {full_staging}"))

    def classify(self, exc: Exception) -> ErrorClass:
        # pandas.to_sql wraps the real SQLAlchemy exception in its own
        # pandas.errors.DatabaseError, hiding it from a plain isinstance
        # check — walk __cause__/__context__ to find the original.
        for candidate in _exception_chain(exc):
            if isinstance(candidate, ProgrammingError | IntegrityError):
                return ErrorClass.PERMANENT
        # Anything else, including OperationalError, is AMBIGUOUS rather than
        # TRANSIENT: that class spans both connection-level transients
        # (timeout, connection reset, lock contention) and, on some DBAPIs —
        # notably sqlite3 — plain SQL mistakes like a missing table or
        # column. The generic driver can't tell those apart from the
        # exception type alone; a native driver (postgres.py) resolves this
        # properly using the backend's own SQLSTATE code.
        return ErrorClass.AMBIGUOUS


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__
