from typing import Any

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine


def build_engine(url: str, *, pool_size: int, connect_args: dict[str, Any]) -> Engine:
    """Create a SQLAlchemy engine, working around two SQLite-specific
    quirks that would otherwise break the one-transaction-per-partition
    guarantee core/runner.py depends on for safe retries.
    """
    kwargs: dict[str, Any] = {"connect_args": connect_args}
    is_sqlite = url.startswith("sqlite")
    if not is_sqlite:
        # SQLite's default pools (NullPool / SingletonThreadPool) don't accept pool_size.
        kwargs["pool_size"] = pool_size

    engine = create_engine(url, **kwargs)

    if is_sqlite:
        # pysqlite's legacy transaction handling commits DDL (e.g. CREATE
        # TABLE) immediately regardless of an open transaction. This is
        # SQLAlchemy's own documented workaround — see "Serializable
        # isolation / Savepoints / Transactional DDL" in the SQLAlchemy
        # SQLite dialect docs.
        @event.listens_for(engine, "connect")
        def _do_connect(dbapi_connection, connection_record):
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def _do_begin(conn):
            conn.exec_driver_sql("BEGIN")

    return engine
