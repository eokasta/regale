import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from regale.core.engines import build_engine
from regale.core.errors import ErrorClass
from regale.core.steps import LoadMode
from regale.drivers.generic import GenericDriver
from regale.drivers.registry import resolve_driver


@pytest.fixture
def engine(tmp_path):
    # file-backed, not :memory: — write_chunk and the verification queries
    # below use separate connections, and an in-memory SQLite database is
    # not shared across connections. Goes through build_engine(), the same
    # helper SQLSource/SQLTarget use, so these tests see the same
    # transactional-DDL behavior production code gets.
    return build_engine(f"sqlite:///{tmp_path / 'data.db'}", pool_size=5, connect_args={})


@pytest.fixture
def driver():
    return GenericDriver()


def test_resolve_driver_returns_generic_for_sqlite():
    assert isinstance(resolve_driver("sqlite:///:memory:"), GenericDriver)


def test_append_creates_table_and_inserts(engine, driver):
    df = pd.DataFrame({"pedido_id": [1, 2], "valor": [10.0, 20.0]})

    with engine.connect() as connection, connection.begin():
        driver.write_chunk(
            connection,
            "fato_pedidos",
            df,
            mode=LoadMode.APPEND,
            keys=(),
            partition_keys=(),
            partition={},
            schema=None,
        )

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT pedido_id, valor FROM fato_pedidos ORDER BY pedido_id")
        ).fetchall()
    assert rows == [(1, 10.0), (2, 20.0)]


def test_append_is_cumulative_across_chunks_in_one_transaction(engine, driver):
    chunk_a = pd.DataFrame({"pedido_id": [1], "valor": [10.0]})
    chunk_b = pd.DataFrame({"pedido_id": [2], "valor": [20.0]})

    with engine.connect() as connection, connection.begin():
        for chunk in (chunk_a, chunk_b):
            driver.write_chunk(
                connection,
                "fato_pedidos",
                chunk,
                mode=LoadMode.APPEND,
                keys=(),
                partition_keys=(),
                partition={},
                schema=None,
            )

    with engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM fato_pedidos")).scalar()
    assert count == 2


def test_read_batches_respects_chunksize(engine, driver):
    seed = pd.DataFrame({"n": range(10)})
    seed.to_sql("numeros", engine, if_exists="replace", index=False)

    chunks = list(driver.read_batches(engine, "SELECT n FROM numeros ORDER BY n", {}, chunksize=4))

    assert [len(c) for c in chunks] == [4, 4, 2]
    assert pd.concat(chunks)["n"].tolist() == list(range(10))


def test_read_batches_without_chunksize_returns_single_frame(engine, driver):
    seed = pd.DataFrame({"n": [1, 2, 3]})
    seed.to_sql("numeros", engine, if_exists="replace", index=False)

    chunks = list(driver.read_batches(engine, "SELECT n FROM numeros", {}, chunksize=None))

    assert len(chunks) == 1
    assert chunks[0]["n"].tolist() == [1, 2, 3]


def test_read_batches_binds_named_params(engine, driver):
    seed = pd.DataFrame({"ano": [2025, 2025, 2026], "n": [1, 2, 3]})
    seed.to_sql("numeros", engine, if_exists="replace", index=False)

    chunks = list(
        driver.read_batches(
            engine, "SELECT n FROM numeros WHERE ano = :ano ORDER BY n", {"ano": 2025}, None
        )
    )

    assert chunks[0]["n"].tolist() == [1, 2]


def test_replace_partition_deletes_matching_rows_then_inserts(engine, driver):
    existing = pd.DataFrame(
        {"ano": [2025, 2025, 2026], "pedido_id": [1, 2, 3], "valor": [1.0, 2.0, 3.0]}
    )
    existing.to_sql("fato_pedidos", engine, if_exists="replace", index=False)

    novos = pd.DataFrame({"ano": [2025], "pedido_id": [99], "valor": [99.0]})
    with engine.connect() as connection, connection.begin():
        driver.write_chunk(
            connection,
            "fato_pedidos",
            novos,
            mode=LoadMode.REPLACE_PARTITION,
            keys=(),
            partition_keys=("ano",),
            partition={"ano": 2025},
            schema=None,
        )

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT ano, pedido_id FROM fato_pedidos ORDER BY ano, pedido_id")
        ).fetchall()
    assert rows == [(2025, 99), (2026, 3)]


def test_upsert_inserts_new_and_updates_existing(engine, driver):
    with engine.connect() as connection:
        connection.execute(
            text("CREATE TABLE fato_pedidos (pedido_id INTEGER PRIMARY KEY, valor REAL)")
        )
        connection.execute(text("INSERT INTO fato_pedidos VALUES (1, 10.0)"))
        connection.commit()

    df = pd.DataFrame({"pedido_id": [1, 2], "valor": [999.0, 20.0]})
    with engine.connect() as connection, connection.begin():
        driver.write_chunk(
            connection,
            "fato_pedidos",
            df,
            mode=LoadMode.UPSERT,
            keys=("pedido_id",),
            partition_keys=(),
            partition={},
            schema=None,
        )

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT pedido_id, valor FROM fato_pedidos ORDER BY pedido_id")
        ).fetchall()
    assert rows == [(1, 999.0), (2, 20.0)]


def test_upsert_cleans_up_its_staging_table(engine, driver):
    with engine.connect() as connection:
        connection.execute(
            text("CREATE TABLE fato_pedidos (pedido_id INTEGER PRIMARY KEY, valor REAL)")
        )
        connection.commit()

    df = pd.DataFrame({"pedido_id": [1], "valor": [1.0]})
    with engine.connect() as connection, connection.begin():
        driver.write_chunk(
            connection,
            "fato_pedidos",
            df,
            mode=LoadMode.UPSERT,
            keys=("pedido_id",),
            partition_keys=(),
            partition={},
            schema=None,
        )

    with engine.connect() as connection:
        staging_query = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='_regale_staging_fato_pedidos'"
        )
        staging = connection.execute(text(staging_query)).fetchone()
    assert staging is None


def test_write_chunk_rolls_back_on_exception(engine, driver):
    df_ok = pd.DataFrame({"pedido_id": [1], "valor": [1.0]})

    with pytest.raises(RuntimeError, match="simulated crash"):
        with engine.connect() as connection, connection.begin():
            driver.write_chunk(
                connection,
                "fato_pedidos",
                df_ok,
                mode=LoadMode.APPEND,
                keys=(),
                partition_keys=(),
                partition={},
                schema=None,
            )
            raise RuntimeError("simulated crash mid-partition")

    with engine.connect() as connection:
        exists = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='fato_pedidos'")
        ).fetchone()
    assert exists is None  # the whole transaction, including table creation, rolled back


def test_classify_maps_integrity_error_to_permanent(engine, driver):
    with engine.connect() as connection:
        connection.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
        connection.execute(text("INSERT INTO t VALUES (1)"))
        connection.commit()

    with engine.connect() as connection:
        try:
            connection.execute(text("INSERT INTO t VALUES (1)"))
            connection.commit()
            pytest.fail("expected a duplicate primary key to raise")
        except Exception as exc:
            assert driver.classify(exc) is ErrorClass.PERMANENT


def test_classify_unwraps_pandas_database_error_to_find_integrity_error(engine, driver):
    # pandas.to_sql wraps the real SQLAlchemy IntegrityError in its own
    # pandas.errors.DatabaseError — classify() must see through that.
    with engine.connect() as connection:
        connection.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
        connection.commit()

    df = pd.DataFrame({"id": [1, 1]})
    with engine.connect() as connection, connection.begin():
        try:
            df.to_sql("t", connection, if_exists="append", index=False)
            pytest.fail("expected a duplicate primary key to raise")
        except Exception as exc:
            assert type(exc).__name__ == "DatabaseError"
            assert driver.classify(exc) is ErrorClass.PERMANENT


def test_classify_maps_programming_error_to_permanent(driver):
    # Unit-tested directly on the exception type rather than by triggering
    # one through a live SQLite query: sqlite3 reports almost every SQL
    # mistake (missing table, missing column, bad syntax) as
    # OperationalError, never ProgrammingError — this classify() branch is
    # exercised for real by dialects that do distinguish the two.
    exc = ProgrammingError("SELECT 1", {}, Exception("boom"))
    assert driver.classify(exc) is ErrorClass.PERMANENT


def test_classify_maps_missing_table_to_ambiguous_on_sqlite(engine, driver):
    with engine.connect() as connection:
        try:
            connection.execute(text("SELECT coluna_inexistente FROM tabela_inexistente"))
            pytest.fail("expected invalid SQL to raise")
        except Exception as exc:
            # Documents the known limitation: on sqlite3, this is an
            # OperationalError indistinguishable by type from a lock
            # timeout, so the generic driver can't call it PERMANENT.
            assert driver.classify(exc) is ErrorClass.AMBIGUOUS
